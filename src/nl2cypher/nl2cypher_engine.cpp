// NL2CypherEngine implementation.
//
// Currently realises only the S0 path (zero-shot full schema dump → one
// LLM call → Cypher extraction). Hooks for S1~S3 are scaffolded but the
// engine never enters them while their config switches are at defaults
// — that wiring lands in subsequent commits.

#include "nl2cypher/nl2cypher_engine.hpp"

#include <cctype>
#include <sstream>
#include <stdexcept>

#include "main/client_context.hpp"
#include "spdlog/spdlog.h"

namespace turbolynx {
namespace nl2cypher {

namespace {

// ---------------------------------------------------------------------
// Cypher extraction — strip ```cypher ... ``` fences and trailing
// semicolons / whitespace from an LLM response. Tolerant of:
//   * unfenced bare Cypher
//   * ```cypher / ``` / ```graphql fences
//   * leading prose ("Here is the Cypher: ...")
//   * trailing prose
//
// Strategy: if a fence exists, take the *first* fenced block; otherwise
// drop any "Here is" / "The query is" prose and take the rest.
// ---------------------------------------------------------------------

std::string Trim(const std::string& s) {
    size_t a = 0, b = s.size();
    while (a < b && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    while (b > a && std::isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    return s.substr(a, b - a);
}

std::string ExtractCypherBlock(const std::string& text) {
    // Try fenced block first.
    auto open = text.find("```");
    if (open != std::string::npos) {
        // Skip the opening fence and optional language tag up to newline.
        size_t after = text.find('\n', open);
        if (after != std::string::npos) {
            size_t close = text.find("```", after + 1);
            if (close != std::string::npos) {
                std::string body = text.substr(after + 1, close - after - 1);
                return Trim(body);
            }
        }
    }
    // No fence: assume the entire response is Cypher (possibly with
    // a leading "Here is the query:" line that we tolerate).
    std::string body = Trim(text);
    // Drop a single leading "Here is ..." line if present.
    if (!body.empty() && (body.rfind("Here is", 0) == 0 ||
                          body.rfind("The query", 0) == 0 ||
                          body.rfind("Cypher:", 0) == 0)) {
        size_t nl = body.find('\n');
        if (nl != std::string::npos) body = Trim(body.substr(nl + 1));
    }
    return body;
}

// Strip a single trailing semicolon (the shell's executor adds its own
// handling, and the parser is happy either way).
std::string StripTrailingSemicolon(std::string s) {
    while (!s.empty() &&
           (s.back() == ';' || std::isspace(static_cast<unsigned char>(s.back())))) {
        s.pop_back();
    }
    return s;
}

// ---------------------------------------------------------------------
// Prompt builder — S0 layer.
//
// Wraps the schema dump in a strict instruction asking for *only* the
// Cypher query inside a ```cypher fence. We also list the dialect
// constraints that TurboGraph's parser needs (semicolon optional,
// `MATCH ... RETURN` core only, no APOC, etc.).
// ---------------------------------------------------------------------

constexpr const char* kSystemPrompt =
    "You are an expert at translating natural-language questions into "
    "Cypher queries against a property graph database (TurboGraph, an "
    "openCypher-compatible engine).\n"
    "\n"
    "Rules:\n"
    "1. Reply with exactly one Cypher query and NOTHING ELSE — no prose, "
    "no explanation, no comments.\n"
    "2. Wrap the query in a ```cypher ... ``` fenced code block.\n"
    "3. Use ONLY the labels, edge types, and properties present in the "
    "schema below. Do NOT invent labels or properties.\n"
    "4. Prefer MATCH/WHERE/RETURN. Use OPTIONAL MATCH for nullable "
    "patterns. Use WITH to chain aggregations.\n"
    "5. Do NOT use APOC, GDS, or any vendor-specific procedures.\n"
    "6. Date literals: `date('YYYY-MM-DD')`. Datetime: "
    "`datetime('YYYY-MM-DDTHH:MM:SS')`.\n"
    "7. Aggregations: count(), sum(), avg(), min(), max(), collect().\n"
    "8. End the query with no semicolon.";

std::string BuildZeroShotPrompt(const GraphSchema& schema,
                                const std::string& question) {
    std::ostringstream os;
    os << "## Schema\n\n";
    os << schema.ToPromptText();
    os << "\n## Question\n\n";
    os << question << "\n\n";
    os << "## Cypher\n";
    return os.str();
}

}  // namespace

// =====================================================================
// Impl
// =====================================================================

class NL2CypherEngine::Impl {
public:
    Impl(duckdb::ClientContext& client, Config cfg)
        : client_(client), cfg_(std::move(cfg)),
          llm_(BuildLLMConfig(cfg_)) {
        // Eagerly cache the schema so the first Translate() call is fast.
        try {
            schema_ = IntrospectGraphSchema(client_);
        } catch (const std::exception& e) {
            spdlog::warn(
                "[nl2cypher] schema introspection failed at construction: "
                "{} — will retry on first Translate()", e.what());
        }
    }

    TranslationResult Translate(const std::string& question) {
        TranslationResult out;

        // Lazy retry if construction-time introspection failed.
        if (schema_.labels.empty() && schema_.edges.empty()) {
            try {
                schema_ = IntrospectGraphSchema(client_);
            } catch (const std::exception& e) {
                out.error = std::string("schema introspection failed: ") + e.what();
                return out;
            }
        }

        // S0: build zero-shot prompt and call the LLM exactly once.
        // S1~S3 hooks would replace this with profile-augmented prompts,
        // multi-variant linking, multi-candidate generation. They are
        // not active in this commit.
        LLMRequest req;
        req.system = kSystemPrompt;
        req.user   = BuildZeroShotPrompt(schema_, question);

        auto resp = llm_.Call(req);
        out.n_candidates_generated = 1;
        out.used_cache             = resp.cache_hit;

        if (!resp.ok) {
            out.error = "LLM call failed: " + resp.error;
            return out;
        }

        std::string cypher = ExtractCypherBlock(resp.text);
        cypher = StripTrailingSemicolon(cypher);
        if (cypher.empty()) {
            out.error = "LLM response did not contain a Cypher block";
            return out;
        }

        out.ok                 = true;
        out.cypher             = std::move(cypher);
        out.n_candidates_valid = 1;
        return out;
    }

    void RefreshSchema() { schema_ = IntrospectGraphSchema(client_); }

    const GraphSchema& schema() const { return schema_; }

private:
    static LLMClient::Config BuildLLMConfig(const Config& cfg) {
        LLMClient::Config out = cfg.llm;
        // If the caller didn't set a cache dir but provided a workspace,
        // co-locate the LLM response cache with the metadata.
        if (out.cache_dir.empty() && !cfg.workspace.empty()) {
            out.cache_dir = cfg.workspace + "/.nl2cypher/llm_cache";
        }
        return out;
    }

    duckdb::ClientContext& client_;
    Config                 cfg_;
    LLMClient              llm_;
    GraphSchema            schema_;
};

// =====================================================================
// Public surface
// =====================================================================

NL2CypherEngine::NL2CypherEngine(duckdb::ClientContext& client, Config cfg)
    : impl_(std::make_unique<Impl>(client, std::move(cfg))) {}

NL2CypherEngine::~NL2CypherEngine() = default;

TranslationResult NL2CypherEngine::Translate(const std::string& nl_question) {
    return impl_->Translate(nl_question);
}

void NL2CypherEngine::RefreshSchema() { impl_->RefreshSchema(); }

const GraphSchema& NL2CypherEngine::schema() const { return impl_->schema(); }

}  // namespace nl2cypher
}  // namespace turbolynx

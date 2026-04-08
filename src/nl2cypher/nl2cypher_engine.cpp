// NL2CypherEngine implementation.
//
// Currently realises only the S0 path (zero-shot full schema dump → one
// LLM call → Cypher extraction). Hooks for S1~S3 are scaffolded but the
// engine never enters them while their config switches are at defaults
// — that wiring lands in subsequent commits.

#include "nl2cypher/nl2cypher_engine.hpp"

#include <cctype>
#include <fstream>
#include <set>
#include <sstream>
#include <stdexcept>

#include "main/client_context.hpp"
#include "nl2cypher/profile_collector.hpp"
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
    "Output rules:\n"
    "1. Reply with exactly one Cypher query and NOTHING ELSE — no prose, "
    "no explanation, no comments.\n"
    "2. Wrap the query in a ```cypher ... ``` fenced code block.\n"
    "3. Use ONLY the labels, edge types, and properties present in the "
    "schema below. Do NOT invent labels or properties.\n"
    "4. Always give each RETURN projection an explicit alias with AS. "
    "Unaliased `RETURN n.prop` may expand to extra columns.\n"
    "5. End the query with no semicolon.\n"
    "\n"
    "Cypher idiom rules (TurboGraph-specific):\n"
    "6. Prefer MATCH/WHERE/RETURN. Use OPTIONAL MATCH for nullable "
    "patterns. Use WITH to chain aggregations.\n"
    "7. Do NOT use APOC, GDS, or any vendor-specific procedures.\n"
    "8. Date literals: `date('YYYY-MM-DD')`. Datetime: "
    "`datetime('YYYY-MM-DDTHH:MM:SS')`.\n"
    "9. Aggregations available: count(), sum(), avg(), min(), max(), "
    "collect().\n"
    "10. Relationship direction: when the edge is semantically "
    "symmetric (e.g. friendship via :KNOWS), always use the UNDIRECTED "
    "form `-[:KNOWS]-` rather than `-[:KNOWS]->`. TurboGraph stores "
    "each symmetric edge only once, so directed matching misses half.\n"
    "11. Label disjunction in WHERE is NOT supported: you MUST NOT "
    "write `WHERE n:Post OR n:Comment`. If the schema exposes a "
    "super-label that already covers both (e.g. `:Message` covering "
    "`:Post` and `:Comment`), use the super-label in the MATCH "
    "pattern directly: `MATCH (m:Message)`.\n"
    "12. Property predicates belong in WHERE or the pattern map, not "
    "in RETURN. Do not invent properties that are not in the schema.";

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

// S1: same format, but the schema dump is the rich one that includes
// row counts, value ranges, and LLM-generated short descriptions.
std::string BuildRichPrompt(const GraphProfile& profile,
                            const std::string& question) {
    std::ostringstream os;
    os << "## Schema (with profile and descriptions)\n\n";
    os << profile.ToRichPromptText();
    os << "\n## Question\n\n";
    os << question << "\n\n";
    os << "## Cypher\n";
    return os.str();
}

// ---------------------------------------------------------------------
// S2 schema linker — prompt variants.
//
// Each variant shows the schema through a different lens so the drafts
// disagree on marginal elements but agree on the core ones. The union
// of the entities they actually use in their drafts is the linked
// schema.
//
// Recognised variant strings (anything else falls back to "rich"):
//   "compact"      — label/edge names + one-line short desc only
//   "samples"      — label/edge names + sample values per property
//   "descriptions" — label/edge names + long_desc only
//   "rich"         — full rich prompt (same as S1)
// ---------------------------------------------------------------------

std::string BuildCompactPromptText(const GraphProfile& p) {
    std::ostringstream os;
    os << "Vertex labels (" << p.labels.size() << "):\n";
    for (const auto& l : p.labels) {
        os << "  (:" << l.label << ")";
        if (!l.short_desc.empty()) os << "  -- " << l.short_desc;
        os << "\n";
    }
    os << "\nEdge types (" << p.edges.size() << "):\n";
    for (const auto& e : p.edges) {
        os << "  [:" << e.type << "]";
        if (!e.short_desc.empty()) os << "  -- " << e.short_desc;
        os << "\n";
        for (const auto& ep : e.endpoints) {
            os << "    (:" << ep.src_label << ")-[:" << e.type
               << "]->(:" << ep.dst_label << ")\n";
        }
    }
    return os.str();
}

std::string BuildSamplesPromptText(const GraphProfile& p) {
    std::ostringstream os;
    os << "Vertex labels:\n";
    for (const auto& l : p.labels) {
        os << "\n  (:" << l.label << ") [" << l.row_count << " rows]\n";
        for (const auto& prop : l.properties) {
            os << "    " << prop.name << ": " << prop.type_name;
            if (!prop.top_k.empty()) {
                os << " e.g. ";
                for (size_t i = 0; i < prop.top_k.size() && i < 3; ++i) {
                    if (i) os << ", ";
                    os << prop.top_k[i].value;
                }
            }
            os << "\n";
        }
    }
    os << "\nEdge types:\n";
    for (const auto& e : p.edges) {
        os << "\n  [:" << e.type << "] [" << e.row_count << " rows]\n";
        for (const auto& ep : e.endpoints) {
            os << "    (:" << ep.src_label << ")-[:" << e.type
               << "]->(:" << ep.dst_label << ")\n";
        }
    }
    return os.str();
}

std::string BuildDescriptionsPromptText(const GraphProfile& p) {
    std::ostringstream os;
    os << "Vertex labels:\n";
    for (const auto& l : p.labels) {
        os << "\n  (:" << l.label << ")";
        if (!l.long_desc.empty()) os << "\n    " << l.long_desc;
        else if (!l.short_desc.empty()) os << "  -- " << l.short_desc;
        os << "\n";
    }
    os << "\nEdge types:\n";
    for (const auto& e : p.edges) {
        os << "\n  [:" << e.type << "]";
        if (!e.long_desc.empty()) os << "\n    " << e.long_desc;
        else if (!e.short_desc.empty()) os << "  -- " << e.short_desc;
        for (const auto& ep : e.endpoints) {
            os << "\n    (:" << ep.src_label << ")-[:" << e.type
               << "]->(:" << ep.dst_label << ")";
        }
        os << "\n";
    }
    return os.str();
}

std::string BuildLinkerVariantPrompt(const GraphProfile& profile,
                                     const std::string& variant,
                                     const std::string& question) {
    std::ostringstream os;
    if (variant == "compact") {
        os << "## Schema (names + one-line descriptions)\n\n"
           << BuildCompactPromptText(profile);
    } else if (variant == "samples") {
        os << "## Schema (with sample values)\n\n"
           << BuildSamplesPromptText(profile);
    } else if (variant == "descriptions") {
        os << "## Schema (with long descriptions)\n\n"
           << BuildDescriptionsPromptText(profile);
    } else {
        os << "## Schema (with profile and descriptions)\n\n"
           << profile.ToRichPromptText();
    }
    os << "\n## Question\n\n" << question << "\n\n";
    os << "## Cypher\n";
    return os.str();
}

// ---------------------------------------------------------------------
// Schema-element parser for linker drafts.
//
// Walks a Cypher string looking for patterns that bind labels and
// edge types: `:Label)`, `:Label {`, `:Label|...`, `[:TYPE]`,
// `[:TYPE*1..3]`, etc. We do not try to track variable bindings or
// property references — label/edge granularity is enough because the
// linker passes whole labels/edges through to the final prompt.
// ---------------------------------------------------------------------

bool IsLabelIdentStart(char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
}
bool IsLabelIdentCont(char c) {
    return IsLabelIdentStart(c) || (c >= '0' && c <= '9');
}

void ParseCypherSchemaTokens(const std::string& draft,
                             const GraphProfile& profile,
                             std::set<std::string>& labels_out,
                             std::set<std::string>& edges_out) {
    // Build lookup sets so we only accept names that actually exist
    // in the schema. This filters out false positives from aliases,
    // function names, etc. (e.g. `:count`, `ORDER BY` tokens).
    std::set<std::string> known_labels;
    for (const auto& l : profile.labels) known_labels.insert(l.label);
    std::set<std::string> known_edges;
    for (const auto& e : profile.edges) known_edges.insert(e.type);

    const size_t n = draft.size();
    for (size_t i = 0; i < n; ++i) {
        if (draft[i] != ':') continue;
        // Skip `::` (type cast) and `{a:1}` (property maps). A leading
        // whitespace or `(` / `[` / `|` strongly indicates a label or
        // edge type binding.
        if (i + 1 >= n) break;
        if (!IsLabelIdentStart(draft[i + 1])) continue;
        size_t j = i + 1;
        while (j < n && IsLabelIdentCont(draft[j])) ++j;
        std::string ident = draft.substr(i + 1, j - i - 1);

        // Look back for context: `[:` → edge type; otherwise label.
        // We also accept `|` (label disjunction) and `(` (pattern).
        bool is_edge = false;
        for (size_t k = i; k > 0; --k) {
            char c = draft[k - 1];
            if (c == '[') { is_edge = true; break; }
            if (c == '(' || c == '|' || c == ' ' || c == '\n'
                || c == '\t') break;
        }
        if (is_edge) {
            if (known_edges.count(ident)) edges_out.insert(ident);
        } else {
            if (known_labels.count(ident)) labels_out.insert(ident);
        }
        i = j - 1;
    }
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
        // Try to load cached metadata.json (S1 profile + summaries).
        // Absence is expected on first run; log at debug only.
        TryLoadProfile();
    }

    // Attempt to load <workspace>/.nl2cypher/metadata.json into
    // `profile_`. Populates `has_profile_` on success. Non-fatal: any
    // error leaves the engine in pure-S0 mode.
    void TryLoadProfile() {
        has_profile_ = false;
        if (cfg_.workspace.empty()) return;
        std::string path = cfg_.workspace + "/.nl2cypher/metadata.json";
        std::ifstream f(path);
        if (!f) {
            spdlog::debug("[nl2cypher] no metadata.json at {}", path);
            return;
        }
        try {
            nlohmann::json j;
            f >> j;
            profile_ = GraphProfile::FromJson(j);
            has_profile_ = true;
            spdlog::info("[nl2cypher] loaded metadata.json: {} labels, "
                         "{} edges, summaries={}",
                         profile_.labels.size(), profile_.edges.size(),
                         profile_.has_summaries ? "yes" : "no");
        } catch (const std::exception& e) {
            spdlog::warn("[nl2cypher] metadata.json load failed: {}",
                         e.what());
            has_profile_ = false;
        }
    }

    // Re-read metadata.json (e.g. after a `.nl profile` or `.nl
    // summarize` run). Exposed to the shell through a dedicated hook.
    void ReloadProfile() { TryLoadProfile(); }

    bool HasProfile() const { return has_profile_; }

    GraphProfile& MutableProfile() { return profile_; }

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

        // S2 schema linking: if enabled and we have a profile, run N
        // draft variants, union the schema elements they mention, and
        // restrict the final prompt to those elements.
        bool linker_on = has_profile_ && profile_.has_summaries
                         && !cfg_.linker_variants.empty();

        std::string final_prompt_text;
        if (linker_on) {
            auto linked = RunSchemaLinker(question, out);
            final_prompt_text = BuildRichPrompt(linked, question);
        } else if (has_profile_ && profile_.has_summaries) {
            final_prompt_text = BuildRichPrompt(profile_, question);
        } else {
            final_prompt_text = BuildZeroShotPrompt(schema_, question);
        }

        LLMRequest req;
        req.system = kSystemPrompt;
        req.user   = final_prompt_text;

        auto resp = llm_.Call(req);
        out.n_candidates_generated += 1;
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

    // ------------------------------------------------------------------
    // S2 schema linker.
    //
    // For each configured variant string, build a draft prompt with a
    // specific "view" of the schema, call the LLM to produce a draft
    // Cypher query, and parse it for label/edge tokens. The union of
    // all parsed labels/edges defines the linked schema, which is
    // returned as a filtered copy of `profile_`.
    // ------------------------------------------------------------------
    GraphProfile RunSchemaLinker(const std::string& question,
                                 TranslationResult& out) {
        std::set<std::string> linked_labels;
        std::set<std::string> linked_edges;

        for (const auto& variant : cfg_.linker_variants) {
            LLMRequest req;
            req.system = kSystemPrompt;
            req.user   = BuildLinkerVariantPrompt(profile_, variant, question);

            auto resp = llm_.Call(req);
            out.n_candidates_generated += 1;
            if (!resp.ok) {
                spdlog::warn("[linker] variant {} failed: {}",
                             variant, resp.error);
                continue;
            }
            std::string draft = ExtractCypherBlock(resp.text);
            ParseCypherSchemaTokens(draft, profile_,
                                    linked_labels, linked_edges);
        }

        if (linked_labels.empty() && linked_edges.empty()) {
            // Safety: if the linker extracted nothing (bad drafts, all
            // failed, empty variants), fall back to the full profile
            // so we never send the LLM an empty schema.
            spdlog::warn("[linker] no schema elements extracted — "
                         "falling back to full profile");
            return profile_;
        }

        // Filter. We always keep the labels that appear as endpoints
        // of any linked edge, even if the drafts did not mention them
        // explicitly — otherwise the final prompt would lose the
        // directionality needed to pick the right pattern.
        for (const auto& e : profile_.edges) {
            if (linked_edges.count(e.type) == 0) continue;
            for (const auto& ep : e.endpoints) {
                linked_labels.insert(ep.src_label);
                linked_labels.insert(ep.dst_label);
            }
        }

        GraphProfile filtered;
        filtered.graph_name    = profile_.graph_name;
        filtered.has_summaries = profile_.has_summaries;
        for (const auto& l : profile_.labels) {
            if (linked_labels.count(l.label)) filtered.labels.push_back(l);
        }
        for (const auto& e : profile_.edges) {
            if (linked_edges.count(e.type)) filtered.edges.push_back(e);
        }
        spdlog::info("[linker] linked {}/{} labels, {}/{} edges",
                     filtered.labels.size(), profile_.labels.size(),
                     filtered.edges.size(), profile_.edges.size());
        return filtered;
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
    GraphProfile           profile_;
    bool                   has_profile_ = false;
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

void NL2CypherEngine::ReloadProfile() { impl_->ReloadProfile(); }

const GraphSchema& NL2CypherEngine::schema() const { return impl_->schema(); }

}  // namespace nl2cypher
}  // namespace turbolynx

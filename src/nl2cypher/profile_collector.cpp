#include "nl2cypher/profile_collector.hpp"

#include <chrono>
#include <sstream>

#include "spdlog/spdlog.h"

namespace turbolynx {
namespace nl2cypher {

namespace {

// Render a duckdb::Value into the form we want to store. We avoid
// Value::ToString() for VARCHAR because it adds surrounding quotes
// in some build configs; for everything else ToString() is fine.
std::string ValueToCanonical(const duckdb::Value& v) {
    if (v.IsNull()) return "";
    return v.ToString();
}

}  // namespace

// Declared in profile_collector.hpp. Bare identifiers (non-empty,
// first char is letter/underscore, remaining chars are alnum/underscore)
// pass through unchanged; anything else — whitespace, punctuation,
// backticks, or a leading digit — is backtick-wrapped with embedded
// backticks doubled.
std::string QuoteCypherIdent(const std::string& s) {
    auto is_ident_start = [](unsigned char c) {
        return std::isalpha(c) || c == '_';
    };
    auto is_ident_cont = [](unsigned char c) {
        return std::isalnum(c) || c == '_';
    };

    bool needs = s.empty()
                 || !is_ident_start(static_cast<unsigned char>(s[0]));
    if (!needs) {
        for (size_t i = 1; i < s.size(); ++i) {
            if (!is_ident_cont(static_cast<unsigned char>(s[i]))) {
                needs = true;
                break;
            }
        }
    }
    if (!needs) return s;

    std::string out = "`";
    for (char c : s) {
        if (c == '`') out += "``";
        else out += c;
    }
    out += "`";
    return out;
}

// =====================================================================
// Constructor
// =====================================================================

ProfileCollector::ProfileCollector(CypherExecutor& executor)
    : exec_(executor), cfg_() {}

ProfileCollector::ProfileCollector(CypherExecutor& executor, Config cfg)
    : exec_(executor), cfg_(std::move(cfg)) {}

// =====================================================================
// Scalar helpers
// =====================================================================

int64_t ProfileCollector::QueryScalarInt(const std::string& cypher,
                                         bool& ok, std::string& err) {
    ok  = true;
    err.clear();
    auto r = exec_.Execute(cypher);
    if (!r.ok) { ok = false; err = r.error; return 0; }
    if (r.rows.empty() || r.rows[0].empty()) {
        ok = false; err = "empty result"; return 0;
    }
    const auto& v = r.rows[0][0];
    if (v.IsNull()) return 0;
    // count() may come back as BIGINT, UBIGINT, HUGEINT, or DOUBLE
    // depending on the agg path; ToString() is the lowest-common-
    // denominator extractor.
    try {
        return std::stoll(v.ToString());
    } catch (const std::exception& e) {
        ok = false;
        err = std::string("scalar parse failed: ") + e.what()
              + " (raw='" + v.ToString() + "')";
        return 0;
    }
}

// =====================================================================
// Per-property sweep
// =====================================================================

PropertyProfile ProfileCollector::CollectProperty(
    const std::string& match_clause,
    const std::string& var,
    int64_t parent_row_count,
    const PropertyInfo& p)
{
    PropertyProfile out;
    out.name        = p.name;
    out.type_name   = p.type_name;
    out.row_count   = parent_row_count;

    const std::string prop = var + "." + QuoteCypherIdent(p.name);

    // Sample clause: keeps per-property aggregates bounded so a
    // multi-million row VARCHAR column doesn't blow the hash agg.
    // Empty when sampling is disabled (cfg_.sample_size <= 0).
    std::string sample_with;
    if (cfg_.sample_size > 0 && parent_row_count > cfg_.sample_size) {
        sample_with = " WITH " + var + " LIMIT "
                      + std::to_string(cfg_.sample_size);
    }

    bool ok;
    std::string err;

    // non-null count (full scan — count() is cheap)
    {
        std::ostringstream q;
        q << match_clause
          << " WHERE " << prop << " IS NOT NULL"
          << " RETURN count(*) AS c";
        out.non_null_count = QueryScalarInt(q.str(), ok, err);
        if (!ok) { out.ok = false; out.error = "non_null: " + err; return out; }
    }

    // distinct count (sampled)
    {
        std::ostringstream q;
        q << match_clause << sample_with
          << " WHERE " << prop << " IS NOT NULL"
          << " RETURN count(DISTINCT " << prop << ") AS c";
        out.distinct_count = QueryScalarInt(q.str(), ok, err);
        if (!ok) {
            // Don't abort the whole property — distinct may not be
            // supported for some types (lists, structs). Mark and
            // continue.
            spdlog::debug("[profile] distinct failed for {}: {}", p.name, err);
            out.distinct_count = -1;
        }
    }

    // min / max (sampled)
    {
        std::ostringstream q;
        q << match_clause << sample_with
          << " WHERE " << prop << " IS NOT NULL"
          << " RETURN min(" << prop << ") AS lo, max(" << prop << ") AS hi";
        auto r = exec_.Execute(q.str());
        if (r.ok && !r.rows.empty() && r.rows[0].size() >= 2) {
            out.min_value = ValueToCanonical(r.rows[0][0]);
            out.max_value = ValueToCanonical(r.rows[0][1]);
        } else {
            spdlog::debug("[profile] min/max failed for {}: {}",
                          p.name, r.error);
        }
    }

    // top-K samples (sampled)
    {
        std::ostringstream q;
        q << match_clause << sample_with
          << " WHERE " << prop << " IS NOT NULL"
          << " RETURN " << prop << " AS v, count(*) AS c"
          << " ORDER BY c DESC LIMIT " << cfg_.top_k;
        auto r = exec_.Execute(q.str());
        if (r.ok) {
            for (const auto& row : r.rows) {
                if (row.size() < 2) continue;
                PropertyProfile::Sample s;
                s.value = ValueToCanonical(row[0]);
                try { s.freq = std::stoll(row[1].ToString()); }
                catch (...) { s.freq = 0; }
                out.top_k.push_back(std::move(s));
            }
        } else {
            spdlog::debug("[profile] top_k failed for {}: {}",
                          p.name, r.error);
        }
    }

    return out;
}

// =====================================================================
// Per-label / per-edge sweeps
// =====================================================================

LabelProfile ProfileCollector::CollectLabel(const LabelInfo& l) {
    LabelProfile out;
    out.label = l.label;

    const std::string match = "MATCH (n:" + QuoteCypherIdent(l.label) + ")";

    // total row count once
    {
        bool ok; std::string err;
        out.row_count = QueryScalarInt(match + " RETURN count(n) AS c", ok, err);
        if (!ok) {
            spdlog::warn("[profile] label {} count failed: {}", l.label, err);
            return out;  // can't profile properties without a row count
        }
    }
    if (out.row_count == 0) return out;

    int n_props = static_cast<int>(l.properties.size());
    if (cfg_.max_properties_per_label > 0)
        n_props = std::min(n_props, cfg_.max_properties_per_label);

    for (int i = 0; i < n_props; ++i) {
        const auto& p = l.properties[i];
        if (cfg_.on_property_start)
            cfg_.on_property_start("label:" + l.label + "." + p.name);
        out.properties.push_back(
            CollectProperty(match, "n", out.row_count, p));
    }
    return out;
}

EdgeProfile ProfileCollector::CollectEdge(const EdgeInfo& e) {
    EdgeProfile out;
    out.type = e.type;

    // Use undirected edge so we count each relationship once even when
    // a property graph stores them in only one direction.
    const std::string match = "MATCH ()-[r:" + QuoteCypherIdent(e.type) + "]->()";

    {
        bool ok; std::string err;
        out.row_count = QueryScalarInt(match + " RETURN count(r) AS c", ok, err);
        if (!ok) {
            spdlog::warn("[profile] edge {} count failed: {}", e.type, err);
            return out;
        }
    }
    if (out.row_count == 0) return out;

    int n_props = static_cast<int>(e.properties.size());
    if (cfg_.max_properties_per_label > 0)
        n_props = std::min(n_props, cfg_.max_properties_per_label);

    for (int i = 0; i < n_props; ++i) {
        const auto& p = e.properties[i];
        if (cfg_.on_property_start)
            cfg_.on_property_start("edge:" + e.type + "." + p.name);
        out.properties.push_back(
            CollectProperty(match, "r", out.row_count, p));
    }

    // Endpoint inference is done at the introspection layer (parsing
    // partition names like `eps_<Type>@<Src>@<Dst>`) — no extra
    // Cypher needed. ProfileCollector copies the schema's endpoints
    // verbatim into EdgeProfile so consumers see them in one place.
    for (const auto& ep : e.endpoints) {
        EdgeProfile::Endpoint copy;
        copy.src_label = ep.src_label;
        copy.dst_label = ep.dst_label;
        copy.freq      = 0;  // not measured at this layer
        out.endpoints.push_back(std::move(copy));
    }
    return out;
}

// =====================================================================
// Top-level
// =====================================================================

GraphProfile ProfileCollector::CollectAll(const GraphSchema& schema) {
    GraphProfile out;
    out.graph_name = schema.graph_name;

    auto t0 = std::chrono::steady_clock::now();

    for (const auto& l : schema.labels) {
        out.labels.push_back(CollectLabel(l));
    }
    for (const auto& e : schema.edges) {
        out.edges.push_back(CollectEdge(e));
    }

    // Tally counters from individual property results.
    auto count_props = [&](const std::vector<PropertyProfile>& props) {
        for (const auto& p : props) {
            out.n_queries += 4;  // non_null, distinct, min/max, top-K
            if (!p.ok) out.n_failed += 1;
        }
    };
    for (const auto& l : out.labels) count_props(l.properties);
    for (const auto& e : out.edges)  count_props(e.properties);

    auto t1 = std::chrono::steady_clock::now();
    out.collect_seconds =
        std::chrono::duration<double>(t1 - t0).count();

    return out;
}

// =====================================================================
// JSON
// =====================================================================

nlohmann::json GraphProfile::ToJson() const {
    auto prop_to_json = [](const PropertyProfile& p) {
        nlohmann::json pj;
        pj["name"]           = p.name;
        pj["type"]           = p.type_name;
        pj["row_count"]      = p.row_count;
        pj["non_null_count"] = p.non_null_count;
        pj["distinct_count"] = p.distinct_count;
        pj["min"]            = p.min_value;
        pj["max"]            = p.max_value;
        pj["top_k"]          = nlohmann::json::array();
        for (const auto& s : p.top_k) {
            pj["top_k"].push_back({{"value", s.value}, {"freq", s.freq}});
        }
        if (!p.short_desc.empty()) pj["short_desc"] = p.short_desc;
        if (!p.long_desc.empty())  pj["long_desc"]  = p.long_desc;
        if (!p.ok) {
            pj["ok"]    = false;
            pj["error"] = p.error;
        }
        return pj;
    };

    nlohmann::json j;
    j["graph_name"]       = graph_name;
    j["collect_seconds"]  = collect_seconds;
    j["n_queries"]        = n_queries;
    j["n_failed"]         = n_failed;
    j["has_summaries"]    = has_summaries;

    j["labels"] = nlohmann::json::array();
    for (const auto& l : labels) {
        nlohmann::json lj;
        lj["label"]      = l.label;
        lj["row_count"]  = l.row_count;
        if (!l.short_desc.empty()) lj["short_desc"] = l.short_desc;
        if (!l.long_desc.empty())  lj["long_desc"]  = l.long_desc;
        lj["properties"] = nlohmann::json::array();
        for (const auto& p : l.properties) lj["properties"].push_back(prop_to_json(p));
        j["labels"].push_back(std::move(lj));
    }

    j["edges"] = nlohmann::json::array();
    for (const auto& e : edges) {
        nlohmann::json ej;
        ej["type"]       = e.type;
        ej["row_count"]  = e.row_count;
        if (!e.short_desc.empty()) ej["short_desc"] = e.short_desc;
        if (!e.long_desc.empty())  ej["long_desc"]  = e.long_desc;
        ej["properties"] = nlohmann::json::array();
        for (const auto& p : e.properties) ej["properties"].push_back(prop_to_json(p));
        ej["endpoints"]  = nlohmann::json::array();
        for (const auto& ep : e.endpoints) {
            ej["endpoints"].push_back({
                {"src", ep.src_label},
                {"dst", ep.dst_label},
                {"freq", ep.freq},
            });
        }
        j["edges"].push_back(std::move(ej));
    }
    return j;
}

// =====================================================================
// FromJson
// =====================================================================

GraphProfile GraphProfile::FromJson(const nlohmann::json& j) {
    GraphProfile out;
    out.graph_name      = j.value("graph_name", "");
    out.collect_seconds = j.value("collect_seconds", 0.0);
    out.n_queries       = j.value("n_queries", 0);
    out.n_failed        = j.value("n_failed", 0);
    out.has_summaries   = j.value("has_summaries", false);

    auto prop_from_json = [](const nlohmann::json& pj) {
        PropertyProfile p;
        p.name           = pj.value("name", "");
        p.type_name      = pj.value("type", "");
        p.row_count      = pj.value("row_count", (int64_t)0);
        p.non_null_count = pj.value("non_null_count", (int64_t)0);
        p.distinct_count = pj.value("distinct_count", (int64_t)0);
        p.min_value      = pj.value("min", "");
        p.max_value      = pj.value("max", "");
        p.short_desc     = pj.value("short_desc", "");
        p.long_desc      = pj.value("long_desc", "");
        p.ok             = pj.value("ok", true);
        p.error          = pj.value("error", "");
        if (pj.contains("top_k")) {
            for (const auto& sj : pj["top_k"]) {
                PropertyProfile::Sample s;
                s.value = sj.value("value", "");
                s.freq  = sj.value("freq", (int64_t)0);
                p.top_k.push_back(std::move(s));
            }
        }
        return p;
    };

    if (j.contains("labels")) {
        for (const auto& lj : j["labels"]) {
            LabelProfile l;
            l.label      = lj.value("label", "");
            l.row_count  = lj.value("row_count", (int64_t)0);
            l.short_desc = lj.value("short_desc", "");
            l.long_desc  = lj.value("long_desc", "");
            if (lj.contains("properties")) {
                for (const auto& pj : lj["properties"])
                    l.properties.push_back(prop_from_json(pj));
            }
            out.labels.push_back(std::move(l));
        }
    }

    if (j.contains("edges")) {
        for (const auto& ej : j["edges"]) {
            EdgeProfile e;
            e.type       = ej.value("type", "");
            e.row_count  = ej.value("row_count", (int64_t)0);
            e.short_desc = ej.value("short_desc", "");
            e.long_desc  = ej.value("long_desc", "");
            if (ej.contains("properties")) {
                for (const auto& pj : ej["properties"])
                    e.properties.push_back(prop_from_json(pj));
            }
            if (ej.contains("endpoints")) {
                for (const auto& epj : ej["endpoints"]) {
                    EdgeProfile::Endpoint ep;
                    ep.src_label = epj.value("src", "");
                    ep.dst_label = epj.value("dst", "");
                    ep.freq      = epj.value("freq", (int64_t)0);
                    e.endpoints.push_back(std::move(ep));
                }
            }
            out.edges.push_back(std::move(e));
        }
    }
    return out;
}

// =====================================================================
// ToRichPromptText
// =====================================================================

namespace {

// Format a single property as one line: `name: TYPE  -- short_desc`
// with optional sample hints in parentheses. Keeps the prompt concise
// while still giving the LLM enough information to pick the right
// field for a question.
void FormatPropertyLine(std::ostringstream& os, const PropertyProfile& p,
                        bool indent) {
    if (indent) os << "    ";
    os << p.name << ": " << p.type_name;
    // Distinct / min / max cues — useful for numeric and low-
    // cardinality string columns (e.g. gender, language).
    if (p.distinct_count > 0 && p.distinct_count <= 12 && !p.top_k.empty()) {
        os << " (values: ";
        for (size_t i = 0; i < p.top_k.size() && i < 8; ++i) {
            if (i) os << ", ";
            os << p.top_k[i].value;
        }
        os << ")";
    } else if (!p.min_value.empty() && !p.max_value.empty()
               && p.min_value != p.max_value) {
        // Only for numeric/date-ish types — skip when they look like
        // unbounded text (VARCHAR with clearly alphabetic min/max
        // would confuse the LLM).
        if (p.type_name == "BIGINT" || p.type_name == "UBIGINT"
            || p.type_name == "INTEGER" || p.type_name == "HUGEINT"
            || p.type_name == "FLOAT" || p.type_name == "DOUBLE"
            || p.type_name == "DATE" || p.type_name == "TIMESTAMP") {
            os << " [" << p.min_value << " .. " << p.max_value << "]";
        }
    }
    if (!p.short_desc.empty()) os << "  -- " << p.short_desc;
    os << "\n";
}

}  // namespace

std::string GraphProfile::ToRichPromptText() const {
    std::ostringstream os;
    os << "Vertex labels (" << labels.size() << "):\n";
    for (const auto& l : labels) {
        os << "\n  (:" << l.label << ")  "
           << "[" << l.row_count << " rows]";
        if (!l.short_desc.empty()) os << "  -- " << l.short_desc;
        os << "\n";
        for (const auto& p : l.properties) FormatPropertyLine(os, p, true);
    }
    os << "\nEdge types (" << edges.size() << "):\n";
    for (const auto& e : edges) {
        if (e.endpoints.empty()) {
            os << "\n  [:" << e.type << "]  "
               << "[" << e.row_count << " rows]";
            if (!e.short_desc.empty()) os << "  -- " << e.short_desc;
            os << "\n";
        } else {
            os << "\n  [:" << e.type << "]  "
               << "[" << e.row_count << " rows]";
            if (!e.short_desc.empty()) os << "  -- " << e.short_desc;
            os << "\n";
            for (const auto& ep : e.endpoints) {
                os << "    (:" << ep.src_label << ")-[:" << e.type
                   << "]->(:" << ep.dst_label << ")\n";
            }
        }
        for (const auto& p : e.properties) FormatPropertyLine(os, p, true);
    }
    return os.str();
}

// =====================================================================
// ProfileSummarizer
// =====================================================================

namespace {

// Prompt skeleton. We ask for a strict JSON object and parse it via
// LLMClient::CallJson (which already tolerates ```json fences).
constexpr const char* kSummarizerSystem =
    "You are a data analyst describing a property graph schema so that "
    "downstream query writers understand what each label, edge type, "
    "and property means. Be precise, concrete, and terse.\n"
    "\n"
    "Rules:\n"
    "1. Respond with ONLY a JSON object — no prose, no code fences.\n"
    "2. `short` descriptions are one sentence, <= 120 characters.\n"
    "3. `long` descriptions are 2-3 sentences, <= 400 characters.\n"
    "4. Base every claim on the stats you were given. Do not invent "
    "business meaning that isn't supported by the samples.\n"
    "5. Mention what the property is *used for* in queries when the "
    "name is generic (e.g. `id`, `name`, `type`).";

std::string BuildEntityPrompt(const std::string& kind,
                              const std::string& name,
                              int64_t row_count,
                              const std::vector<PropertyProfile>& props,
                              const std::vector<EdgeProfile::Endpoint>* endpoints,
                              int top_k_in_prompt) {
    std::ostringstream os;
    os << "## Entity\n\n";
    os << kind << ": " << name << "\n";
    os << "row_count: " << row_count << "\n";
    if (endpoints && !endpoints->empty()) {
        os << "endpoints:\n";
        for (const auto& ep : *endpoints) {
            os << "  (" << ep.src_label << ")-[" << name << "]->("
               << ep.dst_label << ")\n";
        }
    }
    os << "\n## Properties\n\n";
    if (props.empty()) {
        os << "(none)\n";
    }
    for (const auto& p : props) {
        os << "- " << p.name << " : " << p.type_name;
        os << "  (distinct=" << p.distinct_count;
        if (!p.min_value.empty()) os << ", min=" << p.min_value;
        if (!p.max_value.empty()) os << ", max=" << p.max_value;
        os << ")\n";
        if (!p.top_k.empty()) {
            os << "    samples: ";
            for (int i = 0; i < (int)p.top_k.size() && i < top_k_in_prompt; ++i) {
                if (i) os << ", ";
                os << p.top_k[i].value << "(" << p.top_k[i].freq << ")";
            }
            os << "\n";
        }
    }
    os << "\n## Output Schema\n\n";
    os << "{\n";
    os << "  \"entity_short\": \"...\",\n";
    os << "  \"entity_long\": \"...\",\n";
    os << "  \"properties\": {\n";
    os << "    \"<property_name>\": {\"short\": \"...\", \"long\": \"...\"}\n";
    os << "  }\n";
    os << "}\n";
    return os.str();
}

// Helper: apply a parsed summary JSON object back onto a property list.
void ApplyPropertySummaries(const nlohmann::json& props_obj,
                            std::vector<PropertyProfile>& props) {
    if (!props_obj.is_object()) return;
    for (auto& p : props) {
        auto it = props_obj.find(p.name);
        if (it == props_obj.end() || !it->is_object()) continue;
        p.short_desc = it->value("short", "");
        p.long_desc  = it->value("long", "");
    }
}

}  // namespace

ProfileSummarizer::ProfileSummarizer(LLMClient& llm)
    : llm_(llm), cfg_() {}

ProfileSummarizer::ProfileSummarizer(LLMClient& llm, Config cfg)
    : llm_(llm), cfg_(std::move(cfg)) {}

bool ProfileSummarizer::SummarizeLabel(LabelProfile& lp) {
    if (cfg_.on_entity_start) cfg_.on_entity_start("label:" + lp.label);

    // Optional per-entity property cap — keeps prompt cost bounded on
    // wide labels (Person has ~12 properties, Message has ~6, so in
    // LDBC this is a no-op, but we still expose the knob).
    std::vector<PropertyProfile> slice = lp.properties;
    if (cfg_.max_properties_per_entity > 0
        && (int)slice.size() > cfg_.max_properties_per_entity) {
        slice.resize(cfg_.max_properties_per_entity);
    }

    LLMRequest req;
    req.system = kSummarizerSystem;
    req.user   = BuildEntityPrompt("label", lp.label, lp.row_count,
                                   slice, nullptr, cfg_.top_k_in_prompt);
    req.model  = cfg_.model;

    auto resp = llm_.CallJson(req);
    if (!resp.ok) {
        spdlog::warn("[summarize] label {} failed: {}", lp.label, resp.error);
        return false;
    }
    lp.short_desc = resp.json.value("entity_short", "");
    lp.long_desc  = resp.json.value("entity_long", "");
    if (resp.json.contains("properties")) {
        ApplyPropertySummaries(resp.json["properties"], lp.properties);
    }
    return true;
}

bool ProfileSummarizer::SummarizeEdge(EdgeProfile& ep) {
    if (cfg_.on_entity_start) cfg_.on_entity_start("edge:" + ep.type);

    std::vector<PropertyProfile> slice = ep.properties;
    if (cfg_.max_properties_per_entity > 0
        && (int)slice.size() > cfg_.max_properties_per_entity) {
        slice.resize(cfg_.max_properties_per_entity);
    }

    LLMRequest req;
    req.system = kSummarizerSystem;
    req.user   = BuildEntityPrompt("edge", ep.type, ep.row_count,
                                   slice, &ep.endpoints, cfg_.top_k_in_prompt);
    req.model  = cfg_.model;

    auto resp = llm_.CallJson(req);
    if (!resp.ok) {
        spdlog::warn("[summarize] edge {} failed: {}", ep.type, resp.error);
        return false;
    }
    ep.short_desc = resp.json.value("entity_short", "");
    ep.long_desc  = resp.json.value("entity_long", "");
    if (resp.json.contains("properties")) {
        ApplyPropertySummaries(resp.json["properties"], ep.properties);
    }
    return true;
}

int ProfileSummarizer::SummarizeAll(GraphProfile& profile) {
    int ok = 0;
    for (auto& l : profile.labels) {
        if (SummarizeLabel(l)) ok++;
    }
    for (auto& e : profile.edges) {
        if (SummarizeEdge(e)) ok++;
    }
    profile.has_summaries = (ok > 0);
    return ok;
}

}  // namespace nl2cypher
}  // namespace turbolynx

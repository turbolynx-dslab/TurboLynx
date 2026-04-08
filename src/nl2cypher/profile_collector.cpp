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

// Quote a property name for Cypher: backtick-wrap if it contains
// anything that wouldn't be a bare identifier. The schema we get
// from introspection should be all bare-identifier safe, but we
// hedge anyway since LDBC has property names like `place.name` in
// some scenarios.
std::string QuoteIdent(const std::string& s) {
    bool needs = s.empty();
    for (char c : s) {
        if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_')) {
            needs = true;
            break;
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

}  // namespace

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

    const std::string prop = var + "." + QuoteIdent(p.name);

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

    const std::string match = "MATCH (n:" + l.label + ")";

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
    const std::string match = "MATCH ()-[r:" + e.type + "]->()";

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

    j["labels"] = nlohmann::json::array();
    for (const auto& l : labels) {
        nlohmann::json lj;
        lj["label"]      = l.label;
        lj["row_count"]  = l.row_count;
        lj["properties"] = nlohmann::json::array();
        for (const auto& p : l.properties) lj["properties"].push_back(prop_to_json(p));
        j["labels"].push_back(std::move(lj));
    }

    j["edges"] = nlohmann::json::array();
    for (const auto& e : edges) {
        nlohmann::json ej;
        ej["type"]       = e.type;
        ej["row_count"]  = e.row_count;
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

}  // namespace nl2cypher
}  // namespace turbolynx

// ProfileCollector — S1 layer of NL2Cypher.
//
// For every (label, property) and (edge type, property) in the
// `GraphSchema`, runs a small set of profiling Cypher queries
// through a `CypherExecutor`:
//
//   - row_count            : MATCH (n:L) RETURN count(n)
//   - non_null_count       : MATCH (n:L) WHERE n.p IS NOT NULL RETURN count(n)
//   - distinct_count       : MATCH (n:L) WHERE n.p IS NOT NULL RETURN count(DISTINCT n.p)
//   - min / max            : MATCH (n:L) WHERE n.p IS NOT NULL RETURN min(n.p), max(n.p)
//   - top_k_samples (k=5)  : MATCH (n:L) WHERE n.p IS NOT NULL
//                            RETURN n.p, count(*) AS c
//                            ORDER BY c DESC LIMIT k
//
// Edges get the analogous queries with `MATCH ()-[r:T]->()`. Edge
// degree distribution and endpoint label inference are filled in by
// a separate per-edge sweep.
//
// All numbers are stored as int64 / double / std::string so the
// metadata.json round-trip is trivial. The collector is single-shot:
// build it, call CollectAll(), read GraphProfile, throw it away.

#pragma once

#include <chrono>
#include <functional>
#include <string>
#include <vector>

#include "nlohmann/json.hpp"
#include "nl2cypher/cypher_executor.hpp"
#include "nl2cypher/llm_client.hpp"
#include "nl2cypher/schema_introspector.hpp"

namespace turbolynx {
namespace nl2cypher {

// Quote `s` so it is safe to splice into a Cypher query at an identifier
// position (label, edge type, property name, alias). Bare identifiers
// pass through unchanged; anything else is wrapped in backticks with
// embedded backticks doubled. Exposed as a free function so every
// catalog-identifier-splicing site in the codebase can use the same
// escaping logic (and so tests can verify it directly).
std::string QuoteCypherIdent(const std::string& s);

struct PropertyProfile {
    std::string name;
    std::string type_name;

    int64_t row_count      = 0;   // total entities of the parent label/type
    int64_t non_null_count = 0;
    int64_t distinct_count = 0;

    std::string min_value;        // stringified Value
    std::string max_value;

    // Top-K (value, frequency) samples sorted by frequency desc.
    struct Sample { std::string value; int64_t freq = 0; };
    std::vector<Sample> top_k;

    // S1 LLM summaries (filled in by ProfileSummarizer). Empty until
    // summarisation runs. The "short" form is ~1 sentence (used in
    // compact schema dumps) and the "long" form is ~2-3 sentences
    // (used when the prompt is already targeted at one label/edge).
    std::string short_desc;
    std::string long_desc;

    // True iff every profiling query for this property succeeded.
    bool ok = true;
    // Last error string (empty when ok). Useful for debugging
    // unsupported types (e.g. count(DISTINCT struct{})).
    std::string error;
};

struct LabelProfile {
    std::string                  label;
    int64_t                      row_count = 0;
    std::vector<PropertyProfile> properties;
    // S1 LLM summaries (see PropertyProfile::short_desc / long_desc).
    std::string                  short_desc;
    std::string                  long_desc;
};

struct EdgeProfile {
    std::string                  type;
    int64_t                      row_count = 0;
    std::vector<PropertyProfile> properties;
    // Endpoint inference: which (src_label, dst_label) pairs the
    // sampler actually observed for this edge type.
    struct Endpoint { std::string src_label; std::string dst_label; int64_t freq = 0; };
    std::vector<Endpoint>        endpoints;
    // S1 LLM summaries.
    std::string                  short_desc;
    std::string                  long_desc;
};

struct GraphProfile {
    std::string                graph_name;
    std::vector<LabelProfile>  labels;
    std::vector<EdgeProfile>   edges;

    // Wall-clock seconds spent collecting.
    double collect_seconds = 0.0;

    // Number of profiling queries executed (including the ones that
    // failed). Useful as a progress / cost report.
    int n_queries = 0;
    int n_failed  = 0;

    // True once ProfileSummarizer has populated at least one label's
    // short_desc. Persisted to metadata.json so the engine can decide
    // whether to use ToRichPromptText or fall back to the schema dump.
    bool has_summaries = false;

    nlohmann::json ToJson() const;

    // Inverse of ToJson. Throws nlohmann::json::exception on malformed
    // input. Intentionally lenient: missing fields become defaults
    // rather than errors, so older metadata.json files stay loadable.
    static GraphProfile FromJson(const nlohmann::json& j);

    // Rich prompt dump — merges raw schema (labels, edges, endpoints)
    // with the S1 profile stats (distinct count, min/max, top-K) and
    // the S1 LLM summaries (short_desc). Used by the engine when
    // metadata.json is present. Falls back to GraphSchema::ToPromptText
    // semantics when `has_summaries == false`.
    std::string ToRichPromptText() const;
};

class ProfileCollector {
public:
    struct Config {
        // How many top-K samples to keep per property. Paper §2.1 uses
        // 5; the LLM summary later sees these as "frequent values".
        int top_k = 5;

        // Cap on per-label property sweeps. Negative = no cap.
        int max_properties_per_label = -1;

        // Per-label / per-edge sample cap for the *property-level*
        // queries (distinct, min/max, top-K). The total `row_count`
        // is always computed on the full label. Sampling keeps the
        // distinct/group-by hash tables bounded so high-cardinality
        // VARCHAR columns on multi-million-row labels stay tractable.
        // 0 = no sampling (full scan).
        int64_t sample_size = 50000;

        // If set, called once per (label/edge, property) start to give
        // the shell a chance to print progress (e.g. tqdm-style).
        // First arg is "label:Person.firstName" or "edge:KNOWS.creationDate".
        std::function<void(const std::string&)> on_property_start;
    };

    explicit ProfileCollector(CypherExecutor& executor);
    ProfileCollector(CypherExecutor& executor, Config cfg);

    // Run the full sweep over `schema`. Errors on individual properties
    // are recorded in their PropertyProfile and do not abort the run.
    GraphProfile CollectAll(const GraphSchema& schema);

private:
    CypherExecutor& exec_;
    Config          cfg_;

    LabelProfile CollectLabel(const LabelInfo& l);
    EdgeProfile  CollectEdge(const EdgeInfo& e);

    PropertyProfile CollectProperty(const std::string& match_clause,
                                    const std::string& var,
                                    int64_t parent_row_count,
                                    const PropertyInfo& p);

    int64_t QueryScalarInt(const std::string& cypher, bool& ok, std::string& err);
};

// =====================================================================
// ProfileSummarizer — S1 LLM summaries.
//
// Given a GraphProfile (already populated with raw stats), makes one
// LLM call per label and one per edge type to generate short/long
// English descriptions for the label/edge itself and every property.
// The descriptions are written back into the GraphProfile in place.
//
// Batching is per-entity, not per-property: a single prompt carries
// the label/edge name, its row count, and the full property list
// (with stats) and asks the model to return a JSON object with all
// the summaries at once. This keeps cost roughly linear in
// |labels| + |edges| instead of |properties|.
//
// Failures on individual entities are logged and skipped — callers
// can treat a partial summarisation as "better than nothing".
// =====================================================================

class ProfileSummarizer {
public:
    struct Config {
        // Which model to route summarisation through. Sonnet is a
        // good default (the task is descriptive, not reasoning-heavy).
        LLMModel model = LLMModel::Sonnet;

        // Cap on top-K samples shown in the summarisation prompt.
        // Fewer is cheaper; more gives the LLM better context.
        int top_k_in_prompt = 5;

        // Hard cap on properties per entity. -1 = no cap.
        int max_properties_per_entity = -1;

        // Progress callback: invoked once at the start of each entity
        // with e.g. "label:Person" or "edge:KNOWS".
        std::function<void(const std::string&)> on_entity_start;
    };

    explicit ProfileSummarizer(LLMClient& llm);
    ProfileSummarizer(LLMClient& llm, Config cfg);

    // Summarise the whole profile in place. Returns the number of
    // entities successfully summarised.
    int SummarizeAll(GraphProfile& profile);

    // Single-entity helpers (exposed for tests and fine-grained use).
    bool SummarizeLabel(LabelProfile& lp);
    bool SummarizeEdge(EdgeProfile& ep);

private:
    LLMClient& llm_;
    Config     cfg_;
};

}  // namespace nl2cypher
}  // namespace turbolynx

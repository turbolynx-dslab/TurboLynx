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
#include "nl2cypher/schema_introspector.hpp"

namespace turbolynx {
namespace nl2cypher {

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
};

struct EdgeProfile {
    std::string                  type;
    int64_t                      row_count = 0;
    std::vector<PropertyProfile> properties;
    // Endpoint inference: which (src_label, dst_label) pairs the
    // sampler actually observed for this edge type.
    struct Endpoint { std::string src_label; std::string dst_label; int64_t freq = 0; };
    std::vector<Endpoint>        endpoints;
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

    nlohmann::json ToJson() const;
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

}  // namespace nl2cypher
}  // namespace turbolynx

#pragma once

namespace turbolynx {

// Total vertex (node) count of the loaded graph, set once per query by the
// planner. Used by the join-cardinality estimator as the |node domain| in the
// degree-based estimate for graph adjacency joins whose id/edge keys carry no
// histogram (output = outer_rows * edges / domain). Using the full node domain
// (rather than a split subset's NDV) lets a UnionAll branch's cardinality scale
// with its subset, so heterogeneous branches receive different join-order
// estimates. 0 disables the degree estimate (falls back to the generic path).
extern double g_tlx_join_domain_card;

}  // namespace turbolynx

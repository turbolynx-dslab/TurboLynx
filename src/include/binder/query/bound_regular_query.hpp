#pragma once

#include "binder/query/normalized_single_query.hpp"

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
	using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

// Top-level bound query: one or more single queries connected by UNION / UNION ALL.
class BoundRegularQuery {
public:
    explicit BoundRegularQuery(bool is_explain = false) : is_explain(is_explain) {}

    void AddSingleQuery(unique_ptr<NormalizedSingleQuery> q, bool is_union_all = false) {
        if (!single_queries.empty()) {
            is_union_all_flags.push_back(is_union_all);
        }
        single_queries.push_back(std::move(q));
    }

    idx_t GetNumSingleQueries() const { return single_queries.size(); }
    NormalizedSingleQuery* GetSingleQuery(idx_t i) const { return single_queries[i].get(); }

    bool IsUnionAll(idx_t i) const {
        return i < is_union_all_flags.size() && is_union_all_flags[i];
    }

    bool IsExplain() const { return is_explain; }

private:
    vector<unique_ptr<NormalizedSingleQuery>> single_queries;
    vector<bool>                              is_union_all_flags;
    bool                                      is_explain;
};

} // namespace turbolynx

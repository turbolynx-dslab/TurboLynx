#pragma once

#include "parser/query/single_query.hpp"
#include <vector>
#include <memory>

namespace duckdb {

// Top-level Cypher statement: one or more SingleQueries connected by UNION / UNION ALL.
class RegularQuery {
public:
    explicit RegularQuery(unique_ptr<SingleQuery> first) {
        single_queries.push_back(std::move(first));
    }

    void AddSingleQuery(unique_ptr<SingleQuery> q, bool is_union_all) {
        single_queries.push_back(std::move(q));
        is_union_all_flags.push_back(is_union_all);
    }

    idx_t         GetNumSingleQueries() const { return single_queries.size(); }
    SingleQuery*  GetSingleQuery(idx_t i) const { return single_queries[i].get(); }

    // is_union_all_flags[i] corresponds to the join between query i and i+1.
    bool IsUnionAll(idx_t i) const {
        return i < is_union_all_flags.size() && is_union_all_flags[i];
    }

    void SetExplain(bool v) { enable_explain = v; }
    bool IsExplain()  const { return enable_explain; }

private:
    vector<unique_ptr<SingleQuery>> single_queries;
    vector<bool>                    is_union_all_flags;
    bool                            enable_explain = false;
};

} // namespace duckdb

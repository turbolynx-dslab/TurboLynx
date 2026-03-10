#pragma once

#include "binder/query/reading_clause/bound_reading_clause.hpp"
#include "binder/graph_pattern/bound_query_graph.hpp"
#include "binder/expression/bound_expression.hpp"

namespace duckdb {

class BoundMatchClause : public BoundReadingClause {
public:
    explicit BoundMatchClause(unique_ptr<BoundQueryGraphCollection> query_graph_collection,
                               bool is_optional = false)
        : BoundReadingClause(BoundClauseType::MATCH),
          query_graph_collection(std::move(query_graph_collection)),
          is_optional(is_optional) {}

    const BoundQueryGraphCollection* GetQueryGraphCollection() const {
        return query_graph_collection.get();
    }

    bool IsOptional() const { return is_optional; }

    // ---- Predicates (WHERE clause split into CNF conjuncts) ----
    void AddPredicate(shared_ptr<BoundExpression> pred) {
        predicates.push_back(std::move(pred));
    }

    const bound_expression_vector& GetPredicates() const { return predicates; }

private:
    unique_ptr<BoundQueryGraphCollection> query_graph_collection;
    bound_expression_vector               predicates;
    bool                                  is_optional;
};

} // namespace duckdb

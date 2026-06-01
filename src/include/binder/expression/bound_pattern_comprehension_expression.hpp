#pragma once

#include "binder/expression/bound_expression.hpp"
#include "binder/query/reading_clause/bound_match_clause.hpp"
#include "common/typedef.hpp"

namespace duckdb {

// Pattern comprehension: [ [path =] (a) -[:R1]-> (b) -[:R2]-> (c) [WHERE p] | expr ]
//
// Lowering target (in converter):
//   CScalarSubquery(
//     GbAgg(group: outer correlation cols, agg: collect(map_expr))
//       LeftOuterJoin(
//         outer correlation reference,
//         inner pattern (start -> hops -> end nodes/edges)
//       )
//   )
//
// The GbAgg+collect compresses the multi-row inner match into a single row x
// single LIST column, satisfying CScalarSubquery's single-value contract.
// ORCA's CSubqueryHandler decorrelates the rest.

// One hop in the comprehension's pattern chain.
//   (prev_node) -[edge_var:edge_type {edge_props} *lower..upper]-> (end_var:end_label {end_props})
//
// edge_var / end_var are empty when no binding was provided in the source.
// edge_type is empty for an untyped edge ("[]"). end_label is empty for an
// unlabeled end node ("()"). lower/upper encode the varlen quantifier:
// fixed single hop is (1, 1); unbounded upper uses UINT64_MAX.
struct CypherBoundPatternHop {
    // Edge
    string                     edge_var;           // optional binding
    string                     edge_type;          // "" if untyped
    ExpandDirection            direction;          // OUTGOING / INCOMING / BOTH
    shared_ptr<BoundExpression> edge_predicate;    // inline map {prop: val}, nullable
    uint64_t                   lower = 1;
    uint64_t                   upper = 1;

    // End node
    string                     end_var;            // pattern-scoped or outer-bound name
    string                     end_label;          // "" if unlabeled
    shared_ptr<BoundExpression> end_predicate;     // inline map, nullable
};

class CypherBoundPatternComprehensionExpression : public BoundExpression {
public:
    CypherBoundPatternComprehensionExpression(
        LogicalType                          result_type,        // LIST<map_expr's type>
        string                               path_var,           // "" if no `p =` binding
        string                               start_var,
        string                               start_label,
        shared_ptr<BoundExpression>          start_predicate,
        vector<CypherBoundPatternHop>        hops,
        shared_ptr<BoundExpression>          where_expr,         // optional, may be null
        shared_ptr<BoundExpression>          map_expr,           // required
        string                               unique_name)
        : BoundExpression(BoundExpressionType::PATTERN_COMP,
                          std::move(result_type), std::move(unique_name)),
          path_var(std::move(path_var)),
          start_var(std::move(start_var)),
          start_label(std::move(start_label)),
          start_predicate(std::move(start_predicate)),
          hops(std::move(hops)),
          where_expr(std::move(where_expr)),
          map_expr(std::move(map_expr)) {}

    const string&                       GetPathVar()        const { return path_var; }
    const string&                       GetStartVar()       const { return start_var; }
    const string&                       GetStartLabel()     const { return start_label; }
    BoundExpression*                    GetStartPredicate() const { return start_predicate.get(); }
    const vector<CypherBoundPatternHop>& GetHops()          const { return hops; }
    BoundExpression*                    GetWhereExpr()      const { return where_expr.get(); }
    BoundExpression*                    GetMapExpr()        const { return map_expr.get(); }
    bool                                HasWhereExpr()      const { return where_expr != nullptr; }

    // BoundMatchClause-shaped view of the inner pattern. The binder populates
    // this so the converter can call PlanRegularMatch with the standard
    // subquery_outer_nodes / corr_keys interface (matches the
    // BoundExistsSubqueryExpression pattern).
    void                       SetInnerMatch(unique_ptr<BoundMatchClause> m) { inner_match = std::move(m); }
    const BoundMatchClause*    GetInnerMatch() const { return inner_match.get(); }
    bool                       HasInnerMatch() const { return inner_match != nullptr; }

    shared_ptr<BoundExpression> Copy() const override {
        vector<CypherBoundPatternHop> copied_hops;
        copied_hops.reserve(hops.size());
        for (auto &h : hops) {
            copied_hops.push_back({
                h.edge_var, h.edge_type, h.direction,
                h.edge_predicate ? h.edge_predicate->Copy() : nullptr,
                h.lower, h.upper,
                h.end_var, h.end_label,
                h.end_predicate ? h.end_predicate->Copy() : nullptr,
            });
        }
        auto copy = make_shared<CypherBoundPatternComprehensionExpression>(
            data_type,
            path_var, start_var, start_label,
            start_predicate ? start_predicate->Copy() : nullptr,
            std::move(copied_hops),
            where_expr ? where_expr->Copy() : nullptr,
            map_expr   ? map_expr->Copy()   : nullptr,
            unique_name);
        copy->SetAlias(alias);
        // inner_match is not deep-copied: it's only consumed by the converter
        // along the same pass that built it, never re-bound after Copy().
        return copy;
    }

private:
    string                              path_var;
    string                              start_var;
    string                              start_label;
    shared_ptr<BoundExpression>         start_predicate;
    vector<CypherBoundPatternHop>       hops;
    shared_ptr<BoundExpression>         where_expr;
    shared_ptr<BoundExpression>         map_expr;
    unique_ptr<BoundMatchClause>        inner_match;
};

} // namespace duckdb

#pragma once

#include "binder/bind_context.hpp"
#include "binder/query/bound_regular_query.hpp"
#include "binder/query/reading_clause/bound_match_clause.hpp"
#include "binder/query/reading_clause/bound_unwind_clause.hpp"
#include "parser/query/regular_query.hpp"
#include "parser/query/single_query.hpp"
#include "parser/query/query_part.hpp"
#include "parser/query/reading_clause/match_clause.hpp"
#include "parser/query/reading_clause/unwind_clause.hpp"
#include "parser/query/updating_clause/create_clause.hpp"
#include "binder/query/updating_clause/bound_create_clause.hpp"
#include "binder/query/updating_clause/bound_set_clause.hpp"
#include "parser/query/updating_clause/set_clause.hpp"
#include "parser/query/return_with_clause/return_clause.hpp"
#include "parser/query/return_with_clause/with_clause.hpp"
#include "parser/query/graph_pattern/pattern_element.hpp"
#include "parser/expression/property_expression.hpp"
#include "parser/expression/variable_expression.hpp"
#include "parser/expression/function_expression.hpp"
#include "parser/expression/case_expression.hpp"
#include "parser/expression/exists_subquery_expression.hpp"
#include "main/client_context.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "catalog/catalog_entry/property_schema_catalog_entry.hpp"

namespace duckdb {

// TurboLynx-native Binder: resolves parser AST against the catalog to produce
// a fully-typed, OID-resolved BoundRegularQuery.
class Binder {
public:
    explicit Binder(ClientContext* context);

    unique_ptr<BoundRegularQuery> Bind(const RegularQuery& query);

private:
    // ---- Query structure ----
    unique_ptr<NormalizedSingleQuery> BindSingleQuery(const SingleQuery& sq, BindContext& outer_ctx);
    unique_ptr<NormalizedQueryPart>   BindQueryPart(const QueryPart& qp, BindContext& ctx);
    // Bind the final part (trailing reading/updating clauses + RETURN)
    unique_ptr<NormalizedQueryPart>   BindFinalQueryPart(const SingleQuery& sq, BindContext& ctx);

    // ---- Clauses ----
    unique_ptr<BoundMatchClause>    BindMatchClause(const MatchClause& match, BindContext& ctx);
    unique_ptr<BoundUnwindClause>   BindUnwindClause(const UnwindClause& unwind, BindContext& ctx);
    unique_ptr<BoundCreateClause>   BindCreateClause(const CreateClause& create, BindContext& ctx);
    unique_ptr<BoundSetClause>      BindSetClause(const SetClause& set, BindContext& ctx);
    // `out_projected_aliases` (optional, used by BindQueryPart for #19) is
    // filled with the alias names this projection produces. The caller
    // narrows BindContext to those names before binding the next query part.
    unique_ptr<BoundProjectionBody> BindProjectionBody(
        const ProjectionBody& proj, BindContext& ctx,
        unordered_set<string>* out_projected_aliases = nullptr);

    // ---- Graph patterns ----
    unique_ptr<BoundQueryGraph>          BindPatternElement(const PatternElement& pe, BindContext& ctx,
                                                               vector<pair<const NodePattern*, shared_ptr<BoundNodeExpression>>>& node_bindings);
    shared_ptr<BoundNodeExpression>      BindNodePattern(const NodePattern& node, BindContext& ctx);
    shared_ptr<BoundRelExpression>       BindRelPattern(const RelPattern& rel,
                                                         const BoundNodeExpression& src,
                                                         const BoundNodeExpression& dst,
                                                         BindContext& ctx);

    // ---- Expressions ----
    shared_ptr<BoundExpression> BindExpression(const ParsedExpression& expr, BindContext& ctx);
    shared_ptr<BoundExpression> BindPropertyExpression(const ParsedPropertyExpression& expr, BindContext& ctx);
    shared_ptr<BoundExpression> BindVariableExpression(const ParsedVariableExpression& expr, BindContext& ctx);
    shared_ptr<BoundExpression> BindFunctionInvocation(const FunctionExpression& expr, BindContext& ctx);
    shared_ptr<BoundExpression> BindCaseExpression(const CaseExpression& expr, BindContext& ctx);
    shared_ptr<BoundExpression> BindExistsSubquery(const ExistsSubqueryExpression& expr, BindContext& ctx);

    // ---- Catalog helpers ----
    GraphCatalogEntry* GetGraphCatalog();

    // Resolve node labels → partition OIDs + graphlet OIDs
    void ResolveNodeLabels(const vector<string>& labels,
                            vector<uint64_t>& out_partition_ids,
                            vector<uint64_t>& out_graphlet_ids);

    // Resolve edge types → partition OIDs + graphlet OIDs
    void ResolveRelTypes(const vector<string>& types,
                          vector<uint64_t>& out_partition_ids,
                          vector<uint64_t>& out_graphlet_ids);

    // Infer the label of an unlabeled node from the edge definition.
    // Returns the inferred label string, or empty if inference fails.
    //
    // `direction` (passed through from `rel.GetDirection()` by the
    // `InferNodeLabelFromEdge` wrapper) tells the inference which side
    // of the edge the unknown node sits on for the explicit Cypher
    // arrow.  Without it, the ambiguous fall-through (both
    // `other_node` partition flags unset, e.g. anonymous LHS) silently
    // picks `dst_part` and a `<-` pattern ends up mislabeled as the
    // destination type, yielding zero rows for syntactically symmetric
    // traversals (issue #234).
    string InferNodeLabelFromEdge(const BoundNodeExpression& other_node,
                                  const RelPattern& rel);
    string InferNodeLabelFromEdgeTypes(const BoundNodeExpression& other_node,
                                       const vector<string>& edge_types,
                                       RelDirection direction);

    // Look up property expression for a variable (node or rel)
    shared_ptr<BoundExpression> LookupPropertyOnNode(BoundNodeExpression& node,
                                                       const string& prop_name);
    shared_ptr<BoundExpression> LookupPropertyOnRel(BoundRelExpression& rel,
                                                     const string& prop_name);

    // Unique name generator for unnamed variables and expressions
    string GenAnonVarName()  { return "_v" + to_string(anon_counter_++); }
    string GenExprName(const ParsedExpression& expr);

    // ---- Property projection pushdown into collect (q14) ----
    // Pre-scan the whole parsed single query for a WITH alias defined as
    //   head(collect({field: nodeVar, ...}))  or  collect({field: nodeVar})
    // and, when every downstream use of `<alias>.<field>` is a property
    // access `<alias>.<field>.<prop>` (never the whole struct), record the
    // exact set of props needed. The struct_pack expansion then materialises
    // only those props instead of the full node. Conservative: any access we
    // can't prove is property-only → the nodeVar is marked "needs all".
    void PrescanCollectPushdown(const SingleQuery& sq);
    // Recursively collect collect/head map-literal alias definitions.
    void PrescanCollectDefs(const ParsedExpression& proj_expr);
    // Recursively scan an expression for property/whole-node uses.
    void PrescanCollectUses(const ParsedExpression& expr);

private:
    ClientContext*      context_;
    GraphCatalogEntry*  graph_cat_ = nullptr;
    idx_t               anon_counter_ = 0;
    idx_t               expr_counter_  = 0;
    idx_t               exist_counter_ = 0;

    // ---- Collect-pushdown bookkeeping (see PrescanCollectPushdown) ----
    // alias.field  ->  the bare node variable bound to that struct field.
    unordered_map<string, string> collect_field_to_nodevar_;
    // node variable  ->  set of property names accessed via <alias>.<field>.<prop>.
    unordered_map<string, unordered_set<string>> collect_pushdown_props_;
    // node variables whose collected struct is used as a whole node anywhere
    // (or used outside the recognized struct_pack) → must keep ALL properties.
    unordered_set<string> collect_needs_all_;
};

} // namespace duckdb

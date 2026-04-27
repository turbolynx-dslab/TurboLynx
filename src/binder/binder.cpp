#include "binder/binder.hpp"
#include <set>
#include "common/constants.hpp"
#include "binder/expression/bound_literal_expression.hpp"
#include "binder/expression/bound_property_expression.hpp"
#include "binder/expression/bound_variable_expression.hpp"
#include "binder/expression/bound_function_expression.hpp"
#include "binder/expression/bound_agg_function_expression.hpp"
#include "binder/expression/bound_comparison_expression.hpp"
#include "binder/expression/bound_bool_expression.hpp"
#include "binder/expression/bound_null_expression.hpp"
#include "binder/expression/bound_case_expression.hpp"
#include "parser/expression/property_expression.hpp"
#include "parser/expression/variable_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/expression/function_expression.hpp"
#include "parser/expression/case_expression.hpp"
#include "parser/expression/exists_subquery_expression.hpp"
#include "binder/expression/bound_exists_subquery_expression.hpp"
#include "parser/expression/comparison_expression.hpp"
#include "parser/expression/conjunction_expression.hpp"
#include "parser/expression/operator_expression.hpp"
#include "common/enums/expression_type.hpp"
#include "common/enums/graph_component_type.hpp"
#include "common/exception.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "catalog/catalog_entry/property_schema_catalog_entry.hpp"
#include "main/database.hpp"
#include "common/string_util.hpp"
#include "common/types/date.hpp"
#include "common/types/timestamp.hpp"
#include "parser/query/updating_clause/set_clause.hpp"
#include "parser/query/updating_clause/delete_clause.hpp"
#include "binder/query/updating_clause/bound_set_clause.hpp"
#include "binder/query/updating_clause/bound_delete_clause.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_index_info.hpp"
#include "catalog/catalog_entry/index_catalog_entry.hpp"
#include "spdlog/spdlog.h"

namespace duckdb {

static bool ExtractReduceVarName(const ParsedExpression &expr, string &out) {
    if (expr.GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
        return false;
    }
    auto &const_expr = static_cast<const ConstantExpression &>(expr);
    if (const_expr.value.IsNull() ||
        const_expr.value.type().id() != LogicalTypeId::VARCHAR) {
        return false;
    }
    out = const_expr.value.GetValue<string>();
    return true;
}

static bool ParsedExprIsReduceVar(const ParsedExpression &expr, const string &name) {
    if (expr.GetExpressionType() != ExpressionType::COLUMN_REF) {
        return false;
    }
    auto *var = dynamic_cast<const ParsedVariableExpression *>(&expr);
    return var && var->GetVariableName() == name;
}

static bool IsSupportedReduceSumBody(const ParsedExpression &body,
                                     const string &acc_name,
                                     const string &loop_var_name) {
    auto *fn = dynamic_cast<const FunctionExpression *>(&body);
    if (!fn || fn->children.size() != 2 ||
        StringUtil::Lower(fn->function_name) != "+") {
        return false;
    }

    return (ParsedExprIsReduceVar(*fn->children[0], acc_name) &&
            ParsedExprIsReduceVar(*fn->children[1], loop_var_name)) ||
           (ParsedExprIsReduceVar(*fn->children[0], loop_var_name) &&
            ParsedExprIsReduceVar(*fn->children[1], acc_name));
}

static bool ReduceSumTypeAllowed(const LogicalType &type) {
    auto tid = type.id();
    return tid == LogicalTypeId::ANY || tid == LogicalTypeId::UNKNOWN ||
           type.IsNumeric();
}

static bool ReduceSumInputTypeAllowed(const LogicalType &type) {
    auto tid = type.id();
    if (tid == LogicalTypeId::ANY || tid == LogicalTypeId::UNKNOWN) {
        return true;
    }
    if (tid == LogicalTypeId::LIST) {
        return ReduceSumTypeAllowed(ListType::GetChildType(type));
    }
    return type.IsNumeric();
}

static bool IsLiteralNumericZero(const shared_ptr<BoundExpression> &expr) {
    if (!expr || expr->GetExprType() != BoundExpressionType::LITERAL) {
        return false;
    }
    auto &lit = static_cast<const BoundLiteralExpression &>(*expr);
    if (lit.GetValue().IsNull()) {
        return false;
    }
    try {
        return lit.GetValue().GetValue<double>() == 0.0;
    } catch (...) {
        return false;
    }
}

// ---- Construction ----

Binder::Binder(ClientContext* context) : context_(context) {}

// ---- Catalog helpers ----

GraphCatalogEntry* Binder::GetGraphCatalog() {
    if (graph_cat_) return graph_cat_;
    auto& catalog = context_->db->GetCatalog();
    graph_cat_ = (GraphCatalogEntry*)catalog.GetEntry(
        *context_, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
    if (!graph_cat_) {
        throw BinderException("No graph catalog found");
    }
    return graph_cat_;
}

void Binder::ResolveNodeLabels(const vector<string>& labels,
                                vector<uint64_t>& out_partition_ids,
                                vector<uint64_t>& out_graphlet_ids) {
    auto* gcat = GetGraphCatalog();
    auto& catalog = context_->db->GetCatalog();

    vector<string> label_keys = labels.empty() ? vector<string>{} : labels;
    vector<idx_t> part_oids = gcat->LookupPartition(
        *context_, label_keys, GraphComponentType::VERTEX);

    for (auto part_oid : part_oids) {
        out_partition_ids.push_back((uint64_t)part_oid);
        auto* part_cat = (PartitionCatalogEntry*)catalog.GetEntry(
            *context_, DEFAULT_SCHEMA, part_oid);
        if (!part_cat) continue;
        PropertySchemaID_vector* ps_ids = part_cat->GetPropertySchemaIDs();
        if (!ps_ids) continue;
        for (auto ps_id : *ps_ids) {
            out_graphlet_ids.push_back((uint64_t)ps_id);
        }
    }
}

void Binder::ResolveRelTypes(const vector<string>& types,
                              vector<uint64_t>& out_partition_ids,
                              vector<uint64_t>& out_graphlet_ids) {
    auto* gcat = GetGraphCatalog();
    auto& catalog = context_->db->GetCatalog();

    // LookupPartition for EDGE only accepts 0 or 1 keys at a time.
    // When multiple rel types are specified (e.g. [:A|B]), call it once per type.
    auto resolve_one = [&](const vector<string>& keys) {
        vector<idx_t> part_oids = gcat->LookupPartition(
            *context_, keys, GraphComponentType::EDGE);
        for (auto part_oid : part_oids) {
            out_partition_ids.push_back((uint64_t)part_oid);
            auto* part_cat = (PartitionCatalogEntry*)catalog.GetEntry(
                *context_, DEFAULT_SCHEMA, part_oid);
            if (!part_cat) continue;
            PropertySchemaID_vector* ps_ids = part_cat->GetPropertySchemaIDs();
            if (!ps_ids) continue;
            for (auto ps_id : *ps_ids) {
                out_graphlet_ids.push_back((uint64_t)ps_id);
            }
        }
    };

    if (types.empty()) {
        resolve_one({});
    } else {
        for (const auto& t : types) {
            resolve_one({t});
        }
    }
}

// Infer a vertex label from the edge's src/dst partition.
// Given a known node (other_node) and an edge type, determine what label
// the opposite endpoint should have.
string Binder::InferNodeLabelFromEdge(const BoundNodeExpression& other_node,
                                       const RelPattern& rel) {
    auto* gcat = GetGraphCatalog();
    auto& catalog = context_->db->GetCatalog();

    // Resolve edge type to partitions (may be multiple for 1:N edge types)
    vector<uint64_t> edge_part_ids, edge_graphlet_ids;
    ResolveRelTypes(rel.GetTypes(), edge_part_ids, edge_graphlet_ids);
    if (edge_part_ids.empty()) return "";

    // Collect all possible opposite-side partition OIDs across all edge partitions.
    // If they all agree on the same partition, we can infer the label.
    // Virtual unified partitions (sub_partition_oids non-empty) are expanded to
    // their real sub-partitions for matching.
    idx_t inferred_part = 0;
    bool inferred_set = false;

    // Gather real (non-virtual) edge partitions to inspect
    vector<PartitionCatalogEntry*> real_eparts;
    for (auto ep_id : edge_part_ids) {
        auto* epart = (PartitionCatalogEntry*)catalog.GetEntry(
            *context_, DEFAULT_SCHEMA, (idx_t)ep_id);
        if (!epart) continue;
        if (!epart->sub_partition_oids.empty()) {
            // Virtual partition — expand to real sub-partitions
            for (auto sub_oid : epart->sub_partition_oids) {
                auto* sub = (PartitionCatalogEntry*)catalog.GetEntry(
                    *context_, DEFAULT_SCHEMA, sub_oid);
                if (sub) real_eparts.push_back(sub);
            }
        } else {
            real_eparts.push_back(epart);
        }
    }

    for (auto* epart : real_eparts) {
        idx_t src_part = epart->GetSrcPartOid();
        idx_t dst_part = epart->GetDstPartOid();

        // Determine which side the known node matches
        bool other_is_src = false, other_is_dst = false;
        for (auto pid : other_node.GetPartitionIDs()) {
            if ((idx_t)pid == src_part) other_is_src = true;
            if ((idx_t)pid == dst_part) other_is_dst = true;
        }

        idx_t candidate;
        if (other_is_src && !other_is_dst) {
            candidate = dst_part;
        } else if (other_is_dst && !other_is_src) {
            candidate = src_part;
        } else {
            // Self-referential or ambiguous
            candidate = dst_part;
        }

        if (!inferred_set) {
            inferred_part = candidate;
            inferred_set = true;
        } else if (inferred_part != candidate) {
            // Multiple edge partitions point to different opposite-side partitions.
            // Cannot infer a single label — return empty (multi-label binding).
            return "";
        }
    }

    if (!inferred_set) return "";

    // Find the vertex label name from the inferred partition OID.
    return gcat->GetLabelFromVertexPartitionIndex(*context_, inferred_part);
}

// Returns true if any of the node's partitions has a property schema that is
// temporal or fake. Such schemas lack full column layouts and cause type
// mismatches during multi-schema scans when whole_node_required is set.
static bool NodeHasTemporalOrFakeSchema(const BoundNodeExpression& node,
                                         ClientContext& ctx, Catalog& catalog) {
    for (auto part_oid : node.GetPartitionIDs()) {
        auto* part_cat = (PartitionCatalogEntry*)catalog.GetEntry(
            ctx, DEFAULT_SCHEMA, (idx_t)part_oid);
        if (!part_cat) continue;
        PropertySchemaID_vector* ps_ids = part_cat->GetPropertySchemaIDs();
        if (!ps_ids) continue;
        for (auto ps_oid : *ps_ids) {
            auto* ps_cat = (PropertySchemaCatalogEntry*)catalog.GetEntry(
                ctx, DEFAULT_SCHEMA, (idx_t)ps_oid, true);
            if (!ps_cat) continue;
            if (ps_cat->is_fake ||
                ps_cat->GetName().find(DEFAULT_TEMPORAL_INFIX) != string::npos) {
                return true;
            }
        }
    }
    return false;
}

// Populate property expressions on a node from catalog.
// _id (key_id=0) is always prepended as prop_exprs[0] — BuildSchemaProjectionMapping
// relies on col_idx==0 being ID_KEY_ID.
static void PopulateNodeProperties(BoundNodeExpression& node, ClientContext& ctx,
                                    GraphCatalogEntry& gcat, Catalog& catalog) {
    // Always add _id first (key_id=0, col 0 in every graphlet)
    if (!node.HasProperty(0)) {
        auto id_expr = make_shared<BoundPropertyExpression>(
            node.GetUniqueName(), (uint64_t)0, LogicalType::ID,
            node.GetUniqueName() + "._id");
        node.AddPropertyExpression((uint64_t)0, std::move(id_expr));
    }

    auto& part_ids = node.GetPartitionIDs();
    for (auto part_oid : part_ids) {
        auto* part_cat = (PartitionCatalogEntry*)catalog.GetEntry(
            ctx, DEFAULT_SCHEMA, (idx_t)part_oid);
        if (!part_cat) continue;
        PropertySchemaID_vector* ps_ids = part_cat->GetPropertySchemaIDs();
        if (!ps_ids) continue;
        for (auto ps_oid : *ps_ids) {
            auto* ps_cat = (PropertySchemaCatalogEntry*)catalog.GetEntry(
                ctx, DEFAULT_SCHEMA, (idx_t)ps_oid);
            if (!ps_cat) continue;
            PropertyKeyID_vector* key_ids = ps_cat->GetKeyIDs();
            if (!key_ids) continue;
            auto types = ps_cat->GetTypesWithCopy();
            for (idx_t i = 0; i < key_ids->size(); i++) {
                uint64_t kid = (*key_ids)[i];
                if (!node.HasProperty(kid)) {
                    LogicalType lt = i < types.size() ? types[i] : LogicalType::ANY;
                    string uname = node.GetUniqueName() + "._prop_" + to_string(kid);
                    auto prop_expr = make_shared<BoundPropertyExpression>(
                        node.GetUniqueName(), kid, lt, uname);
                    node.AddPropertyExpression(kid, std::move(prop_expr));
                }
            }
        }
    }
}

// Populate property expressions on an edge from catalog.
// _id (key_id=0) is always prepended as prop_exprs[0].
// _sid and _tid will follow from the catalog property schema.
static void PopulateRelProperties(BoundRelExpression& rel, ClientContext& ctx,
                                   GraphCatalogEntry& gcat, Catalog& catalog) {
    // Always add _id first (key_id=0)
    if (!rel.HasProperty(0)) {
        auto id_expr = make_shared<BoundPropertyExpression>(
            rel.GetUniqueName(), (uint64_t)0, LogicalType::ID,
            rel.GetUniqueName() + "._id");
        rel.AddPropertyExpression((uint64_t)0, std::move(id_expr));
    }

    auto& part_ids = rel.GetPartitionIDs();
    for (auto part_oid : part_ids) {
        auto* part_cat = (PartitionCatalogEntry*)catalog.GetEntry(
            ctx, DEFAULT_SCHEMA, (idx_t)part_oid);
        if (!part_cat) continue;
        PropertySchemaID_vector* ps_ids = part_cat->GetPropertySchemaIDs();
        if (!ps_ids) continue;
        for (auto ps_oid : *ps_ids) {
            auto* ps_cat = (PropertySchemaCatalogEntry*)catalog.GetEntry(
                ctx, DEFAULT_SCHEMA, (idx_t)ps_oid);
            if (!ps_cat) continue;
            PropertyKeyID_vector* key_ids = ps_cat->GetKeyIDs();
            if (!key_ids) continue;
            auto types = ps_cat->GetTypesWithCopy();
            for (idx_t i = 0; i < key_ids->size(); i++) {
                uint64_t kid = (*key_ids)[i];
                if (!rel.HasProperty(kid)) {
                    LogicalType lt = i < types.size() ? types[i] : LogicalType::ANY;
                    string uname = rel.GetUniqueName() + "._prop_" + to_string(kid);
                    auto prop_expr = make_shared<BoundPropertyExpression>(
                        rel.GetUniqueName(), kid, lt, uname);
                    rel.AddPropertyExpression(kid, std::move(prop_expr));
                }
            }
        }
    }
}

// ---- Entry point ----

unique_ptr<BoundRegularQuery> Binder::Bind(const RegularQuery& query) {
    auto result = make_unique<BoundRegularQuery>(query.IsExplain());
    BindContext empty_ctx;
    for (idx_t i = 0; i < query.GetNumSingleQueries(); i++) {
        auto nsq = BindSingleQuery(*query.GetSingleQuery(i), empty_ctx);
        bool is_union_all = query.IsUnionAll(i > 0 ? i - 1 : 0);
        result->AddSingleQuery(std::move(nsq), i > 0 && is_union_all);
    }
    return result;
}

// ---- Single query ----

unique_ptr<NormalizedSingleQuery> Binder::BindSingleQuery(const SingleQuery& sq, BindContext& outer_ctx) {
    auto nsq = make_unique<NormalizedSingleQuery>();
    // Create a fresh scope for this query
    BindContext ctx(&outer_ctx);

    // Bind each WITH-delimited query part
    for (idx_t i = 0; i < sq.GetNumQueryParts(); i++) {
        auto nqp = BindQueryPart(*sq.GetQueryPart(i), ctx);
        nsq->AppendQueryPart(std::move(nqp));
        // After WITH, inner scope becomes outer for next part
        // (simplified: reuse same ctx — inner variables visible downstream)
    }

    // Final part: trailing reading/updating clauses + RETURN
    auto final_part = BindFinalQueryPart(sq, ctx);
    nsq->AppendQueryPart(std::move(final_part));

    return nsq;
}

// ---- Query part (WITH-delimited) ----

unique_ptr<NormalizedQueryPart> Binder::BindQueryPart(const QueryPart& qp, BindContext& ctx) {
    auto nqp = make_unique<NormalizedQueryPart>();

    for (idx_t i = 0; i < qp.GetNumReadingClauses(); i++) {
        auto* rc = qp.GetReadingClause(i);
        if (rc->GetClauseType() == CypherClauseType::MATCH) {
            auto& mc = static_cast<const MatchClause&>(*rc);
            nqp->AddReadingClause(BindMatchClause(mc, ctx));
        } else if (rc->GetClauseType() == CypherClauseType::UNWIND) {
            auto& uc = static_cast<const UnwindClause&>(*rc);
            nqp->AddReadingClause(BindUnwindClause(uc, ctx));
        }
    }

    // WITH clause becomes the projection body
    auto* wc = qp.GetWithClause();
    if (wc) {
        auto proj = BindProjectionBody(*wc->GetBody(), ctx);
        nqp->SetProjectionBody(std::move(proj));
        if (wc->HasWhere()) {
            auto pred = BindExpression(*wc->GetWhere(), ctx);
            nqp->SetProjectionBodyPredicate(std::move(pred));
        }
    }

    return nqp;
}

// ---- Final query part (RETURN) ----

unique_ptr<NormalizedQueryPart> Binder::BindFinalQueryPart(const SingleQuery& sq, BindContext& ctx) {
    auto nqp = make_unique<NormalizedQueryPart>();

    // Trailing reading clauses (after last WITH, before RETURN)
    for (idx_t i = 0; i < sq.GetNumReadingClauses(); i++) {
        auto* rc = sq.GetReadingClause(i);
        if (rc->GetClauseType() == CypherClauseType::MATCH) {
            auto& mc = static_cast<const MatchClause&>(*rc);
            nqp->AddReadingClause(BindMatchClause(mc, ctx));
        } else if (rc->GetClauseType() == CypherClauseType::UNWIND) {
            auto& uc = static_cast<const UnwindClause&>(*rc);
            nqp->AddReadingClause(BindUnwindClause(uc, ctx));
        }
    }

    // Updating clauses (CREATE, SET, DELETE)
    for (idx_t i = 0; i < sq.GetNumUpdatingClauses(); i++) {
        auto* uc = sq.GetUpdatingClause(i);
        if (uc->GetClauseType() == UpdatingClauseType::INSERT) {
            auto& cc = static_cast<const CreateClause&>(*uc);
            nqp->AddUpdatingClause(BindCreateClause(cc, ctx));
        }
        else if (uc->GetClauseType() == UpdatingClauseType::SET) {
            auto& sc = static_cast<const SetClause&>(*uc);
            nqp->AddUpdatingClause(BindSetClause(sc, ctx));
        }
        else if (uc->GetClauseType() == UpdatingClauseType::DELETE_CLAUSE) {
            auto& dc = static_cast<const DeleteClause&>(*uc);
            auto bound = make_unique<BoundDeleteClause>();
            for (auto& var : dc.GetVariables()) bound->AddVariable(var);
            nqp->AddUpdatingClause(std::move(bound));
        }
    }

    // RETURN clause → projection body
    if (sq.HasReturnClause()) {
        auto* ret = sq.GetReturnClause();
        auto proj = BindProjectionBody(*ret->GetBody(), ctx);
        nqp->SetProjectionBody(std::move(proj));
    }

    return nqp;
}

// ---- MATCH clause ----

unique_ptr<BoundMatchClause> Binder::BindMatchClause(const MatchClause& match, BindContext& ctx) {
    auto qgc = make_unique<BoundQueryGraphCollection>();

    vector<pair<const NodePattern*, shared_ptr<BoundNodeExpression>>> node_bindings;
    for (auto& pe : match.GetPatterns()) {
        // Register path variable in BindContext if present
        if (pe->HasPathName()) {
            ctx.AddPath(pe->GetPathName());
        }
        auto qg = BindPatternElement(*pe, ctx, node_bindings);
        qgc->AddAndMergeIfConnected(std::move(qg));
    }

    auto bound_match = make_unique<BoundMatchClause>(std::move(qgc), match.IsOptional());

    // WHERE predicates
    if (match.HasWhere()) {
        auto pred = BindExpression(*match.GetWhere(), ctx);
        // Split on AND to add individual predicates
        bound_match->AddPredicate(std::move(pred));
    }

    // Inline property filters: (n:Label {key: value}) or ({key: value}) → n.key = value predicates
    // Use node_bindings (built during BindPatternElement) so anonymous nodes are handled correctly.
    for (auto& [np_ptr, node_expr] : node_bindings) {
        const NodePattern& np = *np_ptr;
        if (np.GetNumProperties() == 0) continue;
        string label = node_expr->GetUniqueName();
        for (idx_t i = 0; i < np.GetNumProperties(); i++) {
            auto lhs = LookupPropertyOnNode(*node_expr, np.GetPropertyKey(i));
            auto rhs = BindExpression(*np.GetPropertyValue(i), ctx);
            string uname = "_iprop_" + label + "." + np.GetPropertyKey(i);
            bound_match->AddPredicate(make_shared<CypherBoundComparisonExpression>(
                ExpressionType::COMPARE_EQUAL, std::move(lhs), std::move(rhs), std::move(uname)));
        }
    }
    auto add_rel_inline_props = [&](const RelPattern& rp) {
        if (rp.GetNumProperties() == 0) return;
        if (rp.GetVarName().empty() || !ctx.HasRel(rp.GetVarName())) return;
        auto rel = ctx.GetRel(rp.GetVarName());
        for (idx_t i = 0; i < rp.GetNumProperties(); i++) {
            auto lhs = LookupPropertyOnRel(*rel, rp.GetPropertyKey(i));
            auto rhs = BindExpression(*rp.GetPropertyValue(i), ctx);
            string uname = "_iprop_" + rp.GetVarName() + "." + rp.GetPropertyKey(i);
            bound_match->AddPredicate(make_shared<CypherBoundComparisonExpression>(
                ExpressionType::COMPARE_EQUAL, std::move(lhs), std::move(rhs), std::move(uname)));
        }
    };
    for (auto& pe : match.GetPatterns()) {
        for (idx_t c = 0; c < pe->GetNumChains(); c++) {
            add_rel_inline_props(*pe->GetChain(c).rel);
        }
    }

    return bound_match;
}

// ---- UNWIND clause ----

unique_ptr<BoundUnwindClause> Binder::BindUnwindClause(const UnwindClause& unwind, BindContext& ctx) {
    auto expr = BindExpression(*unwind.GetExpression(), ctx);
    // UNWIND requires a LIST-typed expression. Reject clearly-scalar inputs with
    // a clean error in the binder instead of letting PhysicalUnwind assert on
    // ListValue::GetChildren at runtime (value.cpp:1546). ANY and SQLNULL are
    // allowed through: ANY means the type will be resolved later (e.g. list
    // literals before element-type inference), and SQLNULL supports the valid
    // `UNWIND null AS x` idiom.
    const auto& dtype = expr->GetDataType();
    const auto tid = dtype.id();
    const bool is_list_like = (tid == LogicalTypeId::LIST ||
                               tid == LogicalTypeId::ANY ||
                               tid == LogicalTypeId::SQLNULL);
    if (!is_list_like) {
        throw BinderException(
            "UNWIND expression must be of LIST type, but got " +
            dtype.ToString() +
            ". Array-typed properties loaded as joined strings (e.g. \"a;b;c\") "
            "are not automatically split — declare the column as an array in the "
            "loader schema or pre-split the value.");
    }
    LogicalType elem_type = LogicalType::ANY;
    if (tid == LogicalTypeId::LIST) {
        elem_type = ListType::GetChildType(dtype);
    }
    ctx.AddAliasType(unwind.GetAlias(), elem_type);
    if (elem_type.id() == LogicalTypeId::PATH) {
        ctx.AddPath(unwind.GetAlias());
    }
    return make_unique<BoundUnwindClause>(std::move(expr), unwind.GetAlias());
}

// ---- CREATE clause: schema bootstrap helpers ----
//
// Cypher's CREATE on Neo4j implicitly creates labels/types if they don't yet
// exist. TurboLynx historically required labels/types to exist before CREATE
// (set up via bulk-load CSV import). The helpers below let CREATE itself
// bootstrap a vertex or edge partition when the label/type is missing, so a
// fresh empty workspace can be populated directly with Cypher.
//
// The infra mirrors CreateVertexCatalogInfos / CreateEdgeCatalogInfos in
// src/loader/bulkload_pipeline.cpp; the only differences are:
//   - id_key_column_idxs is left empty (no user-declared key column),
//   - the new PropertySchema is seeded with num_tuples=1 so ORCA does not
//     mark stats as dummy and collapse plan subtrees on the first MATCH.

static LogicalType InferLogicalTypeFromValue(const Value &v) {
    if (v.IsNull()) {
        return LogicalType(LogicalTypeId::ANY);
    }
    return v.type();
}

static uint16_t EncodeExtraTypeInfoForType(const LogicalType &type) {
    if (type.id() != LogicalTypeId::DECIMAL) {
        return 0;
    }
    uint16_t width_scale = DecimalType::GetWidth(type);
    width_scale = (uint16_t)((width_scale << 8) | DecimalType::GetScale(type));
    return width_scale;
}

static void RegisterPropertyOnPartition(PartitionCatalogEntry *part_cat,
                                        const string &key,
                                        PropertyKeyID key_id,
                                        const LogicalType &type) {
    if (!part_cat) return;
    auto existing = part_cat->global_property_key_to_location.find(key_id);
    if (existing != part_cat->global_property_key_to_location.end()) return;
    idx_t next_idx = part_cat->global_property_key_names.size();
    part_cat->global_property_key_to_location[key_id] = next_idx;
    part_cat->global_property_key_names.push_back(key);
    part_cat->global_property_key_ids.push_back(key_id);
    part_cat->global_property_typesid.push_back(type.id());
    part_cat->extra_typeinfo_vec.push_back(EncodeExtraTypeInfoForType(type));
    part_cat->min_max_array.resize(part_cat->global_property_typesid.size());
    part_cat->welford_array.resize(part_cat->global_property_typesid.size());
    part_cat->num_columns++;
}

static idx_t BootstrapVertexPartition(
    ClientContext &context, GraphCatalogEntry *gcat, Catalog &catalog,
    const string &label,
    const vector<pair<string, Value>> &props) {

    string partition_name = string(DEFAULT_VERTEX_PARTITION_PREFIX) + label;
    string property_schema_name =
        string(DEFAULT_VERTEX_PROPERTYSCHEMA_PREFIX) + label;

    // Build property key list — always include the internal _id slot first
    // (PropertyKeyID 0) so downstream ID lookups stay aligned with bulkload.
    vector<string> key_names;
    vector<LogicalType> types;
    key_names.reserve(props.size());
    types.reserve(props.size());
    for (auto &kv : props) {
        key_names.push_back(kv.first);
        types.push_back(InferLogicalTypeFromValue(kv.second));
    }

    CreatePartitionInfo partition_info(DEFAULT_SCHEMA, partition_name.c_str());
    auto *partition_cat = (PartitionCatalogEntry *)catalog.CreatePartition(
        context, &partition_info);
    PartitionID new_pid = gcat->GetNewPartitionID();

    CreatePropertySchemaInfo propertyschema_info(
        DEFAULT_SCHEMA, property_schema_name.c_str(), new_pid,
        partition_cat->GetOid());
    auto *property_schema_cat =
        (PropertySchemaCatalogEntry *)catalog.CreatePropertySchema(
            context, &propertyschema_info);

    CreateIndexInfo idx_info(DEFAULT_SCHEMA, label + "_id",
                             IndexType::PHYSICAL_ID, partition_cat->GetOid(),
                             property_schema_cat->GetOid(), 0, {-1});
    auto *index_cat =
        (IndexCatalogEntry *)catalog.CreateIndex(context, &idx_info);

    vector<string> labels_vec{label};
    gcat->AddVertexPartition(context, new_pid, partition_cat->GetOid(),
                             labels_vec);

    vector<PropertyKeyID> property_key_ids;
    gcat->GetPropertyKeyIDs(context, key_names, types, property_key_ids);

    partition_cat->AddPropertySchema(context, property_schema_cat->GetOid(),
                                     property_key_ids);
    partition_cat->SetSchema(context, key_names, types, property_key_ids);
    // No user-declared id column for Cypher-bootstrap; internal _id is enough.
    vector<idx_t> empty_id_cols;
    partition_cat->SetIdKeyColumnIdxs(empty_id_cols);
    partition_cat->SetPhysicalIDIndex(index_cat->GetOid());
    partition_cat->SetPartitionID(new_pid);

    property_schema_cat->SetSchema(context, key_names, types, property_key_ids);
    property_schema_cat->SetPhysicalIDIndex(index_cat->GetOid());
    // Seed num_tuples=1 so ORCA's RetrieveRelStats does not see num_rows==0 →
    // is_dummy_stats=true → plan-subtree collapse with dangling references.
    property_schema_cat->SetNumberOfLastExtentNumTuples(1);

    return partition_cat->GetOid();
}

static idx_t BootstrapEdgePartition(
    ClientContext &context, GraphCatalogEntry *gcat, Catalog &catalog,
    const string &type, const string &src_label, const string &dst_label,
    const vector<pair<string, Value>> &props,
    idx_t src_partition_oid, idx_t dst_partition_oid) {

    string internal_name = type + "@" + src_label + "@" + dst_label;
    string partition_name = string(DEFAULT_EDGE_PARTITION_PREFIX) + internal_name;
    string property_schema_name =
        string(DEFAULT_EDGE_PROPERTYSCHEMA_PREFIX) + internal_name;

    // Edge property schema starts with implicit _sid/_tid endpoint keys, then
    // user-declared properties — same convention used by BuildEdgeDeltaRow.
    // Use UBIGINT (not ID): the storage layer emits the row's stored value
    // for UBIGINT but reconstructs a PhysicalID for the LogicalType::ID
    // column, which would clobber endpoint references with the *edge's*
    // own pid on every scan.
    vector<string> key_names = {"_sid", "_tid"};
    vector<LogicalType> types = {LogicalType::UBIGINT, LogicalType::UBIGINT};
    key_names.reserve(props.size() + 2);
    types.reserve(props.size() + 2);
    for (auto &kv : props) {
        key_names.push_back(kv.first);
        types.push_back(InferLogicalTypeFromValue(kv.second));
    }

    CreatePartitionInfo partition_info(DEFAULT_SCHEMA, partition_name.c_str());
    auto *partition_cat = (PartitionCatalogEntry *)catalog.CreatePartition(
        context, &partition_info);
    PartitionID new_pid = gcat->GetNewPartitionID();

    CreatePropertySchemaInfo propertyschema_info(
        DEFAULT_SCHEMA, property_schema_name.c_str(), new_pid,
        partition_cat->GetOid());
    auto *property_schema_cat =
        (PropertySchemaCatalogEntry *)catalog.CreatePropertySchema(
            context, &propertyschema_info);

    CreateIndexInfo id_idx_info(DEFAULT_SCHEMA, internal_name + "_id",
                                IndexType::PHYSICAL_ID,
                                partition_cat->GetOid(),
                                property_schema_cat->GetOid(), 0, {-1});
    auto *id_index_cat =
        (IndexCatalogEntry *)catalog.CreateIndex(context, &id_idx_info);

    gcat->AddEdgePartition(context, new_pid, partition_cat->GetOid(), type);
    vector<PropertyKeyID> property_key_ids;
    gcat->GetPropertyKeyIDs(context, key_names, types, property_key_ids);

    partition_cat->AddPropertySchema(context, property_schema_cat->GetOid(),
                                     property_key_ids);
    partition_cat->SetSchema(context, key_names, types, property_key_ids);
    partition_cat->SetPhysicalIDIndex(id_index_cat->GetOid());
    partition_cat->SetPartitionID(new_pid);

    property_schema_cat->SetSchema(context, key_names, types, property_key_ids);
    property_schema_cat->SetPhysicalIDIndex(id_index_cat->GetOid());
    property_schema_cat->SetNumberOfLastExtentNumTuples(1);

    // Annotate the first two columns as endpoint references so storage
    // and plan layers don't treat them as ordinary user properties or
    // (worse) as system _id columns to be reconstructed from row pid.
    {
        std::vector<ColumnKind> kinds(key_names.size(), ColumnKind::PROPERTY);
        if (!kinds.empty()) kinds[0] = ColumnKind::ENDPOINT_REF;  // _sid
        if (kinds.size() > 1) kinds[1] = ColumnKind::ENDPOINT_REF;  // _tid
        property_schema_cat->SetKeyKinds(kinds);
    }

    // Wire AdjList metadata + CSR indexes on src/dst vertex PSes so future
    // MATCH-based traversals can find the edge partition.
    auto wire_side = [&](idx_t vertex_partition_oid, LogicalType direction,
                          int64_t src_key_col_idx, int64_t dst_key_col_idx,
                          const string &csr_suffix, IndexType csr_index_type) {
        auto *vertex_part = (PartitionCatalogEntry *)catalog.GetEntry(
            context, DEFAULT_SCHEMA, vertex_partition_oid);
        if (!vertex_part) return;
        idx_t adj_col_idx = 0;
        auto *vps_oids = vertex_part->GetPropertySchemaIDs();
        if (vps_oids) {
            for (auto vps_oid : *vps_oids) {
                auto *vps = (PropertySchemaCatalogEntry *)catalog.GetEntry(
                    context, DEFAULT_SCHEMA, vps_oid);
                if (!vps) continue;
                vps->AppendAdjListType({direction});
                adj_col_idx = vps->AppendAdjListKey(context, internal_name);
            }
        }
        CreateIndexInfo adj_idx_info(
            DEFAULT_SCHEMA, internal_name + csr_suffix, csr_index_type,
            partition_cat->GetOid(), property_schema_cat->GetOid(), adj_col_idx,
            {src_key_col_idx, dst_key_col_idx});
        auto *adj_idx = (IndexCatalogEntry *)catalog.CreateIndex(
            context, &adj_idx_info);
        partition_cat->AddAdjIndex(adj_idx->GetOid());
    };

    // FORWARD adjacency on src side (src→dst)
    wire_side(src_partition_oid, LogicalType::FORWARD_ADJLIST,
              /*src_key_col_idx=*/1, /*dst_key_col_idx=*/2, "_fwd",
              IndexType::FORWARD_CSR);
    // BACKWARD adjacency on dst side (dst→src)
    wire_side(dst_partition_oid, LogicalType::BACKWARD_ADJLIST,
              /*src_key_col_idx=*/1, /*dst_key_col_idx=*/2, "_bwd",
              IndexType::BACKWARD_CSR);

    gcat->AddEdgeConnectionInfo(context, src_partition_oid,
                                partition_cat->GetOid());
    partition_cat->SetSrcDstPartOid(src_partition_oid, dst_partition_oid);

    return partition_cat->GetOid();
}

// Look up a vertex partition by single label; if absent, bootstrap one using
// the property KV pairs from the CREATE pattern. Returns the partition OID.
static idx_t ResolveOrBootstrapVertexPartition(
    ClientContext &context, GraphCatalogEntry *gcat, Catalog &catalog,
    const string &label, const vector<pair<string, Value>> &props) {
    auto it = gcat->vertexlabel_map.find(label);
    if (it != gcat->vertexlabel_map.end()) {
        auto pit = gcat->label_to_partition_index.find(it->second);
        if (pit != gcat->label_to_partition_index.end() && !pit->second.empty()) {
            return pit->second.front();
        }
    }
    return BootstrapVertexPartition(context, gcat, catalog, label, props);
}

// Look up an edge partition by (type, src_label, dst_label); if absent,
// bootstrap one. Returns the partition OID.
static idx_t ResolveOrBootstrapEdgePartition(
    ClientContext &context, GraphCatalogEntry *gcat, Catalog &catalog,
    const string &type, const string &src_label, const string &dst_label,
    const vector<pair<string, Value>> &props,
    idx_t src_partition_oid, idx_t dst_partition_oid) {

    // Exact (type, src, dst) match by partition name lookup.
    string internal_name = type + "@" + src_label + "@" + dst_label;
    string partition_name = string(DEFAULT_EDGE_PARTITION_PREFIX) + internal_name;
    auto *existing = catalog.GetEntry(context, CatalogType::PARTITION_ENTRY,
                                       DEFAULT_SCHEMA, partition_name, true);
    if (existing) {
        return ((PartitionCatalogEntry *)existing)->GetOid();
    }
    return BootstrapEdgePartition(context, gcat, catalog, type, src_label,
                                   dst_label, props, src_partition_oid,
                                   dst_partition_oid);
}

// ---- CREATE clause ----

unique_ptr<BoundCreateClause> Binder::BindCreateClause(const CreateClause& create, BindContext& ctx) {
    auto bound = make_unique<BoundCreateClause>();

    idx_t synthetic_create_node_idx = 0;
    auto ensure_create_var_name = [&](const string &name) {
        if (!name.empty()) {
            return name;
        }
        return string("__create_node_") +
               std::to_string(synthetic_create_node_idx++);
    };

    auto* gcat = GetGraphCatalog();
    auto& catalog = context_->db->GetCatalog();

    auto collect_props = [](const NodePattern &n) {
        vector<pair<string, Value>> props;
        for (idx_t i = 0; i < n.GetNumProperties(); i++) {
            const auto& key = n.GetPropertyKey(i);
            auto* val_expr = n.GetPropertyValue(i);
            if (val_expr->type != ExpressionType::VALUE_CONSTANT) {
                throw BinderException("CREATE property values must be constants (got non-constant for key '" + key + "')");
            }
            auto& const_expr = static_cast<const ConstantExpression&>(*val_expr);
            props.emplace_back(key, const_expr.value);
        }
        return props;
    };
    auto collect_rel_props = [](const RelPattern &r) {
        vector<pair<string, Value>> props;
        for (idx_t i = 0; i < r.GetNumProperties(); i++) {
            const auto& key = r.GetPropertyKey(i);
            auto* val_expr = r.GetPropertyValue(i);
            if (val_expr->type == ExpressionType::VALUE_CONSTANT) {
                auto& const_expr = static_cast<const ConstantExpression&>(*val_expr);
                props.emplace_back(key, const_expr.value);
            }
        }
        return props;
    };

    // Resolve a CREATE-side node into (label, partition_oid). Three cases:
    //   1. Inline label on CREATE → bootstrap (or reuse) partition for it.
    //   2. No inline label, but variable was bound by an earlier MATCH →
    //      reuse the partition/label that the MATCH resolved.
    //   3. Neither → leave the node un-partitioned (legacy unlabeled CREATE).
    auto resolve_create_node =
        [&](const NodePattern &n, BoundCreateNodeInfo &info) -> uint64_t {
        auto &labels = n.GetLabels();
        if (!labels.empty()) {
            info.label = labels[0];
            idx_t part_oid = ResolveOrBootstrapVertexPartition(
                *context_, gcat, catalog, info.label, info.properties);
            info.partition_ids.push_back((uint64_t)part_oid);
            return (uint64_t)part_oid;
        }
        const string &var = n.GetVarName();
        if (!var.empty() && ctx.HasNode(var)) {
            auto bound_node = ctx.GetNode(var);
            if (bound_node) {
                const auto &bound_labels = bound_node->GetLabels();
                if (!bound_labels.empty()) {
                    info.label = bound_labels[0];
                }
                const auto &bound_pids = bound_node->GetPartitionIDs();
                if (!bound_pids.empty()) {
                    info.partition_ids.push_back(bound_pids.front());
                    return bound_pids.front();
                }
            }
        }
        return 0;
    };

    for (auto& pattern : create.GetPatterns()) {
        const auto& node = pattern->GetFirstNode();
        BoundCreateNodeInfo info;
        info.variable_name = ensure_create_var_name(node.GetVarName());

        info.properties = collect_props(node);

        uint64_t prev_partition_oid = resolve_create_node(node, info);
        string prev_label = info.label;
        bound->AddNode(std::move(info));

        // Process edge chains: (a)-[:TYPE]->(b)
        string prev_var_name = bound->GetNodes().back().variable_name;
        for (idx_t ci = 0; ci < pattern->GetNumChains(); ci++) {
            auto& chain = pattern->GetChain(ci);
            auto& rel = *chain.rel;
            auto& tgt_node = *chain.node;

            // Bind the target node too
            BoundCreateNodeInfo tgt_info;
            auto tgt_var_name = ensure_create_var_name(tgt_node.GetVarName());
            tgt_info.variable_name = tgt_var_name;
            tgt_info.properties = collect_props(tgt_node);
            uint64_t tgt_partition_oid = resolve_create_node(tgt_node, tgt_info);
            string tgt_label = tgt_info.label;
            bound->AddNode(std::move(tgt_info));

            // Bind the edge
            BoundCreateEdgeInfo edge_info;
            edge_info.variable_name = rel.GetVarName();
            if (!rel.GetTypes().empty()) {
                edge_info.type = rel.GetTypes()[0];
            }
            edge_info.src_label = prev_label;
            edge_info.dst_label = tgt_label;
            edge_info.src_variable_name = prev_var_name;
            edge_info.dst_variable_name = tgt_var_name;
            edge_info.src_vid = 0;  // resolved at execution time from node id property
            edge_info.dst_vid = 0;
            edge_info.properties = collect_rel_props(rel);

            // Resolve (or bootstrap) the edge partition. Bootstrapping requires
            // both endpoint partition OIDs; if either is missing (e.g. labelless
            // pattern with no MATCH binding), fall back to the legacy
            // lookup-by-type path.
            if (!edge_info.type.empty() && !edge_info.src_label.empty() &&
                !edge_info.dst_label.empty() && prev_partition_oid != 0 &&
                tgt_partition_oid != 0) {
                idx_t edge_oid = ResolveOrBootstrapEdgePartition(
                    *context_, gcat, catalog, edge_info.type,
                    edge_info.src_label, edge_info.dst_label,
                    edge_info.properties, (idx_t)prev_partition_oid,
                    (idx_t)tgt_partition_oid);
                edge_info.edge_partition_ids.push_back((uint64_t)edge_oid);
            } else {
                vector<uint64_t> edge_graphlet_ids;
                ResolveRelTypes(rel.GetTypes(), edge_info.edge_partition_ids,
                                edge_graphlet_ids);
            }
            bound->AddEdge(std::move(edge_info));

            prev_var_name = tgt_var_name;
            prev_partition_oid = tgt_partition_oid;
            prev_label = tgt_label;
        }
    }

    return bound;
}

unique_ptr<BoundSetClause> Binder::BindSetClause(const SetClause& set, BindContext& ctx) {
    auto bound = make_unique<BoundSetClause>();
    auto* gcat = GetGraphCatalog();
    auto& catalog = context_->db->GetCatalog();
    for (auto& item : set.GetItems()) {
        BoundSetItem bi;
        bi.variable_name = item.variable_name;
        bi.property_key = item.property_key;
        // Evaluate constant value
        if (item.value->type == ExpressionType::VALUE_CONSTANT) {
            auto& const_expr = static_cast<const ConstantExpression&>(*item.value);
            bi.value = const_expr.value;
        } else {
            throw BinderException("SET values must be constants (got non-constant for key '" + item.property_key + "')");
        }
        // Capture target metadata for the mutation post-processor: which
        // partition this variable refers to and the PS keys for its current
        // representative property schema. Lets the executor emit per-variable
        // readback columns rather than guessing from the first ID-typed
        // column. Falls through quietly when the variable isn't bound in
        // this scope (e.g. SET on a freshly-CREATE-d but unmatched var).
        if (!bi.variable_name.empty() && ctx.HasNode(bi.variable_name)) {
            auto bound_node = ctx.GetNode(bi.variable_name);
            if (bound_node) {
                auto &pids = bound_node->GetPartitionIDs();
                if (!pids.empty()) {
                    bi.target_partition_oid = pids.front();
                    auto* part_cat = (PartitionCatalogEntry*)catalog.GetEntry(
                        *context_, DEFAULT_SCHEMA, pids.front(), true);
                    if (part_cat) {
                        auto* ps_ids = part_cat->GetPropertySchemaIDs();
                        if (ps_ids && !ps_ids->empty()) {
                            auto* ps = (PropertySchemaCatalogEntry*)catalog.GetEntry(
                                *context_, DEFAULT_SCHEMA, ps_ids->front(), true);
                            if (ps) {
                                if (auto* keys = ps->GetKeys()) {
                                    bi.target_ps_keys = *keys;
                                }
                            }
                        }
                    }
                }
            }
        }
        bound->AddItem(std::move(bi));
    }
    return bound;
}

// ---- PatternElement → BoundQueryGraph ----

unique_ptr<BoundQueryGraph> Binder::BindPatternElement(const PatternElement& pe, BindContext& ctx,
    vector<pair<const NodePattern*, shared_ptr<BoundNodeExpression>>>& node_bindings) {
    auto qg = make_unique<BoundQueryGraph>();

    // First node
    auto first_node = BindNodePattern(pe.GetFirstNode(), ctx);
    node_bindings.push_back({&pe.GetFirstNode(), first_node});
    if (!qg->ContainsNode(first_node->GetUniqueName())) {
        qg->AddQueryNode(first_node);
    }
    auto* prev_node = qg->GetQueryNode(pe.GetFirstNode().GetVarName().empty()
                                        ? first_node->GetUniqueName()
                                        : first_node->GetUniqueName()).get();

    // Chains: rel → node
    for (idx_t i = 0; i < pe.GetNumChains(); i++) {
        const PatternElementChain& chain = pe.GetChain(i);

        // If the destination node has no labels, infer from the edge definition.
        // This ensures the node is bound with the correct single partition,
        // allowing IdSeek to project properties from the right graphlet.
        if (chain.node->GetLabels().empty() && !chain.rel->GetTypes().empty()) {
            auto inferred = InferNodeLabelFromEdge(*prev_node, *chain.rel);
            if (!inferred.empty()) {
                const_cast<NodePattern*>(chain.node.get())->SetLabels({inferred});
            }
        }

        // Bind destination node first (needed by rel binder)
        auto dst_node = BindNodePattern(*chain.node, ctx);
        node_bindings.push_back({chain.node.get(), dst_node});
        auto* src_node_ptr = prev_node;

        auto rel = BindRelPattern(*chain.rel, *src_node_ptr, *dst_node, ctx);

        if (!qg->ContainsRel(rel->GetUniqueName())) {
            qg->AddQueryRel(std::move(rel));
        }
        if (!qg->ContainsNode(dst_node->GetUniqueName())) {
            qg->AddQueryNode(dst_node);
        }
        prev_node = qg->GetQueryNode(dst_node->GetUniqueName()).get();
    }

    // Path type and path variable name
    if (pe.GetPathType() == PatternPathType::SHORTEST) {
        qg->SetPathType(BoundQueryGraph::PathType::SHORTEST);
    } else if (pe.GetPathType() == PatternPathType::ALL_SHORTEST) {
        qg->SetPathType(BoundQueryGraph::PathType::ALL_SHORTEST);
    }
    if (pe.HasPathName()) {
        qg->SetPathName(pe.GetPathName());
    }

    return qg;
}

// ---- NodePattern ----

shared_ptr<BoundNodeExpression> Binder::BindNodePattern(const NodePattern& node, BindContext& ctx) {
    string var_name = node.GetVarName().empty() ? GenAnonVarName() : node.GetVarName();

    // If already bound, return existing (same variable in multiple MATCH patterns)
    if (ctx.HasNode(var_name)) {
        return ctx.GetNode(var_name);
    }

    vector<uint64_t> partition_ids, graphlet_ids;
    ResolveNodeLabels(node.GetLabels(), partition_ids, graphlet_ids);

    auto bound_node = make_shared<BoundNodeExpression>(
        var_name, node.GetLabels(), partition_ids, graphlet_ids);

    // Populate property expressions from catalog
    if (!partition_ids.empty()) {
        auto* gcat = GetGraphCatalog();
        auto& catalog = context_->db->GetCatalog();
        PopulateNodeProperties(*bound_node, *context_, *gcat, catalog);
    }

    ctx.AddNode(var_name, bound_node);
    return bound_node;
}

// ---- RelPattern ----

shared_ptr<BoundRelExpression> Binder::BindRelPattern(const RelPattern& rel,
                                                       const BoundNodeExpression& src,
                                                       const BoundNodeExpression& dst,
                                                       BindContext& ctx) {
    string var_name = rel.GetVarName().empty() ? GenAnonVarName() : rel.GetVarName();

    // If already bound, return existing
    if (ctx.HasRel(var_name)) {
        return ctx.GetRel(var_name);
    }

    vector<uint64_t> partition_ids, graphlet_ids;
    ResolveRelTypes(rel.GetTypes(), partition_ids, graphlet_ids);

    // M27: Filter edge partitions by src/dst node labels.
    // When the query specifies explicit node labels (e.g. (a:Comment)-[:REPLY_OF]->(b:Post)),
    // only keep edge partitions whose stored src/dst match the bound node partitions.
    // For VarLen patterns (*N..M), skip dst filtering — intermediate hops may use
    // different partitions and the endpoint type is checked by downstream operators.
    bool is_varlen = rel.GetPatternType() == RelPatternType::VARIABLE_LENGTH;
    if (partition_ids.size() > 1) {
        auto& catalog = context_->db->GetCatalog();
        auto *gcat = static_cast<GraphCatalogEntry *>(
            catalog.GetEntry(*context_, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA,
                             DEFAULT_GRAPH, true));
        const auto& raw_src_pids = src.GetPartitionIDs();
        const auto& raw_dst_pids = dst.GetPartitionIDs();

        // Expand virtual vertex partition OIDs to real sub-partition OIDs.
        // Edge partitions store real partition OIDs as src/dst, so we must
        // compare against real OIDs when filtering.
        auto expand_vpids = [&](const vector<uint64_t> &pids) -> vector<uint64_t> {
            vector<uint64_t> expanded;
            for (auto pid : pids) {
                auto *part = static_cast<PartitionCatalogEntry *>(
                    catalog.GetEntry(*context_, DEFAULT_SCHEMA, (idx_t)pid, true));
                if (part && !part->sub_partition_oids.empty()) {
                    for (auto sub_oid : part->sub_partition_oids)
                        expanded.push_back((uint64_t)sub_oid);
                } else {
                    expanded.push_back(pid);
                }
            }
            return expanded;
        };
        auto src_pids = expand_vpids(raw_src_pids);
        auto dst_pids = expand_vpids(raw_dst_pids);

        bool has_src_constraint = !src_pids.empty();
        bool has_dst_constraint = !dst_pids.empty() && !is_varlen;
        std::unordered_map<idx_t, std::unordered_set<idx_t>> connected_edge_cache;

        auto has_connected_edge = [&](const vector<uint64_t> &node_pids,
                                      idx_t edge_part_oid) -> bool {
            if (!gcat) {
                return false;
            }
            for (auto pid : node_pids) {
                auto cache_it = connected_edge_cache.find((idx_t)pid);
                if (cache_it == connected_edge_cache.end()) {
                    vector<idx_t> connected_edge_oids;
                    gcat->GetConnectedEdgeOids(*context_, (idx_t)pid,
                                               connected_edge_oids);
                    std::unordered_set<idx_t> connected_set(
                        connected_edge_oids.begin(), connected_edge_oids.end());
                    cache_it = connected_edge_cache
                                   .emplace((idx_t)pid, std::move(connected_set))
                                   .first;
                }
                if (cache_it->second.find(edge_part_oid) !=
                    cache_it->second.end()) {
                    return true;
                }
            }
            return false;
        };

        // Helper: check if edge partition matches src/dst node labels.
        // For virtual partitions (sub_partition_oids non-empty), check sub-partitions.
        auto partition_matches = [&](PartitionCatalogEntry *epart) -> bool {
            bool src_ok = true, dst_ok = true;
            if (epart->sub_partition_oids.empty()) {
                // Real partition: check directly
                idx_t ep_src = epart->GetSrcPartOid();
                idx_t ep_dst = epart->GetDstPartOid();
                if (has_src_constraint) {
                    bool ms = false, md = false;
                    for (auto sp : src_pids) {
                        if ((idx_t)sp == ep_src) ms = true;
                        if ((idx_t)sp == ep_dst) md = true;
                    }
                    src_ok = ms || md ||
                             has_connected_edge(src_pids, epart->GetOid());
                }
                if (has_dst_constraint) {
                    bool ms = false, md = false;
                    for (auto dp : dst_pids) {
                        if ((idx_t)dp == ep_src) ms = true;
                        if ((idx_t)dp == ep_dst) md = true;
                    }
                    dst_ok = ms || md ||
                             has_connected_edge(dst_pids, epart->GetOid());
                }
            } else {
                // Virtual partition: check if ANY sub-partition matches.
                // Use conjunctive orientation check: a sub-partition matches if
                // there exists an orientation (normal or reversed) where BOTH
                // src/dst constraints are satisfied simultaneously.
                bool any_match = false;
                for (auto sub_oid : epart->sub_partition_oids) {
                    auto *sub = static_cast<PartitionCatalogEntry *>(
                        catalog.GetEntry(*context_, DEFAULT_SCHEMA, (idx_t)sub_oid, true));
                    if (!sub) continue;
                    idx_t sub_src = sub->GetSrcPartOid();
                    idx_t sub_dst = sub->GetDstPartOid();
                    // Normal orientation: pattern_src→edge_src, pattern_dst→edge_dst
                    // Reversed orientation: pattern_src→edge_dst, pattern_dst→edge_src
                    bool match_normal = true, match_reversed = true;
                    if (has_src_constraint) {
                        bool src_in_src = false, dst_in_src = false;
                        for (auto sp : src_pids) {
                            if ((idx_t)sp == sub_src) src_in_src = true;
                            if ((idx_t)sp == sub_dst) dst_in_src = true;
                        }
                        if (!src_in_src) match_normal = false;
                        if (!dst_in_src) match_reversed = false;
                        if (!src_in_src && !dst_in_src &&
                            has_connected_edge(src_pids, sub->GetOid())) {
                            match_normal = true;
                            match_reversed = true;
                        }
                    }
                    if (has_dst_constraint) {
                        bool dst_in_dst = false, src_in_dst = false;
                        for (auto dp : dst_pids) {
                            if ((idx_t)dp == sub_dst) dst_in_dst = true;
                            if ((idx_t)dp == sub_src) src_in_dst = true;
                        }
                        if (!dst_in_dst) match_normal = false;
                        if (!src_in_dst) match_reversed = false;
                        if (!dst_in_dst && !src_in_dst &&
                            has_connected_edge(dst_pids, sub->GetOid())) {
                            match_normal = true;
                            match_reversed = true;
                        }
                    }
                    if (match_normal || match_reversed) { any_match = true; break; }
                }
                src_ok = dst_ok = any_match;
            }
            return src_ok && dst_ok;
        };

        if (has_src_constraint || has_dst_constraint) {
            vector<uint64_t> filtered;
            for (auto ep_oid : partition_ids) {
                auto* epart = static_cast<PartitionCatalogEntry*>(
                    catalog.GetEntry(*context_, DEFAULT_SCHEMA, (idx_t)ep_oid, true));
                if (!epart) continue;
                if (partition_matches(epart)) {
                    filtered.push_back(ep_oid);
                }
            }

            // M30: Always exclude virtual unified partitions from partition_ids.
            // Virtual partitions are catalog metadata (for label resolution);
            // query planning uses real sub-partitions. The converter's
            // multi_edge_partitions_ mechanism handles sibling expansion.
            // Use strict conjunctive orientation check on each sub-partition
            // to decide which real ones to keep.
            {
                // Helper: strict sub-partition match using conjunctive orientation
                auto sub_strictly_matches = [&](PartitionCatalogEntry *sub) -> bool {
                    idx_t sub_src = sub->GetSrcPartOid();
                    idx_t sub_dst = sub->GetDstPartOid();
                    bool match_normal = true, match_reversed = true;
                    if (has_src_constraint) {
                        bool src_in_src = false, dst_in_src = false;
                        for (auto sp : src_pids) {
                            if ((idx_t)sp == sub_src) src_in_src = true;
                            if ((idx_t)sp == sub_dst) dst_in_src = true;
                        }
                        if (!src_in_src) match_normal = false;
                        if (!dst_in_src) match_reversed = false;
                        if (!src_in_src && !dst_in_src &&
                            has_connected_edge(src_pids, sub->GetOid())) {
                            match_normal = true;
                            match_reversed = true;
                        }
                    }
                    if (has_dst_constraint) {
                        bool dst_in_dst = false, src_in_dst = false;
                        for (auto dp : dst_pids) {
                            if ((idx_t)dp == sub_dst) dst_in_dst = true;
                            if ((idx_t)dp == sub_src) src_in_dst = true;
                        }
                        if (!dst_in_dst) match_normal = false;
                        if (!src_in_dst) match_reversed = false;
                        if (!dst_in_dst && !src_in_dst &&
                            has_connected_edge(dst_pids, sub->GetOid())) {
                            match_normal = true;
                            match_reversed = true;
                        }
                    }
                    return match_normal || match_reversed;
                };

                std::set<uint64_t> excluded;
                for (auto ep_oid : filtered) {
                    auto *epart = static_cast<PartitionCatalogEntry *>(
                        catalog.GetEntry(*context_, DEFAULT_SCHEMA, (idx_t)ep_oid, true));
                    if (!epart || epart->sub_partition_oids.empty()) continue;

                    // For variable-length paths, always exclude virtual partitions.
                    // VarLen/DFS execution uses its own per-adj-col iterators and
                    // doesn't support M30 dispatch.
                    if (is_varlen) {
                        excluded.insert(ep_oid);
                        continue;
                    }

                    bool all_subs_match = true;
                    for (auto sub_oid : epart->sub_partition_oids) {
                        auto *sub = static_cast<PartitionCatalogEntry *>(
                            catalog.GetEntry(*context_, DEFAULT_SCHEMA, (idx_t)sub_oid, true));
                        if (!sub || !sub_strictly_matches(sub)) {
                            all_subs_match = false;
                            break;
                        }
                    }
                    if (all_subs_match) {
                        // All sub-partitions match → exclude virtual, keep real subs.
                        // Virtual partitions are catalog metadata only — the converter's
                        // multi_edge_partitions_ mechanism handles multiple sub-partitions
                        // via AdjIdxJoin sibling expansion (use_single_edge path).
                        excluded.insert(ep_oid);
                    } else {
                        // Not all sub-partitions match → exclude virtual AND non-matching subs
                        excluded.insert(ep_oid);
                        for (auto sub_oid : epart->sub_partition_oids) {
                            auto *sub = static_cast<PartitionCatalogEntry *>(
                                catalog.GetEntry(*context_, DEFAULT_SCHEMA, (idx_t)sub_oid, true));
                            if (!sub || !sub_strictly_matches(sub)) {
                                excluded.insert((uint64_t)sub_oid);
                            }
                        }
                    }
                }
                if (!excluded.empty()) {
                    vector<uint64_t> pruned;
                    for (auto oid : filtered) {
                        if (excluded.count(oid) == 0) pruned.push_back(oid);
                    }
                    filtered = std::move(pruned);
                }
            }

            if (!filtered.empty()) {
                // Rebuild graphlet_ids to match filtered partitions.
                // After virtual exclusion, all remaining partitions are real
                // (have their own PropertySchema with physical data).
                vector<uint64_t> filtered_graphlets;
                for (auto ep_oid : filtered) {
                    auto* epart = static_cast<PartitionCatalogEntry*>(
                        catalog.GetEntry(*context_, DEFAULT_SCHEMA, (idx_t)ep_oid, true));
                    if (!epart) continue;
                    PropertySchemaID_vector* ps_ids = epart->GetPropertySchemaIDs();
                    if (!ps_ids) continue;
                    for (auto ps_id : *ps_ids) {
                        filtered_graphlets.push_back((uint64_t)ps_id);
                    }
                }
                partition_ids = std::move(filtered);
                graphlet_ids = std::move(filtered_graphlets);
            }
        }
    }

    // Parse range bounds
    uint64_t lower = 1, upper = 1;
    if (!rel.GetLowerBound().empty()) {
        try { lower = (uint64_t)std::stoul(rel.GetLowerBound()); } catch (...) { lower = 1; }
    }
    if (!rel.GetUpperBound().empty()) {
        if (rel.GetUpperBound() == "inf") {
            upper = UINT64_MAX;
        } else {
            try { upper = (uint64_t)std::stoul(rel.GetUpperBound()); } catch (...) { upper = lower; }
        }
    }
    if (rel.GetPatternType() == RelPatternType::VARIABLE_LENGTH) {
        if (lower == 1 && upper == 1) upper = UINT64_MAX; // no bound specified
    }

    auto bound_rel = make_shared<BoundRelExpression>(
        var_name, rel.GetTypes(), rel.GetDirection(),
        partition_ids, graphlet_ids,
        src.GetUniqueName(), dst.GetUniqueName(),
        lower, upper);

    // Populate property expressions from catalog
    if (!partition_ids.empty()) {
        auto* gcat = GetGraphCatalog();
        auto& catalog = context_->db->GetCatalog();
        PopulateRelProperties(*bound_rel, *context_, *gcat, catalog);
    }

    ctx.AddRel(var_name, bound_rel);
    return bound_rel;
}

// ---- ProjectionBody ----

unique_ptr<BoundProjectionBody> Binder::BindProjectionBody(const ProjectionBody& proj, BindContext& ctx) {
    bound_expression_vector projections;
    bool contains_star = proj.ContainsStar();

    auto& catalog = context_->db->GetCatalog();
    if (contains_star) {
        // RETURN * — emit all visible variables
        for (auto& name : ctx.GetAllNodeNames()) {
            auto node = ctx.GetNode(name);
            if (!NodeHasTemporalOrFakeSchema(*node, *context_, catalog)) {
                node->MarkAllPropertiesUsed();
            }
            auto expr = make_shared<BoundVariableExpression>(
                name, LogicalType::BIGINT, name); // type placeholder
            expr->SetAlias(name);
            projections.push_back(std::move(expr));
        }
        for (auto& name : ctx.GetAllRelNames()) {
            auto expr = make_shared<BoundVariableExpression>(
                name, LogicalType::BIGINT, name);
            expr->SetAlias(name);
            projections.push_back(std::move(expr));
        }
    } else {
        for (auto& item_expr : proj.GetProjections()) {
            auto bound = BindExpression(*item_expr, ctx);
            string alias;
            if (!item_expr->alias.empty()) {
                alias = item_expr->alias;
            } else {
                alias = item_expr->ToString();
            }
            bound->SetAlias(alias);

            // Bare node/edge reference in projection (e.g. `RETURN n`) needs
            // all properties — NodeScan binds only filter-referenced columns
            // by default, which truncates the resulting chunk.
            // Skip if any partition has a temporal/fake property schema:
            // mixed-schema scans with whole_node_required hit type mismatches
            // (see test "REMOVE label restores original label set").
            if (bound->GetExprType() == BoundExpressionType::VARIABLE) {
                auto &var = static_cast<const BoundVariableExpression &>(*bound);
                if (ctx.HasNode(var.GetVarName())) {
                    auto n = ctx.GetNode(var.GetVarName());
                    if (!NodeHasTemporalOrFakeSchema(*n, *context_, catalog)) {
                        n->MarkAllPropertiesUsed();
                    }
                }
            }

            // Track alias types for complex types (STRUCT, LIST, etc.)
            // so downstream struct_extract/list_extract can resolve field types.
            if (bound->GetExprType() == BoundExpressionType::FUNCTION) {
                auto &fn = static_cast<const CypherBoundFunctionExpression &>(*bound);
                if (fn.GetFuncName() == "struct_pack") {
                    child_list_t<LogicalType> fields;
                    for (idx_t ci = 0; ci < fn.GetNumChildren(); ci++) {
                        auto *child = fn.GetChild(ci);
                        string field_name = child->HasAlias() ? child->GetAlias()
                                          : "v" + to_string(ci + 1);
                        fields.push_back({field_name, child->GetDataType()});
                    }
                    ctx.AddAliasType(alias, LogicalType::STRUCT(std::move(fields)));
                }
            }
            // Track alias types for all projections with known types.
            // Needed for downstream function bind resolution (collect → LIST,
            // struct_pack → STRUCT, property → concrete type, etc.)
            auto bound_type = bound->GetDataType();
            // Always register alias — even for ANY-typed expressions (e.g., sum(expr)).
            // Downstream query parts need to resolve these aliases by name.
            ctx.AddAliasType(alias, bound_type);
            if (bound_type.id() == LogicalTypeId::PATH) {
                ctx.AddPath(alias);
            }
            if (bound->GetExprType() == BoundExpressionType::FUNCTION) {
                auto &fn = static_cast<const CypherBoundFunctionExpression &>(*bound);
                if (fn.GetFuncName() == "path_rels" && fn.GetNumChildren() == 1 &&
                    fn.GetChild(0)->GetExprType() == BoundExpressionType::VARIABLE) {
                    auto &path_var =
                        static_cast<const BoundVariableExpression &>(*fn.GetChild(0));
                    if (fn.GetChild(0)->GetDataType().id() == LogicalTypeId::PATH ||
                        ctx.HasPath(path_var.GetVarName())) {
                        ctx.AddPathRelsAlias(alias, path_var.GetVarName());
                    }
                }
            }
            projections.push_back(std::move(bound));
        }
    }

    auto body = make_unique<BoundProjectionBody>(proj.IsDistinct(), std::move(projections));

    // ORDER BY
    if (proj.HasOrderBy()) {
        vector<BoundOrderByItem> order_items;
        for (auto& ob : proj.GetOrderBy()) {
            BoundOrderByItem item;
            item.expr      = BindExpression(*ob.expr, ctx);
            item.ascending = ob.ascending;
            order_items.push_back(std::move(item));
        }
        body->SetOrderBy(std::move(order_items));
    }

    // SKIP / LIMIT (literal integers only for now)
    if (proj.HasSkip()) {
        auto* skip_expr = proj.GetSkip();
        if (skip_expr && skip_expr->GetExpressionClass() == ExpressionClass::CONSTANT) {
            auto& cv = static_cast<const ConstantExpression&>(*skip_expr);
            if (cv.value.type().id() == LogicalTypeId::INTEGER ||
                cv.value.type().id() == LogicalTypeId::BIGINT) {
                body->SetSkipNumber((uint64_t)cv.value.GetValue<int64_t>());
            }
        }
    }
    if (proj.HasLimit()) {
        auto* lim_expr = proj.GetLimit();
        if (lim_expr && lim_expr->GetExpressionClass() == ExpressionClass::CONSTANT) {
            auto& cv = static_cast<const ConstantExpression&>(*lim_expr);
            if (cv.value.type().id() == LogicalTypeId::INTEGER ||
                cv.value.type().id() == LogicalTypeId::BIGINT) {
                body->SetLimitNumber((uint64_t)cv.value.GetValue<int64_t>());
            }
        }
    }

    return body;
}

// ---- Expression binding ----

string Binder::GenExprName(const ParsedExpression& expr) {
    return "_expr_" + to_string(expr_counter_++);
}

shared_ptr<BoundExpression> Binder::BindExpression(const ParsedExpression& expr, BindContext& ctx) {
    auto ec = expr.GetExpressionClass();

    if (ec == ExpressionClass::CONSTANT) {
        auto& ce = static_cast<const ConstantExpression&>(expr);
        string uname = GenExprName(expr);
        return make_shared<BoundLiteralExpression>(ce.value, std::move(uname));
    }

    if (ec == ExpressionClass::COLUMN_REF) {
        // Could be ParsedPropertyExpression or ParsedVariableExpression
        auto* prop = dynamic_cast<const ParsedPropertyExpression*>(&expr);
        if (prop) return BindPropertyExpression(*prop, ctx);
        auto* var = dynamic_cast<const ParsedVariableExpression*>(&expr);
        if (var) return BindVariableExpression(*var, ctx);
        // Fall through to unknown
    }

    if (ec == ExpressionClass::FUNCTION) {
        auto& fe = static_cast<const FunctionExpression&>(expr);
        return BindFunctionInvocation(fe, ctx);
    }

    if (ec == ExpressionClass::COMPARISON) {
        auto& ce = static_cast<const ComparisonExpression&>(expr);
        auto left  = BindExpression(*ce.left,  ctx);
        auto right = BindExpression(*ce.right, ctx);
        return make_shared<CypherBoundComparisonExpression>(ce.type, std::move(left), std::move(right),
                                                       GenExprName(expr));
    }

    if (ec == ExpressionClass::CONJUNCTION) {
        auto& cj = static_cast<const ConjunctionExpression&>(expr);
        bound_expression_vector children;
        for (auto& child : cj.children) children.push_back(BindExpression(*child, ctx));
        BoundBoolOpType op = (cj.type == ExpressionType::CONJUNCTION_AND)
                             ? BoundBoolOpType::AND : BoundBoolOpType::OR;
        return make_shared<BoundBoolExpression>(op, std::move(children), GenExprName(expr));
    }

    if (ec == ExpressionClass::OPERATOR) {
        auto& oe = static_cast<const OperatorExpression&>(expr);
        if (oe.type == ExpressionType::OPERATOR_NOT) {
            D_ASSERT(!oe.children.empty());
            bound_expression_vector children;
            children.push_back(BindExpression(*oe.children[0], ctx));
            return make_shared<BoundBoolExpression>(BoundBoolOpType::NOT, std::move(children), GenExprName(expr));
        }
        if (oe.type == ExpressionType::OPERATOR_IS_NULL || oe.type == ExpressionType::OPERATOR_IS_NOT_NULL) {
            D_ASSERT(!oe.children.empty());
            auto child = BindExpression(*oe.children[0], ctx);
            bool is_not_null = (oe.type == ExpressionType::OPERATOR_IS_NOT_NULL);
            return make_shared<BoundNullExpression>(is_not_null, std::move(child), GenExprName(expr));
        }
        if (oe.type == ExpressionType::OPERATOR_COALESCE) {
            // coalesce(a, b, ...) → CASE WHEN a IS NOT NULL THEN a
            //                            WHEN b IS NOT NULL THEN b ... ELSE NULL END
            vector<CypherBoundCaseCheck> checks;
            for (auto& c : oe.children) {
                CypherBoundCaseCheck bc;
                auto bound_child = BindExpression(*c, ctx);
                bc.when_expr = make_shared<BoundNullExpression>(
                    /*is_not_null=*/true, bound_child->Copy(), GenExprName(*c));
                bc.then_expr = std::move(bound_child);
                checks.push_back(std::move(bc));
            }
            return make_shared<CypherBoundCaseExpression>(
                LogicalType::ANY, std::move(checks), nullptr, GenExprName(expr));
        }
        // x IN listVar → list_contains(listVar, x)
        // x NOT IN listVar → NOT list_contains(listVar, x)
        if (oe.type == ExpressionType::COMPARE_IN || oe.type == ExpressionType::COMPARE_NOT_IN) {
            if (oe.children.size() == 2) {
                // Two children: value IN list_variable
                bound_expression_vector lc_children;
                lc_children.push_back(BindExpression(*oe.children[1], ctx)); // list
                lc_children.push_back(BindExpression(*oe.children[0], ctx)); // element
                auto contains = make_shared<CypherBoundFunctionExpression>(
                    "list_contains", LogicalType::BOOLEAN, std::move(lc_children), GenExprName(expr));
                if (oe.type == ExpressionType::COMPARE_NOT_IN) {
                    bound_expression_vector not_children;
                    not_children.push_back(std::move(contains));
                    return make_shared<BoundBoolExpression>(BoundBoolOpType::NOT,
                        std::move(not_children), GenExprName(expr));
                }
                return contains;
            }
            // 3+ children: x IN [a, b, c] — fall through to generic handler
        }
        // Other operator types — treat as function by operator string
        string op_name = ExpressionTypeToOperator(oe.type);
        if (op_name.empty()) op_name = ExpressionTypeToString(oe.type);
        bound_expression_vector children;
        for (auto& c : oe.children) children.push_back(BindExpression(*c, ctx));
        return make_shared<CypherBoundFunctionExpression>(op_name, LogicalType::ANY, std::move(children),
                                                     GenExprName(expr));
    }

    if (ec == ExpressionClass::CASE) {
        auto& ce = static_cast<const CaseExpression&>(expr);
        return BindCaseExpression(ce, ctx);
    }

    if (ec == ExpressionClass::SUBQUERY) {
        auto* exists_expr = dynamic_cast<const ExistsSubqueryExpression*>(&expr);
        if (exists_expr) {
            return BindExistsSubquery(*exists_expr, ctx);
        }
    }

    // Fallback: unknown expression type — produce a placeholder literal NULL
    return make_shared<BoundLiteralExpression>(Value(), GenExprName(expr));
}

shared_ptr<BoundExpression> Binder::BindPropertyExpression(const ParsedPropertyExpression& expr, BindContext& ctx) {
    const string& var = expr.GetVariableName();
    const string& prop = expr.GetPropertyName();

    if (ctx.HasNode(var)) {
        auto node = ctx.GetNode(var);
        return LookupPropertyOnNode(*node, prop);
    }
    if (ctx.HasRel(var)) {
        auto rel = ctx.GetRel(var);
        return LookupPropertyOnRel(*rel, prop);
    }
    // Handle chained property: a.b.c → struct_extract(struct_extract(a,'b'),'c')
    if (var.find('.') != string::npos) {
        auto dot_pos = var.find('.');
        string base = var.substr(0, dot_pos);
        string mid = var.substr(dot_pos + 1);
        ParsedPropertyExpression inner(base, mid);
        auto inner_bound = BindPropertyExpression(inner, ctx);
        // If inner is a temporal type, use date_part instead of struct_extract
        auto inner_type = inner_bound->GetDataType();
        if (inner_type.id() == LogicalTypeId::DATE ||
            inner_type.id() == LogicalTypeId::TIMESTAMP ||
            inner_type.id() == LogicalTypeId::TIMESTAMP_MS ||
            inner_type.id() == LogicalTypeId::TIMESTAMP_NS) {
            string prop_lower = StringUtil::Lower(prop);
            static const unordered_map<string, string> temporal_props = {
                {"year", "year"}, {"month", "month"}, {"day", "day"},
                {"hour", "hour"}, {"minute", "minute"}, {"second", "second"},
                {"quarter", "quarter"}, {"week", "week"},
            };
            auto it = temporal_props.find(prop_lower);
            if (it != temporal_props.end()) {
                auto part_name = make_shared<BoundLiteralExpression>(Value(it->second), "_date_part");
                bound_expression_vector dp_args;
                dp_args.push_back(std::move(part_name));
                dp_args.push_back(std::move(inner_bound));
                return make_shared<CypherBoundFunctionExpression>(
                    "date_part", LogicalType::BIGINT, std::move(dp_args), var + "." + prop);
            }
        }
        auto field_name = make_shared<BoundLiteralExpression>(Value(prop), "_field_" + prop);
        bound_expression_vector args;
        args.push_back(std::move(inner_bound));
        args.push_back(std::move(field_name));
        return make_shared<CypherBoundFunctionExpression>(
            "struct_extract", LogicalType::ANY, std::move(args), var + "." + prop);
    }
    // Not a node or edge — check alias type for specialized handling.
    LogicalType var_type = ctx.HasAliasType(var) ? ctx.GetAliasType(var) : LogicalType::ANY;

    // Temporal property access: birthday.month → date_part('month', birthday)
    if (var_type.id() == LogicalTypeId::TIMESTAMP ||
        var_type.id() == LogicalTypeId::TIMESTAMP_MS ||
        var_type.id() == LogicalTypeId::TIMESTAMP_SEC ||
        var_type.id() == LogicalTypeId::TIMESTAMP_NS ||
        var_type.id() == LogicalTypeId::DATE) {
        string prop_lower = StringUtil::Lower(prop);
        // Map Cypher temporal property names to date_part specifiers
        static const unordered_map<string, string> temporal_props = {
            {"year", "year"}, {"month", "month"}, {"day", "day"},
            {"hour", "hour"}, {"minute", "minute"}, {"second", "second"},
            {"quarter", "quarter"}, {"week", "week"},
            {"dayofweek", "dow"}, {"dayofyear", "doy"},
        };
        auto it = temporal_props.find(prop_lower);
        if (it != temporal_props.end()) {
            auto var_expr = make_shared<BoundVariableExpression>(var, var_type, var);
            auto part_name = make_shared<BoundLiteralExpression>(Value(it->second), "_date_part");
            bound_expression_vector args;
            args.push_back(std::move(part_name));
            args.push_back(std::move(var_expr));
            return make_shared<CypherBoundFunctionExpression>(
                "date_part", LogicalType::BIGINT, std::move(args), var + "." + prop);
        }
    }

    // If variable is completely unknown (not node, edge, alias, or temporal),
    // throw an error to prevent converter from dereferencing NULL colref.
    // Exception: $-prefixed names are query parameters — pass through as literals.
    if (var_type.id() == LogicalTypeId::ANY && !ctx.HasNode(var) && !ctx.HasRel(var) &&
        !ctx.HasAliasType(var) && !ctx.HasPath(var)) {
        if (var.size() > 1 && var[0] == '$') {
            // Parameter placeholder — return as literal NULL (will be substituted at execute time)
            return make_shared<BoundLiteralExpression>(Value(), var);
        }
        throw BinderException("Variable '" + var + "' is not defined");
    }

    // VID property access: when variable is UBIGINT/BIGINT/ANY (node VID from list comp or path),
    // and property is "id", return the VID itself (internal ID = identity).
    // For other properties, would need graph lookup (not yet supported).
    if (var_type.id() == LogicalTypeId::UBIGINT || var_type.id() == LogicalTypeId::BIGINT ||
        var_type.id() == LogicalTypeId::ANY) {
        if (prop == "id") {
            return make_shared<BoundVariableExpression>(var, var_type, var);
        }
        // For other properties on VID, pass through (will be resolved at converter level)
        return make_shared<CypherBoundFunctionExpression>(
            "node_property", LogicalType::ANY,
            bound_expression_vector{
                make_shared<BoundVariableExpression>(var, var_type, var),
                make_shared<BoundLiteralExpression>(Value(prop), "_prop")
            }, var + "." + prop);
    }

    // Map/struct field access: var.prop → struct_extract(var, 'prop')
    auto var_expr = make_shared<BoundVariableExpression>(var, var_type, var);
    auto field_name = make_shared<BoundLiteralExpression>(Value(prop), "_field_" + prop);
    bound_expression_vector args;
    args.push_back(std::move(var_expr));
    args.push_back(std::move(field_name));
    // Resolve field type from STRUCT if possible
    LogicalType ret_type = LogicalType::ANY;
    if (var_type.id() == LogicalTypeId::STRUCT) {
        auto &fields = StructType::GetChildTypes(var_type);
        for (auto &f : fields) {
            if (f.first == prop) { ret_type = f.second; break; }
        }
    }
    string uname = var + "." + prop;
    return make_shared<CypherBoundFunctionExpression>(
        "struct_extract", ret_type, std::move(args), std::move(uname));
}

shared_ptr<BoundExpression> Binder::LookupPropertyOnNode(BoundNodeExpression& node,
                                                          const string& prop_name) {
    auto* gcat = GetGraphCatalog();
    PropertyKeyID kid = gcat->GetPropertyKeyID(*context_, prop_name);
    if (kid == (PropertyKeyID)-1) {
        throw std::runtime_error("Unknown property '" + prop_name + "' on node " + node.GetUniqueName());
    }
    if (node.HasProperty((uint64_t)kid)) {
        return node.GetPropertyExpression((uint64_t)kid);
    }
    // Property exists in graph but not on this node's graphlets — return typed NULL literal
    string uname = node.GetUniqueName() + "." + prop_name;
    LogicalTypeId type_id = gcat->GetTypeIdFromPropertyKeyID(kid);
    return make_shared<BoundLiteralExpression>(Value(LogicalType(type_id)), uname);
}

shared_ptr<BoundExpression> Binder::LookupPropertyOnRel(BoundRelExpression& rel,
                                                         const string& prop_name) {
    auto* gcat = GetGraphCatalog();
    PropertyKeyID kid = gcat->GetPropertyKeyID(*context_, prop_name);
    if (kid == (PropertyKeyID)-1) {
        throw std::runtime_error("Unknown property '" + prop_name + "' on edge " + rel.GetUniqueName());
    }
    if (rel.HasProperty((uint64_t)kid)) {
        return rel.GetPropertyExpression((uint64_t)kid);
    }
    // Property exists in graph but not on this rel's partitions — return typed NULL literal
    string uname = rel.GetUniqueName() + "." + prop_name;
    LogicalTypeId type_id = gcat->GetTypeIdFromPropertyKeyID(kid);
    return make_shared<BoundLiteralExpression>(Value(LogicalType(type_id)), uname);
}

shared_ptr<BoundExpression> Binder::BindVariableExpression(const ParsedVariableExpression& expr, BindContext& ctx) {
    const string& var = expr.GetVariableName();
    // $-prefixed names are query parameters — return as literal placeholder
    if (var.size() > 1 && var[0] == '$') {
        return make_shared<BoundLiteralExpression>(Value(), var);
    }
    if (ctx.HasNode(var)) {
        auto node = ctx.GetNode(var);
        return make_shared<BoundVariableExpression>(var, LogicalType::BIGINT, var);
    }
    if (ctx.HasRel(var)) {
        return make_shared<BoundVariableExpression>(var, LogicalType::BIGINT, var);
    }
    if (ctx.HasPath(var)) {
        return make_shared<BoundVariableExpression>(var, LogicalType::PATH(LogicalType::ANY), var);
    }
    // Check alias type registry (for WITH aliases like collect() → LIST, struct_pack → STRUCT)
    if (ctx.HasAliasType(var)) {
        return make_shared<BoundVariableExpression>(var, ctx.GetAliasType(var), var);
    }
    // Unknown — pass through as placeholder (may be resolved later by converter)
    return make_shared<BoundVariableExpression>(var, LogicalType::ANY, var);
}

shared_ptr<BoundExpression> Binder::BindFunctionInvocation(const FunctionExpression& expr, BindContext& ctx) {
    string fname = StringUtil::Lower(expr.function_name);

    // negate(x) → constant fold to -x if child is a literal
    if (fname == "negate" && expr.children.size() == 1) {
        auto child = BindExpression(*expr.children[0], ctx);
        if (child->GetExprType() == BoundExpressionType::LITERAL) {
            auto &lit = static_cast<const BoundLiteralExpression &>(*child);
            auto val = lit.GetValue();
            if (!val.IsNull()) {
                try {
                    auto neg_val = Value::BIGINT(-val.GetValue<int64_t>());
                    return make_shared<BoundLiteralExpression>(neg_val, GenExprName(expr));
                } catch (...) {
                    try {
                        auto neg_val = Value::DOUBLE(-val.GetValue<double>());
                        return make_shared<BoundLiteralExpression>(neg_val, GenExprName(expr));
                    } catch (...) {}
                }
            }
        }
        // Non-literal: pass through as function
        bound_expression_vector children;
        children.push_back(std::move(child));
        return make_shared<CypherBoundFunctionExpression>(
            "negate", LogicalType::BIGINT, std::move(children), GenExprName(expr));
    }

    // toInteger/toFloat are already registered as DuckDB scalar functions
    // with lowercase names. The binder lowercases fname above, so they
    // just fall through to the general function binding path.

    // __pattern_comprehension(...) → pass through as placeholder
    // Children are pattern metadata (constants), not bindable expressions.
    // Full binding + decorrelation happens in M5 converter integration.
    if (fname == "__pattern_comprehension") {
        bound_expression_vector children;
        for (auto &c : expr.children) {
            // Bind constants and simple expressions, skip complex pattern refs
            try {
                children.push_back(BindExpression(*c, ctx));
            } catch (...) {
                // Pattern-internal variable not in scope — pass as literal placeholder
                children.push_back(make_shared<BoundLiteralExpression>(
                    Value(), "_pc_placeholder"));
            }
        }
        return make_shared<CypherBoundFunctionExpression>(
            "__pattern_comprehension", LogicalType::LIST(LogicalType::DOUBLE),
            std::move(children), GenExprName(expr));
    }

    // __reduce(init, 'acc', list, 'var', body)
    // Only rewrite the proven summation shape reduce(acc=init, var IN list | acc + var).
    // Unsupported forms should fail clearly rather than silently returning list_sum(list).
    if (fname == "__reduce" && expr.children.size() == 5) {
        string acc_name, loop_var_name;
        if (!ExtractReduceVarName(*expr.children[1], acc_name) ||
            !ExtractReduceVarName(*expr.children[3], loop_var_name) ||
            !IsSupportedReduceSumBody(*expr.children[4], acc_name, loop_var_name)) {
            throw BinderException(
                "Unsupported reduce() form: only numeric acc + var summation is currently supported");
        }

        auto init = BindExpression(*expr.children[0], ctx);
        auto list = BindExpression(*expr.children[2], ctx);
        if (!ReduceSumTypeAllowed(init->GetDataType()) ||
            !ReduceSumInputTypeAllowed(list->GetDataType())) {
            throw BinderException(
                "Unsupported reduce() form: summation rewrite requires numeric init and input values");
        }

        bound_expression_vector sum_args;
        sum_args.push_back(std::move(list));
        auto sum_expr = make_shared<CypherBoundFunctionExpression>(
            "list_sum", LogicalType::DOUBLE, std::move(sum_args), GenExprName(expr));

        // Preserve the existing IC14 path_weight rewrite shape when init is a literal zero.
        if (IsLiteralNumericZero(init)) {
            return sum_expr;
        }

        bound_expression_vector plus_args;
        plus_args.push_back(std::move(init));
        plus_args.push_back(std::move(sum_expr));
        return make_shared<CypherBoundFunctionExpression>(
            "+", LogicalType::DOUBLE, std::move(plus_args), GenExprName(expr));
    }

    // ---- id(n) → access _id property (key_id=0) ----
    // Neo4j id() returns the internal node/relationship ID.
    if (fname == "id" && expr.children.size() == 1) {
        if (expr.children[0]->GetExpressionType() == ExpressionType::COLUMN_REF) {
            auto *var = dynamic_cast<const ParsedVariableExpression *>(expr.children[0].get());
            if (var) {
                string vname = var->GetVariableName();
                if (ctx.HasNode(vname) || ctx.HasRel(vname)) {
                    return make_shared<BoundPropertyExpression>(
                        vname, (uint64_t)0, LogicalType::ID,
                        vname + "._id");
                }
            }
        }
        throw BinderException("id() requires a node or relationship variable");
    }

    // ---- toString(x) → DuckDB "tostring" scalar function ----
    if (fname == "tostring" && expr.children.size() == 1) {
        auto child = BindExpression(*expr.children[0], ctx);
        bound_expression_vector args;
        args.push_back(std::move(child));
        return make_shared<CypherBoundFunctionExpression>(
            "tostring", LogicalType::VARCHAR, std::move(args), GenExprName(expr));
    }

    // ---- toUpper/toLower → DuckDB upper/lower ----
    if (fname == "toupper" || fname == "tolower") {
        auto child = BindExpression(*expr.children[0], ctx);
        bound_expression_vector args;
        args.push_back(std::move(child));
        string duckdb_name = (fname == "toupper") ? "upper" : "lower";
        return make_shared<CypherBoundFunctionExpression>(
            duckdb_name, LogicalType::VARCHAR, std::move(args), GenExprName(expr));
    }

    // ---- date() / datetime() — cast string literal to DATE/TIMESTAMP ----
    // Skip when child is struct_pack (i.e. datetime({epochMillis: ...})) — handled later.
    if ((fname == "date" || fname == "localdatetime" || fname == "datetime") && expr.children.size() == 1) {
        auto &raw_child = *expr.children[0];
        bool is_struct_pack = false;
        if (raw_child.GetExpressionType() == ExpressionType::FUNCTION) {
            auto &cf = static_cast<const FunctionExpression &>(raw_child);
            if (StringUtil::Lower(cf.function_name) == "struct_pack") {
                is_struct_pack = true;
            }
        }
        if (!is_struct_pack) {
            auto child = BindExpression(*expr.children[0], ctx);
            if (child->GetExprType() == BoundExpressionType::LITERAL) {
                auto &lit = static_cast<const BoundLiteralExpression &>(*child);
                auto val = lit.GetValue();
                if (!val.IsNull() && val.type().id() == LogicalTypeId::VARCHAR) {
                    string date_str = val.GetValue<string>();
                    if (fname == "date") {
                        auto date_val = Value::CreateValue(Date::FromString(date_str));
                        return make_shared<BoundLiteralExpression>(date_val, GenExprName(expr));
                    } else {
                        auto ts_val = Value::CreateValue(Timestamp::FromString(date_str));
                        return make_shared<BoundLiteralExpression>(ts_val, GenExprName(expr));
                    }
                }
            }
            // Non-literal: if child is already the target type, just return it
            LogicalType target = (fname == "date") ? LogicalType::DATE : LogicalType::TIMESTAMP;
            if (child->GetDataType().id() == target.id()) {
                return child;
            }
            // Otherwise treat as a cast via IsCastingFunction path
            bound_expression_vector args;
            args.push_back(std::move(child));
            return make_shared<CypherBoundFunctionExpression>(
                fname == "date" ? "TO_DATE" : "TO_TIMESTAMP", target, std::move(args), GenExprName(expr));
        }
    }

    // ---- String `+` concatenation (Neo4j compatibility) ----
    // Neo4j uses `+` for string concat. When either operand is VARCHAR, rewrite to concat().
    if (fname == "+" && expr.children.size() == 2) {
        auto lhs = BindExpression(*expr.children[0], ctx);
        auto rhs = BindExpression(*expr.children[1], ctx);
        if (lhs->GetDataType().id() == LogicalTypeId::VARCHAR ||
            rhs->GetDataType().id() == LogicalTypeId::VARCHAR) {
            bound_expression_vector args;
            args.push_back(std::move(lhs));
            args.push_back(std::move(rhs));
            return make_shared<CypherBoundFunctionExpression>(
                "||", LogicalType::VARCHAR, std::move(args), GenExprName(expr));
        }
        // Numeric +: fall through to default function handling
    }

    // ---- Cypher meta functions: labels(), type(), keys(), properties() ----

    // labels(n) — resolve from the runtime logical id so relabelled nodes
    // report their current label set.
    if (fname == "labels" && expr.children.size() == 1) {
        if (expr.children[0]->GetExpressionType() == ExpressionType::COLUMN_REF) {
            auto *var = dynamic_cast<const ParsedVariableExpression *>(expr.children[0].get());
            if (var && ctx.HasNode(var->GetVariableName())) {
                bound_expression_vector args;
                args.push_back(make_shared<BoundPropertyExpression>(
                    var->GetVariableName(), (uint64_t)0, LogicalType::ID,
                    var->GetVariableName() + "._id"));
                return make_shared<CypherBoundFunctionExpression>(
                    "__tl_node_labels", LogicalType::VARCHAR,
                    std::move(args), GenExprName(expr));
            }
        }
        throw BinderException("labels() requires a node variable");
    }

    // type(r) → constant string of relationship type (resolved at bind time)
    if (fname == "type" && expr.children.size() == 1) {
        if (expr.children[0]->GetExpressionType() == ExpressionType::COLUMN_REF) {
            auto *var = dynamic_cast<const ParsedVariableExpression *>(expr.children[0].get());
            if (var && ctx.HasRel(var->GetVariableName())) {
                auto rel = ctx.GetRel(var->GetVariableName());
                auto &types = rel->GetTypes();
                string type_str = types.empty() ? "" : types[0];
                return make_shared<BoundLiteralExpression>(Value(type_str), GenExprName(expr));
            }
        }
        throw BinderException("type() requires a relationship variable");
    }

    // keys(n) or keys(r) — resolve from the runtime logical id so
    // schema-evolved rows expose their current property surface.
    if (fname == "keys" && expr.children.size() == 1) {
        if (expr.children[0]->GetExpressionType() == ExpressionType::COLUMN_REF) {
            auto *var = dynamic_cast<const ParsedVariableExpression *>(expr.children[0].get());
            if (var) {
                if (ctx.HasNode(var->GetVariableName())) {
                    bound_expression_vector args;
                    args.push_back(make_shared<BoundPropertyExpression>(
                        var->GetVariableName(), (uint64_t)0, LogicalType::ID,
                        var->GetVariableName() + "._id"));
                    return make_shared<CypherBoundFunctionExpression>(
                        "__tl_node_keys", LogicalType::VARCHAR,
                        std::move(args), GenExprName(expr));
                } else if (ctx.HasRel(var->GetVariableName())) {
                    bound_expression_vector args;
                    args.push_back(make_shared<BoundPropertyExpression>(
                        var->GetVariableName(), (uint64_t)0, LogicalType::ID,
                        var->GetVariableName() + "._id"));
                    return make_shared<CypherBoundFunctionExpression>(
                        "__tl_rel_keys", LogicalType::VARCHAR,
                        std::move(args), GenExprName(expr));
                } else {
                    throw BinderException("keys() requires a node or relationship variable");
                }
            }
        }
        throw BinderException("keys() requires a node or relationship variable");
    }

    // properties(n) or properties(r) → struct_pack(key1: n.key1, key2: n.key2, ...)
    if (fname == "properties" && expr.children.size() == 1) {
        if (expr.children[0]->GetExpressionType() == ExpressionType::COLUMN_REF) {
            auto *var = dynamic_cast<const ParsedVariableExpression *>(expr.children[0].get());
            if (var) {
                bool is_node = ctx.HasNode(var->GetVariableName());
                bool is_rel = ctx.HasRel(var->GetVariableName());
                if (is_node || is_rel) {
                    auto *gcat = GetGraphCatalog();
                    bound_expression_vector prop_children;
                    child_list_t<LogicalType> fields;

                    if (is_node) {
                        auto node = ctx.GetNode(var->GetVariableName());
                        node->MarkAllPropertiesUsed();
                        for (auto &[kid, idx] : node->GetKeyIdToIdx()) {
                            auto pe = node->GetPropertyExpression(kid);
                            string prop_name = gcat->property_key_id_to_name_vec.size() > kid
                                ? gcat->property_key_id_to_name_vec[kid] : "p" + to_string(kid);
                            pe->SetAlias(prop_name);
                            fields.push_back(make_pair(prop_name, pe->GetDataType()));
                            prop_children.push_back(std::move(pe));
                        }
                    } else {
                        auto rel = ctx.GetRel(var->GetVariableName());
                        for (auto &[kid, idx] : rel->GetKeyIdToIdx()) {
                            auto pe = rel->GetPropertyExpression(kid);
                            string prop_name = gcat->property_key_id_to_name_vec.size() > kid
                                ? gcat->property_key_id_to_name_vec[kid] : "p" + to_string(kid);
                            pe->SetAlias(prop_name);
                            fields.push_back(make_pair(prop_name, pe->GetDataType()));
                            prop_children.push_back(std::move(pe));
                        }
                    }
                    return make_shared<CypherBoundFunctionExpression>(
                        "struct_pack", LogicalType::STRUCT(std::move(fields)),
                        std::move(prop_children), GenExprName(expr));
                }
            }
        }
        throw BinderException("properties() requires a node or relationship variable");
    }

    // nodes(path) → path_nodes(path) — extract node IDs from path
    if (fname == "nodes" && expr.children.size() == 1) {
        auto child = BindExpression(*expr.children[0], ctx);
        bound_expression_vector args;
        args.push_back(std::move(child));
        return make_shared<CypherBoundFunctionExpression>(
            "path_nodes", LogicalType::LIST(LogicalType::UBIGINT), std::move(args), GenExprName(expr));
    }

    // relationships(path) → path_rels(path) — extract edge IDs from path
    if ((fname == "relationships" || fname == "rels") && expr.children.size() == 1) {
        auto child = BindExpression(*expr.children[0], ctx);
        bound_expression_vector args;
        args.push_back(std::move(child));
        return make_shared<CypherBoundFunctionExpression>(
            "path_rels", LogicalType::LIST(LogicalType::UBIGINT), std::move(args), GenExprName(expr));
    }

    // startNode(r) → path_start_node(r) — source node of edge
    if (fname == "startnode" && expr.children.size() == 1) {
        auto child = BindExpression(*expr.children[0], ctx);
        bound_expression_vector args;
        args.push_back(std::move(child));
        return make_shared<CypherBoundFunctionExpression>(
            "path_start_node", LogicalType::UBIGINT, std::move(args), GenExprName(expr));
    }

    // endNode(r) → path_end_node(r) — target node of edge
    if (fname == "endnode" && expr.children.size() == 1) {
        auto child = BindExpression(*expr.children[0], ctx);
        bound_expression_vector args;
        args.push_back(std::move(child));
        return make_shared<CypherBoundFunctionExpression>(
            "path_end_node", LogicalType::UBIGINT, std::move(args), GenExprName(expr));
    }

    // length(path) → path_length(path) — path hop count (only for PATH-typed variables)
    // length(string) → DuckDB length() — left as-is
    if (fname == "length" && expr.children.size() == 1) {
        // Check if the child is a path variable
        bool is_path = false;
        if (expr.children[0]->GetExpressionType() == ExpressionType::COLUMN_REF) {
            auto *var = dynamic_cast<const ParsedVariableExpression *>(expr.children[0].get());
            if (var && ctx.HasPath(var->GetVariableName())) {
                is_path = true;
            }
        }
        if (is_path) {
            auto child = BindExpression(*expr.children[0], ctx);
            bound_expression_vector args;
            args.push_back(std::move(child));
            return make_shared<CypherBoundFunctionExpression>(
                "path_length", LogicalType::BIGINT, std::move(args), GenExprName(expr));
        }
        // else: fall through to normal length() function binding
    }

    // list_extract(list, idx) — element access with type inference
    if (fname == "list_extract" && expr.children.size() == 2) {
        auto list_child = BindExpression(*expr.children[0], ctx);
        auto idx_child = BindExpression(*expr.children[1], ctx);

        // Cypher meta-function subscript: `labels(n)[i]` / `keys(n)[i]`.
        // labels()/keys() report VARCHAR (a JSON-formatted string) rather than
        // a real LIST(VARCHAR), so list_extract types element as ANY and the
        // physical layer crashes on PhysicalType::INVALID. Reroute to a
        // dedicated index-aware function that resolves the same partition /
        // live schema and returns the i-th entry as VARCHAR.
        if (list_child->GetExprType() == BoundExpressionType::FUNCTION) {
            auto &fn_child =
                static_cast<const CypherBoundFunctionExpression &>(*list_child);
            const string &cn = fn_child.GetFuncName();
            const string *target_fn = nullptr;
            if (cn == "__tl_node_labels") {
                static const string n = "__tl_node_label_at";
                target_fn = &n;
            } else if (cn == "__tl_node_keys") {
                static const string n = "__tl_node_key_at";
                target_fn = &n;
            } else if (cn == "__tl_rel_keys") {
                static const string n = "__tl_rel_key_at";
                target_fn = &n;
            }
            if (target_fn != nullptr && fn_child.GetNumChildren() == 1) {
                bound_expression_vector args;
                args.push_back(fn_child.GetChildShared(0));
                args.push_back(std::move(idx_child));
                return make_shared<CypherBoundFunctionExpression>(
                    *target_fn, LogicalType::VARCHAR, std::move(args),
                    GenExprName(expr));
            }
        }

        LogicalType elem_type = LogicalType::ANY;
        if (list_child->GetDataType().id() == LogicalTypeId::LIST) {
            elem_type = ListType::GetChildType(list_child->GetDataType());
        }
        // Constant folding: if both list and index are literals, evaluate at bind time
        if (list_child->GetExprType() == BoundExpressionType::LITERAL &&
            idx_child->GetExprType() == BoundExpressionType::LITERAL) {
            auto &list_val = static_cast<const BoundLiteralExpression &>(*list_child).GetValue();
            auto &idx_val = static_cast<const BoundLiteralExpression &>(*idx_child).GetValue();
            if (!list_val.IsNull() && !idx_val.IsNull() &&
                list_val.type().InternalType() == PhysicalType::LIST) {
                auto &children = ListValue::GetChildren(list_val);
                int64_t idx = idx_val.GetValue<int64_t>();
                // Cypher uses 0-indexed access
                if (idx < 0) idx = (int64_t)children.size() + idx;
                if (idx >= 0 && idx < (int64_t)children.size()) {
                    return make_shared<BoundLiteralExpression>(children[idx], GenExprName(expr));
                }
            }
        }
        bound_expression_vector args;
        args.push_back(std::move(list_child));
        args.push_back(std::move(idx_child));
        return make_shared<CypherBoundFunctionExpression>(
            "list_extract", elem_type, std::move(args), GenExprName(expr));
    }

    // head(list) → list_extract(list, 1) — first element of a list
    if (fname == "head" && expr.children.size() == 1) {
        auto child = BindExpression(*expr.children[0], ctx);
        // Resolve element type from LIST child
        LogicalType elem_type = LogicalType::ANY;
        if (child->GetDataType().id() == LogicalTypeId::LIST) {
            elem_type = ListType::GetChildType(child->GetDataType());
        }
        auto idx = make_shared<BoundLiteralExpression>(Value::INTEGER(0), "_head_idx");
        bound_expression_vector args;
        args.push_back(std::move(child));
        args.push_back(std::move(idx));
        return make_shared<CypherBoundFunctionExpression>(
            "list_extract", elem_type, std::move(args), GenExprName(expr));
    }

    // size(x) → char_length(x) for strings, list_size(x) for lists
    if (fname == "size" && expr.children.size() == 1) {
        auto child = BindExpression(*expr.children[0], ctx);
        auto child_type = child->GetDataType().id();
        string func_name = (child_type == LogicalTypeId::VARCHAR)
                               ? "length" : "list_size";
        bound_expression_vector args;
        args.push_back(std::move(child));
        return make_shared<CypherBoundFunctionExpression>(
            func_name, LogicalType::BIGINT, std::move(args), GenExprName(expr));
    }

    // datetime({epochMillis: expr}) → epoch_ms(expr) or timestamp cast
    // Cypher datetime constructor. Handles both BIGINT (epoch ms) and DATE inputs.
    if (fname == "datetime" && expr.children.size() == 1) {
        // The map literal {epochMillis: expr} is parsed as struct_pack(expr AS epochMillis)
        auto &child = *expr.children[0];
        if (child.GetExpressionType() == ExpressionType::FUNCTION) {
            auto &inner_func = static_cast<const FunctionExpression &>(child);
            if (StringUtil::Lower(inner_func.function_name) == "struct_pack") {
                for (auto &arg : inner_func.children) {
                    if (!arg->alias.empty() && StringUtil::Lower(arg->alias) == "epochmillis") {
                        auto bound_arg = BindExpression(*arg, ctx);
                        auto arg_type = bound_arg->GetDataType();
                        if (arg_type.id() == LogicalTypeId::DATE ||
                            arg_type.id() == LogicalTypeId::TIMESTAMP ||
                            arg_type.id() == LogicalTypeId::TIMESTAMP_MS) {
                            // DATE/TIMESTAMP: pass through — date_part handles these directly
                            return bound_arg;
                        }
                        // BIGINT: epoch millis → epoch_ms(expr) → TIMESTAMP
                        bound_expression_vector args;
                        args.push_back(std::move(bound_arg));
                        return make_shared<CypherBoundFunctionExpression>(
                            "epoch_ms", LogicalType::TIMESTAMP, std::move(args), GenExprName(expr));
                    }
                }
            }
        }
    }

    // __list_comprehension(source, 'loop_var', filter, [map])
    // Bind with loop variable temporarily added to context
    if (fname == "__list_comprehension" && expr.children.size() >= 3) {
        // child 0: source list
        auto source = BindExpression(*expr.children[0], ctx);
        LogicalType source_type = source->GetDataType();
        LogicalType elem_type = LogicalType::ANY;
        if (source_type.id() == LogicalTypeId::LIST) {
            elem_type = ListType::GetChildType(source_type);
        }

        // child 1: loop variable name (constant string)
        string loop_var;
        if (expr.children[1]->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
            loop_var = static_cast<const ConstantExpression &>(*expr.children[1])
                .value.GetValue<string>();
        }

        // Temporarily register loop variable in context for binding filter/map
        ctx.AddAliasType(loop_var, elem_type);

        // child 2: filter expression (or constant true)
        auto filter = BindExpression(*expr.children[2], ctx);

        // child 3: optional mapping expression
        shared_ptr<BoundExpression> mapping;
        LogicalType result_elem_type = elem_type;
        if (expr.children.size() > 3) {
            mapping = BindExpression(*expr.children[3], ctx);
            result_elem_type = mapping->GetDataType();

            // Optimization: if mapping is identity (n → n or n → n.id where n is VID),
            // return source list directly — no UNWIND/collect needed.
            bool is_identity = false;
            if (mapping->GetExprType() == BoundExpressionType::VARIABLE) {
                auto &mv = static_cast<const BoundVariableExpression &>(*mapping);
                if (mv.GetVarName() == loop_var) is_identity = true;
            }

            // Check if filter is constant true
            bool filter_is_true = false;
            if (expr.children[2]->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
                auto &cv = static_cast<const ConstantExpression &>(*expr.children[2]);
                if (!cv.value.IsNull() && cv.value.type().id() == LogicalTypeId::BOOLEAN &&
                    cv.value.GetValue<bool>()) filter_is_true = true;
            }

            if (is_identity && filter_is_true) {
                // [n IN list | n] with no filter → return list as-is
                return source;
            }

            // IC14 pattern: [r IN rels | reduce(w, v IN [pattern_comp | val] | w+v)]
            // = per-edge weight computation. Detect and rewrite to path_weight.
            if (mapping->GetExprType() == BoundExpressionType::FUNCTION) {
                auto &map_fn = static_cast<const CypherBoundFunctionExpression &>(*mapping);
                if (map_fn.GetFuncName() == "list_sum" && map_fn.GetNumChildren() == 1 &&
                    map_fn.GetChild(0)->GetExprType() == BoundExpressionType::FUNCTION) {
                    auto &inner = static_cast<const CypherBoundFunctionExpression &>(*map_fn.GetChild(0));
                    if (inner.GetFuncName() == "__pattern_comprehension") {
                        auto extract_string_literal =
                            [&](BoundExpression *child, string &out) -> bool {
                            if (child->GetExprType() != BoundExpressionType::LITERAL) {
                                return false;
                            }
                            auto &lit =
                                static_cast<const BoundLiteralExpression &>(*child);
                            if (lit.GetValue().IsNull()) {
                                return false;
                            }
                            if (lit.GetValue().type().id() !=
                                LogicalTypeId::VARCHAR) {
                                return false;
                            }
                            out = lit.GetValue().GetValue<string>();
                            return true;
                        };
                        auto extract_numeric_literal =
                            [&](BoundExpression *child, double &out) -> bool {
                            if (child->GetExprType() != BoundExpressionType::LITERAL) {
                                return false;
                            }
                            auto &lit =
                                static_cast<const BoundLiteralExpression &>(*child);
                            if (lit.GetValue().IsNull()) {
                                return false;
                            }
                            switch (lit.GetValue().type().id()) {
                            case LogicalTypeId::DOUBLE:
                                out = lit.GetValue().GetValue<double>();
                                return true;
                            case LogicalTypeId::FLOAT:
                                out = lit.GetValue().GetValue<float>();
                                return true;
                            case LogicalTypeId::INTEGER:
                                out = lit.GetValue().GetValue<int32_t>();
                                return true;
                            case LogicalTypeId::BIGINT:
                                out = lit.GetValue().GetValue<int64_t>();
                                return true;
                            default:
                                return false;
                            }
                        };
                        auto resolve_path_var =
                            [&](const shared_ptr<BoundExpression> &source_expr)
                            -> string {
                            if (source_expr->GetExprType() ==
                                BoundExpressionType::FUNCTION) {
                                auto &source_fn =
                                    static_cast<const CypherBoundFunctionExpression &>(
                                        *source_expr);
                                if (source_fn.GetFuncName() == "path_rels" &&
                                    source_fn.GetNumChildren() == 1 &&
                                    source_fn.GetChild(0)->GetExprType() ==
                                        BoundExpressionType::VARIABLE) {
                                    auto &path_var =
                                        static_cast<const BoundVariableExpression &>(
                                            *source_fn.GetChild(0));
                                    if (source_fn.GetChild(0)->GetDataType().id() ==
                                            LogicalTypeId::PATH ||
                                        ctx.HasPath(path_var.GetVarName())) {
                                        return path_var.GetVarName();
                                    }
                                }
                            }
                            if (source_expr->GetExprType() ==
                                BoundExpressionType::VARIABLE) {
                                auto &source_var =
                                    static_cast<const BoundVariableExpression &>(
                                        *source_expr);
                                if (ctx.HasPathRelsAlias(
                                        source_var.GetVarName())) {
                                    return ctx.GetPathRelsAlias(
                                        source_var.GetVarName());
                                }
                            }
                            return string();
                        };

                        string path_var = resolve_path_var(source);
                        if (path_var.empty()) {
                            throw BinderException(
                                "Unsupported weighted path rewrite: "
                                "relationships(path) provenance could not be "
                                "resolved");
                        }

                        if (inner.GetNumChildren() < 3 ||
                            inner.GetChild(2)->GetExprType() !=
                                BoundExpressionType::LITERAL) {
                            throw BinderException(
                                "Unsupported weighted path rewrite: invalid "
                                "pattern comprehension metadata");
                        }

                        auto &num_hops_expr =
                            static_cast<const BoundLiteralExpression &>(
                                *inner.GetChild(2));
                        if (num_hops_expr.GetValue().IsNull()) {
                            throw BinderException(
                                "Unsupported weighted path rewrite: null hop "
                                "count");
                        }
                        idx_t num_hops =
                            (idx_t)num_hops_expr.GetValue().GetValue<int32_t>();
                        if (num_hops != 3) {
                            throw BinderException(
                                "Unsupported weighted path rewrite: only 3-hop "
                                "pattern comprehensions are supported");
                        }

                        string start_label;
                        if (!extract_string_literal(inner.GetChild(1),
                                                    start_label)) {
                            throw BinderException(
                                "Unsupported weighted path rewrite: missing "
                                "start label");
                        }

                        idx_t map_expr_idx = 3 + num_hops * 4;
                        if (map_expr_idx >= inner.GetNumChildren()) {
                            throw BinderException(
                                "Unsupported weighted path rewrite: missing "
                                "mapping expression");
                        }
                        double per_match = 0.0;
                        if (!extract_numeric_literal(inner.GetChild(map_expr_idx),
                                                     per_match)) {
                            throw BinderException(
                                "Unsupported weighted path rewrite: mapping "
                                "value must be a numeric literal");
                        }

                        vector<string> hop_edge_labels;
                        vector<string> hop_directions;
                        vector<string> hop_end_labels;
                        hop_edge_labels.reserve(num_hops);
                        hop_directions.reserve(num_hops);
                        hop_end_labels.reserve(num_hops);
                        for (idx_t hop = 0; hop < num_hops; hop++) {
                            idx_t base_idx = 3 + hop * 4;
                            string edge_label, direction, end_label;
                            if (!extract_string_literal(inner.GetChild(base_idx),
                                                        edge_label) ||
                                !extract_string_literal(
                                    inner.GetChild(base_idx + 1), direction) ||
                                !extract_string_literal(
                                    inner.GetChild(base_idx + 3), end_label)) {
                                throw BinderException(
                                    "Unsupported weighted path rewrite: "
                                    "pattern metadata must be literal");
                            }
                            if (direction != "OUT" && direction != "IN") {
                                throw BinderException(
                                    "Unsupported weighted path rewrite: only "
                                    "directed 3-hop patterns are supported");
                            }
                            hop_edge_labels.push_back(std::move(edge_label));
                            hop_directions.push_back(std::move(direction));
                            hop_end_labels.push_back(std::move(end_label));
                        }

                        bound_expression_vector pw_args;
                        pw_args.push_back(make_shared<BoundVariableExpression>(
                            path_var, LogicalType::PATH(LogicalType::ANY),
                            path_var));
                        pw_args.push_back(make_shared<BoundLiteralExpression>(
                            Value(start_label), "_pw_start_label"));
                        for (idx_t hop = 0; hop < num_hops; hop++) {
                            pw_args.push_back(make_shared<BoundLiteralExpression>(
                                Value(hop_edge_labels[hop]),
                                "_pw_edge_" + to_string(hop)));
                            pw_args.push_back(make_shared<BoundLiteralExpression>(
                                Value(hop_directions[hop]),
                                "_pw_dir_" + to_string(hop)));
                            pw_args.push_back(make_shared<BoundLiteralExpression>(
                                Value(hop_end_labels[hop]),
                                "_pw_label_" + to_string(hop)));
                        }
                        pw_args.push_back(make_shared<BoundLiteralExpression>(
                            Value::DOUBLE(per_match), "_pw_weight"));
                        return make_shared<CypherBoundFunctionExpression>(
                            "path_weight", LogicalType::DOUBLE,
                            std::move(pw_args), GenExprName(expr));
                    }
                }
            }
        }

        bound_expression_vector children;
        children.push_back(std::move(source));
        children.push_back(make_shared<BoundLiteralExpression>(Value(loop_var), "_loop_var"));
        children.push_back(std::move(filter));
        if (mapping) children.push_back(std::move(mapping));

        LogicalType ret_type = LogicalType::LIST(result_elem_type);
        return make_shared<CypherBoundFunctionExpression>(
            "__list_comprehension", ret_type, std::move(children), GenExprName(expr));
    }

    // __pattern_exists_2hop(a, 'R1', 'OUT', 'R2', 'IN', b) → boolean
    if (fname == "__pattern_exists_2hop" && expr.children.size() == 6) {
        bound_expression_vector children;
        for (auto &c : expr.children) children.push_back(BindExpression(*c, ctx));
        return make_shared<CypherBoundFunctionExpression>(
            "__pattern_exists_2hop", LogicalType::BOOLEAN, std::move(children), GenExprName(expr));
    }

    // __pattern_exists(a, 'R', 'OUT', b) → boolean
    if (fname == "__pattern_exists" && expr.children.size() == 4) {
        bound_expression_vector children;
        for (auto &c : expr.children) children.push_back(BindExpression(*c, ctx));
        return make_shared<CypherBoundFunctionExpression>(
            "__pattern_exists", LogicalType::BOOLEAN, std::move(children), GenExprName(expr));
    }

    // coalesce(a, b, ...) → CASE WHEN a IS NOT NULL THEN a WHEN b IS NOT NULL THEN b ... END
    if (fname == "coalesce") {
        vector<CypherBoundCaseCheck> checks;
        for (auto& c : expr.children) {
            CypherBoundCaseCheck bc;
            auto bound_child = BindExpression(*c, ctx);
            bc.when_expr = make_shared<BoundNullExpression>(
                /*is_not_null=*/true, bound_child->Copy(), GenExprName(*c));
            bc.then_expr = std::move(bound_child);
            checks.push_back(std::move(bc));
        }
        return make_shared<CypherBoundCaseExpression>(
            LogicalType::ANY, std::move(checks), nullptr, GenExprName(expr));
    }

    // Known aggregate functions
    static const unordered_set<string> AGG_FUNCS = {
        "count", "count_star", "sum", "avg", "min", "max",
        "collect", "count_distinct"
    };

    bound_expression_vector children;
    for (auto& c : expr.children) {
        auto bound = BindExpression(*c, ctx);
        // Preserve alias from parsed expression (used by struct_pack for field names)
        if (!c->alias.empty() && !bound->HasAlias()) {
            bound->SetAlias(c->alias);
        }
        // For struct_pack: expand node variables to nested struct of properties.
        // {msg: message} where message is a node → {msg: {id: message.id, content: message.content, ...}}
        if (fname == "struct_pack" && bound->GetExprType() == BoundExpressionType::VARIABLE) {
            auto &var = static_cast<const BoundVariableExpression &>(*bound);
            if (ctx.HasNode(var.GetVarName())) {
                auto node = ctx.GetNode(var.GetVarName());
                // Expand node to nested struct_pack with ALL properties the
                // node already has (populated by the binder during MATCH binding).
                // No partition lookup needed — BoundNodeExpression is authoritative.
                bound_expression_vector prop_children;
                child_list_t<LogicalType> nested_fields;
                node->MarkAllPropertiesUsed();
                auto *gcat = GetGraphCatalog();
                auto &key_id_map = node->GetKeyIdToIdx();
                for (auto &[kid, idx] : key_id_map) {
                    auto pe = node->GetPropertyExpression(kid);
                    // Look up property name from graph catalog
                    string prop_name = gcat->property_key_id_to_name_vec.size() > kid
                        ? gcat->property_key_id_to_name_vec[kid] : "p" + to_string(kid);
                    pe->SetAlias(prop_name);
                    nested_fields.push_back({prop_name, pe->GetDataType()});
                    prop_children.push_back(std::move(pe));
                }
                if (!prop_children.empty()) {
                    auto nested = make_shared<CypherBoundFunctionExpression>(
                        "struct_pack", LogicalType::STRUCT(std::move(nested_fields)),
                        std::move(prop_children), "_nested_" + var.GetVarName());
                    nested->SetAlias(bound->GetAlias());
                    bound = std::move(nested);
                }
            }
        }
        children.push_back(std::move(bound));
    }

    if (AGG_FUNCS.count(fname)) {
        shared_ptr<BoundExpression> child_arg = children.empty() ? nullptr : children[0];
        // Infer aggregate return type from function name and child type
        LogicalType agg_ret_type = LogicalType::ANY;
        if (fname == "collect" && child_arg) {
            agg_ret_type = LogicalType::LIST(child_arg->GetDataType());
        } else if ((fname == "sum" || fname == "avg" || fname == "min" || fname == "max") && child_arg) {
            agg_ret_type = child_arg->GetDataType();
        } else if (fname == "count" || fname == "count_star" || fname == "count_distinct") {
            agg_ret_type = LogicalType::BIGINT;
        }
        string uname = GenExprName(expr);
        return make_shared<BoundAggFunctionExpression>(
            fname, agg_ret_type, std::move(child_arg), expr.distinct, std::move(uname));
    }

    // Validate that the function exists in the DuckDB catalog before passing to ORCA.
    // Skip internal/casting functions that are resolved by the converter, not the catalog.
    {
        static const std::unordered_set<std::string> converter_funcs = {
            "to_double", "to_float", "to_integer", "to_timestamp", "to_date",
            "__list_comprehension", "__pattern_comprehension", "__pattern_exists",
            "__pattern_exists_2hop", "__exists_subquery__",
            "path_nodes", "path_rels", "path_start_node", "path_end_node",
            "path_length", "path_weight",
        };
        if (converter_funcs.find(fname) == converter_funcs.end()) {
            auto &catalog = context_->db->GetCatalog();
            auto *func_entry = catalog.GetFuncEntry(*context_, CatalogType::SCALAR_FUNCTION_ENTRY,
                                                      DEFAULT_SCHEMA, fname, true);
            if (!func_entry) {
                throw BinderException("Unknown function '" + expr.function_name + "'");
            }
        }
    }

    string uname = GenExprName(expr);
    // Resolve return type for known functions
    LogicalType func_ret_type = LogicalType::ANY;
    if (fname == "struct_pack") {
        child_list_t<LogicalType> fields;
        for (idx_t i = 0; i < children.size(); i++) {
            string name = children[i]->HasAlias() ? children[i]->GetAlias()
                        : "v" + to_string(i + 1);
            fields.push_back({name, children[i]->GetDataType()});
        }
        func_ret_type = LogicalType::STRUCT(std::move(fields));
    }
    return make_shared<CypherBoundFunctionExpression>(fname, func_ret_type, std::move(children),
                                                std::move(uname));
}

shared_ptr<BoundExpression> Binder::BindCaseExpression(const CaseExpression& expr, BindContext& ctx) {
    vector<CypherBoundCaseCheck> checks;
    for (auto& c : expr.case_checks) {
        CypherBoundCaseCheck bc;
        bc.when_expr = BindExpression(*c.when_expr, ctx);
        bc.then_expr = BindExpression(*c.then_expr, ctx);
        checks.push_back(std::move(bc));
    }
    shared_ptr<BoundExpression> else_expr;
    if (expr.else_expr) {
        else_expr = BindExpression(*expr.else_expr, ctx);
    }
    // Infer CASE return type from THEN/ELSE branches instead of leaving as ANY
    LogicalType result_type = LogicalType::ANY;
    for (auto& bc : checks) {
        auto tid = bc.then_expr->GetDataType().id();
        if (tid != LogicalTypeId::ANY && tid != LogicalTypeId::UNKNOWN && tid != LogicalTypeId::SQLNULL) {
            result_type = bc.then_expr->GetDataType();
            break;
        }
    }
    if (result_type.id() == LogicalTypeId::ANY && else_expr) {
        auto tid = else_expr->GetDataType().id();
        if (tid != LogicalTypeId::ANY && tid != LogicalTypeId::UNKNOWN && tid != LogicalTypeId::SQLNULL) {
            result_type = else_expr->GetDataType();
        }
    }
    return make_shared<CypherBoundCaseExpression>(result_type, std::move(checks),
                                             std::move(else_expr), GenExprName(expr));
}

shared_ptr<BoundExpression> Binder::BindExistsSubquery(const ExistsSubqueryExpression& expr, BindContext& ctx) {
    // Create a temporary MatchClause-like structure from the EXISTS patterns
    auto qgc = make_unique<BoundQueryGraphCollection>();

    vector<pair<const NodePattern*, shared_ptr<BoundNodeExpression>>> node_bindings;
    for (auto& pe : expr.patterns) {
        auto qg = BindPatternElement(*pe, ctx, node_bindings);
        qgc->AddAndMergeIfConnected(std::move(qg));
    }

    auto bound_match = make_unique<BoundMatchClause>(std::move(qgc), false /* not optional */);

    // WHERE predicates
    if (expr.where_expr) {
        auto pred = BindExpression(*expr.where_expr, ctx);
        bound_match->AddPredicate(std::move(pred));
    }

    // Inline property filters from node patterns
    for (auto& [np_ptr, node_expr] : node_bindings) {
        const NodePattern& np = *np_ptr;
        if (np.GetNumProperties() == 0) continue;
        string label = node_expr->GetUniqueName();
        for (idx_t i = 0; i < np.GetNumProperties(); i++) {
            auto lhs = LookupPropertyOnNode(*node_expr, np.GetPropertyKey(i));
            auto rhs = BindExpression(*np.GetPropertyValue(i), ctx);
            auto cmp = make_shared<CypherBoundComparisonExpression>(
                ExpressionType::COMPARE_EQUAL, std::move(lhs), std::move(rhs),
                "__exists_prop_" + label + "_" + np.GetPropertyKey(i));
            bound_match->AddPredicate(std::move(cmp));
        }
    }

    return make_shared<BoundExistsSubqueryExpression>(std::move(bound_match),
                                                       "__exists_" + std::to_string(exist_counter_++));
}

} // namespace duckdb

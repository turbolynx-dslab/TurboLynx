#include "binder/binder.hpp"
#include <set>
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

namespace duckdb {

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
    return make_unique<BoundUnwindClause>(std::move(expr), unwind.GetAlias());
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
                    src_ok = ms || md;
                }
                if (has_dst_constraint) {
                    bool ms = false, md = false;
                    for (auto dp : dst_pids) {
                        if ((idx_t)dp == ep_src) ms = true;
                        if ((idx_t)dp == ep_dst) md = true;
                    }
                    dst_ok = ms || md;
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
                    }
                    if (has_dst_constraint) {
                        bool dst_in_dst = false, src_in_dst = false;
                        for (auto dp : dst_pids) {
                            if ((idx_t)dp == sub_dst) dst_in_dst = true;
                            if ((idx_t)dp == sub_src) src_in_dst = true;
                        }
                        if (!dst_in_dst) match_normal = false;
                        if (!src_in_dst) match_reversed = false;
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
                    }
                    if (has_dst_constraint) {
                        bool dst_in_dst = false, src_in_dst = false;
                        for (auto dp : dst_pids) {
                            if ((idx_t)dp == sub_dst) dst_in_dst = true;
                            if ((idx_t)dp == sub_src) src_in_dst = true;
                        }
                        if (!dst_in_dst) match_normal = false;
                        if (!src_in_dst) match_reversed = false;
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

    if (contains_star) {
        // RETURN * — emit all visible variables
        for (auto& name : ctx.GetAllNodeNames()) {
            auto node = ctx.GetNode(name);
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
            if (bound_type.id() != LogicalTypeId::ANY &&
                bound_type.id() != LogicalTypeId::INVALID) {
                ctx.AddAliasType(alias, bound_type);
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
    if (var_type.id() == LogicalTypeId::ANY && !ctx.HasNode(var) && !ctx.HasRel(var) &&
        !ctx.HasAliasType(var) && !ctx.HasPath(var)) {
        throw BinderException("Variable '" + var + "' is not defined");
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

    // toInteger/toFloat are already registered as DuckDB scalar functions
    // with lowercase names. The binder lowercases fname above, so they
    // just fall through to the general function binding path.

    // length(path) → path_length(path) — path hop count
    // Path is stored as [node, edge, node, edge, ..., node], so hops = (size-1)/2
    if (fname == "length" && expr.children.size() == 1) {
        auto child = BindExpression(*expr.children[0], ctx);
        bound_expression_vector args;
        args.push_back(std::move(child));
        return make_shared<CypherBoundFunctionExpression>(
            "path_length", LogicalType::BIGINT, std::move(args), GenExprName(expr));
    }

    // head(list) → list_extract(list, 1) — first element of a list
    if (fname == "head" && expr.children.size() == 1) {
        auto child = BindExpression(*expr.children[0], ctx);
        // Resolve element type from LIST child
        LogicalType elem_type = LogicalType::ANY;
        if (child->GetDataType().id() == LogicalTypeId::LIST) {
            elem_type = ListType::GetChildType(child->GetDataType());
        }
        auto idx = make_shared<BoundLiteralExpression>(Value::INTEGER(1), "_head_idx");
        bound_expression_vector args;
        args.push_back(std::move(child));
        args.push_back(std::move(idx));
        return make_shared<CypherBoundFunctionExpression>(
            "list_extract", elem_type, std::move(args), GenExprName(expr));
    }

    // size(list) → list_size(list) — number of elements in a list
    if (fname == "size" && expr.children.size() == 1) {
        auto child = BindExpression(*expr.children[0], ctx);
        bound_expression_vector args;
        args.push_back(std::move(child));
        return make_shared<CypherBoundFunctionExpression>(
            "list_size", LogicalType::BIGINT, std::move(args), GenExprName(expr));
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

    // __pattern_exists_2hop(a, 'R1', 'R2', b) → boolean (2-hop pattern check)
    if (fname == "__pattern_exists_2hop" && expr.children.size() == 4) {
        bound_expression_vector children;
        for (auto &c : expr.children) children.push_back(BindExpression(*c, ctx));
        return make_shared<CypherBoundFunctionExpression>(
            "__pattern_exists_2hop", LogicalType::BOOLEAN, std::move(children), GenExprName(expr));
    }

    // __pattern_exists(a, 'R', b) → placeholder boolean (resolved at converter level)
    // The converter adds OPTIONAL MATCH and replaces with null check
    if (fname == "__pattern_exists" && expr.children.size() == 3) {
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
        "count", "sum", "avg", "min", "max",
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
        // Infer aggregate return type for collect → LIST(child_type)
        LogicalType agg_ret_type = LogicalType::ANY;
        if (fname == "collect" && child_arg) {
            agg_ret_type = LogicalType::LIST(child_arg->GetDataType());
        }
        string uname = GenExprName(expr);
        return make_shared<BoundAggFunctionExpression>(
            fname, agg_ret_type, std::move(child_arg), expr.distinct, std::move(uname));
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

} // namespace duckdb

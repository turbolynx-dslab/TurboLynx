#include "parser/cypher_transformer.hpp"

#include "parser/expression/exists_subquery_expression.hpp"
#include "parser/query/reading_clause/match_clause.hpp"
#include "parser/query/reading_clause/unwind_clause.hpp"
#include "parser/query/updating_clause/updating_clause.hpp"
#include "parser/query/updating_clause/create_clause.hpp"
#include "parser/query/updating_clause/set_clause.hpp"
#include "parser/query/updating_clause/delete_clause.hpp"
#include "parser/expression/property_expression.hpp"
#include "parser/expression/variable_expression.hpp"
#include "parser/expression/comparison_expression.hpp"
#include "parser/expression/conjunction_expression.hpp"
#include "parser/expression/operator_expression.hpp"
#include "parser/expression/function_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/expression/case_expression.hpp"
#include "parser/expression/subquery_expression.hpp"
#include "common/types/value.hpp"
#include "common/exception.hpp"

#include <cassert>
#include <stdexcept>

namespace duckdb {

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

unique_ptr<RegularQuery> CypherTransformer::transform() {
    auto* stmt = root_.oC_Statement();
    if (!stmt) throw std::runtime_error("Parse error: no statement found");
    auto* qry = stmt->oC_Query();
    if (!qry) throw std::runtime_error("Parse error: no query found");
    auto* reg = qry->oC_RegularQuery();
    if (!reg) throw std::runtime_error("Parse error: unsupported query type (CREATE/DELETE/MERGE are not supported)");
    return transformRegularQuery(*reg);
}

// ---------------------------------------------------------------------------
// Query structure
// ---------------------------------------------------------------------------

unique_ptr<RegularQuery> CypherTransformer::transformRegularQuery(
    CypherParser::OC_RegularQueryContext& ctx) {
    auto first = transformSingleQuery(*ctx.oC_SingleQuery());
    auto query = make_unique<RegularQuery>(std::move(first));
    for (auto& unionCtx : ctx.oC_Union()) {
        bool is_all = (unionCtx->ALL() != nullptr);
        query->AddSingleQuery(transformSingleQuery(*unionCtx->oC_SingleQuery()), is_all);
    }
    return query;
}

unique_ptr<SingleQuery> CypherTransformer::transformSingleQuery(
    CypherParser::OC_SingleQueryContext& ctx) {
    if (ctx.oC_SinglePartQuery()) {
        return transformSinglePartQuery(*ctx.oC_SinglePartQuery());
    }
    // Multi-part query: query parts + final single part
    auto& mpq = *ctx.oC_MultiPartQuery();
    auto single = make_unique<SingleQuery>();
    for (auto& partCtx : mpq.kU_QueryPart()) {
        single->AddQueryPart(transformQueryPart(*partCtx));
    }
    // Final single-part query provides the RETURN
    auto final_part = transformSinglePartQuery(*mpq.oC_SinglePartQuery());
    // Merge reading/updating/return from final_part into single
    for (idx_t i = 0; i < final_part->GetNumReadingClauses(); ++i) {
        // Re-add clauses: we need ownership — pull them via move
        // (single_query stores them; we rebuild from the final_part by merging)
    }
    // Simpler: just return the final single part and attach the query parts
    // Actually, re-build correctly:
    auto result = make_unique<SingleQuery>();
    for (auto& partCtx : mpq.kU_QueryPart()) {
        result->AddQueryPart(transformQueryPart(*partCtx));
    }
    // The final SinglePartQuery's reading/updating/return go directly into result
    auto& spq = *mpq.oC_SinglePartQuery();
    for (auto& rc : spq.oC_ReadingClause()) {
        result->AddReadingClause(transformReadingClause(*rc));
    }
    for (auto& uc : spq.oC_UpdatingClause()) {
        result->AddUpdatingClause(transformUpdatingClause(*uc));
    }
    if (spq.oC_Return()) {
        result->SetReturnClause(transformReturn(*spq.oC_Return()));
    }
    return result;
}

unique_ptr<SingleQuery> CypherTransformer::transformSinglePartQuery(
    CypherParser::OC_SinglePartQueryContext& ctx) {
    auto single = make_unique<SingleQuery>();
    for (auto& rc : ctx.oC_ReadingClause()) {
        single->AddReadingClause(transformReadingClause(*rc));
    }
    for (auto& uc : ctx.oC_UpdatingClause()) {
        single->AddUpdatingClause(transformUpdatingClause(*uc));
    }
    if (ctx.oC_Return()) {
        single->SetReturnClause(transformReturn(*ctx.oC_Return()));
    }
    return single;
}

unique_ptr<QueryPart> CypherTransformer::transformQueryPart(
    CypherParser::KU_QueryPartContext& ctx) {
    auto with = transformWith(*ctx.oC_With());
    auto part = make_unique<QueryPart>(std::move(with));
    for (auto& rc : ctx.oC_ReadingClause()) {
        part->AddReadingClause(transformReadingClause(*rc));
    }
    for (auto& uc : ctx.oC_UpdatingClause()) {
        part->AddUpdatingClause(transformUpdatingClause(*uc));
    }
    return part;
}

// ---------------------------------------------------------------------------
// Clauses
// ---------------------------------------------------------------------------

unique_ptr<ReadingClause> CypherTransformer::transformReadingClause(
    CypherParser::OC_ReadingClauseContext& ctx) {
    if (ctx.oC_Match()) return transformMatch(*ctx.oC_Match());
    if (ctx.oC_Unwind()) return transformUnwind(*ctx.oC_Unwind());
    throw InternalException("Unsupported reading clause");
}

unique_ptr<ReadingClause> CypherTransformer::transformMatch(
    CypherParser::OC_MatchContext& ctx) {
    auto clause = make_unique<MatchClause>(transformPattern(*ctx.oC_Pattern()),
                                           ctx.OPTIONAL() != nullptr);
    if (ctx.oC_Where()) {
        clause->SetWhere(transformWhere(*ctx.oC_Where()));
    }
    return clause;
}

unique_ptr<ReadingClause> CypherTransformer::transformUnwind(
    CypherParser::OC_UnwindContext& ctx) {
    return make_unique<UnwindClause>(transformExpression(*ctx.oC_Expression()),
                                     transformVariable(*ctx.oC_Variable()));
}

unique_ptr<UpdatingClause> CypherTransformer::transformUpdatingClause(
    CypherParser::OC_UpdatingClauseContext& ctx) {
    if (ctx.oC_Create()) {
        return transformCreateClause(*ctx.oC_Create());
    }
    if (ctx.oC_Set()) {
        return transformSetClause(*ctx.oC_Set());
    }
    if (ctx.oC_Delete()) {
        return transformDeleteClause(*ctx.oC_Delete());
    }
    throw InternalException("Unsupported updating clause");
}

unique_ptr<UpdatingClause> CypherTransformer::transformSetClause(
    CypherParser::OC_SetContext& ctx) {
    auto clause = make_unique<SetClause>();
    for (auto* item_ctx : ctx.oC_SetItem()) {
        // oC_SetItem : oC_PropertyExpression '=' oC_Expression
        auto* prop_expr_ctx = item_ctx->oC_PropertyExpression();
        // oC_PropertyExpression : oC_Atom oC_PropertyLookup
        // oC_Atom → variable name, oC_PropertyLookup → .propertyKey
        string var_name = prop_expr_ctx->oC_Atom()->getText();
        string prop_key = prop_expr_ctx->oC_PropertyLookup()
                              ->oC_PropertyKeyName()->getText();
        auto value = transformExpression(*item_ctx->oC_Expression());
        clause->AddItem(SetItem(var_name, prop_key, std::move(value)));
    }
    return clause;
}

unique_ptr<UpdatingClause> CypherTransformer::transformDeleteClause(
    CypherParser::OC_DeleteContext& ctx) {
    auto clause = make_unique<DeleteClause>();
    // oC_Delete : DELETE SP? oC_Expression ( SP? ',' SP? oC_Expression )*
    for (auto* expr_ctx : ctx.oC_Expression()) {
        // Each expression should be a simple variable name
        clause->AddVariable(expr_ctx->getText());
    }
    return clause;
}

unique_ptr<UpdatingClause> CypherTransformer::transformCreateClause(
    CypherParser::OC_CreateContext& ctx) {
    auto clause = make_unique<CreateClause>();
    // oC_Create : CREATE SP? oC_Pattern
    // oC_Pattern : oC_PatternPart (SP? ',' SP? oC_PatternPart)*
    auto* pattern_ctx = ctx.oC_Pattern();
    if (!pattern_ctx) {
        throw InternalException("CREATE clause has no pattern");
    }
    for (auto& pp : pattern_ctx->oC_PatternPart()) {
        auto* anon = pp->oC_AnonymousPatternPart();
        if (!anon) continue;
        clause->AddPattern(transformPatternElement(*anon->oC_PatternElement()));
    }
    return clause;
}

// ---------------------------------------------------------------------------
// Return / With / Projection
// ---------------------------------------------------------------------------

unique_ptr<WithClause> CypherTransformer::transformWith(CypherParser::OC_WithContext& ctx) {
    auto clause = make_unique<WithClause>(transformProjectionBody(*ctx.oC_ProjectionBody()));
    if (ctx.oC_Where()) {
        clause->SetWhere(transformWhere(*ctx.oC_Where()));
    }
    return clause;
}

unique_ptr<ReturnClause> CypherTransformer::transformReturn(CypherParser::OC_ReturnContext& ctx) {
    return make_unique<ReturnClause>(transformProjectionBody(*ctx.oC_ProjectionBody()));
}

unique_ptr<ProjectionBody> CypherTransformer::transformProjectionBody(
    CypherParser::OC_ProjectionBodyContext& ctx) {
    auto body = make_unique<ProjectionBody>(
        ctx.DISTINCT() != nullptr,
        ctx.oC_ProjectionItems()->STAR() != nullptr,
        transformProjectionItems(*ctx.oC_ProjectionItems()));

    if (ctx.oC_Order()) {
        vector<OrderByItem> order;
        for (auto& sortItem : ctx.oC_Order()->oC_SortItem()) {
            bool asc = !(sortItem->DESC() || sortItem->DESCENDING());
            order.emplace_back(transformExpression(*sortItem->oC_Expression()), asc);
        }
        body->SetOrderBy(std::move(order));
    }
    if (ctx.oC_Skip()) {
        body->SetSkip(transformExpression(*ctx.oC_Skip()->oC_Expression()));
    }
    if (ctx.oC_Limit()) {
        body->SetLimit(transformExpression(*ctx.oC_Limit()->oC_Expression()));
    }
    return body;
}

vector<unique_ptr<ParsedExpression>> CypherTransformer::transformProjectionItems(
    CypherParser::OC_ProjectionItemsContext& ctx) {
    vector<unique_ptr<ParsedExpression>> result;
    for (auto& item : ctx.oC_ProjectionItem()) {
        result.push_back(transformProjectionItem(*item));
    }
    return result;
}

unique_ptr<ParsedExpression> CypherTransformer::transformProjectionItem(
    CypherParser::OC_ProjectionItemContext& ctx) {
    auto expr = transformExpression(*ctx.oC_Expression());
    if (ctx.AS()) {
        expr->alias = transformVariable(*ctx.oC_Variable());
    }
    return expr;
}

unique_ptr<ParsedExpression> CypherTransformer::transformWhere(
    CypherParser::OC_WhereContext& ctx) {
    return transformExpression(*ctx.oC_Expression());
}

// ---------------------------------------------------------------------------
// Graph patterns
// ---------------------------------------------------------------------------

vector<unique_ptr<PatternElement>> CypherTransformer::transformPattern(
    CypherParser::OC_PatternContext& ctx) {
    vector<unique_ptr<PatternElement>> result;
    for (auto& part : ctx.oC_PatternPart()) {
        result.push_back(transformPatternPart(*part));
    }
    return result;
}

unique_ptr<PatternElement> CypherTransformer::transformPatternPart(
    CypherParser::OC_PatternPartContext& ctx) {
    auto elem = transformAnonymousPatternPart(*ctx.oC_AnonymousPatternPart());
    if (ctx.oC_Variable()) {
        elem->SetPathName(transformVariable(*ctx.oC_Variable()));
    }
    return elem;
}

unique_ptr<PatternElement> CypherTransformer::transformAnonymousPatternPart(
    CypherParser::OC_AnonymousPatternPartContext& ctx) {
    if (ctx.oC_ShortestPathPattern()) {
        auto elem = transformPatternElement(*ctx.oC_ShortestPathPattern()->oC_PatternElement());
        elem->SetPathType(ctx.oC_ShortestPathPattern()->SHORTESTPATH()
                              ? PatternPathType::SHORTEST
                              : PatternPathType::ALL_SHORTEST);
        return elem;
    }
    return transformPatternElement(*ctx.oC_PatternElement());
}

unique_ptr<PatternElement> CypherTransformer::transformPatternElement(
    CypherParser::OC_PatternElementContext& ctx) {
    if (ctx.oC_PatternElement()) {
        return transformPatternElement(*ctx.oC_PatternElement()); // parenthesized
    }
    auto elem = make_unique<PatternElement>(transformNodePattern(*ctx.oC_NodePattern()));
    for (auto& chain : ctx.oC_PatternElementChain()) {
        elem->AddChain(transformRelationshipPattern(*chain->oC_RelationshipPattern()),
                       transformNodePattern(*chain->oC_NodePattern()));
    }
    return elem;
}

unique_ptr<NodePattern> CypherTransformer::transformNodePattern(
    CypherParser::OC_NodePatternContext& ctx) {
    string var = ctx.oC_Variable() ? transformVariable(*ctx.oC_Variable()) : string();
    auto labels = ctx.oC_NodeLabels() ? transformNodeLabels(*ctx.oC_NodeLabels()) : vector<string>{};
    auto props = ctx.kU_Properties() ? transformProperties(*ctx.kU_Properties())
                                     : vector<pair<string, unique_ptr<ParsedExpression>>>{};
    return make_unique<NodePattern>(std::move(var), std::move(labels), std::move(props));
}

unique_ptr<RelPattern> CypherTransformer::transformRelationshipPattern(
    CypherParser::OC_RelationshipPatternContext& ctx) {
    auto* detail = ctx.oC_RelationshipDetail();
    if (!detail) {
        throw std::runtime_error("Relationship pattern requires edge detail (e.g., -[r:TYPE]->)");
    }
    string var = detail->oC_Variable() ? transformVariable(*detail->oC_Variable()) : string();
    auto types = detail->oC_RelationshipTypes() ? transformRelTypes(*detail->oC_RelationshipTypes())
                                                : vector<string>{};
    string lower = "1", upper = "1";
    const string inf = "inf";
    if (detail->oC_RangeLiteral()) {
        auto* range = detail->oC_RangeLiteral();
        if (range->RANGE()) { // *lower..upper
            lower = range->oC_RangeStartLiteral() ? range->oC_RangeStartLiteral()->getText() : "1";
            upper = range->oC_RangeEndLiteral()   ? range->oC_RangeEndLiteral()->getText()   : inf;
        } else { // *n (exact)
            lower = upper = range->oC_RangeStartLiteral() ? range->oC_RangeStartLiteral()->getText() : inf;
        }
    }
    RelDirection dir = ctx.oC_LeftArrowHead() ? RelDirection::LEFT : RelDirection::RIGHT;
    // BOTH direction: no arrow heads at all
    if (!ctx.oC_LeftArrowHead() && !ctx.oC_RightArrowHead()) {
        dir = RelDirection::BOTH;
    }
    RelPatternType pat_type = RelPatternType::SIMPLE;
    if (lower != "1" || upper != "1") pat_type = RelPatternType::VARIABLE_LENGTH;

    auto props = detail->kU_Properties() ? transformProperties(*detail->kU_Properties())
                                         : vector<pair<string, unique_ptr<ParsedExpression>>>{};
    return make_unique<RelPattern>(std::move(var), std::move(types), dir, pat_type,
                                   std::move(props), std::move(lower), std::move(upper));
}

vector<string> CypherTransformer::transformNodeLabels(CypherParser::OC_NodeLabelsContext& ctx) {
    vector<string> result;
    for (auto& nl : ctx.oC_NodeLabel()) {
        result.push_back(transformSchemaName(*nl->oC_LabelName()->oC_SchemaName()));
    }
    return result;
}

vector<string> CypherTransformer::transformRelTypes(CypherParser::OC_RelationshipTypesContext& ctx) {
    vector<string> result;
    for (auto& rt : ctx.oC_RelTypeName()) {
        result.push_back(transformSchemaName(*rt->oC_SchemaName()));
    }
    return result;
}

vector<pair<string, unique_ptr<ParsedExpression>>> CypherTransformer::transformProperties(
    CypherParser::KU_PropertiesContext& ctx) {
    vector<pair<string, unique_ptr<ParsedExpression>>> result;
    assert(ctx.oC_PropertyKeyName().size() == ctx.oC_Expression().size());
    for (size_t i = 0; i < ctx.oC_PropertyKeyName().size(); ++i) {
        result.emplace_back(transformPropertyKeyName(*ctx.oC_PropertyKeyName(i)),
                            transformExpression(*ctx.oC_Expression(i)));
    }
    return result;
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

unique_ptr<ParsedExpression> CypherTransformer::transformExpression(
    CypherParser::OC_ExpressionContext& ctx) {
    return transformOrExpression(*ctx.oC_OrExpression());
}

unique_ptr<ParsedExpression> CypherTransformer::transformOrExpression(
    CypherParser::OC_OrExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expr;
    for (auto& xor_ctx : ctx.oC_XorExpression()) {
        auto next = transformXorExpression(*xor_ctx);
        if (!expr) { expr = std::move(next); continue; }
        expr = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR,
                                                  std::move(expr), std::move(next));
    }
    return expr;
}

unique_ptr<ParsedExpression> CypherTransformer::transformXorExpression(
    CypherParser::OC_XorExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expr;
    for (auto& and_ctx : ctx.oC_AndExpression()) {
        auto next = transformAndExpression(*and_ctx);
        if (!expr) { expr = std::move(next); continue; }
        // XOR: (A OR B) AND NOT (A AND B)
        auto a_or_b = make_unique<ConjunctionExpression>(
            ExpressionType::CONJUNCTION_OR, expr->Copy(), next->Copy());
        auto a_and_b = make_unique<ConjunctionExpression>(
            ExpressionType::CONJUNCTION_AND, std::move(expr), std::move(next));
        auto not_a_and_b = make_unique<OperatorExpression>(
            ExpressionType::OPERATOR_NOT);
        not_a_and_b->children.push_back(std::move(a_and_b));
        expr = make_unique<ConjunctionExpression>(
            ExpressionType::CONJUNCTION_AND, std::move(a_or_b), std::move(not_a_and_b));
    }
    return expr;
}

unique_ptr<ParsedExpression> CypherTransformer::transformAndExpression(
    CypherParser::OC_AndExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expr;
    for (auto& not_ctx : ctx.oC_NotExpression()) {
        auto next = transformNotExpression(*not_ctx);
        if (!expr) { expr = std::move(next); continue; }
        expr = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
                                                  std::move(expr), std::move(next));
    }
    return expr;
}

unique_ptr<ParsedExpression> CypherTransformer::transformNotExpression(
    CypherParser::OC_NotExpressionContext& ctx) {
    auto child = transformComparisonExpression(*ctx.oC_ComparisonExpression());
    if (ctx.NOT()) {
        return make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(child));
    }
    return child;
}

static ExpressionType CompOpToExprType(const string& op) {
    if      (op == "=")  return ExpressionType::COMPARE_EQUAL;
    else if (op == "<>") return ExpressionType::COMPARE_NOTEQUAL;
    else if (op == ">")  return ExpressionType::COMPARE_GREATERTHAN;
    else if (op == ">=") return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
    else if (op == "<")  return ExpressionType::COMPARE_LESSTHAN;
    else                 return ExpressionType::COMPARE_LESSTHANOREQUALTO;
}

unique_ptr<ParsedExpression> CypherTransformer::transformComparisonExpression(
    CypherParser::OC_ComparisonExpressionContext& ctx) {
    auto operands = ctx.kU_BitwiseOrOperatorExpression();
    if (operands.size() == 1) {
        return transformBitwiseOrOperatorExpression(*operands[0]);
    }
    auto ops = ctx.kU_ComparisonOperator();

    // Helper: normalize const op var → var flipped_op const
    // so the ORCA converter never sees const-on-left comparisons.
    auto makeNormalizedCmp = [](ExpressionType op,
                                unique_ptr<ParsedExpression> left,
                                unique_ptr<ParsedExpression> right) {
        if (left->type == ExpressionType::VALUE_CONSTANT &&
            right->type != ExpressionType::VALUE_CONSTANT) {
            ExpressionType flipped = op;
            switch (op) {
            case ExpressionType::COMPARE_LESSTHAN:           flipped = ExpressionType::COMPARE_GREATERTHAN; break;
            case ExpressionType::COMPARE_LESSTHANOREQUALTO:  flipped = ExpressionType::COMPARE_GREATERTHANOREQUALTO; break;
            case ExpressionType::COMPARE_GREATERTHAN:        flipped = ExpressionType::COMPARE_LESSTHAN; break;
            case ExpressionType::COMPARE_GREATERTHANOREQUALTO: flipped = ExpressionType::COMPARE_LESSTHANOREQUALTO; break;
            default: break;
            }
            return make_unique<ComparisonExpression>(flipped, std::move(right), std::move(left));
        }
        return make_unique<ComparisonExpression>(op, std::move(left), std::move(right));
    };

    // Single binary comparison (common case)
    if (ops.size() == 1) {
        auto left  = transformBitwiseOrOperatorExpression(*operands[0]);
        auto right = transformBitwiseOrOperatorExpression(*operands[1]);
        return makeNormalizedCmp(
            CompOpToExprType(ops[0]->getText()), std::move(left), std::move(right));
    }
    // Chained comparison: a > b >= c  →  (a > b) AND (b >= c)
    // Each intermediate operand is used in two comparisons, so we transform it twice.
    unique_ptr<ParsedExpression> result;
    for (size_t i = 0; i < ops.size(); i++) {
        auto left  = transformBitwiseOrOperatorExpression(*operands[i]);
        auto right = transformBitwiseOrOperatorExpression(*operands[i + 1]);
        auto cmp = makeNormalizedCmp(
            CompOpToExprType(ops[i]->getText()), std::move(left), std::move(right));
        if (!result) {
            result = std::move(cmp);
        } else {
            result = make_unique<ConjunctionExpression>(
                ExpressionType::CONJUNCTION_AND, std::move(result), std::move(cmp));
        }
    }
    return result;
}

unique_ptr<ParsedExpression> CypherTransformer::transformBitwiseOrOperatorExpression(
    CypherParser::KU_BitwiseOrOperatorExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expr;
    for (auto& and_ctx : ctx.kU_BitwiseAndOperatorExpression()) {
        auto next = transformBitwiseAndOperatorExpression(*and_ctx);
        if (!expr) { expr = std::move(next); continue; }
        vector<unique_ptr<ParsedExpression>> args;
        args.push_back(std::move(expr)); args.push_back(std::move(next));
        expr = make_unique<FunctionExpression>("|", std::move(args));
    }
    return expr;
}

unique_ptr<ParsedExpression> CypherTransformer::transformBitwiseAndOperatorExpression(
    CypherParser::KU_BitwiseAndOperatorExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expr;
    for (auto& shift_ctx : ctx.kU_BitShiftOperatorExpression()) {
        auto next = transformBitShiftOperatorExpression(*shift_ctx);
        if (!expr) { expr = std::move(next); continue; }
        vector<unique_ptr<ParsedExpression>> args;
        args.push_back(std::move(expr)); args.push_back(std::move(next));
        expr = make_unique<FunctionExpression>("&", std::move(args));
    }
    return expr;
}

unique_ptr<ParsedExpression> CypherTransformer::transformBitShiftOperatorExpression(
    CypherParser::KU_BitShiftOperatorExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expr;
    for (size_t i = 0; i < ctx.oC_AddOrSubtractExpression().size(); ++i) {
        auto next = transformAddOrSubtractExpression(*ctx.oC_AddOrSubtractExpression(i));
        if (!expr) { expr = std::move(next); continue; }
        string func = ctx.kU_BitShiftOperator(i - 1)->getText();  // "<<" or ">>"
        vector<unique_ptr<ParsedExpression>> args;
        args.push_back(std::move(expr)); args.push_back(std::move(next));
        expr = make_unique<FunctionExpression>(func, std::move(args));
    }
    return expr;
}

unique_ptr<ParsedExpression> CypherTransformer::transformAddOrSubtractExpression(
    CypherParser::OC_AddOrSubtractExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expr;
    for (size_t i = 0; i < ctx.oC_MultiplyDivideModuloExpression().size(); ++i) {
        auto next = transformMultiplyDivideModuloExpression(*ctx.oC_MultiplyDivideModuloExpression(i));
        if (!expr) { expr = std::move(next); continue; }
        string func = ctx.kU_AddOrSubtractOperator(i - 1)->getText(); // "+" or "-"
        vector<unique_ptr<ParsedExpression>> args;
        args.push_back(std::move(expr)); args.push_back(std::move(next));
        expr = make_unique<FunctionExpression>(func, std::move(args));
    }
    return expr;
}

unique_ptr<ParsedExpression> CypherTransformer::transformMultiplyDivideModuloExpression(
    CypherParser::OC_MultiplyDivideModuloExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expr;
    for (size_t i = 0; i < ctx.oC_PowerOfExpression().size(); ++i) {
        auto next = transformPowerOfExpression(*ctx.oC_PowerOfExpression(i));
        if (!expr) { expr = std::move(next); continue; }
        string func = ctx.kU_MultiplyDivideModuloOperator(i - 1)->getText(); // "*", "/", "%"
        vector<unique_ptr<ParsedExpression>> args;
        args.push_back(std::move(expr)); args.push_back(std::move(next));
        expr = make_unique<FunctionExpression>(func, std::move(args));
    }
    return expr;
}

unique_ptr<ParsedExpression> CypherTransformer::transformPowerOfExpression(
    CypherParser::OC_PowerOfExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expr;
    for (auto& unary_ctx : ctx.oC_UnaryAddSubtractOrFactorialExpression()) {
        auto next = transformUnaryAddSubtractOrFactorialExpression(*unary_ctx);
        if (!expr) { expr = std::move(next); continue; }
        vector<unique_ptr<ParsedExpression>> args;
        args.push_back(std::move(expr)); args.push_back(std::move(next));
        expr = make_unique<FunctionExpression>("^", std::move(args));
    }
    return expr;
}

unique_ptr<ParsedExpression> CypherTransformer::transformUnaryAddSubtractOrFactorialExpression(
    CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext& ctx) {
    auto inner = transformStringListNullOperatorExpression(*ctx.oC_StringListNullOperatorExpression());
    if (ctx.MINUS()) {
        vector<unique_ptr<ParsedExpression>> args;
        args.push_back(std::move(inner));
        return make_unique<FunctionExpression>("negate", std::move(args));
    }
    return inner;
}

unique_ptr<ParsedExpression> CypherTransformer::transformStringListNullOperatorExpression(
    CypherParser::OC_StringListNullOperatorExpressionContext& ctx) {
    auto expr = transformPropertyOrLabelsExpression(*ctx.oC_PropertyOrLabelsExpression());
    if (ctx.oC_NullOperatorExpression()) {
        return transformNullOperatorExpression(*ctx.oC_NullOperatorExpression(), std::move(expr));
    }
    if (ctx.oC_ListOperatorExpression()) {
        return transformListOperatorExpression(*ctx.oC_ListOperatorExpression(), std::move(expr));
    }
    if (ctx.oC_StringOperatorExpression()) {
        return transformStringOperatorExpression(*ctx.oC_StringOperatorExpression(), std::move(expr));
    }
    return expr;
}

unique_ptr<ParsedExpression> CypherTransformer::transformStringOperatorExpression(
    CypherParser::OC_StringOperatorExpressionContext& ctx,
    unique_ptr<ParsedExpression> left) {
    auto right = transformPropertyOrLabelsExpression(*ctx.oC_PropertyOrLabelsExpression());
    string func;
    if (ctx.STARTS())    func = "prefix";
    else if (ctx.ENDS()) func = "suffix";
    else                 func = "contains";
    vector<unique_ptr<ParsedExpression>> args;
    args.push_back(std::move(left));
    args.push_back(std::move(right));
    return make_unique<FunctionExpression>(func, std::move(args));
}

unique_ptr<ParsedExpression> CypherTransformer::transformListOperatorExpression(
    CypherParser::OC_ListOperatorExpressionContext& ctx,
    unique_ptr<ParsedExpression> left) {
    // Basic list indexing: list[idx]
    if (ctx.kU_ListExtractOperatorExpression()) {
        auto idx = transformExpression(*ctx.kU_ListExtractOperatorExpression()->oC_Expression());
        vector<unique_ptr<ParsedExpression>> args;
        args.push_back(std::move(left));
        args.push_back(std::move(idx));
        auto result = make_unique<FunctionExpression>("list_extract", std::move(args));
        if (ctx.oC_ListOperatorExpression()) {
            return transformListOperatorExpression(*ctx.oC_ListOperatorExpression(), std::move(result));
        }
        return result;
    }
    // list[a:b] slice
    if (ctx.kU_ListSliceOperatorExpression()) {
        auto& slice = *ctx.kU_ListSliceOperatorExpression();
        vector<unique_ptr<ParsedExpression>> args;
        args.push_back(std::move(left));
        auto zero = make_unique<ConstantExpression>(Value((int64_t)0));
        if (slice.oC_Expression().size() == 2) {
            args.push_back(transformExpression(*slice.oC_Expression(0)));
            args.push_back(transformExpression(*slice.oC_Expression(1)));
        } else if (slice.oC_Expression().size() == 1) {
            if (slice.children[1]->getText() == ":") {
                args.push_back(std::move(zero));
                args.push_back(transformExpression(*slice.oC_Expression(0)));
            } else {
                args.push_back(transformExpression(*slice.oC_Expression(0)));
                args.push_back(make_unique<ConstantExpression>(Value((int64_t)0)));
            }
        } else {
            args.push_back(std::move(zero));
            args.push_back(make_unique<ConstantExpression>(Value((int64_t)0)));
        }
        auto result = make_unique<FunctionExpression>("list_slice", std::move(args));
        if (ctx.oC_ListOperatorExpression()) {
            return transformListOperatorExpression(*ctx.oC_ListOperatorExpression(), std::move(result));
        }
        return result;
    }
    // list property contains check
    if (ctx.kU_ListPropertyOrLabelsExpression()) {
        auto right = transformPropertyOrLabelsExpression(
            *ctx.kU_ListPropertyOrLabelsExpression()->oC_PropertyOrLabelsExpression());
        vector<unique_ptr<ParsedExpression>> args;
        args.push_back(std::move(right)); // right contains left
        args.push_back(std::move(left));
        return make_unique<FunctionExpression>("list_contains", std::move(args));
    }
    return left;
}

unique_ptr<ParsedExpression> CypherTransformer::transformNullOperatorExpression(
    CypherParser::OC_NullOperatorExpressionContext& ctx,
    unique_ptr<ParsedExpression> left) {
    if (ctx.NOT()) {
        return make_unique<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, std::move(left));
    }
    return make_unique<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, std::move(left));
}

unique_ptr<ParsedExpression> CypherTransformer::transformPropertyOrLabelsExpression(
    CypherParser::OC_PropertyOrLabelsExpressionContext& ctx) {
    auto lookups = ctx.oC_PropertyLookup();
    if (!lookups.empty()) {
        auto atom = transformAtom(*ctx.oC_Atom());

        // Apply property lookups one at a time (supports a.b.c chains)
        unique_ptr<ParsedExpression> result = std::move(atom);
        for (auto *lookup : lookups) {
            auto prop = transformPropertyLookup(*lookup);
            if (auto *var = dynamic_cast<ParsedVariableExpression *>(result.get())) {
                result = make_unique<ParsedPropertyExpression>(var->GetVariableName(), std::move(prop));
            } else if (auto *propExpr = dynamic_cast<ParsedPropertyExpression *>(result.get())) {
                // Chained: a.b.c → property access on the result of a.b
                // Convert to: struct_extract(a.b, 'c') or nested property
                string parent_var = propExpr->GetVariableName();
                string parent_prop = propExpr->GetPropertyName();
                string combined_var = parent_var + "." + parent_prop;
                // For nested access: treat "a.b" as a variable and ".c" as property
                result = make_unique<ParsedPropertyExpression>(combined_var, std::move(prop));
            } else {
                // Complex case: (expr).property
                vector<unique_ptr<ParsedExpression>> args;
                args.push_back(std::move(result));
                args.push_back(make_unique<ConstantExpression>(Value(prop)));
                result = make_unique<FunctionExpression>("property", std::move(args));
            }
        }
        return result;
    }
    return transformAtom(*ctx.oC_Atom());
}

unique_ptr<ParsedExpression> CypherTransformer::transformAtom(CypherParser::OC_AtomContext& ctx) {
    if (ctx.oC_Literal())               return transformLiteral(*ctx.oC_Literal());
    if (ctx.oC_CaseExpression())        return transformCaseExpression(*ctx.oC_CaseExpression());
    if (ctx.oC_ParenthesizedExpression()) return transformParenthesizedExpression(*ctx.oC_ParenthesizedExpression());
    if (ctx.oC_FunctionInvocation())    return transformFunctionInvocation(*ctx.oC_FunctionInvocation());
    if (ctx.oC_ExistentialSubquery())   return transformExistentialSubquery(*ctx.oC_ExistentialSubquery());
    if (ctx.oC_PatternComprehension()) {
        // Pattern comprehension: [(pattern) WHERE cond | expr]
        // → __pattern_comprehension(start_var, pattern_chains..., where_expr, map_expr)
        auto *pc = ctx.oC_PatternComprehension();
        auto *rp = pc->oC_RelationshipsPattern();
        auto *start_node = rp->oC_NodePattern();
        auto chains = rp->oC_PatternElementChain();

        // Extract all node variables and edge types from the pattern
        vector<unique_ptr<ParsedExpression>> args;

        // arg 0: start node variable
        string start_var = start_node->oC_Variable()
            ? start_node->oC_Variable()->getText() : "";
        args.push_back(make_unique<ConstantExpression>(Value(start_var)));

        // arg 1: start node label
        string start_label = "";
        if (start_node->oC_NodeLabels() && !start_node->oC_NodeLabels()->oC_NodeLabel().empty()) {
            start_label = start_node->oC_NodeLabels()->oC_NodeLabel(0)->oC_LabelName()->getText();
        }
        args.push_back(make_unique<ConstantExpression>(Value(start_label)));

        // arg 2: number of hops
        args.push_back(make_unique<ConstantExpression>(Value((int32_t)chains.size())));

        // For each chain: edge type, direction, end node var, end node label
        for (auto *chain : chains) {
            string rel_type = "";
            auto *rd = chain->oC_RelationshipPattern()->oC_RelationshipDetail();
            if (rd && rd->oC_RelationshipTypes() && !rd->oC_RelationshipTypes()->oC_RelTypeName().empty()) {
                rel_type = rd->oC_RelationshipTypes()->oC_RelTypeName(0)->getText();
            }
            args.push_back(make_unique<ConstantExpression>(Value(rel_type)));

            // Direction: check for < and >
            string dir_text = chain->oC_RelationshipPattern()->getText();
            string direction = "BOTH";
            if (dir_text.find("->") != string::npos) direction = "OUT";
            else if (dir_text.find("<-") != string::npos) direction = "IN";
            args.push_back(make_unique<ConstantExpression>(Value(direction)));

            string end_var = chain->oC_NodePattern()->oC_Variable()
                ? chain->oC_NodePattern()->oC_Variable()->getText() : "";
            args.push_back(make_unique<ConstantExpression>(Value(end_var)));

            string end_label = "";
            if (chain->oC_NodePattern()->oC_NodeLabels() &&
                !chain->oC_NodePattern()->oC_NodeLabels()->oC_NodeLabel().empty()) {
                end_label = chain->oC_NodePattern()->oC_NodeLabels()->oC_NodeLabel(0)->oC_LabelName()->getText();
            }
            args.push_back(make_unique<ConstantExpression>(Value(end_label)));
        }

        // WHERE expression (optional)
        if (pc->oC_Expression().size() > 1) {
            // Expression(0) is WHERE, Expression(last) is mapping
            // Actually: oC_Expression(0) is the WHERE condition if WHERE exists
            // and the last oC_Expression is the mapping expression
            // Grammar: (WHERE SP? oC_Expression SP?)? '|' SP? oC_Expression
            // So: expressions[0] = WHERE (if exists), last expression = mapping
        }

        // Mapping expression (after |)
        auto map_expr = transformExpression(*pc->oC_Expression().back());
        args.push_back(std::move(map_expr));

        // WHERE expression if present
        if (pc->WHERE() != nullptr && pc->oC_Expression().size() >= 2) {
            auto where_expr = transformExpression(*pc->oC_Expression(0));
            args.push_back(std::move(where_expr));
        }

        return make_unique<FunctionExpression>("__pattern_comprehension", std::move(args));
    }
    if (ctx.oC_RelationshipsPattern()) {
        // Pattern expression in expression context.
        // 1-hop: (a)-[:R]-(b) → __pattern_exists(a, 'R', b)
        // 2-hop: (a)-[:R1]->()<-[:R2]-(b) → __pattern_exists_2hop(a, 'R1', 'R2', b)
        auto *rp = ctx.oC_RelationshipsPattern();
        auto *start_node = rp->oC_NodePattern();
        auto chains = rp->oC_PatternElementChain();
        if (start_node && chains.size() == 2) {
            // 2-hop pattern: (start)-[:R1]->(mid)<-[:R2]-(end)
            string start_var = start_node->oC_Variable()
                ? start_node->oC_Variable()->getText() : "";
            string end_var = chains[1]->oC_NodePattern()->oC_Variable()
                ? chains[1]->oC_NodePattern()->oC_Variable()->getText() : "";
            string rel_type1 = "", rel_type2 = "";
            auto *rd1 = chains[0]->oC_RelationshipPattern()->oC_RelationshipDetail();
            if (rd1 && rd1->oC_RelationshipTypes())
                rel_type1 = rd1->oC_RelationshipTypes()->oC_RelTypeName(0)->getText();
            auto *rd2 = chains[1]->oC_RelationshipPattern()->oC_RelationshipDetail();
            if (rd2 && rd2->oC_RelationshipTypes())
                rel_type2 = rd2->oC_RelationshipTypes()->oC_RelTypeName(0)->getText();
            vector<unique_ptr<ParsedExpression>> args;
            args.push_back(make_unique<ParsedVariableExpression>(start_var));
            args.push_back(make_unique<ConstantExpression>(Value(rel_type1)));
            args.push_back(make_unique<ConstantExpression>(Value(rel_type2)));
            args.push_back(make_unique<ParsedVariableExpression>(end_var));
            return make_unique<FunctionExpression>("__pattern_exists_2hop", std::move(args));
        } else if (start_node && !chains.empty()) {
            // 1-hop pattern
            string start_var = start_node->oC_Variable()
                ? start_node->oC_Variable()->getText() : "";
            string end_var = chains[0]->oC_NodePattern()->oC_Variable()
                ? chains[0]->oC_NodePattern()->oC_Variable()->getText() : "";
            string rel_type = "";
            auto *rel_detail = chains[0]->oC_RelationshipPattern()->oC_RelationshipDetail();
            if (rel_detail && rel_detail->oC_RelationshipTypes()) {
                rel_type = rel_detail->oC_RelationshipTypes()->oC_RelTypeName(0)->getText();
            }
            vector<unique_ptr<ParsedExpression>> args;
            args.push_back(make_unique<ParsedVariableExpression>(start_var));
            args.push_back(make_unique<ConstantExpression>(Value(rel_type)));
            args.push_back(make_unique<ParsedVariableExpression>(end_var));
            return make_unique<FunctionExpression>("__pattern_exists", std::move(args));
        }
    }
    if (ctx.oC_ReduceExpression()) {
        // reduce(acc = init, var IN list | expr)
        // → __reduce(init, 'acc', list, 'var', expr)
        auto *re = ctx.oC_ReduceExpression();
        string acc_var = re->oC_Variable(0)->getText();
        string loop_var = re->oC_Variable(1)->getText();
        auto init_expr = transformExpression(*re->oC_Expression(0));
        auto list_expr = transformExpression(*re->oC_Expression(1));
        auto body_expr = transformExpression(*re->oC_Expression(2));

        vector<unique_ptr<ParsedExpression>> args;
        args.push_back(std::move(init_expr));
        args.push_back(make_unique<ConstantExpression>(Value(acc_var)));
        args.push_back(std::move(list_expr));
        args.push_back(make_unique<ConstantExpression>(Value(loop_var)));
        args.push_back(std::move(body_expr));
        return make_unique<FunctionExpression>("__reduce", std::move(args));
    }
    if (ctx.oC_ListComprehension()) {
        // [p IN list WHERE cond | expr]
        // → __list_comprehension(list, 'p', cond_or_true, map_or_null)
        auto *lc = ctx.oC_ListComprehension();
        auto *fe = lc->oC_FilterExpression();
        auto *iic = fe->oC_IdInColl();
        string loop_var = iic->oC_Variable()->getText();
        auto source = transformBitwiseAndOperatorExpression(*iic->kU_BitwiseAndOperatorExpression());

        vector<unique_ptr<ParsedExpression>> args;
        args.push_back(std::move(source));
        args.push_back(make_unique<ConstantExpression>(Value(loop_var)));

        // Optional WHERE filter
        if (fe->oC_Where()) {
            args.push_back(transformExpression(*fe->oC_Where()->oC_Expression()));
        } else {
            args.push_back(make_unique<ConstantExpression>(Value(true)));
        }

        // Optional | mapping expression
        if (lc->oC_Expression()) {
            args.push_back(transformExpression(*lc->oC_Expression()));
        }

        return make_unique<FunctionExpression>("__list_comprehension", std::move(args));
    }
    if (ctx.oC_Variable())              return make_unique<ParsedVariableExpression>(transformVariable(*ctx.oC_Variable()));
    if (ctx.oC_MapLiteral()) return transformMapLiteral(*ctx.oC_MapLiteral());
    if (ctx.oC_Parameter()) {
        // Parameter: $name or $0 — represent as a named constant placeholder
        auto param_name = ctx.oC_Parameter()->oC_SymbolicName()
                              ? ctx.oC_Parameter()->oC_SymbolicName()->getText()
                              : ctx.oC_Parameter()->DecimalInteger()->getText();
        // Use a variable expression with $ prefix as a lightweight placeholder
        return make_unique<ParsedVariableExpression>("$" + param_name);
    }
    throw InternalException("Unsupported atom type in CypherTransformer");
}

unique_ptr<ParsedExpression> CypherTransformer::transformLiteral(CypherParser::OC_LiteralContext& ctx) {
    if (ctx.oC_NumberLiteral()) return transformNumberLiteral(*ctx.oC_NumberLiteral());
    if (ctx.oC_BooleanLiteral()) return transformBooleanLiteral(*ctx.oC_BooleanLiteral());
    if (ctx.StringLiteral())
        return make_unique<ConstantExpression>(Value(transformStringLiteral(*ctx.StringLiteral())));
    if (ctx.NULL_())
        return make_unique<ConstantExpression>(Value());  // SQLNULL
    if (ctx.oC_ListLiteral()) return transformListLiteral(*ctx.oC_ListLiteral());
    throw InternalException("Unknown literal type");
}

unique_ptr<ParsedExpression> CypherTransformer::transformNumberLiteral(
    CypherParser::OC_NumberLiteralContext& ctx) {
    if (ctx.oC_DoubleLiteral()) {
        double d = std::stod(ctx.oC_DoubleLiteral()->getText());
        return make_unique<ConstantExpression>(Value(d));
    }
    // IntegerLiteral: try int32, then int64
    auto text = ctx.oC_IntegerLiteral()->DecimalInteger()->getText();
    try {
        return make_unique<ConstantExpression>(Value((int32_t)std::stoi(text)));
    } catch (...) {}
    try {
        return make_unique<ConstantExpression>(Value((int64_t)std::stoll(text)));
    } catch (...) {}
    throw InternalException("Integer literal out of range: " + text);
}

unique_ptr<ParsedExpression> CypherTransformer::transformBooleanLiteral(
    CypherParser::OC_BooleanLiteralContext& ctx) {
    bool val = ctx.TRUE() != nullptr;
    return make_unique<ConstantExpression>(Value::BOOLEAN(val));
}

unique_ptr<ParsedExpression> CypherTransformer::transformListLiteral(
    CypherParser::OC_ListLiteralContext& ctx) {
    vector<unique_ptr<ParsedExpression>> args;
    for (auto& expr_ctx : ctx.oC_Expression()) {
        args.push_back(transformExpression(*expr_ctx));
    }
    return make_unique<FunctionExpression>("list_value", std::move(args));
}

unique_ptr<ParsedExpression> CypherTransformer::transformMapLiteral(
    CypherParser::OC_MapLiteralContext& ctx) {
    // {key1: expr1, key2: expr2, ...} → FunctionExpression("struct_pack", children)
    // Each child has its alias set to the key name so the binder can
    // construct the STRUCT type with named fields.
    auto keys = ctx.oC_PropertyKeyName();
    auto values = ctx.oC_Expression();
    D_ASSERT(keys.size() == values.size());
    vector<unique_ptr<ParsedExpression>> args;
    for (size_t i = 0; i < keys.size(); i++) {
        auto child = transformExpression(*values[i]);
        child->alias = keys[i]->getText();
        args.push_back(std::move(child));
    }
    return make_unique<FunctionExpression>("struct_pack", std::move(args));
}

unique_ptr<ParsedExpression> CypherTransformer::transformFunctionInvocation(
    CypherParser::OC_FunctionInvocationContext& ctx) {
    string name = transformFunctionName(*ctx.oC_FunctionName());
    bool distinct = ctx.DISTINCT() != nullptr;
    if (ctx.STAR()) {
        // COUNT(*)
        return make_unique<FunctionExpression>("count_star", vector<unique_ptr<ParsedExpression>>{},
                                              nullptr, nullptr, false);
    }
    vector<unique_ptr<ParsedExpression>> args;
    for (auto& expr_ctx : ctx.oC_Expression()) {
        args.push_back(transformExpression(*expr_ctx));
    }
    return make_unique<FunctionExpression>(name, std::move(args), nullptr, nullptr, distinct);
}

unique_ptr<ParsedExpression> CypherTransformer::transformCaseExpression(
    CypherParser::OC_CaseExpressionContext& ctx) {
    auto case_expr = make_unique<CaseExpression>();
    // CASE subject WHEN ... — the subject expression (optional)
    unique_ptr<ParsedExpression> subject;
    if (ctx.ELSE()) {
        if (ctx.oC_Expression().size() == 1) {
            case_expr->else_expr = transformExpression(*ctx.oC_Expression(0));
        } else {
            subject = transformExpression(*ctx.oC_Expression(0));
            case_expr->else_expr = transformExpression(*ctx.oC_Expression(1));
        }
    } else {
        if (!ctx.oC_Expression().empty()) {
            subject = transformExpression(*ctx.oC_Expression(0));
        }
    }
    for (auto& alt : ctx.oC_CaseAlternative()) {
        CaseCheck check;
        if (subject) {
            // Simple CASE: CASE x WHEN v THEN r → CASE WHEN x=v THEN r
            auto when_val = transformExpression(*alt->oC_Expression(0));
            // CASE x WHEN null → dead code (x = null is always null in
            // Cypher three-valued logic, so the branch is never taken).
            // Skip this WHEN clause entirely — the ELSE branch applies.
            if (when_val->type == ExpressionType::VALUE_CONSTANT &&
                static_cast<ConstantExpression *>(when_val.get())->value.IsNull()) {
                continue;
            }
            check.when_expr = make_unique<ComparisonExpression>(
                ExpressionType::COMPARE_EQUAL, subject->Copy(), std::move(when_val));
        } else {
            // Searched CASE: CASE WHEN cond THEN r
            check.when_expr = transformExpression(*alt->oC_Expression(0));
        }
        check.then_expr = transformExpression(*alt->oC_Expression(1));
        case_expr->case_checks.push_back(std::move(check));
    }
    if (!case_expr->else_expr) {
        case_expr->else_expr = make_unique<ConstantExpression>(Value()); // NULL
    }
    return case_expr;
}

unique_ptr<ParsedExpression> CypherTransformer::transformParenthesizedExpression(
    CypherParser::OC_ParenthesizedExpressionContext& ctx) {
    return transformExpression(*ctx.oC_Expression());
}

unique_ptr<ParsedExpression> CypherTransformer::transformExistentialSubquery(
    CypherParser::OC_ExistentialSubqueryContext& ctx) {
    // EXISTS { MATCH pattern [WHERE expr] }
    auto expr = make_unique<ExistsSubqueryExpression>(false);

    // Transform MATCH pattern
    auto* pattern_ctx = ctx.oC_Pattern();
    if (pattern_ctx) {
        for (auto* part_ctx : pattern_ctx->oC_PatternPart()) {
            auto* elem_ctx = part_ctx->oC_AnonymousPatternPart();
            if (elem_ctx) {
                expr->patterns.push_back(transformPatternElement(*elem_ctx->oC_PatternElement()));
            }
        }
    }

    // Transform optional WHERE clause
    auto* where_ctx = ctx.oC_Where();
    if (where_ctx) {
        expr->where_expr = transformExpression(*where_ctx->oC_Expression());
    }

    return expr;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

string CypherTransformer::transformVariable(CypherParser::OC_VariableContext& ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

string CypherTransformer::transformFunctionName(CypherParser::OC_FunctionNameContext& ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

string CypherTransformer::transformPropertyKeyName(CypherParser::OC_PropertyKeyNameContext& ctx) {
    return transformSchemaName(*ctx.oC_SchemaName());
}

string CypherTransformer::transformPropertyLookup(CypherParser::OC_PropertyLookupContext& ctx) {
    return transformPropertyKeyName(*ctx.oC_PropertyKeyName());
}

string CypherTransformer::transformSchemaName(CypherParser::OC_SchemaNameContext& ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

string CypherTransformer::transformSymbolicName(CypherParser::OC_SymbolicNameContext& ctx) {
    // Strip backtick-escaping if present
    auto text = ctx.getText();
    if (!text.empty() && text.front() == '`' && text.back() == '`') {
        return text.substr(1, text.size() - 2);
    }
    return text;
}

string CypherTransformer::transformStringLiteral(antlr4::tree::TerminalNode& literal) {
    auto text = literal.getText();
    // Strip surrounding quotes (single or double)
    if (text.size() >= 2 &&
        ((text.front() == '\'' && text.back() == '\'') ||
         (text.front() == '"'  && text.back() == '"'))) {
        return text.substr(1, text.size() - 2);
    }
    return text;
}

} // namespace duckdb

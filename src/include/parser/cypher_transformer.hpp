#pragma once

#include "CypherParser.h"
// ANTLR4 defines INVALID_INDEX as a macro; undef it to avoid collisions with
// DuckDB's DConstants::INVALID_INDEX in other headers included after this.
#ifdef INVALID_INDEX
#undef INVALID_INDEX
#endif

#include "parser/query/regular_query.hpp"
#include "parser/query/graph_pattern/pattern_element.hpp"
#include "parser/parsed_expression.hpp"

#include <string>
#include <vector>
#include <memory>
#include <utility>

namespace duckdb {

// Transforms an ANTLR4 Cypher parse tree into TurboLynx-native AST types.
// All output types live in namespace duckdb (TurboLynx codebase convention).
class CypherTransformer {
public:
    explicit CypherTransformer(CypherParser::OC_CypherContext& root) : root_(root) {}

    // Entry point: returns the top-level RegularQuery (or nullptr on DDL/unsupported).
    unique_ptr<RegularQuery> transform();

private:
    // ---- Query structure ----
    unique_ptr<RegularQuery>  transformRegularQuery(CypherParser::OC_RegularQueryContext& ctx);
    unique_ptr<SingleQuery>   transformSingleQuery(CypherParser::OC_SingleQueryContext& ctx);
    unique_ptr<SingleQuery>   transformSinglePartQuery(CypherParser::OC_SinglePartQueryContext& ctx);
    unique_ptr<QueryPart>     transformQueryPart(CypherParser::KU_QueryPartContext& ctx);

    // ---- Clauses ----
    unique_ptr<ReadingClause>  transformReadingClause(CypherParser::OC_ReadingClauseContext& ctx);
    unique_ptr<ReadingClause>  transformMatch(CypherParser::OC_MatchContext& ctx);
    unique_ptr<ReadingClause>  transformUnwind(CypherParser::OC_UnwindContext& ctx);
    unique_ptr<UpdatingClause> transformUpdatingClause(CypherParser::OC_UpdatingClauseContext& ctx);
    unique_ptr<UpdatingClause> transformCreateClause(CypherParser::OC_CreateContext& ctx);
    unique_ptr<UpdatingClause> transformSetClause(CypherParser::OC_SetContext& ctx);

    // ---- Return / With / Projection ----
    unique_ptr<WithClause>      transformWith(CypherParser::OC_WithContext& ctx);
    unique_ptr<ReturnClause>    transformReturn(CypherParser::OC_ReturnContext& ctx);
    unique_ptr<ProjectionBody>  transformProjectionBody(CypherParser::OC_ProjectionBodyContext& ctx);
    vector<unique_ptr<ParsedExpression>> transformProjectionItems(CypherParser::OC_ProjectionItemsContext& ctx);
    unique_ptr<ParsedExpression> transformProjectionItem(CypherParser::OC_ProjectionItemContext& ctx);
    unique_ptr<ParsedExpression> transformWhere(CypherParser::OC_WhereContext& ctx);

    // ---- Graph patterns ----
    vector<unique_ptr<PatternElement>> transformPattern(CypherParser::OC_PatternContext& ctx);
    unique_ptr<PatternElement>  transformPatternPart(CypherParser::OC_PatternPartContext& ctx);
    unique_ptr<PatternElement>  transformAnonymousPatternPart(CypherParser::OC_AnonymousPatternPartContext& ctx);
    unique_ptr<PatternElement>  transformPatternElement(CypherParser::OC_PatternElementContext& ctx);
    unique_ptr<NodePattern>     transformNodePattern(CypherParser::OC_NodePatternContext& ctx);
    unique_ptr<RelPattern>      transformRelationshipPattern(CypherParser::OC_RelationshipPatternContext& ctx);

    vector<string> transformNodeLabels(CypherParser::OC_NodeLabelsContext& ctx);
    vector<string> transformRelTypes(CypherParser::OC_RelationshipTypesContext& ctx);
    vector<pair<string, unique_ptr<ParsedExpression>>> transformProperties(CypherParser::KU_PropertiesContext& ctx);

    // ---- Expressions ----
    unique_ptr<ParsedExpression> transformExpression(CypherParser::OC_ExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformOrExpression(CypherParser::OC_OrExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformXorExpression(CypherParser::OC_XorExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformAndExpression(CypherParser::OC_AndExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformNotExpression(CypherParser::OC_NotExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformComparisonExpression(CypherParser::OC_ComparisonExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformBitwiseOrOperatorExpression(CypherParser::KU_BitwiseOrOperatorExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformBitwiseAndOperatorExpression(CypherParser::KU_BitwiseAndOperatorExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformBitShiftOperatorExpression(CypherParser::KU_BitShiftOperatorExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformAddOrSubtractExpression(CypherParser::OC_AddOrSubtractExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformMultiplyDivideModuloExpression(CypherParser::OC_MultiplyDivideModuloExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformPowerOfExpression(CypherParser::OC_PowerOfExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformUnaryAddSubtractOrFactorialExpression(CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformStringListNullOperatorExpression(CypherParser::OC_StringListNullOperatorExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformStringOperatorExpression(CypherParser::OC_StringOperatorExpressionContext& ctx, unique_ptr<ParsedExpression> left);
    unique_ptr<ParsedExpression> transformListOperatorExpression(CypherParser::OC_ListOperatorExpressionContext& ctx, unique_ptr<ParsedExpression> left);
    unique_ptr<ParsedExpression> transformNullOperatorExpression(CypherParser::OC_NullOperatorExpressionContext& ctx, unique_ptr<ParsedExpression> left);
    unique_ptr<ParsedExpression> transformPropertyOrLabelsExpression(CypherParser::OC_PropertyOrLabelsExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformAtom(CypherParser::OC_AtomContext& ctx);
    unique_ptr<ParsedExpression> transformLiteral(CypherParser::OC_LiteralContext& ctx);
    unique_ptr<ParsedExpression> transformNumberLiteral(CypherParser::OC_NumberLiteralContext& ctx);
    unique_ptr<ParsedExpression> transformBooleanLiteral(CypherParser::OC_BooleanLiteralContext& ctx);
    unique_ptr<ParsedExpression> transformListLiteral(CypherParser::OC_ListLiteralContext& ctx);
    unique_ptr<ParsedExpression> transformMapLiteral(CypherParser::OC_MapLiteralContext& ctx);
    unique_ptr<ParsedExpression> transformFunctionInvocation(CypherParser::OC_FunctionInvocationContext& ctx);
    unique_ptr<ParsedExpression> transformCaseExpression(CypherParser::OC_CaseExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformParenthesizedExpression(CypherParser::OC_ParenthesizedExpressionContext& ctx);
    unique_ptr<ParsedExpression> transformExistentialSubquery(CypherParser::OC_ExistentialSubqueryContext& ctx);

    // ---- Helpers ----
    string transformVariable(CypherParser::OC_VariableContext& ctx);
    string transformFunctionName(CypherParser::OC_FunctionNameContext& ctx);
    string transformPropertyKeyName(CypherParser::OC_PropertyKeyNameContext& ctx);
    string transformPropertyLookup(CypherParser::OC_PropertyLookupContext& ctx);
    string transformSchemaName(CypherParser::OC_SchemaNameContext& ctx);
    string transformSymbolicName(CypherParser::OC_SymbolicNameContext& ctx);
    string transformStringLiteral(antlr4::tree::TerminalNode& literal);

    CypherParser::OC_CypherContext& root_;
};

} // namespace duckdb

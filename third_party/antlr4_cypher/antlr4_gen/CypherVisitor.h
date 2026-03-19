
// Generated from Cypher.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"
#include "CypherParser.h"



/**
 * This class defines an abstract visitor for a parse tree
 * produced by CypherParser.
 */
class  CypherVisitor : public antlr4::tree::AbstractParseTreeVisitor {
public:

  /**
   * Visit parse trees produced by CypherParser.
   */
    virtual std::any visitOC_Cypher(CypherParser::OC_CypherContext *context) = 0;

    virtual std::any visitKU_CopyCSV(CypherParser::KU_CopyCSVContext *context) = 0;

    virtual std::any visitKU_ParsingOptions(CypherParser::KU_ParsingOptionsContext *context) = 0;

    virtual std::any visitKU_ParsingOption(CypherParser::KU_ParsingOptionContext *context) = 0;

    virtual std::any visitKU_DDL(CypherParser::KU_DDLContext *context) = 0;

    virtual std::any visitKU_CreateNode(CypherParser::KU_CreateNodeContext *context) = 0;

    virtual std::any visitKU_CreateRel(CypherParser::KU_CreateRelContext *context) = 0;

    virtual std::any visitKU_DropTable(CypherParser::KU_DropTableContext *context) = 0;

    virtual std::any visitKU_RelConnections(CypherParser::KU_RelConnectionsContext *context) = 0;

    virtual std::any visitKU_RelConnection(CypherParser::KU_RelConnectionContext *context) = 0;

    virtual std::any visitKU_NodeLabels(CypherParser::KU_NodeLabelsContext *context) = 0;

    virtual std::any visitKU_PropertyDefinitions(CypherParser::KU_PropertyDefinitionsContext *context) = 0;

    virtual std::any visitKU_PropertyDefinition(CypherParser::KU_PropertyDefinitionContext *context) = 0;

    virtual std::any visitKU_CreateNodeConstraint(CypherParser::KU_CreateNodeConstraintContext *context) = 0;

    virtual std::any visitKU_DataType(CypherParser::KU_DataTypeContext *context) = 0;

    virtual std::any visitKU_ListIdentifiers(CypherParser::KU_ListIdentifiersContext *context) = 0;

    virtual std::any visitKU_ListIdentifier(CypherParser::KU_ListIdentifierContext *context) = 0;

    virtual std::any visitOC_AnyCypherOption(CypherParser::OC_AnyCypherOptionContext *context) = 0;

    virtual std::any visitOC_Explain(CypherParser::OC_ExplainContext *context) = 0;

    virtual std::any visitOC_Profile(CypherParser::OC_ProfileContext *context) = 0;

    virtual std::any visitOC_Statement(CypherParser::OC_StatementContext *context) = 0;

    virtual std::any visitOC_Query(CypherParser::OC_QueryContext *context) = 0;

    virtual std::any visitOC_RegularQuery(CypherParser::OC_RegularQueryContext *context) = 0;

    virtual std::any visitOC_Union(CypherParser::OC_UnionContext *context) = 0;

    virtual std::any visitOC_SingleQuery(CypherParser::OC_SingleQueryContext *context) = 0;

    virtual std::any visitOC_SinglePartQuery(CypherParser::OC_SinglePartQueryContext *context) = 0;

    virtual std::any visitOC_MultiPartQuery(CypherParser::OC_MultiPartQueryContext *context) = 0;

    virtual std::any visitKU_QueryPart(CypherParser::KU_QueryPartContext *context) = 0;

    virtual std::any visitOC_UpdatingClause(CypherParser::OC_UpdatingClauseContext *context) = 0;

    virtual std::any visitOC_ReadingClause(CypherParser::OC_ReadingClauseContext *context) = 0;

    virtual std::any visitOC_Match(CypherParser::OC_MatchContext *context) = 0;

    virtual std::any visitOC_Unwind(CypherParser::OC_UnwindContext *context) = 0;

    virtual std::any visitOC_Create(CypherParser::OC_CreateContext *context) = 0;

    virtual std::any visitOC_Set(CypherParser::OC_SetContext *context) = 0;

    virtual std::any visitOC_SetItem(CypherParser::OC_SetItemContext *context) = 0;

    virtual std::any visitOC_Delete(CypherParser::OC_DeleteContext *context) = 0;

    virtual std::any visitOC_With(CypherParser::OC_WithContext *context) = 0;

    virtual std::any visitOC_Return(CypherParser::OC_ReturnContext *context) = 0;

    virtual std::any visitOC_ProjectionBody(CypherParser::OC_ProjectionBodyContext *context) = 0;

    virtual std::any visitOC_ProjectionItems(CypherParser::OC_ProjectionItemsContext *context) = 0;

    virtual std::any visitOC_ProjectionItem(CypherParser::OC_ProjectionItemContext *context) = 0;

    virtual std::any visitOC_Order(CypherParser::OC_OrderContext *context) = 0;

    virtual std::any visitOC_Skip(CypherParser::OC_SkipContext *context) = 0;

    virtual std::any visitOC_Limit(CypherParser::OC_LimitContext *context) = 0;

    virtual std::any visitOC_SortItem(CypherParser::OC_SortItemContext *context) = 0;

    virtual std::any visitOC_Where(CypherParser::OC_WhereContext *context) = 0;

    virtual std::any visitOC_Pattern(CypherParser::OC_PatternContext *context) = 0;

    virtual std::any visitOC_PatternPart(CypherParser::OC_PatternPartContext *context) = 0;

    virtual std::any visitOC_AnonymousPatternPart(CypherParser::OC_AnonymousPatternPartContext *context) = 0;

    virtual std::any visitOC_ShortestPathPattern(CypherParser::OC_ShortestPathPatternContext *context) = 0;

    virtual std::any visitOC_PatternElement(CypherParser::OC_PatternElementContext *context) = 0;

    virtual std::any visitOC_NodePattern(CypherParser::OC_NodePatternContext *context) = 0;

    virtual std::any visitOC_PatternElementChain(CypherParser::OC_PatternElementChainContext *context) = 0;

    virtual std::any visitOC_RelationshipPattern(CypherParser::OC_RelationshipPatternContext *context) = 0;

    virtual std::any visitOC_RelationshipDetail(CypherParser::OC_RelationshipDetailContext *context) = 0;

    virtual std::any visitKU_Properties(CypherParser::KU_PropertiesContext *context) = 0;

    virtual std::any visitOC_RelationshipTypes(CypherParser::OC_RelationshipTypesContext *context) = 0;

    virtual std::any visitOC_NodeLabels(CypherParser::OC_NodeLabelsContext *context) = 0;

    virtual std::any visitOC_NodeLabel(CypherParser::OC_NodeLabelContext *context) = 0;

    virtual std::any visitOC_RangeLiteral(CypherParser::OC_RangeLiteralContext *context) = 0;

    virtual std::any visitOC_RangeStartLiteral(CypherParser::OC_RangeStartLiteralContext *context) = 0;

    virtual std::any visitOC_RangeEndLiteral(CypherParser::OC_RangeEndLiteralContext *context) = 0;

    virtual std::any visitOC_LabelName(CypherParser::OC_LabelNameContext *context) = 0;

    virtual std::any visitOC_RelTypeName(CypherParser::OC_RelTypeNameContext *context) = 0;

    virtual std::any visitOC_Expression(CypherParser::OC_ExpressionContext *context) = 0;

    virtual std::any visitOC_OrExpression(CypherParser::OC_OrExpressionContext *context) = 0;

    virtual std::any visitOC_XorExpression(CypherParser::OC_XorExpressionContext *context) = 0;

    virtual std::any visitOC_AndExpression(CypherParser::OC_AndExpressionContext *context) = 0;

    virtual std::any visitOC_NotExpression(CypherParser::OC_NotExpressionContext *context) = 0;

    virtual std::any visitOC_ComparisonExpression(CypherParser::OC_ComparisonExpressionContext *context) = 0;

    virtual std::any visitKU_ComparisonOperator(CypherParser::KU_ComparisonOperatorContext *context) = 0;

    virtual std::any visitKU_BitwiseOrOperatorExpression(CypherParser::KU_BitwiseOrOperatorExpressionContext *context) = 0;

    virtual std::any visitKU_BitwiseAndOperatorExpression(CypherParser::KU_BitwiseAndOperatorExpressionContext *context) = 0;

    virtual std::any visitKU_BitShiftOperatorExpression(CypherParser::KU_BitShiftOperatorExpressionContext *context) = 0;

    virtual std::any visitKU_BitShiftOperator(CypherParser::KU_BitShiftOperatorContext *context) = 0;

    virtual std::any visitOC_AddOrSubtractExpression(CypherParser::OC_AddOrSubtractExpressionContext *context) = 0;

    virtual std::any visitKU_AddOrSubtractOperator(CypherParser::KU_AddOrSubtractOperatorContext *context) = 0;

    virtual std::any visitOC_MultiplyDivideModuloExpression(CypherParser::OC_MultiplyDivideModuloExpressionContext *context) = 0;

    virtual std::any visitKU_MultiplyDivideModuloOperator(CypherParser::KU_MultiplyDivideModuloOperatorContext *context) = 0;

    virtual std::any visitOC_PowerOfExpression(CypherParser::OC_PowerOfExpressionContext *context) = 0;

    virtual std::any visitOC_UnaryAddSubtractOrFactorialExpression(CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext *context) = 0;

    virtual std::any visitOC_StringListNullOperatorExpression(CypherParser::OC_StringListNullOperatorExpressionContext *context) = 0;

    virtual std::any visitOC_ListOperatorExpression(CypherParser::OC_ListOperatorExpressionContext *context) = 0;

    virtual std::any visitKU_ListPropertyOrLabelsExpression(CypherParser::KU_ListPropertyOrLabelsExpressionContext *context) = 0;

    virtual std::any visitKU_ListExtractOperatorExpression(CypherParser::KU_ListExtractOperatorExpressionContext *context) = 0;

    virtual std::any visitKU_ListSliceOperatorExpression(CypherParser::KU_ListSliceOperatorExpressionContext *context) = 0;

    virtual std::any visitOC_StringOperatorExpression(CypherParser::OC_StringOperatorExpressionContext *context) = 0;

    virtual std::any visitOC_NullOperatorExpression(CypherParser::OC_NullOperatorExpressionContext *context) = 0;

    virtual std::any visitOC_PropertyOrLabelsExpression(CypherParser::OC_PropertyOrLabelsExpressionContext *context) = 0;

    virtual std::any visitOC_Atom(CypherParser::OC_AtomContext *context) = 0;

    virtual std::any visitOC_MapLiteral(CypherParser::OC_MapLiteralContext *context) = 0;

    virtual std::any visitOC_Literal(CypherParser::OC_LiteralContext *context) = 0;

    virtual std::any visitOC_BooleanLiteral(CypherParser::OC_BooleanLiteralContext *context) = 0;

    virtual std::any visitOC_ListLiteral(CypherParser::OC_ListLiteralContext *context) = 0;

    virtual std::any visitOC_ParenthesizedExpression(CypherParser::OC_ParenthesizedExpressionContext *context) = 0;

    virtual std::any visitOC_RelationshipsPattern(CypherParser::OC_RelationshipsPatternContext *context) = 0;

    virtual std::any visitOC_FilterExpression(CypherParser::OC_FilterExpressionContext *context) = 0;

    virtual std::any visitOC_IdInColl(CypherParser::OC_IdInCollContext *context) = 0;

    virtual std::any visitOC_FunctionInvocation(CypherParser::OC_FunctionInvocationContext *context) = 0;

    virtual std::any visitOC_FunctionName(CypherParser::OC_FunctionNameContext *context) = 0;

    virtual std::any visitOC_ExistentialSubquery(CypherParser::OC_ExistentialSubqueryContext *context) = 0;

    virtual std::any visitOC_ListComprehension(CypherParser::OC_ListComprehensionContext *context) = 0;

    virtual std::any visitOC_PatternComprehension(CypherParser::OC_PatternComprehensionContext *context) = 0;

    virtual std::any visitOC_PropertyLookup(CypherParser::OC_PropertyLookupContext *context) = 0;

    virtual std::any visitOC_CaseExpression(CypherParser::OC_CaseExpressionContext *context) = 0;

    virtual std::any visitOC_CaseAlternative(CypherParser::OC_CaseAlternativeContext *context) = 0;

    virtual std::any visitOC_Variable(CypherParser::OC_VariableContext *context) = 0;

    virtual std::any visitOC_NumberLiteral(CypherParser::OC_NumberLiteralContext *context) = 0;

    virtual std::any visitOC_Parameter(CypherParser::OC_ParameterContext *context) = 0;

    virtual std::any visitOC_PropertyExpression(CypherParser::OC_PropertyExpressionContext *context) = 0;

    virtual std::any visitOC_PropertyKeyName(CypherParser::OC_PropertyKeyNameContext *context) = 0;

    virtual std::any visitOC_IntegerLiteral(CypherParser::OC_IntegerLiteralContext *context) = 0;

    virtual std::any visitOC_DoubleLiteral(CypherParser::OC_DoubleLiteralContext *context) = 0;

    virtual std::any visitOC_SchemaName(CypherParser::OC_SchemaNameContext *context) = 0;

    virtual std::any visitOC_SymbolicName(CypherParser::OC_SymbolicNameContext *context) = 0;

    virtual std::any visitOC_LeftArrowHead(CypherParser::OC_LeftArrowHeadContext *context) = 0;

    virtual std::any visitOC_RightArrowHead(CypherParser::OC_RightArrowHeadContext *context) = 0;

    virtual std::any visitOC_Dash(CypherParser::OC_DashContext *context) = 0;


};


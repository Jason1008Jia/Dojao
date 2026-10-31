// Generated from SqlBase.g4 by ANTLR 4.13.2
package com.dojao.grammar.sql;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link SqlBaseParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface SqlBaseVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleStatement(SqlBaseParser.SingleStatementContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementDefault(SqlBaseParser.StatementDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementInsert}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementInsert(SqlBaseParser.StatementInsertContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementCreateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementCreateTable(SqlBaseParser.StatementCreateTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementCreateTableAsSelect}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementCreateTableAsSelect(SqlBaseParser.StatementCreateTableAsSelectContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementCreateCatalog}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementCreateCatalog(SqlBaseParser.StatementCreateCatalogContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementCreateSchema}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementCreateSchema(SqlBaseParser.StatementCreateSchemaContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementCreateView}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementCreateView(SqlBaseParser.StatementCreateViewContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementCreateFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementCreateFunction(SqlBaseParser.StatementCreateFunctionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementDropCatalog}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementDropCatalog(SqlBaseParser.StatementDropCatalogContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementDropSchema}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementDropSchema(SqlBaseParser.StatementDropSchemaContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementDropTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementDropTable(SqlBaseParser.StatementDropTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementTruncateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementTruncateTable(SqlBaseParser.StatementTruncateTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementDropView}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementDropView(SqlBaseParser.StatementDropViewContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementDropFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementDropFunction(SqlBaseParser.StatementDropFunctionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementAlterTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementAlterTable(SqlBaseParser.StatementAlterTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code use}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUse(SqlBaseParser.UseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showCatalogs}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowCatalogs(SqlBaseParser.ShowCatalogsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showSchemas}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowSchemas(SqlBaseParser.ShowSchemasContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showTables}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowTables(SqlBaseParser.ShowTablesContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowColumns(SqlBaseParser.ShowColumnsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showCreateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowCreateTable(SqlBaseParser.ShowCreateTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code describe}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribe(SqlBaseParser.DescribeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code explain}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExplain(SqlBaseParser.ExplainContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#insertStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInsertStatement(SqlBaseParser.InsertStatementContext ctx);
	/**
	 * Visit a parse tree produced by the {@code values}
	 * labeled alternative in {@link SqlBaseParser#statementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementqueryTermqueryTermqueryTermqueryPrimaryqueryPrimaryselectItemselectItemrelationrelationrelationrelationPrimaryrelationPrimaryrelationPrimaryrelationPrimaryrelationPrimarybooleanExpressionbooleanExpressionbooleanExpressionbooleanExpressionpredicatepredicatepredicatepredicatepredicatepredicatepredicatevalueExpressionvalueExpressionvalueExpressionvalueExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionwindowSpecificationwindowSpecificationgroupBygroupBygroupBygroupBygroupBymultidimGroupmultidimGroupmultidimGroup}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValues(SqlBaseParser.ValuesContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#createTableStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateTableStatement(SqlBaseParser.CreateTableStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableElement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableElement(SqlBaseParser.TableElementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#columnDefinition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnDefinition(SqlBaseParser.ColumnDefinitionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableProperties}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableProperties(SqlBaseParser.TablePropertiesContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#property}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitProperty(SqlBaseParser.PropertyContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#createTableAsSelectStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateTableAsSelectStatement(SqlBaseParser.CreateTableAsSelectStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#createCatalogStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateCatalogStatement(SqlBaseParser.CreateCatalogStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#dropCatalogStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropCatalogStatement(SqlBaseParser.DropCatalogStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#createSchemaStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateSchemaStatement(SqlBaseParser.CreateSchemaStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#dropSchemaStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropSchemaStatement(SqlBaseParser.DropSchemaStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#dropTableStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropTableStatement(SqlBaseParser.DropTableStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#truncateTableStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTruncateTableStatement(SqlBaseParser.TruncateTableStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#alterTableStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAlterTableStatement(SqlBaseParser.AlterTableStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#createViewStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateViewStatement(SqlBaseParser.CreateViewStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#dropViewStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropViewStatement(SqlBaseParser.DropViewStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#createFunctionStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateFunctionStatement(SqlBaseParser.CreateFunctionStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#dropFunctionStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropFunctionStatement(SqlBaseParser.DropFunctionStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#query}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuery(SqlBaseParser.QueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#queryNoWith}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryNoWith(SqlBaseParser.QueryNoWithContext ctx);
	/**
	 * Visit a parse tree produced by the {@code intersect}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntersect(SqlBaseParser.IntersectContext ctx);
	/**
	 * Visit a parse tree produced by the {@code queryTermDefault}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetOperation(SqlBaseParser.SetOperationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubquery(SqlBaseParser.SubqueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#querySpecification}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuerySpecification(SqlBaseParser.QuerySpecificationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code selectSingle}
	 * labeled alternative in {@link SqlBaseParser#selectItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelectSingle(SqlBaseParser.SelectSingleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code selectAll}
	 * labeled alternative in {@link SqlBaseParser#selectItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelectAll(SqlBaseParser.SelectAllContext ctx);
	/**
	 * Visit a parse tree produced by the {@code relationDefault}
	 * labeled alternative in {@link SqlBaseParser#relation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelationDefault(SqlBaseParser.RelationDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code subqueryRelation}
	 * labeled alternative in {@link SqlBaseParser#relation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubqueryRelation(SqlBaseParser.SubqueryRelationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code joinRelation}
	 * labeled alternative in {@link SqlBaseParser#relation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinRelation(SqlBaseParser.JoinRelationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#sampledRelation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSampledRelation(SqlBaseParser.SampledRelationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#aliasedRelation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAliasedRelation(SqlBaseParser.AliasedRelationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code tableNameRef}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableNameRef(SqlBaseParser.TableNameRefContext ctx);
	/**
	 * Visit a parse tree produced by the {@code qualifiedNameRef}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQualifiedNameRef(SqlBaseParser.QualifiedNameRefContext ctx);
	/**
	 * Visit a parse tree produced by the {@code subqueryRelationPrimary}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubqueryRelationPrimary(SqlBaseParser.SubqueryRelationPrimaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code unnest}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnnest(SqlBaseParser.UnnestContext ctx);
	/**
	 * Visit a parse tree produced by the {@code tableFunction}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableFunction(SqlBaseParser.TableFunctionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#functionArgument}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionArgument(SqlBaseParser.FunctionArgumentContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#columnAliases}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnAliases(SqlBaseParser.ColumnAliasesContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpression(SqlBaseParser.ExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogicalNot(SqlBaseParser.LogicalNotContext ctx);
	/**
	 * Visit a parse tree produced by the {@code booleanDefault}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanDefault(SqlBaseParser.BooleanDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code booleanPredicate}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanPredicate(SqlBaseParser.BooleanPredicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logicalBinary}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComparison(SqlBaseParser.ComparisonContext ctx);
	/**
	 * Visit a parse tree produced by the {@code between}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBetween(SqlBaseParser.BetweenContext ctx);
	/**
	 * Visit a parse tree produced by the {@code inList}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInList(SqlBaseParser.InListContext ctx);
	/**
	 * Visit a parse tree produced by the {@code inSubquery}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInSubquery(SqlBaseParser.InSubqueryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExists(SqlBaseParser.ExistsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code like}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLike(SqlBaseParser.LikeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code nullPredicate}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNullPredicate(SqlBaseParser.NullPredicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code atTimeZone}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAtTimeZone(SqlBaseParser.AtTimeZoneContext ctx);
	/**
	 * Visit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSimpleCase(SqlBaseParser.SimpleCaseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnReference(SqlBaseParser.ColumnReferenceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNullLiteral(SqlBaseParser.NullLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubscript(SqlBaseParser.SubscriptContext ctx);
	/**
	 * Visit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCast(SqlBaseParser.CastContext ctx);
	/**
	 * Visit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStringLiteral(SqlBaseParser.StringLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arrayConstructor}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArrayConstructor(SqlBaseParser.ArrayConstructorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDoubleLiteral(SqlBaseParser.DoubleLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code functionCallExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionCallExpression(SqlBaseParser.FunctionCallExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntervalLiteral(SqlBaseParser.IntervalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code tryCast}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTryCast(SqlBaseParser.TryCastContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#functionCall}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionCall(SqlBaseParser.FunctionCallContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#whenClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWhenClause(SqlBaseParser.WhenClauseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code windowReference}
	 * labeled alternative in {@link SqlBaseParser#windowSpecification}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindowReference(SqlBaseParser.WindowReferenceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code windowDefinitionInline}
	 * labeled alternative in {@link SqlBaseParser#windowSpecification}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindowDefinitionInline(SqlBaseParser.WindowDefinitionInlineContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#windowName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindowName(SqlBaseParser.WindowNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#windowDefinition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindowDefinition(SqlBaseParser.WindowDefinitionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#windowFrame}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindowFrame(SqlBaseParser.WindowFrameContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#frameBound}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFrameBound(SqlBaseParser.FrameBoundContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#sortItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSortItem(SqlBaseParser.SortItemContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#setQuantifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetQuantifier(SqlBaseParser.SetQuantifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code groupingSets}
	 * labeled alternative in {@link SqlBaseParser#groupBy}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupingSets(SqlBaseParser.GroupingSetsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code cube}
	 * labeled alternative in {@link SqlBaseParser#groupBy}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCube(SqlBaseParser.CubeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code rollup}
	 * labeled alternative in {@link SqlBaseParser#groupBy}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRollup(SqlBaseParser.RollupContext ctx);
	/**
	 * Visit a parse tree produced by the {@code simpleGroupBy}
	 * labeled alternative in {@link SqlBaseParser#groupBy}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSimpleGroupBy(SqlBaseParser.SimpleGroupByContext ctx);
	/**
	 * Visit a parse tree produced by the {@code parenthesizedGroupBy}
	 * labeled alternative in {@link SqlBaseParser#groupBy}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParenthesizedGroupBy(SqlBaseParser.ParenthesizedGroupByContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#groupingSet}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupingSet(SqlBaseParser.GroupingSetContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#dataType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDataType(SqlBaseParser.DataTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#commonTableExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCommonTableExpression(SqlBaseParser.CommonTableExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#timeZoneSpecifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTimeZoneSpecifier(SqlBaseParser.TimeZoneSpecifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#comparisonOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComparisonOperator(SqlBaseParser.ComparisonOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#booleanValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanValue(SqlBaseParser.BooleanValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#interval}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInterval(SqlBaseParser.IntervalContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#intervalField}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntervalField(SqlBaseParser.IntervalFieldContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#sampleType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSampleType(SqlBaseParser.SampleTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#qualifiedName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQualifiedName(SqlBaseParser.QualifiedNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableName(SqlBaseParser.TableNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifier(SqlBaseParser.IdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#string}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitString(SqlBaseParser.StringContext ctx);
	/**
	 * Visit a parse tree produced by the {@code bareGroupingSets}
	 * labeled alternative in {@link SqlBaseParser#multidimGroup}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBareGroupingSets(SqlBaseParser.BareGroupingSetsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code bareCube}
	 * labeled alternative in {@link SqlBaseParser#multidimGroup}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBareCube(SqlBaseParser.BareCubeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code bareRollup}
	 * labeled alternative in {@link SqlBaseParser#multidimGroup}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBareRollup(SqlBaseParser.BareRollupContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#nonReserved}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNonReserved(SqlBaseParser.NonReservedContext ctx);
}
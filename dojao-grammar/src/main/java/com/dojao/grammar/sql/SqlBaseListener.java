// Generated from SqlBase.g4 by ANTLR 4.13.2
package com.dojao.grammar.sql;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link SqlBaseParser}.
 */
public interface SqlBaseListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void enterSingleStatement(SqlBaseParser.SingleStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void exitSingleStatement(SqlBaseParser.SingleStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementDefault(SqlBaseParser.StatementDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementDefault(SqlBaseParser.StatementDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementInsert}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementInsert(SqlBaseParser.StatementInsertContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementInsert}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementInsert(SqlBaseParser.StatementInsertContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementCreateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementCreateTable(SqlBaseParser.StatementCreateTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementCreateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementCreateTable(SqlBaseParser.StatementCreateTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementCreateTableAsSelect}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementCreateTableAsSelect(SqlBaseParser.StatementCreateTableAsSelectContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementCreateTableAsSelect}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementCreateTableAsSelect(SqlBaseParser.StatementCreateTableAsSelectContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementCreateCatalog}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementCreateCatalog(SqlBaseParser.StatementCreateCatalogContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementCreateCatalog}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementCreateCatalog(SqlBaseParser.StatementCreateCatalogContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementCreateSchema}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementCreateSchema(SqlBaseParser.StatementCreateSchemaContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementCreateSchema}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementCreateSchema(SqlBaseParser.StatementCreateSchemaContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementCreateView}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementCreateView(SqlBaseParser.StatementCreateViewContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementCreateView}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementCreateView(SqlBaseParser.StatementCreateViewContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementCreateFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementCreateFunction(SqlBaseParser.StatementCreateFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementCreateFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementCreateFunction(SqlBaseParser.StatementCreateFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementDropCatalog}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementDropCatalog(SqlBaseParser.StatementDropCatalogContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementDropCatalog}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementDropCatalog(SqlBaseParser.StatementDropCatalogContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementDropSchema}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementDropSchema(SqlBaseParser.StatementDropSchemaContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementDropSchema}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementDropSchema(SqlBaseParser.StatementDropSchemaContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementDropTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementDropTable(SqlBaseParser.StatementDropTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementDropTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementDropTable(SqlBaseParser.StatementDropTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementTruncateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementTruncateTable(SqlBaseParser.StatementTruncateTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementTruncateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementTruncateTable(SqlBaseParser.StatementTruncateTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementDropView}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementDropView(SqlBaseParser.StatementDropViewContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementDropView}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementDropView(SqlBaseParser.StatementDropViewContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementDropFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementDropFunction(SqlBaseParser.StatementDropFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementDropFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementDropFunction(SqlBaseParser.StatementDropFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementAlterTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementAlterTable(SqlBaseParser.StatementAlterTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementAlterTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementAlterTable(SqlBaseParser.StatementAlterTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code use}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterUse(SqlBaseParser.UseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code use}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitUse(SqlBaseParser.UseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCatalogs}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowCatalogs(SqlBaseParser.ShowCatalogsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCatalogs}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowCatalogs(SqlBaseParser.ShowCatalogsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showSchemas}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowSchemas(SqlBaseParser.ShowSchemasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showSchemas}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowSchemas(SqlBaseParser.ShowSchemasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTables}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowTables(SqlBaseParser.ShowTablesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTables}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowTables(SqlBaseParser.ShowTablesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowColumns(SqlBaseParser.ShowColumnsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowColumns(SqlBaseParser.ShowColumnsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCreateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowCreateTable(SqlBaseParser.ShowCreateTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCreateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowCreateTable(SqlBaseParser.ShowCreateTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code describe}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDescribe(SqlBaseParser.DescribeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code describe}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDescribe(SqlBaseParser.DescribeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code explain}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterExplain(SqlBaseParser.ExplainContext ctx);
	/**
	 * Exit a parse tree produced by the {@code explain}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitExplain(SqlBaseParser.ExplainContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#insertStatement}.
	 * @param ctx the parse tree
	 */
	void enterInsertStatement(SqlBaseParser.InsertStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#insertStatement}.
	 * @param ctx the parse tree
	 */
	void exitInsertStatement(SqlBaseParser.InsertStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code values}
	 * labeled alternative in {@link SqlBaseParser#statementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementqueryTermqueryTermqueryTermqueryPrimaryqueryPrimaryselectItemselectItemrelationrelationrelationrelationPrimaryrelationPrimaryrelationPrimaryrelationPrimaryrelationPrimarybooleanExpressionbooleanExpressionbooleanExpressionbooleanExpressionpredicatepredicatepredicatepredicatepredicatepredicatepredicatevalueExpressionvalueExpressionvalueExpressionvalueExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionwindowSpecificationwindowSpecificationgroupBygroupBygroupBygroupBygroupBymultidimGroupmultidimGroupmultidimGroup}.
	 * @param ctx the parse tree
	 */
	void enterValues(SqlBaseParser.ValuesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code values}
	 * labeled alternative in {@link SqlBaseParser#statementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementstatementqueryTermqueryTermqueryTermqueryPrimaryqueryPrimaryselectItemselectItemrelationrelationrelationrelationPrimaryrelationPrimaryrelationPrimaryrelationPrimaryrelationPrimarybooleanExpressionbooleanExpressionbooleanExpressionbooleanExpressionpredicatepredicatepredicatepredicatepredicatepredicatepredicatevalueExpressionvalueExpressionvalueExpressionvalueExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionprimaryExpressionwindowSpecificationwindowSpecificationgroupBygroupBygroupBygroupBygroupBymultidimGroupmultidimGroupmultidimGroup}.
	 * @param ctx the parse tree
	 */
	void exitValues(SqlBaseParser.ValuesContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#createTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateTableStatement(SqlBaseParser.CreateTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#createTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateTableStatement(SqlBaseParser.CreateTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#tableElement}.
	 * @param ctx the parse tree
	 */
	void enterTableElement(SqlBaseParser.TableElementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#tableElement}.
	 * @param ctx the parse tree
	 */
	void exitTableElement(SqlBaseParser.TableElementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#columnDefinition}.
	 * @param ctx the parse tree
	 */
	void enterColumnDefinition(SqlBaseParser.ColumnDefinitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#columnDefinition}.
	 * @param ctx the parse tree
	 */
	void exitColumnDefinition(SqlBaseParser.ColumnDefinitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#tableProperties}.
	 * @param ctx the parse tree
	 */
	void enterTableProperties(SqlBaseParser.TablePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#tableProperties}.
	 * @param ctx the parse tree
	 */
	void exitTableProperties(SqlBaseParser.TablePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#property}.
	 * @param ctx the parse tree
	 */
	void enterProperty(SqlBaseParser.PropertyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#property}.
	 * @param ctx the parse tree
	 */
	void exitProperty(SqlBaseParser.PropertyContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#createTableAsSelectStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateTableAsSelectStatement(SqlBaseParser.CreateTableAsSelectStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#createTableAsSelectStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateTableAsSelectStatement(SqlBaseParser.CreateTableAsSelectStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#createCatalogStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateCatalogStatement(SqlBaseParser.CreateCatalogStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#createCatalogStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateCatalogStatement(SqlBaseParser.CreateCatalogStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#dropCatalogStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropCatalogStatement(SqlBaseParser.DropCatalogStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#dropCatalogStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropCatalogStatement(SqlBaseParser.DropCatalogStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#createSchemaStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateSchemaStatement(SqlBaseParser.CreateSchemaStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#createSchemaStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateSchemaStatement(SqlBaseParser.CreateSchemaStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#dropSchemaStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropSchemaStatement(SqlBaseParser.DropSchemaStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#dropSchemaStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropSchemaStatement(SqlBaseParser.DropSchemaStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#dropTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropTableStatement(SqlBaseParser.DropTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#dropTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropTableStatement(SqlBaseParser.DropTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#truncateTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterTruncateTableStatement(SqlBaseParser.TruncateTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#truncateTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitTruncateTableStatement(SqlBaseParser.TruncateTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#alterTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterTableStatement(SqlBaseParser.AlterTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#alterTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterTableStatement(SqlBaseParser.AlterTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#createViewStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateViewStatement(SqlBaseParser.CreateViewStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#createViewStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateViewStatement(SqlBaseParser.CreateViewStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#dropViewStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropViewStatement(SqlBaseParser.DropViewStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#dropViewStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropViewStatement(SqlBaseParser.DropViewStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#createFunctionStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateFunctionStatement(SqlBaseParser.CreateFunctionStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#createFunctionStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateFunctionStatement(SqlBaseParser.CreateFunctionStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#dropFunctionStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropFunctionStatement(SqlBaseParser.DropFunctionStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#dropFunctionStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropFunctionStatement(SqlBaseParser.DropFunctionStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#query}.
	 * @param ctx the parse tree
	 */
	void enterQuery(SqlBaseParser.QueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#query}.
	 * @param ctx the parse tree
	 */
	void exitQuery(SqlBaseParser.QueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#queryNoWith}.
	 * @param ctx the parse tree
	 */
	void enterQueryNoWith(SqlBaseParser.QueryNoWithContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#queryNoWith}.
	 * @param ctx the parse tree
	 */
	void exitQueryNoWith(SqlBaseParser.QueryNoWithContext ctx);
	/**
	 * Enter a parse tree produced by the {@code intersect}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void enterIntersect(SqlBaseParser.IntersectContext ctx);
	/**
	 * Exit a parse tree produced by the {@code intersect}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void exitIntersect(SqlBaseParser.IntersectContext ctx);
	/**
	 * Enter a parse tree produced by the {@code queryTermDefault}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void enterQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code queryTermDefault}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void exitQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void enterSetOperation(SqlBaseParser.SetOperationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void exitSetOperation(SqlBaseParser.SetOperationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterSubquery(SqlBaseParser.SubqueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitSubquery(SqlBaseParser.SubqueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	void enterQuerySpecification(SqlBaseParser.QuerySpecificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	void exitQuerySpecification(SqlBaseParser.QuerySpecificationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code selectSingle}
	 * labeled alternative in {@link SqlBaseParser#selectItem}.
	 * @param ctx the parse tree
	 */
	void enterSelectSingle(SqlBaseParser.SelectSingleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code selectSingle}
	 * labeled alternative in {@link SqlBaseParser#selectItem}.
	 * @param ctx the parse tree
	 */
	void exitSelectSingle(SqlBaseParser.SelectSingleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code selectAll}
	 * labeled alternative in {@link SqlBaseParser#selectItem}.
	 * @param ctx the parse tree
	 */
	void enterSelectAll(SqlBaseParser.SelectAllContext ctx);
	/**
	 * Exit a parse tree produced by the {@code selectAll}
	 * labeled alternative in {@link SqlBaseParser#selectItem}.
	 * @param ctx the parse tree
	 */
	void exitSelectAll(SqlBaseParser.SelectAllContext ctx);
	/**
	 * Enter a parse tree produced by the {@code relationDefault}
	 * labeled alternative in {@link SqlBaseParser#relation}.
	 * @param ctx the parse tree
	 */
	void enterRelationDefault(SqlBaseParser.RelationDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code relationDefault}
	 * labeled alternative in {@link SqlBaseParser#relation}.
	 * @param ctx the parse tree
	 */
	void exitRelationDefault(SqlBaseParser.RelationDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code subqueryRelation}
	 * labeled alternative in {@link SqlBaseParser#relation}.
	 * @param ctx the parse tree
	 */
	void enterSubqueryRelation(SqlBaseParser.SubqueryRelationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subqueryRelation}
	 * labeled alternative in {@link SqlBaseParser#relation}.
	 * @param ctx the parse tree
	 */
	void exitSubqueryRelation(SqlBaseParser.SubqueryRelationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code joinRelation}
	 * labeled alternative in {@link SqlBaseParser#relation}.
	 * @param ctx the parse tree
	 */
	void enterJoinRelation(SqlBaseParser.JoinRelationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code joinRelation}
	 * labeled alternative in {@link SqlBaseParser#relation}.
	 * @param ctx the parse tree
	 */
	void exitJoinRelation(SqlBaseParser.JoinRelationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#sampledRelation}.
	 * @param ctx the parse tree
	 */
	void enterSampledRelation(SqlBaseParser.SampledRelationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#sampledRelation}.
	 * @param ctx the parse tree
	 */
	void exitSampledRelation(SqlBaseParser.SampledRelationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#aliasedRelation}.
	 * @param ctx the parse tree
	 */
	void enterAliasedRelation(SqlBaseParser.AliasedRelationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#aliasedRelation}.
	 * @param ctx the parse tree
	 */
	void exitAliasedRelation(SqlBaseParser.AliasedRelationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tableNameRef}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterTableNameRef(SqlBaseParser.TableNameRefContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tableNameRef}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitTableNameRef(SqlBaseParser.TableNameRefContext ctx);
	/**
	 * Enter a parse tree produced by the {@code qualifiedNameRef}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterQualifiedNameRef(SqlBaseParser.QualifiedNameRefContext ctx);
	/**
	 * Exit a parse tree produced by the {@code qualifiedNameRef}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitQualifiedNameRef(SqlBaseParser.QualifiedNameRefContext ctx);
	/**
	 * Enter a parse tree produced by the {@code subqueryRelationPrimary}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterSubqueryRelationPrimary(SqlBaseParser.SubqueryRelationPrimaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subqueryRelationPrimary}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitSubqueryRelationPrimary(SqlBaseParser.SubqueryRelationPrimaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unnest}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterUnnest(SqlBaseParser.UnnestContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unnest}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitUnnest(SqlBaseParser.UnnestContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tableFunction}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterTableFunction(SqlBaseParser.TableFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tableFunction}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitTableFunction(SqlBaseParser.TableFunctionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#functionArgument}.
	 * @param ctx the parse tree
	 */
	void enterFunctionArgument(SqlBaseParser.FunctionArgumentContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#functionArgument}.
	 * @param ctx the parse tree
	 */
	void exitFunctionArgument(SqlBaseParser.FunctionArgumentContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#columnAliases}.
	 * @param ctx the parse tree
	 */
	void enterColumnAliases(SqlBaseParser.ColumnAliasesContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#columnAliases}.
	 * @param ctx the parse tree
	 */
	void exitColumnAliases(SqlBaseParser.ColumnAliasesContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(SqlBaseParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(SqlBaseParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogicalNot(SqlBaseParser.LogicalNotContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogicalNot(SqlBaseParser.LogicalNotContext ctx);
	/**
	 * Enter a parse tree produced by the {@code booleanDefault}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterBooleanDefault(SqlBaseParser.BooleanDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code booleanDefault}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitBooleanDefault(SqlBaseParser.BooleanDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code booleanPredicate}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterBooleanPredicate(SqlBaseParser.BooleanPredicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code booleanPredicate}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitBooleanPredicate(SqlBaseParser.BooleanPredicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logicalBinary}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logicalBinary}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterComparison(SqlBaseParser.ComparisonContext ctx);
	/**
	 * Exit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitComparison(SqlBaseParser.ComparisonContext ctx);
	/**
	 * Enter a parse tree produced by the {@code between}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterBetween(SqlBaseParser.BetweenContext ctx);
	/**
	 * Exit a parse tree produced by the {@code between}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitBetween(SqlBaseParser.BetweenContext ctx);
	/**
	 * Enter a parse tree produced by the {@code inList}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterInList(SqlBaseParser.InListContext ctx);
	/**
	 * Exit a parse tree produced by the {@code inList}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitInList(SqlBaseParser.InListContext ctx);
	/**
	 * Enter a parse tree produced by the {@code inSubquery}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterInSubquery(SqlBaseParser.InSubqueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code inSubquery}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitInSubquery(SqlBaseParser.InSubqueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code exists}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterExists(SqlBaseParser.ExistsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitExists(SqlBaseParser.ExistsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code like}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterLike(SqlBaseParser.LikeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code like}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitLike(SqlBaseParser.LikeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code nullPredicate}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterNullPredicate(SqlBaseParser.NullPredicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code nullPredicate}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitNullPredicate(SqlBaseParser.NullPredicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code atTimeZone}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterAtTimeZone(SqlBaseParser.AtTimeZoneContext ctx);
	/**
	 * Exit a parse tree produced by the {@code atTimeZone}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitAtTimeZone(SqlBaseParser.AtTimeZoneContext ctx);
	/**
	 * Enter a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSimpleCase(SqlBaseParser.SimpleCaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSimpleCase(SqlBaseParser.SimpleCaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterColumnReference(SqlBaseParser.ColumnReferenceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitColumnReference(SqlBaseParser.ColumnReferenceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterNullLiteral(SqlBaseParser.NullLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitNullLiteral(SqlBaseParser.NullLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSubscript(SqlBaseParser.SubscriptContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSubscript(SqlBaseParser.SubscriptContext ctx);
	/**
	 * Enter a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cast}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterCast(SqlBaseParser.CastContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitCast(SqlBaseParser.CastContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterStringLiteral(SqlBaseParser.StringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitStringLiteral(SqlBaseParser.StringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arrayConstructor}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterArrayConstructor(SqlBaseParser.ArrayConstructorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arrayConstructor}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitArrayConstructor(SqlBaseParser.ArrayConstructorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterDoubleLiteral(SqlBaseParser.DoubleLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitDoubleLiteral(SqlBaseParser.DoubleLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code functionCallExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterFunctionCallExpression(SqlBaseParser.FunctionCallExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code functionCallExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitFunctionCallExpression(SqlBaseParser.FunctionCallExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterIntervalLiteral(SqlBaseParser.IntervalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitIntervalLiteral(SqlBaseParser.IntervalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tryCast}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterTryCast(SqlBaseParser.TryCastContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tryCast}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitTryCast(SqlBaseParser.TryCastContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#functionCall}.
	 * @param ctx the parse tree
	 */
	void enterFunctionCall(SqlBaseParser.FunctionCallContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#functionCall}.
	 * @param ctx the parse tree
	 */
	void exitFunctionCall(SqlBaseParser.FunctionCallContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#whenClause}.
	 * @param ctx the parse tree
	 */
	void enterWhenClause(SqlBaseParser.WhenClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#whenClause}.
	 * @param ctx the parse tree
	 */
	void exitWhenClause(SqlBaseParser.WhenClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code windowReference}
	 * labeled alternative in {@link SqlBaseParser#windowSpecification}.
	 * @param ctx the parse tree
	 */
	void enterWindowReference(SqlBaseParser.WindowReferenceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code windowReference}
	 * labeled alternative in {@link SqlBaseParser#windowSpecification}.
	 * @param ctx the parse tree
	 */
	void exitWindowReference(SqlBaseParser.WindowReferenceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code windowDefinitionInline}
	 * labeled alternative in {@link SqlBaseParser#windowSpecification}.
	 * @param ctx the parse tree
	 */
	void enterWindowDefinitionInline(SqlBaseParser.WindowDefinitionInlineContext ctx);
	/**
	 * Exit a parse tree produced by the {@code windowDefinitionInline}
	 * labeled alternative in {@link SqlBaseParser#windowSpecification}.
	 * @param ctx the parse tree
	 */
	void exitWindowDefinitionInline(SqlBaseParser.WindowDefinitionInlineContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#windowName}.
	 * @param ctx the parse tree
	 */
	void enterWindowName(SqlBaseParser.WindowNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#windowName}.
	 * @param ctx the parse tree
	 */
	void exitWindowName(SqlBaseParser.WindowNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#windowDefinition}.
	 * @param ctx the parse tree
	 */
	void enterWindowDefinition(SqlBaseParser.WindowDefinitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#windowDefinition}.
	 * @param ctx the parse tree
	 */
	void exitWindowDefinition(SqlBaseParser.WindowDefinitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#windowFrame}.
	 * @param ctx the parse tree
	 */
	void enterWindowFrame(SqlBaseParser.WindowFrameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#windowFrame}.
	 * @param ctx the parse tree
	 */
	void exitWindowFrame(SqlBaseParser.WindowFrameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#frameBound}.
	 * @param ctx the parse tree
	 */
	void enterFrameBound(SqlBaseParser.FrameBoundContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#frameBound}.
	 * @param ctx the parse tree
	 */
	void exitFrameBound(SqlBaseParser.FrameBoundContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#sortItem}.
	 * @param ctx the parse tree
	 */
	void enterSortItem(SqlBaseParser.SortItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#sortItem}.
	 * @param ctx the parse tree
	 */
	void exitSortItem(SqlBaseParser.SortItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
	void enterSetQuantifier(SqlBaseParser.SetQuantifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
	void exitSetQuantifier(SqlBaseParser.SetQuantifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code groupingSets}
	 * labeled alternative in {@link SqlBaseParser#groupBy}.
	 * @param ctx the parse tree
	 */
	void enterGroupingSets(SqlBaseParser.GroupingSetsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code groupingSets}
	 * labeled alternative in {@link SqlBaseParser#groupBy}.
	 * @param ctx the parse tree
	 */
	void exitGroupingSets(SqlBaseParser.GroupingSetsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cube}
	 * labeled alternative in {@link SqlBaseParser#groupBy}.
	 * @param ctx the parse tree
	 */
	void enterCube(SqlBaseParser.CubeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cube}
	 * labeled alternative in {@link SqlBaseParser#groupBy}.
	 * @param ctx the parse tree
	 */
	void exitCube(SqlBaseParser.CubeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code rollup}
	 * labeled alternative in {@link SqlBaseParser#groupBy}.
	 * @param ctx the parse tree
	 */
	void enterRollup(SqlBaseParser.RollupContext ctx);
	/**
	 * Exit a parse tree produced by the {@code rollup}
	 * labeled alternative in {@link SqlBaseParser#groupBy}.
	 * @param ctx the parse tree
	 */
	void exitRollup(SqlBaseParser.RollupContext ctx);
	/**
	 * Enter a parse tree produced by the {@code simpleGroupBy}
	 * labeled alternative in {@link SqlBaseParser#groupBy}.
	 * @param ctx the parse tree
	 */
	void enterSimpleGroupBy(SqlBaseParser.SimpleGroupByContext ctx);
	/**
	 * Exit a parse tree produced by the {@code simpleGroupBy}
	 * labeled alternative in {@link SqlBaseParser#groupBy}.
	 * @param ctx the parse tree
	 */
	void exitSimpleGroupBy(SqlBaseParser.SimpleGroupByContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parenthesizedGroupBy}
	 * labeled alternative in {@link SqlBaseParser#groupBy}.
	 * @param ctx the parse tree
	 */
	void enterParenthesizedGroupBy(SqlBaseParser.ParenthesizedGroupByContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parenthesizedGroupBy}
	 * labeled alternative in {@link SqlBaseParser#groupBy}.
	 * @param ctx the parse tree
	 */
	void exitParenthesizedGroupBy(SqlBaseParser.ParenthesizedGroupByContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#groupingSet}.
	 * @param ctx the parse tree
	 */
	void enterGroupingSet(SqlBaseParser.GroupingSetContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#groupingSet}.
	 * @param ctx the parse tree
	 */
	void exitGroupingSet(SqlBaseParser.GroupingSetContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#dataType}.
	 * @param ctx the parse tree
	 */
	void enterDataType(SqlBaseParser.DataTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#dataType}.
	 * @param ctx the parse tree
	 */
	void exitDataType(SqlBaseParser.DataTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#commonTableExpression}.
	 * @param ctx the parse tree
	 */
	void enterCommonTableExpression(SqlBaseParser.CommonTableExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#commonTableExpression}.
	 * @param ctx the parse tree
	 */
	void exitCommonTableExpression(SqlBaseParser.CommonTableExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#timeZoneSpecifier}.
	 * @param ctx the parse tree
	 */
	void enterTimeZoneSpecifier(SqlBaseParser.TimeZoneSpecifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#timeZoneSpecifier}.
	 * @param ctx the parse tree
	 */
	void exitTimeZoneSpecifier(SqlBaseParser.TimeZoneSpecifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	void enterComparisonOperator(SqlBaseParser.ComparisonOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	void exitComparisonOperator(SqlBaseParser.ComparisonOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void enterBooleanValue(SqlBaseParser.BooleanValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void exitBooleanValue(SqlBaseParser.BooleanValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#interval}.
	 * @param ctx the parse tree
	 */
	void enterInterval(SqlBaseParser.IntervalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#interval}.
	 * @param ctx the parse tree
	 */
	void exitInterval(SqlBaseParser.IntervalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#intervalField}.
	 * @param ctx the parse tree
	 */
	void enterIntervalField(SqlBaseParser.IntervalFieldContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#intervalField}.
	 * @param ctx the parse tree
	 */
	void exitIntervalField(SqlBaseParser.IntervalFieldContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#sampleType}.
	 * @param ctx the parse tree
	 */
	void enterSampleType(SqlBaseParser.SampleTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#sampleType}.
	 * @param ctx the parse tree
	 */
	void exitSampleType(SqlBaseParser.SampleTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	void enterQualifiedName(SqlBaseParser.QualifiedNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	void exitQualifiedName(SqlBaseParser.QualifiedNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#tableName}.
	 * @param ctx the parse tree
	 */
	void enterTableName(SqlBaseParser.TableNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#tableName}.
	 * @param ctx the parse tree
	 */
	void exitTableName(SqlBaseParser.TableNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(SqlBaseParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(SqlBaseParser.IdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#string}.
	 * @param ctx the parse tree
	 */
	void enterString(SqlBaseParser.StringContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#string}.
	 * @param ctx the parse tree
	 */
	void exitString(SqlBaseParser.StringContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bareGroupingSets}
	 * labeled alternative in {@link SqlBaseParser#multidimGroup}.
	 * @param ctx the parse tree
	 */
	void enterBareGroupingSets(SqlBaseParser.BareGroupingSetsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bareGroupingSets}
	 * labeled alternative in {@link SqlBaseParser#multidimGroup}.
	 * @param ctx the parse tree
	 */
	void exitBareGroupingSets(SqlBaseParser.BareGroupingSetsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bareCube}
	 * labeled alternative in {@link SqlBaseParser#multidimGroup}.
	 * @param ctx the parse tree
	 */
	void enterBareCube(SqlBaseParser.BareCubeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bareCube}
	 * labeled alternative in {@link SqlBaseParser#multidimGroup}.
	 * @param ctx the parse tree
	 */
	void exitBareCube(SqlBaseParser.BareCubeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bareRollup}
	 * labeled alternative in {@link SqlBaseParser#multidimGroup}.
	 * @param ctx the parse tree
	 */
	void enterBareRollup(SqlBaseParser.BareRollupContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bareRollup}
	 * labeled alternative in {@link SqlBaseParser#multidimGroup}.
	 * @param ctx the parse tree
	 */
	void exitBareRollup(SqlBaseParser.BareRollupContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void enterNonReserved(SqlBaseParser.NonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void exitNonReserved(SqlBaseParser.NonReservedContext ctx);
}
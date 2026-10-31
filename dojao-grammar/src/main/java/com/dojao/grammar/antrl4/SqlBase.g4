// SqlBase.g4
grammar SqlBase;

// 入口规则
singleStatement
    : statement EOF
    ;

// 所有支持的语句类型
statement
    : query                                                            # statementDefault
    | insertStatement                                                  # statementInsert
    | createTableStatement                                             # statementCreateTable
    | createTableAsSelectStatement                                     # statementCreateTableAsSelect
    | createCatalogStatement                                           # statementCreateCatalog
    | createSchemaStatement                                            # statementCreateSchema
    | createViewStatement                                              # statementCreateView
    | createFunctionStatement                                          # statementCreateFunction
    | dropCatalogStatement                                             # statementDropCatalog
    | dropSchemaStatement                                              # statementDropSchema
    | dropTableStatement                                               # statementDropTable
    | truncateTableStatement                                           # statementTruncateTable
    | dropViewStatement                                                # statementDropView
    | dropFunctionStatement                                            # statementDropFunction
    | alterTableStatement                                              # statementAlterTable
    | USE catalog=identifier ('.' schema=identifier)?                  # use
    | SHOW CATALOGS                                                    # showCatalogs
    | SHOW SCHEMAS ((FROM|IN) catalog=identifier)?                     # showSchemas
    | SHOW TABLES ((FROM|IN) qualifiedName)? (LIKE pattern=string)?    # showTables
    | SHOW COLUMNS (FROM|IN) qualifiedName                           # showColumns
    | SHOW CREATE TABLE qualifiedName                               # showCreateTable
    | (DESCRIBE | DESC) qualifiedName                               # describe
    | EXPLAIN (ANALYZE VERBOSE?)? statement                            # explain
    ;

// ============== INSERT 语句 ==============
insertStatement
    : INSERT (INTO | OVERWRITE) qualifiedName
      columnAliases?
      (query | VALUES values (',' values)*)
    ;

values
    : '(' expression (',' expression)* ')'
    ;

// ============== CREATE TABLE 语句 ==============
createTableStatement
    : CREATE TABLE (IF NOT EXISTS)? qualifiedName
      '(' tableElement (',' tableElement)* ')'
      (WITH tableProperties)?
      (COMMENT string)?
    ;

tableElement
    : columnDefinition
    ;

columnDefinition
    : identifier dataType (COMMENT string)? (DEFAULT expression)? (NOT NULL)?
    ;

tableProperties
    : '(' property (',' property)* ')'
    ;

property
    : identifier EQ expression
    | string EQ expression
    ;

// ============== CREATE TABLE AS SELECT (CTAS) ==============
createTableAsSelectStatement
    : CREATE TABLE (IF NOT EXISTS)? qualifiedName
      columnAliases?
      (WITH tableProperties)?
      (COMMENT string)?
      AS query
    ;

// ============== CREATE CATALOG ==============
createCatalogStatement
    : CREATE CATALOG (IF NOT EXISTS)? identifier
      USING identifier
      (WITH tableProperties)?
    ;

// ============== DROP CATALOG ==============
dropCatalogStatement
    : DROP CATALOG (IF EXISTS)? identifier
    ;

// ============== CREATE SCHEMA ==============
createSchemaStatement
    : CREATE SCHEMA (IF NOT EXISTS)? qualifiedName
      (WITH tableProperties)?
    ;

// ============== DROP SCHEMA ==============
dropSchemaStatement
    : DROP SCHEMA (IF EXISTS)? qualifiedName (CASCADE | RESTRICT)?
    ;

// ============== DROP TABLE ==============
dropTableStatement
    : DROP TABLE (IF EXISTS)? qualifiedName
    ;

// ============== TRUNCATE TABLE ==============
truncateTableStatement
    : TRUNCATE TABLE qualifiedName
    ;

// ============== ALTER TABLE ==============
alterTableStatement
    : ALTER TABLE qualifiedName
      ( RENAME TO identifier
      | ADD COLUMN columnDefinition
      | DROP COLUMN identifier
      | RENAME COLUMN identifier TO identifier
      )
    ;

// ============== CREATE VIEW ==============
createViewStatement
    : CREATE (OR REPLACE)? VIEW qualifiedName
      AS query
    ;

// ============== DROP VIEW ==============
dropViewStatement
    : DROP VIEW (IF EXISTS)? qualifiedName
    ;

// ============== CREATE FUNCTION ==============
createFunctionStatement
    : CREATE (TEMPORARY | SESSION)? FUNCTION (IF NOT EXISTS)? qualifiedName
      (WITH tableProperties)?
      (COMMENT string)?
    ;

// ============== DROP FUNCTION ==============
dropFunctionStatement
    : DROP (TEMPORARY | SESSION)? FUNCTION (IF EXISTS)? qualifiedName
    ;

// ============== 查询核心 ==============
query
    : WITH RECURSIVE? commonTableExpression (',' commonTableExpression)*
      queryNoWith
    | queryNoWith
    ;

queryNoWith
    : queryTerm
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=(INTEGER_VALUE | ALL))?
      (OFFSET offset=INTEGER_VALUE (ROW | ROWS)?)?
    ;

queryTerm
    : queryPrimary                                                             # queryTermDefault
    | left=queryTerm operator=INTERSECT setQuantifier? right=queryTerm         # intersect
    | left=queryTerm operator=(UNION | EXCEPT) setQuantifier? right=queryTerm  # setOperation
    ;

queryPrimary
    : querySpecification                   # queryPrimaryDefault
    | '(' query ')'                        # subquery
    ;

querySpecification
    : SELECT setQuantifier? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy (',' groupBy)*)?
      (multidimGroup)?
      (HAVING having=booleanExpression)?
    ;

// ============== 表达式 ==============
selectItem
    : expression (AS? identifier)?               # selectSingle
    | qualifiedName '.' ASTERISK                 # selectAll
    | ASTERISK                                   # selectAll
    ;

relation
    : left=relation
      (CROSS JOIN right=sampledRelation
      | INNER? JOIN right=sampledRelation ON booleanExpression
      | (LEFT | RIGHT | FULL) OUTER? JOIN right=sampledRelation ON booleanExpression
      | (LEFT | RIGHT)? SEMI JOIN right=sampledRelation ON booleanExpression)
                                                              # joinRelation
    | sampledRelation                                         # relationDefault
    | '(' query ')' (AS? identifier columnAliases?)?          # subqueryRelation
    ;

sampledRelation
    : aliasedRelation (TABLESAMPLE sampleType '(' percentage=INTEGER_VALUE ')')?
    ;

aliasedRelation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

relationPrimary
    : tableName                                                       # tableNameRef
    | qualifiedName                                                   # qualifiedNameRef
    | '(' query ')'                                                   # subqueryRelationPrimary
    | UNNEST '(' expression (',' expression)* ')' (WITH ORDINALITY)?  # unnest
    | TABLE '(' qualifiedName '(' (functionArgument (',' functionArgument)*)? ')' ')'  # tableFunction
    ;

functionArgument
    : expression
    ;

columnAliases
    : '(' identifier (',' identifier)* ')'
    ;

// ============== 表达式层级 ==============
expression
    : booleanExpression
    ;

booleanExpression
    : valueExpression predicate?                               # booleanDefault
    | NOT booleanExpression                                    # logicalNot
    | predicate                                                # booleanPredicate
    | left=booleanExpression operator=AND right=booleanExpression   # logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression    # logicalBinary
    ;

predicate
    : comparisonOperator valueExpression                               # comparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression    # between
    | NOT? IN '(' expression (',' expression)* ')'                     # inList
    | NOT? IN '(' query ')'                                            # inSubquery
    | NOT? EXISTS '(' query ')'                                        # exists
    | NOT? LIKE pattern=valueExpression (ESCAPE escape=valueExpression)?  # like
    | IS NOT? NULL                                                     # nullPredicate
    ;

valueExpression
    : primaryExpression                                                                 # valueExpressionDefault
    | valueExpression AT timeZoneSpecifier                                              # atTimeZone
    | operator=(MINUS | PLUS) valueExpression                                           # arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  # arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                # arithmeticBinary
    ;

primaryExpression
    : NULL                                                                          # nullLiteral
    | booleanValue                                                                  # booleanLiteral
    | string                                                                        # stringLiteral
    | INTEGER_VALUE                                                                 # integerLiteral
    | DECIMAL_VALUE                                                                 # decimalLiteral
    | DOUBLE_VALUE                                                                  # doubleLiteral
    | interval                                                                      # intervalLiteral
    | identifier ('.' identifier)*                                                  # columnReference
    | functionCall                                                                  # functionCallExpression
    | '(' query ')'                                                                  # subqueryExpression
    | '(' expression (',' expression)* ')'                                           # parenthesizedExpression
    | CASE whenClause (whenClause)* (ELSE elseExpression=expression)? END           # simpleCase
    | CAST '(' expression AS dataType ')'                                           # cast
    | TRY_CAST '(' expression AS dataType ')'                                       # tryCast
    | ARRAY '[' (expression (',' expression)*)? ']'                                 # arrayConstructor
    | value=primaryExpression '[' index=valueExpression ']'                         # subscript
    ;

functionCall
    : functionName=qualifiedName '(' (setQuantifier? expression (',' expression)* | ASTERISK)? ')'
      (FILTER '(' WHERE where=booleanExpression ')')?
      (OVER windowSpecification)?
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

// ============== 窗口函数 ==============
windowSpecification
    : windowName                              # windowReference
    | '(' windowDefinition ')'                # windowDefinitionInline
    ;

windowName
    : identifier
    ;

windowDefinition
    : (PARTITION BY partition+=expression (',' partition+=expression)*)?
      (ORDER BY sortItem (',' sortItem)*)?
      windowFrame?
    ;

windowFrame
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED PRECEDING
    | UNBOUNDED FOLLOWING
    | CURRENT ROW
    | expression PRECEDING
    | expression FOLLOWING
    ;

// ============== 排序 ==============
sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

groupBy
    // ★ 关键字锚定的多维分组备选必须排在 expression 之前：CUBE/ROLLUP 是 nonReserved 关键字，
    //   可被 expression→functionCall 当普通函数调用吞掉（ANTLR 歧义时取编号更小的备选），
    //   排前面才能让 `group by cube(...)`/`rollup(...)`/`grouping sets(...)` 解析成结构化节点。
    : GROUPING SETS '(' groupingSet (',' groupingSet)* ')'               # groupingSets
    | CUBE '(' expression (',' expression)* ')'                           # cube
    | ROLLUP '(' expression (',' expression)* ')'                         # rollup
    | expression                                                          # simpleGroupBy
    | '(' expression (',' expression)* ')'                                # parenthesizedGroupBy
    ;

groupingSet
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

// ============== 数据类型 ==============
dataType
    : type = BOOLEAN
    | type = TINYINT
    | type = SMALLINT
    | type = INT
    | type = INTEGER
    | type = BIGINT
    | type = FLOAT
    | type = DOUBLE
    | type = DECIMAL ('(' precision=INTEGER_VALUE (',' scale=INTEGER_VALUE)? ')')?
    | type = STRING_TYPE ('(' length=INTEGER_VALUE ')')?
    | type = VARCHAR ('(' length=INTEGER_VALUE ')')?
    | type = CHAR ('(' length=INTEGER_VALUE ')')?
    | type = VARBINARY
    | type = JSON
    | type = DATE
    | type = TIME ('(' precision=INTEGER_VALUE ')')? (WITH TIME ZONE)?
    | type = TIMESTAMP ('(' precision=INTEGER_VALUE ')')? (WITH TIME ZONE)?
    | type = ARRAY '<' dataType '>'
    | type = LIST '<' dataType '>'
    | type = MAP '<' dataType ',' dataType '>'
    | type = ROW '(' identifier dataType (',' identifier dataType)* ')'
    | type = UUID
    | type = IPADDRESS
    ;

// ============== 通用表达式 ==============
commonTableExpression
    : name=identifier (columnAliases)? AS '(' query ')'
    ;

timeZoneSpecifier
    : TIME ZONE (string | INTERVAL string)
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTERVAL sign=(PLUS | MINUS)? string from=intervalField (TO to=intervalField)?
    ;

intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

sampleType
    : BERNOULLI | SYSTEM
    ;

// ============== 标识符和字面量 ==============
qualifiedName
    : identifier ('.' identifier)*
    ;

tableName
    : identifier '.' identifier '.' identifier
    ;

identifier
    : IDENTIFIER
    | quotedIdentifier
    | nonReserved
    ;

quotedIdentifier
    : '"' ( ~'"' | '""' )* '"'
    | '`' ( ~'`' )+ '`'
    ;

string
    : STRING
    ;

// ★ 裸多维分组（语法层面去掉 GROUP BY）：CUBE(...) / ROLLUP(...) / GROUPING SETS(...)
//   直接跟在 WHERE / FROM 之后、HAVING 之前，语义等价于 GROUP BY CUBE(...) 等。
//   定义放在解析规则末尾以避免改动规则索引（querySpecification 前向引用本规则）。
multidimGroup
    : GROUPING SETS '(' groupingSet (',' groupingSet)* ')'   # bareGroupingSets
    | CUBE '(' expression (',' expression)* ')'              # bareCube
    | ROLLUP '(' expression (',' expression)* ')'            # bareRollup
    ;

// ============== 词法规则 ==============
// 保留关键字
SELECT: [Ss][Ee][Ll][Ee][Cc][Tt];
FROM: [Ff][Rr][Oo][Mm];
WHERE: [Ww][Hh][Ee][Rr][Ee];
GROUP: [Gg][Rr][Oo][Uu][Pp];
BY: [Bb][Yy];
HAVING: [Hh][Aa][Vv][Ii][Nn][Gg];
ORDER: [Oo][Rr][Dd][Ee][Rr];
LIMIT: [Ll][Ii][Mm][Ii][Tt];
OFFSET: [Oo][Ff][Ff][Ss][Ee][Tt];
JOIN: [Jj][Oo][Ii][Nn];
INNER: [Ii][Nn][Nn][Ee][Rr];
LEFT: [Ll][Ee][Ff][Tt];
RIGHT: [Rr][Ii][Gg][Hh][Tt];
FULL: [Ff][Uu][Ll][Ll];
OUTER: [Oo][Uu][Tt][Ee][Rr];
CROSS: [Cc][Rr][Oo][Ss][Ss];
ON: [Oo][Nn];
UNION: [Uu][Nn][Ii][Oo][Nn];
INTERSECT: [Ii][Nn][Tt][Ee][Rr][Ss][Ee][Cc][Tt];
EXCEPT: [Ee][Xx][Cc][Ee][Pp][Tt];
ALL: [Aa][Ll][Ll];
DISTINCT: [Dd][Ii][Ss][Tt][Ii][Nn][Cc][Tt];
AS: [Aa][Ss];
WITH: [Ww][Ii][Tt][Hh];
RECURSIVE: [Rr][Ee][Cc][Uu][Rr][Ss][Ii][Vv][Ee];
AND: [Aa][Nn][Dd];
OR: [Oo][Rr];
NOT: [Nn][Oo][Tt];
IN: [Ii][Nn];
LIKE: [Ll][Ii][Kk][Ee];
IS: [Ii][Ss];
NULL: [Nn][Uu][Ll][Ll];
TRUE: [Tt][Rr][Uu][Ee];
FALSE: [Ff][Aa][Ll][Ss][Ee];
CASE: [Cc][Aa][Ss][Ee];
WHEN: [Ww][Hh][Ee][Nn];
THEN: [Tt][Hh][Ee][Nn];
ELSE: [Ee][Ll][Ss][Ee];
END: [Ee][Nn][Dd];
CAST: [Cc][Aa][Ss][Tt];
TRY_CAST: [Tt][Rr][Yy][__][Cc][Aa][Ss][Tt];
ARRAY: [Aa][Rr][Rr][Aa][Yy];
LIST: [Ll][Ii][Ss][Tt];
MAP: [Mm][Aa][Pp];
ROW: [Rr][Oo][Ww];
UNNEST: [Uu][Nn][Nn][Ee][Ss][Tt];
OVER: [Oo][Vv][Ee][Rr];
PARTITION: [Pp][Aa][Rr][Tt][Ii][Tt][Ii][Oo][Nn];
RANGE: [Rr][Aa][Nn][Gg][Ee];
ROWS: [Rr][Oo][Ww][Ss];
PRECEDING: [Pp][Rr][Ee][Cc][Ee][Dd][Ii][Nn][Gg];
FOLLOWING: [Ff][Oo][Ll][Ll][Oo][Ww][Ii][Nn][Gg];
CURRENT: [Cc][Uu][Rr][Rr][Ee][Nn][Tt];
UNBOUNDED: [Uu][Nn][Bb][Oo][Uu][Nn][Dd][Ee][Dd];
FILTER: [Ff][Ii][Ll][Tt][Ee][Rr];
TABLESAMPLE: [Tt][Aa][Bb][Ll][Ee][Ss][Aa][Mm][Pp][Ll][Ee];
BERNOULLI: [Bb][Ee][Rr][Nn][Oo][Uu][Ll][Ll][Ii];
SYSTEM: [Ss][Yy][Ss][Tt][Ee][Mm];
ORDINALITY: [Oo][Rr][Dd][Ii][Nn][Aa][Ll][Ii][Tt][Yy];
ZONE: [Zz][Oo][Nn][Ee];
AT: [Aa][Tt];
TIME: [Tt][Ii][Mm][Ee];
SHOW: [Ss][Hh][Oo][Ww];
CATALOGS: [Cc][Aa][Tt][Aa][Ll][Oo][Gg][Ss];
SCHEMAS: [Ss][Cc][Hh][Ee][Mm][Aa][Ss];
TABLES: [Tt][Aa][Bb][Ll][Ee][Ss];
COLUMNS: [Cc][Oo][Ll][Uu][Mm][Nn][Ss];
USE: [Uu][Ss][Ee];
DESCRIBE: [Dd][Ee][Ss][Cc][Rr][Ii][Bb][Ee];
EXPLAIN: [Ee][Xx][Pp][Ll][Aa][Ii][Nn];
ANALYZE: [Aa][Nn][Aa][Ll][Yy][Zz][Ee];
VERBOSE: [Vv][Ee][Rr][Bb][Oo][Ss][Ee];
ASC: [Aa][Ss][Cc];
DESC: [Dd][Ee][Ss][Cc];
FIRST: [Ff][Ii][Rr][Ss][Tt];
LAST: [Ll][Aa][Ss][Tt];
YEAR: [Yy][Ee][Aa][Rr];
MONTH: [Mm][Oo][Nn][Tt][Hh];
DAY: [Dd][Aa][Yy];
HOUR: [Hh][Oo][Uu][Rr];
MINUTE: [Mm][Ii][Nn][Uu][Tt][Ee];
SECOND: [Ss][Ee][Cc][Oo][Nn][Dd];
BOOLEAN: [Bb][Oo][Oo][Ll][Ee][Aa][Nn];
TINYINT: [Tt][Ii][Nn][Yy][Ii][Nn][Tt];
SMALLINT: [Ss][Mm][Aa][Ll][Ll][Ii][Nn][Tt];
INT: [Ii][Nn][Tt];
INTEGER: [Ii][Nn][Tt][Ee][Gg][Ee][Rr];
BIGINT: [Bb][Ii][Gg][Ii][Nn][Tt];
FLOAT: [Ff][Ll][Oo][Aa][Tt];
DOUBLE: [Dd][Oo][Uu][Bb][Ll][Ee];
DECIMAL: [Dd][Ee][Cc][Ii][Mm][Aa][Ll];
VARCHAR: [Vv][Aa][Rr][Cc][Hh][Aa][Rr];
CHAR: [Cc][Hh][Aa][Rr];
STRING_TYPE: [Ss][Tt][Rr][Ii][Nn][Gg];
VARBINARY: [Vv][Aa][Rr][Bb][Ii][Nn][Aa][Rr][Yy];
JSON: [Jj][Ss][Oo][Nn];
DATE: [Dd][Aa][Tt][Ee];
TIMESTAMP: [Tt][Ii][Mm][Ee][Ss][Tt][Aa][Mm][Pp];
INTERVAL: [Ii][Nn][Tt][Ee][Rr][Vv][Aa][Ll];
BETWEEN: [Bb][Ee][Tt][Ww][Ee][Ee][Nn];
ESCAPE: [Ee][Ss][Cc][Aa][Pp][Ee];
NULLS: [Nn][Uu][Ll][Ll][Ss];
TO: [Tt][Oo];

// 新增 DDL 关键字
INSERT: [Ii][Nn][Ss][Ee][Rr][Tt];
VALUES: [Vv][Aa][Ll][Uu][Ee][Ss];
SEMI: [Ss][Ee][Mm][Ii];
CREATE: [Cc][Rr][Ee][Aa][Tt][Ee];
TABLE: [Tt][Aa][Bb][Ll][Ee];
IF: [Ii][Ff];
EXISTS: [Ee][Xx][Ii][Ss][Tt][Ss];
DEFAULT: [Dd][Ee][Ff][Aa][Uu][Ll][Tt];
COMMENT: [Cc][Oo][Mm][Mm][Ee][Nn][Tt];
OVERWRITE: [Oo][Vv][Ee][Rr][Ww][Rr][Ii][Tt][Ee];
INTO: [Ii][Nn][Tt][Oo];
USING: [Uu][Ss][Ii][Nn][Gg];
DROP: [Dd][Rr][Oo][Pp];
TRUNCATE: [Tt][Rr][Uu][Nn][Cc][Aa][Tt][Ee];
CATALOG: [Cc][Aa][Tt][Aa][Ll][Oo][Gg];
SCHEMA: [Ss][Cc][Hh][Ee][Mm][Aa];
VIEW: [Vv][Ii][Ee][Ww];
ALTER: [Aa][Ll][Tt][Ee][Rr];
ADD: [Aa][Dd][Dd];
RENAME: [Rr][Ee][Nn][Aa][Mm][Ee];
COLUMN: [Cc][Oo][Ll][Uu][Mm][Nn];
REPLACE: [Rr][Ee][Pp][Ll][Aa][Cc][Ee];
RESTRICT: [Rr][Ee][Ss][Tt][Rr][Ii][Cc][Tt];
CASCADE: [Cc][Aa][Ss][Cc][Aa][Dd][Ee];
FUNCTION: [Ff][Uu][Nn][Cc][Tt][Ii][Oo][Nn];
TEMPORARY: [Tt][Ee][Mm][Pp][Oo][Rr][Aa][Rr][Yy];
SESSION: [Ss][Ee][Ss][Ss][Ii][Oo][Nn];
UUID: [Uu][Uu][Ii][Dd];
IPADDRESS: [Ii][Pp][Aa][Dd][Dd][Rr][Ee][Ss][Ss];
GROUPING: [Gg][Rr][Oo][Uu][Pp][Ii][Nn][Gg];
SETS: [Ss][Ee][Tt][Ss];
CUBE: [Cc][Uu][Bb][Ee];
ROLLUP: [Rr][Oo][Ll][Ll][Uu][Pp];

// 操作符
EQ: '=';
NEQ: '<>' | '!=';
LT: '<';
LTE: '<=';
GT: '>';
GTE: '>=';
PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';

// 字面量
STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

DOUBLE_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Za-z]
    ;

// 注释
LINE_COMMENT
    : '--' ~[\r\n]* -> skip
    ;

BLOCK_COMMENT
    : '/*' .*? '*/' -> skip
    ;

// 空白
WS
    : [ \r\n\t]+ -> skip
    ;

// 非保留关键字（可以作为标识符使用）
nonReserved
    : AT | ZONE | YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    | TABLESAMPLE | SYSTEM | BERNOULLI | ORDINALITY
    | FILTER | OVER | PARTITION | RANGE | ROWS | PRECEDING | FOLLOWING
    | CURRENT | UNBOUNDED | ROW
    | CATALOGS | SCHEMAS | TABLES | COLUMNS | USE | DESCRIBE | EXPLAIN
    | ANALYZE | VERBOSE | FIRST | LAST
    | TIME | INNER | DESC
    | USING | DROP | TRUNCATE | CATALOG | SCHEMA | VIEW | ALTER | ADD | RENAME | IF
    | COLUMN | REPLACE | RESTRICT | CASCADE
    | FUNCTION | TEMPORARY | SESSION
    | STRING_TYPE | VARBINARY | JSON | UUID | IPADDRESS
    | GROUPING | SETS | CUBE | ROLLUP
    ;

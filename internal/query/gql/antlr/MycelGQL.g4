// MycelGQL is a deliberately small GQL-compatible grammar slice.
//
// v0 supports the minimum statements needed to insert and return one node, for
// example:
//
//   INSERT (:Person {name: 'Alice', age: 42})
//   MATCH (p:Person {name: 'Alice'}) RETURN p
//   MATCH (p:Person) WHERE p.name = 'Alice' RETURN p
//
// This is not a complete ISO GQL grammar. Extend it clause by clause as Mycel's
// query execution support grows.
grammar MycelGQL;

query
  : statement EOF
  ;

statement
  : insertStatement
  | mergeNodeStatement
  | matchCreateStatement
  | matchSetStatement
  | matchDeleteStatement
  | matchMergeRelationshipStatement
  | matchStatement
  ;

insertStatement
  : INSERT nodePattern
  ;

matchStatement
  : MATCH pathBinding? matchPattern whereClause? RETURN GRAPH? DISTINCT? returnProjection (COMMA returnProjection)* orderByClause? offsetClause? fetchFirstClause?
  ;

pathBinding
  : variable EQ
  ;

matchCreateStatement
  : MATCH nodePattern (COMMA nodePattern)+ CREATE createRelationshipPattern
  ;

matchSetStatement
  : MATCH matchPattern whereClause? SET setAssignment (COMMA setAssignment)* RETURN GRAPH? returnProjection (COMMA returnProjection)* fetchFirstClause?
  ;

matchDeleteStatement
  : MATCH matchPattern whereClause? DELETE variable (COMMA variable)* RETURN GRAPH? returnProjection (COMMA returnProjection)* fetchFirstClause?
  ;

mergeNodeStatement
  : MERGE nodePattern RETURN GRAPH? returnProjection (COMMA returnProjection)* fetchFirstClause?
  ;

matchMergeRelationshipStatement
  : MATCH nodePattern (COMMA nodePattern)+ MERGE createRelationshipPattern RETURN GRAPH? returnProjection (COMMA returnProjection)* fetchFirstClause?
  ;

setAssignment
  : propertyReference EQ value
  ;

createRelationshipPattern
  : LPAREN variable RPAREN MINUS edgePattern? MINUS GT LPAREN variable RPAREN
  ;

matchPattern
  : nodePattern (relationshipPattern nodePattern)*
  ;

relationshipPattern
  : MINUS edgePattern? MINUS GT
  | LT MINUS edgePattern? MINUS
  | MINUS edgePattern? MINUS
  ;

edgePattern
  : LBRACK variable? labelExpression? relationshipQuantifier? propertyMap? RBRACK
  ;

relationshipQuantifier
  : STAR INTEGER
  | STAR INTEGER DOT DOT INTEGER?
  ;

whereClause
  : WHERE predicate
  ;

predicate
  : predicateOr
  ;

predicateOr
  : predicateAnd (OR predicateAnd)*
  ;

predicateAnd
  : predicateFactor (AND predicateFactor)*
  ;

predicateFactor
  : LPAREN predicate RPAREN
  | predicateTerm
  ;

predicateTerm
  : propertyBetween
  | propertyComparison
  | propertyNullPredicate
  | propertyStringPredicate
  | textContainsPredicate
  | semanticSimilarPredicate
  ;

propertyComparison
  : propertyReference comparisonOperator value
  ;

propertyBetween
  : propertyReference BETWEEN value AND value
  ;

propertyNullPredicate
  : propertyReference IS NOT? NULL
  ;

propertyStringPredicate
  : propertyReference stringPredicateOperator STRING
  ;

stringPredicateOperator
  : CONTAINS
  | STARTS WITH
  | ENDS WITH
  ;

comparisonOperator
  : EQ
  | NEQ
  | LT
  | LTE
  | GT
  | GTE
  ;

textContainsPredicate
  : TEXT_CONTAINS LPAREN propertyReference COMMA STRING RPAREN
  ;

semanticSimilarPredicate
  : SEMANTIC_SIMILAR LPAREN variable COMMA STRING COMMA TOP INTEGER RPAREN
  ;

returnProjection
  : returnItem (AS identifierName)?
  ;

returnItem
  : aggregateFunction
  | propertyReference
  | variable
  ;

aggregateFunction
  : aggregateName LPAREN (STAR | propertyReference | variable) RPAREN
  ;

aggregateName
  : COUNT
  | SUM
  | AVG
  | MIN
  | MAX
  ;

orderByClause
  : ORDER BY orderItem (COMMA orderItem)*
  ;

orderItem
  : propertyReference sortDirection?
  ;

sortDirection
  : ASC
  | DESC
  ;

propertyReference
  : IDENTIFIER DOT IDENTIFIER (DOT IDENTIFIER)?
  ;

offsetClause
  : OFFSET INTEGER rowWord?
  ;

fetchFirstClause
  : FETCH FIRST INTEGER rowWord ONLY
  ;

rowWord
  : ROW
  | ROWS
  ;

nodePattern
  : LPAREN variable? labelExpression? propertyMap? RPAREN
  ;

variable
  : IDENTIFIER
  ;

labelExpression
  : COLON labelName (COLON labelName)*
  ;

labelName
  : identifierName (DOT identifierName)*
  ;

identifierName
  : IDENTIFIER
  | CONTAINS
  | STARTS
  | WITH
  | ENDS
  | COUNT
  | SUM
  | AVG
  | MIN
  | MAX
  | DISTINCT
  | OFFSET
  | OR
  | IS
  | NOT
  ;

propertyMap
  : LBRACE propertyPair (COMMA propertyPair)* COMMA? RBRACE
  ;

propertyPair
  : propertyKey COLON value
  ;

propertyKey
  : identifierName
  ;

value
  : STRING
  | FLOAT
  | INTEGER
  | TRUE
  | FALSE
  | NULL
  | PARAMETER
  ;

INSERT : [Ii] [Nn] [Ss] [Ee] [Rr] [Tt];
CREATE : [Cc] [Rr] [Ee] [Aa] [Tt] [Ee];
MERGE  : [Mm] [Ee] [Rr] [Gg] [Ee];
MATCH  : [Mm] [Aa] [Tt] [Cc] [Hh];
SET    : [Ss] [Ee] [Tt];
DELETE : [Dd] [Ee] [Ll] [Ee] [Tt] [Ee];
AS     : [Aa] [Ss];
WHERE  : [Ww] [Hh] [Ee] [Rr] [Ee];
RETURN : [Rr] [Ee] [Tt] [Uu] [Rr] [Nn];
FETCH  : [Ff] [Ee] [Tt] [Cc] [Hh];
FIRST  : [Ff] [Ii] [Rr] [Ss] [Tt];
ROW    : [Rr] [Oo] [Ww];
ROWS   : [Rr] [Oo] [Ww] [Ss];
ONLY   : [Oo] [Nn] [Ll] [Yy];
ORDER  : [Oo] [Rr] [Dd] [Ee] [Rr];
BY     : [Bb] [Yy];
ASC    : [Aa] [Ss] [Cc];
DESC   : [Dd] [Ee] [Ss] [Cc];
AND    : [Aa] [Nn] [Dd];
OR     : [Oo] [Rr];
IS     : [Ii] [Ss];
NOT    : [Nn] [Oo] [Tt];
CONTAINS : [Cc] [Oo] [Nn] [Tt] [Aa] [Ii] [Nn] [Ss];
STARTS : [Ss] [Tt] [Aa] [Rr] [Tt] [Ss];
WITH   : [Ww] [Ii] [Tt] [Hh];
ENDS   : [Ee] [Nn] [Dd] [Ss];
COUNT  : [Cc] [Oo] [Uu] [Nn] [Tt];
SUM    : [Ss] [Uu] [Mm];
AVG    : [Aa] [Vv] [Gg];
MIN    : [Mm] [Ii] [Nn];
MAX    : [Mm] [Aa] [Xx];
DISTINCT : [Dd] [Ii] [Ss] [Tt] [Ii] [Nn] [Cc] [Tt];
OFFSET : [Oo] [Ff] [Ff] [Ss] [Ee] [Tt];
BETWEEN : [Bb] [Ee] [Tt] [Ww] [Ee] [Ee] [Nn];
GRAPH  : [Gg] [Rr] [Aa] [Pp] [Hh];
TEXT_CONTAINS : [Tt] [Ee] [Xx] [Tt] '_' [Cc] [Oo] [Nn] [Tt] [Aa] [Ii] [Nn] [Ss];
SEMANTIC_SIMILAR : [Ss] [Ee] [Mm] [Aa] [Nn] [Tt] [Ii] [Cc] '_' [Ss] [Ii] [Mm] [Ii] [Ll] [Aa] [Rr];
TOP    : [Tt] [Oo] [Pp];
TRUE   : [Tt] [Rr] [Uu] [Ee];
FALSE  : [Ff] [Aa] [Ll] [Ss] [Ee];
NULL   : [Nn] [Uu] [Ll] [Ll];

LPAREN : '(';
RPAREN : ')';
LBRACE : '{';
RBRACE : '}';
LBRACK : '[';
RBRACK : ']';
COLON  : ':';
COMMA  : ',';
DOT    : '.';
EQ     : '=';
NEQ    : '<>' | '!=';
LTE    : '<=';
GTE    : '>=';
STAR   : '*';
MINUS  : '-';
LT     : '<';
GT     : '>';

FLOAT
  : '-'? DIGIT+ '.' DIGIT+
  ;

INTEGER
  : '-'? DIGIT+
  ;

STRING
  : '\'' ( ~['\\] | ESCAPE_SEQUENCE )* '\''
  | '"'  ( ~["\\] | ESCAPE_SEQUENCE )* '"'
  ;

PARAMETER
  : '$' IDENTIFIER_START IDENTIFIER_PART*
  ;

IDENTIFIER
  : IDENTIFIER_START IDENTIFIER_PART*
  ;

WS
  : [ \t\r\n]+ -> skip
  ;

fragment DIGIT
  : [0-9]
  ;

fragment IDENTIFIER_START
  : [A-Za-z_]
  ;

fragment IDENTIFIER_PART
  : [A-Za-z0-9_]
  ;

fragment ESCAPE_SEQUENCE
  : '\\' .
  ;

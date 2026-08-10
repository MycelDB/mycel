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
  | matchCreateStatement
  | matchStatement
  ;

insertStatement
  : INSERT nodePattern
  ;

matchStatement
  : MATCH matchPattern whereClause? RETURN GRAPH? returnItem (COMMA returnItem)* orderByClause? fetchFirstClause?
  ;

matchCreateStatement
  : MATCH nodePattern (COMMA nodePattern)+ CREATE createRelationshipPattern
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
  : predicateTerm (AND predicateTerm)*
  ;

predicateTerm
  : propertyBetween
  | propertyComparison
  | textContainsPredicate
  | semanticSimilarPredicate
  ;

propertyComparison
  : IDENTIFIER DOT IDENTIFIER comparisonOperator value
  ;

propertyBetween
  : IDENTIFIER DOT IDENTIFIER BETWEEN value AND value
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

returnItem
  : propertyReference
  | variable
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
  : IDENTIFIER (DOT IDENTIFIER)*
  ;

propertyMap
  : LBRACE propertyPair (COMMA propertyPair)* COMMA? RBRACE
  ;

propertyPair
  : propertyKey COLON value
  ;

propertyKey
  : IDENTIFIER
  ;

value
  : STRING
  | FLOAT
  | INTEGER
  | TRUE
  | FALSE
  | NULL
  ;

INSERT : [Ii] [Nn] [Ss] [Ee] [Rr] [Tt];
CREATE : [Cc] [Rr] [Ee] [Aa] [Tt] [Ee];
MATCH  : [Mm] [Aa] [Tt] [Cc] [Hh];
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

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
  | matchStatement
  ;

insertStatement
  : INSERT nodePattern
  ;

matchStatement
  : MATCH matchPattern whereClause? RETURN returnItem (COMMA returnItem)* fetchFirstClause?
  ;

matchPattern
  : nodePattern (relationshipPattern nodePattern)?
  ;

relationshipPattern
  : MINUS edgePattern? MINUS GT
  | LT MINUS edgePattern? MINUS
  | MINUS edgePattern? MINUS
  ;

edgePattern
  : LBRACK variable? labelExpression? propertyMap? RBRACK
  ;

whereClause
  : WHERE predicate
  ;

predicate
  : propertyComparison (AND propertyComparison)*
  ;

propertyComparison
  : IDENTIFIER DOT IDENTIFIER EQ value
  ;

returnItem
  : propertyReference
  | variable
  ;

propertyReference
  : IDENTIFIER DOT IDENTIFIER
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
  : IDENTIFIER
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
MATCH  : [Mm] [Aa] [Tt] [Cc] [Hh];
WHERE  : [Ww] [Hh] [Ee] [Rr] [Ee];
RETURN : [Rr] [Ee] [Tt] [Uu] [Rr] [Nn];
FETCH  : [Ff] [Ee] [Tt] [Cc] [Hh];
FIRST  : [Ff] [Ii] [Rr] [Ss] [Tt];
ROW    : [Rr] [Oo] [Ww];
ROWS   : [Rr] [Oo] [Ww] [Ss];
ONLY   : [Oo] [Nn] [Ll] [Yy];
AND    : [Aa] [Nn] [Dd];
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

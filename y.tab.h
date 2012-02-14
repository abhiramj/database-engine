#define OR 257
#define AND 258
#define Name 259
#define String 260
#define Float 261
#define Int 262
#ifdef YYSTYPE
#undef  YYSTYPE_IS_DECLARED
#define YYSTYPE_IS_DECLARED 1
#endif
#ifndef YYSTYPE_IS_DECLARED
#define YYSTYPE_IS_DECLARED 1
typedef union {
 	struct Operand *myOperand;
	struct ComparisonOp *myComparison; 
  	struct OrList *myOrList;
  	struct AndList *myAndList;
	char *actualChars;
} YYSTYPE;
#endif /* !YYSTYPE_IS_DECLARED */
extern YYSTYPE yylval;

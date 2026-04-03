
// Generated from Cypher.g4 by ANTLR 4.11.1

#pragma once


#include "antlr4-runtime.h"




class  CypherLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, T__21 = 22, T__22 = 23, T__23 = 24, T__24 = 25, T__25 = 26, 
    T__26 = 27, T__27 = 28, T__28 = 29, T__29 = 30, T__30 = 31, T__31 = 32, 
    T__32 = 33, T__33 = 34, T__34 = 35, T__35 = 36, T__36 = 37, T__37 = 38, 
    T__38 = 39, T__39 = 40, T__40 = 41, T__41 = 42, T__42 = 43, T__43 = 44, 
    T__44 = 45, COPY = 46, FROM = 47, NODE = 48, TABLE = 49, DROP = 50, 
    PRIMARY = 51, KEY = 52, REL = 53, TO = 54, EXPLAIN = 55, PROFILE = 56, 
    UNION = 57, ALL = 58, FOREACH = 59, OPTIONAL = 60, MATCH = 61, UNWIND = 62, 
    CREATE = 63, SET = 64, DELETE = 65, WITH = 66, RETURN = 67, DISTINCT = 68, 
    STAR = 69, AS = 70, ORDER = 71, BY = 72, L_SKIP = 73, LIMIT = 74, ASCENDING = 75, 
    ASC = 76, DESCENDING = 77, DESC = 78, WHERE = 79, SHORTESTPATH = 80, 
    ALLSHORTESTPATHS = 81, RANGE = 82, OR = 83, XOR = 84, AND = 85, NOT = 86, 
    INVALID_NOT_EQUAL = 87, MINUS = 88, FACTORIAL = 89, IN = 90, STARTS = 91, 
    ENDS = 92, CONTAINS = 93, IS = 94, NULL_ = 95, REDUCE = 96, TRUE = 97, 
    FALSE = 98, EXISTS = 99, CASE = 100, ELSE = 101, END = 102, WHEN = 103, 
    THEN = 104, StringLiteral = 105, EscapedChar = 106, DecimalInteger = 107, 
    HexLetter = 108, HexDigit = 109, Digit = 110, NonZeroDigit = 111, NonZeroOctDigit = 112, 
    ZeroDigit = 113, RegularDecimalReal = 114, UnescapedSymbolicName = 115, 
    IdentifierStart = 116, IdentifierPart = 117, EscapedSymbolicName = 118, 
    SP = 119, WHITESPACE = 120, Comment = 121, Unknown = 122
  };

  explicit CypherLexer(antlr4::CharStream *input);

  ~CypherLexer() override;


  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  // By default the static state used to implement the lexer is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:

  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

};


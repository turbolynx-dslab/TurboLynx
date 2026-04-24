// =============================================================================
// [nl2cypher] QuoteCypherIdent unit tests
// =============================================================================
// Tag: [nl2cypher][ident]
//
// Covers the identifier quoting helper used when splicing catalog names
// (labels, edge types, property names) into generated Cypher. Regression
// coverage for issue #28 — label and edge-type identifiers must not be
// inserted raw into MATCH clauses.
// =============================================================================

#include <string>

#include "catch.hpp"
#include "nl2cypher/profile_collector.hpp"

using turbolynx::nl2cypher::QuoteCypherIdent;

TEST_CASE("QuoteCypherIdent — bare identifiers pass through",
          "[nl2cypher][ident]") {
    REQUIRE(QuoteCypherIdent("Person")      == "Person");
    REQUIRE(QuoteCypherIdent("KNOWS")       == "KNOWS");
    REQUIRE(QuoteCypherIdent("firstName")   == "firstName");
    REQUIRE(QuoteCypherIdent("_underscore") == "_underscore");
    REQUIRE(QuoteCypherIdent("a1b2")        == "a1b2");
}

TEST_CASE("QuoteCypherIdent — empty string is quoted to empty backticks",
          "[nl2cypher][ident]") {
    REQUIRE(QuoteCypherIdent("") == "``");
}

TEST_CASE("QuoteCypherIdent — leading digit requires quoting",
          "[nl2cypher][ident]") {
    // Bare Cypher identifiers can't start with a digit, even though
    // every subsequent char is alnum.
    REQUIRE(QuoteCypherIdent("1abc") == "`1abc`");
    REQUIRE(QuoteCypherIdent("9")    == "`9`");
}

TEST_CASE("QuoteCypherIdent — whitespace and punctuation are quoted",
          "[nl2cypher][ident]") {
    REQUIRE(QuoteCypherIdent("has space")  == "`has space`");
    REQUIRE(QuoteCypherIdent("place.name") == "`place.name`");
    REQUIRE(QuoteCypherIdent("a-b")        == "`a-b`");
    REQUIRE(QuoteCypherIdent("a:b")        == "`a:b`");
}

TEST_CASE("QuoteCypherIdent — embedded backticks are doubled",
          "[nl2cypher][ident]") {
    REQUIRE(QuoteCypherIdent("a`b")   == "`a``b`");
    // Two input backticks → opening ` + `` + `` + closing ` = 6 backticks.
    REQUIRE(QuoteCypherIdent("``")    == "``````");
    REQUIRE(QuoteCypherIdent("`x`y`") == "```x``y```");
}

TEST_CASE("QuoteCypherIdent — output is splicable into MATCH clauses",
          "[nl2cypher][ident]") {
    // This is the actual shape ProfileCollector builds. Confirming the
    // result parses as one label / edge-type token rather than two is
    // the whole point of the fix.
    const std::string label = QuoteCypherIdent("weird label");
    REQUIRE(("MATCH (n:" + label + ")") == "MATCH (n:`weird label`)");

    const std::string edge = QuoteCypherIdent("HAS-TAG");
    REQUIRE(("MATCH ()-[r:" + edge + "]->()")
            == "MATCH ()-[r:`HAS-TAG`]->()");
}

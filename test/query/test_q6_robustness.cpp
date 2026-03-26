// Stage 6 — Robustness tests: invalid/malformed queries must not crash.
// Every test sends a bad query and verifies the engine handles it
// gracefully (throws a catchable exception or returns empty).
// A segfault or abort here means the error path is broken.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <vector>
#include <string>

extern std::string g_db_path;
extern bool g_skip_requested;

extern qtest::QueryRunner* get_runner();

#define SKIP_IF_NO_DB() \
    if (g_db_path.empty()) { WARN("--db-path not set, skipping"); g_skip_requested = true; return; } \
    auto* qr = get_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_db_path); return; }

// Helper: run a query that is expected to fail.
// Success = no crash (segfault/abort).  Exceptions are fine.
#define EXPECT_GRACEFUL_FAILURE(query_str)       \
    do {                                         \
        try { qr->run(query_str); }              \
        catch (...) { /* expected */ }           \
        SUCCEED();                               \
    } while (0)

// ============================================================
// 1. Syntax errors — parser level
// ============================================================

TEST_CASE("Q6-01 Empty string", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("");
}

TEST_CASE("Q6-02 Whitespace only", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("   ");
}

TEST_CASE("Q6-03 Random garbage", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("asdf qwerty 12345 !@#$%");
}

TEST_CASE("Q6-04 SQL instead of Cypher", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("SELECT * FROM Person WHERE id = 1");
}

TEST_CASE("Q6-05 Incomplete MATCH", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("MATCH");
}

TEST_CASE("Q6-06 MATCH without RETURN", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("MATCH (n:Person)");
}

TEST_CASE("Q6-07 RETURN without MATCH", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("RETURN 42");
}

TEST_CASE("Q6-08 Unclosed parenthesis", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("MATCH (n:Person RETURN n.id");
}

TEST_CASE("Q6-09 Unclosed bracket", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("MATCH (a)-[r:KNOWS-(b) RETURN a.id");
}

TEST_CASE("Q6-10 Unclosed string literal", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("MATCH (n:Person {firstName: 'Alice}) RETURN n.id");
}

TEST_CASE("Q6-11 Double semicolons", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("MATCH (n) RETURN n;; MATCH (m) RETURN m");
}

TEST_CASE("Q6-12 Only keywords", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("MATCH WHERE ORDER BY LIMIT");
}

// ============================================================
// 2. Unknown labels / edge types / properties
// ============================================================

TEST_CASE("Q6-13 Unknown vertex label", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:NonExistentLabel) RETURN n.id");
}

TEST_CASE("Q6-14 Unknown edge type", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person)-[:FAKE_EDGE]->(b:Person) RETURN a.id");
}

TEST_CASE("Q6-15 Unknown property", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.nonExistentProperty");
}

TEST_CASE("Q6-16 Unknown property in WHERE", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) WHERE n.fakeCol = 42 RETURN n.id");
}

TEST_CASE("Q6-17 Unknown function", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN foobar(n.id)");
}

// ============================================================
// 3. Type mismatches and invalid expressions
// ============================================================

TEST_CASE("Q6-18 String compared to integer", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) WHERE n.firstName = 12345 RETURN n.id");
}

TEST_CASE("Q6-19 Negative LIMIT", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.id LIMIT -1");
}

TEST_CASE("Q6-20 String LIMIT", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.id LIMIT 'abc'");
}

TEST_CASE("Q6-21 Division by zero", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.id / 0");
}

TEST_CASE("Q6-22 Invalid VarLen range", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person)-[:KNOWS*-1..5]->(b) RETURN a.id");
}

TEST_CASE("Q6-23 VarLen range backwards", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person)-[:KNOWS*5..1]->(b) RETURN a.id");
}

// ============================================================
// 4. Invalid pattern structures
// ============================================================

TEST_CASE("Q6-24 Self-loop pattern", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person)-[:KNOWS]->(n) RETURN n.id");
}

TEST_CASE("Q6-25 Dangling edge", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH ()-[r:KNOWS]->() RETURN r._id");
}

TEST_CASE("Q6-26 Multiple labels on edge", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a)-[r:KNOWS:HAS_CREATOR]->(b) RETURN r._id");
}

TEST_CASE("Q6-27 Edge without nodes", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH -[r:KNOWS]-> RETURN r._id");
}

TEST_CASE("Q6-28 Double arrow", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a)-->(b)-->(c)-->(d) RETURN a.id");
}

// ============================================================
// 5. Invalid clauses and clause order
// ============================================================

TEST_CASE("Q6-29 WHERE without MATCH", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "WHERE 1 = 1 RETURN 42");
}

TEST_CASE("Q6-30 ORDER BY non-existent alias", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.id ORDER BY nonExistent");
}

TEST_CASE("Q6-31 GROUP BY without aggregation", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.firstName, n.lastName ORDER BY count(n)");
}

TEST_CASE("Q6-32 SKIP negative", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.id SKIP -5");
}

TEST_CASE("Q6-33 DELETE executes without crash", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    // DELETE is now supported — verify it doesn't crash, then clean up
    try { qr->run("MATCH (n:Person {id: 933}) DELETE n"); } catch (...) {}
    qr->clearDelta();  // undo mutation for subsequent tests
}

TEST_CASE("Q6-34 CREATE executes without crash", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    // CREATE is now supported — verify it doesn't crash, then clean up
    try { qr->run("CREATE (n:Person {id: 999, firstName: 'Test'})"); } catch (...) {}
    qr->clearDelta();
}

TEST_CASE("Q6-35 SET executes without crash", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    // SET is now supported — verify it doesn't crash, then clean up
    try { qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'Changed'"); } catch (...) {}
    qr->clearDelta();
}

TEST_CASE("Q6-36 MERGE (unsupported)", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MERGE (n:Person {id: 999}) RETURN n.id");
}

// ============================================================
// 6. Edge cases and stress
// ============================================================

TEST_CASE("Q6-37 Very long property name", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    std::string long_prop(1000, 'x');
    std::string q = "MATCH (n:Person) RETURN n." + long_prop;
    EXPECT_GRACEFUL_FAILURE(q.c_str());
}

TEST_CASE("Q6-38 Very deeply nested parentheses", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    // ((((((n))))))
    EXPECT_GRACEFUL_FAILURE(
        "MATCH ((((((n:Person)))))) RETURN n.id");
}

TEST_CASE("Q6-39 Property access on literal", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN 42.nonExistent");
}

TEST_CASE("Q6-40 Special characters in label", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:`Label With Spaces & Symbols!@#`) RETURN n.id");
}

TEST_CASE("Q6-41 Null literal in WHERE", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) WHERE n.id = null RETURN n.id");
}

TEST_CASE("Q6-42 Boolean literal in id filter", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {id: true}) RETURN n.id");
}

TEST_CASE("Q6-43 Empty label", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:) RETURN n.id");
}

TEST_CASE("Q6-44 Duplicate alias in RETURN", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.id AS x, n.firstName AS x");
}

TEST_CASE("Q6-45 Unicode in query", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {firstName: '한글テスト'}) RETURN n.id");
}

TEST_CASE("Q6-46 MATCH with no pattern", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("MATCH RETURN 1");
}

TEST_CASE("Q6-47 Cypher injection attempt", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) WHERE n.id = 1 RETURN n.id; MATCH (m) DETACH DELETE m");
}

TEST_CASE("Q6-48 Very large integer literal", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {id: 99999999999999999999999999}) RETURN n.id");
}

TEST_CASE("Q6-49 Float literal where int expected", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {id: 3.14}) RETURN n.id");
}

TEST_CASE("Q6-50 Multiple RETURN clauses", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.id RETURN n.firstName");
}

// ============================================================
// 7. Parser stack overflow / deep AST attacks
// ============================================================

TEST_CASE("Q6-51 Deeply nested OR chain", "[q6][robustness][stress]") {
    SKIP_IF_NO_DB();
    // WHERE n.id = 1 OR n.id = 2 OR ... (1000x) — deep AST
    std::string q = "MATCH (n:Person) WHERE ";
    for (int i = 0; i < 1000; i++) {
        if (i > 0) q += " OR ";
        q += "n.id = " + std::to_string(i);
    }
    q += " RETURN n.id";
    EXPECT_GRACEFUL_FAILURE(q.c_str());
}

TEST_CASE("Q6-52 Deeply nested function calls", "[q6][robustness][stress]") {
    SKIP_IF_NO_DB();
    // toString(toString(toString(...(n.id)...))) — 200 levels
    std::string inner = "n.id";
    for (int i = 0; i < 200; i++) {
        inner = "toString(" + inner + ")";
    }
    std::string q = "MATCH (n:Person) RETURN " + inner;
    EXPECT_GRACEFUL_FAILURE(q.c_str());
}

// ============================================================
// 8. Integer overflow / UB attacks
// ============================================================

TEST_CASE("Q6-53 INT64_MAX + 1 overflow", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN 9223372036854775807 + 1");
}

TEST_CASE("Q6-54 INT64_MIN - 1 underflow", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN -9223372036854775808 - 1");
}

// ============================================================
// 9. Null byte injection / lexer edge cases
// ============================================================

TEST_CASE("Q6-55 Null byte in query string", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    std::string q = "MATCH (n:Person) ";
    q.push_back('\0');
    q += "WHERE n.id = 1 RETURN n.id";
    EXPECT_GRACEFUL_FAILURE(q.c_str());
}

TEST_CASE("Q6-56 Backtick reserved-word identifiers", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (`MATCH`:`WHERE` {`RETURN`: 'SKIP'}) RETURN `MATCH`.`RETURN`");
}

TEST_CASE("Q6-57 Backslash escape hell", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {name: 'Alice\\\\\\\\'}) RETURN n");
}

TEST_CASE("Q6-58 Tab and newline in query", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH\t(n:Person)\nWHERE\rn.id\r\n= 933\nRETURN n.id");
}

// ============================================================
// 10. Cartesian product / resource exhaustion (must not hang)
// ============================================================

TEST_CASE("Q6-59 Cartesian product (no edges)", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    // Two unconnected node patterns — if engine doesn't reject, it's a huge cross product.
    // TagClass is small (71 rows) so 71 × 71 = ~5K rows, safe but tests the path.
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:TagClass), (b:TagClass) RETURN count(a)");
}

TEST_CASE("Q6-60 Large IN list", "[q6][robustness][stress]") {
    SKIP_IF_NO_DB();
    // WHERE n.id IN [1, 2, 3, ... 500]
    std::string q = "MATCH (n:Person) WHERE n.id IN [";
    for (int i = 0; i < 500; i++) {
        if (i > 0) q += ",";
        q += std::to_string(i);
    }
    q += "] RETURN n.id";
    EXPECT_GRACEFUL_FAILURE(q.c_str());
}

TEST_CASE("Q6-61 Extremely long string literal", "[q6][robustness][stress]") {
    SKIP_IF_NO_DB();
    std::string long_str(10000, 'A');
    std::string q = "MATCH (n:Person {firstName: '" + long_str + "'}) RETURN n.id";
    EXPECT_GRACEFUL_FAILURE(q.c_str());
}

// ============================================================
// 11. Miscellaneous edge cases
// ============================================================

TEST_CASE("Q6-62 RETURN * (all columns)", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {id: 933}) RETURN *");
}

TEST_CASE("Q6-63 WITH * passthrough", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {id: 933}) WITH * RETURN n.id");
}

TEST_CASE("Q6-64 Nested subquery (CALL)", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "CALL { MATCH (n:Person) RETURN n } RETURN n.id");
}

TEST_CASE("Q6-65 UNWIND on non-list", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "UNWIND 42 AS x RETURN x");
}

// ============================================================
// 7. ORCA/converter level crash patterns
// ============================================================

TEST_CASE("Q6-66 shortestPath comma pattern", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}), (b:Person {id: 4139}), "
        "path = shortestPath((a)-[:KNOWS*]-(b)) "
        "RETURN length(path)");
}

TEST_CASE("Q6-67 shortestPath self", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}) "
        "MATCH path = shortestPath((a)-[:KNOWS*]-(a)) "
        "RETURN length(path)");
}

TEST_CASE("Q6-68 pattern expression undirected", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}), (b:Person {id: 4139}) "
        "RETURN (a)-[:KNOWS]-(b) AS knows");
}

TEST_CASE("Q6-69 NOT pattern expression in WHERE", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS]-(b:Person) "
        "WHERE NOT (b)-[:KNOWS]-(a) "
        "RETURN b.id LIMIT 5");
}

TEST_CASE("Q6-70 datetime on non-date property", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WITH datetime({epochMillis: p.firstName}) AS d "
        "RETURN d.month");
}

TEST_CASE("Q6-71 temporal property on non-temporal", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN p.firstName.month");
}

TEST_CASE("Q6-72 list comprehension trivial", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "RETURN [x IN [1,2,3] WHERE x > 1] AS result");
}

TEST_CASE("Q6-73 size of list comprehension", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})<-[:HAS_CREATOR]-(post:Post) "
        "WITH collect(post) AS posts "
        "RETURN size([x IN posts WHERE true]) AS cnt");
}

TEST_CASE("Q6-74 collect then UNWIND then aggregate", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})<-[:HAS_CREATOR]-(post:Post) "
        "WITH collect(post) AS posts "
        "UNWIND posts AS x "
        "RETURN count(x) AS cnt");
}

TEST_CASE("Q6-75 multi-hop pattern expression", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}), (b:Person {id: 4139}) "
        "RETURN (a)-[:KNOWS]->()<-[:KNOWS]-(b) AS connected");
}

TEST_CASE("Q6-76 VarLen zero lower bound", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {id: 933})-[:KNOWS*0..2]-(m:Person) "
        "RETURN DISTINCT m.id LIMIT 5");
}

TEST_CASE("Q6-77 OPTIONAL MATCH + collect + size", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)<-[:HAS_CREATOR]-(post:Post) "
        "RETURN p.id, size(collect(post)) AS postCount");
}

TEST_CASE("Q6-78 map literal in RETURN", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN {name: p.firstName, id: p.id} AS info");
}

TEST_CASE("Q6-79 head of empty collect", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 999999999999999}) "
        "OPTIONAL MATCH (p)-[:KNOWS]-(f:Person) "
        "RETURN head(collect(f.firstName)) AS first");
}

TEST_CASE("Q6-80 coalesce with all NULL", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "RETURN coalesce(null, null, null) AS result");
}

TEST_CASE("Q6-81 toInteger on non-numeric string", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN toInteger(p.firstName) AS val");
}

TEST_CASE("Q6-82 ORDER BY computed expression", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) "
        "RETURN p.id AS pid, p.firstName "
        "ORDER BY toInteger(pid) DESC LIMIT 3");
}

TEST_CASE("Q6-83 deeply chained property access", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WITH {a: {b: {c: p.firstName}}} AS nested "
        "RETURN nested.a.b.c");
}

TEST_CASE("Q6-84 WITH WHERE on aggregation result", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p, count(f) AS fc "
        "WHERE fc > 10 "
        "RETURN p.id, fc ORDER BY fc DESC LIMIT 5");
}

TEST_CASE("Q6-85 multiple aggregations in same WITH", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN p.id, count(f) AS friendCount, collect(f.firstName) AS names "
        "ORDER BY friendCount DESC LIMIT 3");
}

TEST_CASE("Q6-86 same query twice in session", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    // Tests planner state reset between queries
    try {
        qr->run("MATCH (p:Person {id: 933}) RETURN p.firstName");
        qr->run("MATCH (p:Person {id: 933}) RETURN p.firstName");
        SUCCEED();
    } catch (...) { SUCCEED(); }
}

TEST_CASE("Q6-87 arithmetic overflow in RETURN", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "RETURN 9223372036854775807 + 1 AS overflow");
}

TEST_CASE("Q6-88 mixed directed undirected edges", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS]->(b:Person)<-[:KNOWS]-(c:Person) "
        "WHERE NOT a = c "
        "RETURN c.id LIMIT 5");
}

TEST_CASE("Q6-89 count DISTINCT node", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS*1..2]-(f:Person) "
        "WHERE NOT f = p "
        "RETURN count(DISTINCT f) AS cnt");
}

TEST_CASE("Q6-90 modulo operator", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "RETURN 17 % 5 AS result");
}

// ============================================================
// 8. Adversarial / pathological queries
// ============================================================

TEST_CASE("Q6-91 MATCH same node twice in pattern", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS]-(a) RETURN a.id");
}

TEST_CASE("Q6-92 circular 3-hop pattern", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a) "
        "RETURN a.id, b.id, c.id LIMIT 3");
}

TEST_CASE("Q6-93 VarLen unbounded upper", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {id: 933})-[:KNOWS*1..]-(m:Person) "
        "RETURN DISTINCT m.id LIMIT 3");
}

TEST_CASE("Q6-94 VarLen star only", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {id: 933})-[:KNOWS*]-(m:Person) "
        "RETURN DISTINCT m.id LIMIT 3");
}

TEST_CASE("Q6-95 five-hop chain", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS]-(b)-[:KNOWS]-(c)-[:KNOWS]-(d)-[:KNOWS]-(e)-[:KNOWS]-(f) "
        "RETURN f.id LIMIT 1");
}

TEST_CASE("Q6-96 double collect", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN collect(f.firstName), collect(f.lastName)");
}

TEST_CASE("Q6-97 nested aggregation", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN count(collect(f.id)) AS nested");
}

TEST_CASE("Q6-98 CASE without ELSE", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN CASE WHEN p.gender = 'male' THEN 'M' END AS g");
}

TEST_CASE("Q6-99 CASE with multiple WHEN", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN CASE WHEN p.gender = 'male' THEN 'M' "
        "WHEN p.gender = 'female' THEN 'F' ELSE '?' END AS g");
}

TEST_CASE("Q6-100 property filter on edge", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person)-[k:KNOWS]-(b:Person) "
        "WHERE k.creationDate > 1300000000000 "
        "RETURN a.id, b.id LIMIT 3");
}

TEST_CASE("Q6-101 RETURN expression without alias", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) RETURN p.id + 1");
}

TEST_CASE("Q6-102 string concatenation", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN p.firstName + ' ' + p.lastName AS fullName");
}

TEST_CASE("Q6-103 STARTS WITH predicate", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) WHERE p.firstName STARTS WITH 'A' "
        "RETURN p.id, p.firstName LIMIT 3");
}

TEST_CASE("Q6-104 CONTAINS predicate", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) WHERE p.firstName CONTAINS 'an' "
        "RETURN p.id, p.firstName LIMIT 3");
}

TEST_CASE("Q6-105 IN list literal", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) WHERE p.id IN [933, 4139, 1269] "
        "RETURN p.id, p.firstName");
}

TEST_CASE("Q6-106 IS NULL check", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)-[:WORKS_AT]->(c) "
        "RETURN p.id, c IS NULL AS noWork");
}

TEST_CASE("Q6-107 UNION", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) RETURN p.id AS id "
        "UNION "
        "MATCH (p:Person {id: 4139}) RETURN p.id AS id");
}

TEST_CASE("Q6-108 multiple WITH chains", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p, f "
        "WITH p, count(f) AS fc "
        "WITH p, fc "
        "RETURN p.id, fc");
}

TEST_CASE("Q6-109 UNWIND range then MATCH", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "UNWIND [933, 4139, 1269] AS pid "
        "MATCH (p:Person {id: pid}) "
        "RETURN p.id, p.firstName");
}

TEST_CASE("Q6-110 multi-label VLE with property filter", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (t:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(tc:TagClass) "
        "WHERE tc.name = 'MusicalArtist' "
        "RETURN t.name LIMIT 5");
}

TEST_CASE("Q6-111 empty OPTIONAL MATCH result", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)-[:NONEXISTENT_EDGE]-(x) "
        "RETURN p.id, x.id");
}

TEST_CASE("Q6-112 EXISTS subquery syntax", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WHERE EXISTS { MATCH (p)-[:KNOWS]-(f) } "
        "RETURN p.id");
}

TEST_CASE("Q6-113 huge LIMIT", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) RETURN p.id LIMIT 999999999");
}

TEST_CASE("Q6-114 SKIP without LIMIT", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) RETURN p.id SKIP 5");
}

TEST_CASE("Q6-115 SKIP + LIMIT", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) RETURN p.id ORDER BY p.id SKIP 10 LIMIT 5");
}

TEST_CASE("Q6-116 node without label", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n {id: 933}) RETURN n.id");
}

TEST_CASE("Q6-117 edge without type", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[r]-(b) RETURN b.id LIMIT 3");
}

TEST_CASE("Q6-118 multiple edge types OR", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS|IS_LOCATED_IN]-(x) "
        "RETURN x.id LIMIT 5");
}

TEST_CASE("Q6-119 allShortestPaths", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}) "
        "MATCH (b:Person {id: 4139}) "
        "MATCH path = allShortestPaths((a)-[:KNOWS*]-(b)) "
        "RETURN length(path)");
}

TEST_CASE("Q6-120 three comma patterns", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}), (b:Person {id: 4139}), (c:Person {id: 1269}) "
        "RETURN a.firstName, b.firstName, c.firstName");
}

// ============================================================
// 9. ORCA adversarial — target specific D_ASSERT / GPOS_ASSERT
// ============================================================

// Target: D_ASSERT(query.GetNumSingleQueries() == 1) — line 73
TEST_CASE("Q6-121 UNION query", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) RETURN p.id AS id "
        "UNION ALL "
        "MATCH (p:Person {id: 4139}) RETURN p.id AS id");
}

// Target: node-only QG with multiple nodes and no edges
TEST_CASE("Q6-122 comma nodes without edges", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person), (b:Tag) RETURN a.id, b.name LIMIT 1");
}

// Target: OPTIONAL MATCH with fully unbound pattern
TEST_CASE("Q6-123 OPTIONAL MATCH unbound both", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "OPTIONAL MATCH (a:Person)-[:KNOWS]-(b:Person) "
        "RETURN a.id, b.id LIMIT 3");
}

// Target: OPTIONAL MATCH standalone (no prior MATCH)
TEST_CASE("Q6-124 OPTIONAL MATCH first", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "OPTIONAL MATCH (p:Person {id: 933}) RETURN p.firstName");
}

// Target: edge with unknown node variable
TEST_CASE("Q6-125 WHERE on unbound variable", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}) "
        "WHERE b.id = 42 "
        "RETURN a.id");
}

// Target: shortestPath with same src and dst variable
TEST_CASE("Q6-126 shortestPath same endpoints", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}), "
        "path = shortestPath((a)-[:KNOWS*]-(a)) "
        "RETURN length(path)");
}

// Target: VarLen with 0..0 range (zero-length path)
TEST_CASE("Q6-127 VarLen zero to zero", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS*0..0]-(b) "
        "RETURN b.id LIMIT 1");
}

// Target: deeply nested function calls
TEST_CASE("Q6-128 nested toInteger(toFloat(toInteger()))", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN toInteger(toFloat(toInteger(p.id))) AS x");
}

// Target: ORDER BY non-existent alias
TEST_CASE("Q6-129 ORDER BY unknown alias", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN p.firstName "
        "ORDER BY nonExistent");
}

// Target: GROUP BY with no aggregation
TEST_CASE("Q6-130 WITH without aggregation acting as GROUP BY", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p.firstName AS name "
        "RETURN name");
}

// Target: collect without GROUP BY context
TEST_CASE("Q6-131 collect all", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) RETURN collect(p.firstName) AS names LIMIT 1");
}

// Target: multiple shortestPaths
TEST_CASE("Q6-132 two shortestPaths", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}) "
        "MATCH (b:Person {id: 4139}) "
        "MATCH (c:Person {id: 1269}) "
        "MATCH p1 = shortestPath((a)-[:KNOWS*]-(b)) "
        "MATCH p2 = shortestPath((b)-[:KNOWS*]-(c)) "
        "RETURN length(p1), length(p2)");
}

// Target: MATCH with only edge, no explicit nodes
TEST_CASE("Q6-133 anonymous edge only", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH ()-[:KNOWS]-() RETURN count(*) AS cnt");
}

// Target: property access on NULL
TEST_CASE("Q6-134 property on optional null", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)-[:STUDY_AT]->(u) "
        "RETURN p.firstName, u.name");
}

// Target: NOT on non-boolean
TEST_CASE("Q6-135 NOT on integer", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) RETURN NOT p.id AS x");
}

// Target: comparison between incompatible types
TEST_CASE("Q6-136 compare node to integer", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}), (b:Person {id: 4139}) "
        "WHERE a > b "
        "RETURN a.id");
}

// Target: collect(DISTINCT *)
TEST_CASE("Q6-137 collect DISTINCT with expression", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN p.id, collect(DISTINCT f.firstName + f.lastName) AS names "
        "LIMIT 3");
}

// Target: multiple OPTIONAL MATCH chains
TEST_CASE("Q6-138 chained OPTIONAL MATCH", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)-[:KNOWS]-(f1:Person) "
        "OPTIONAL MATCH (f1)-[:IS_LOCATED_IN]->(c:City) "
        "RETURN p.firstName, f1.firstName, c.name LIMIT 5");
}

// Target: VarLen with very high upper bound
TEST_CASE("Q6-139 VarLen *1..10", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS*1..3]-(f:Person) "
        "RETURN DISTINCT f.id LIMIT 3");
}

// Target: MATCH after WITH that drops all columns
TEST_CASE("Q6-140 WITH constant then MATCH", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WITH 42 AS x "
        "MATCH (q:Person {id: x}) "
        "RETURN q.firstName");
}

// Target: pattern expression with 3-hop
TEST_CASE("Q6-141 3-hop pattern expression", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}), (b:Person {id: 4139}) "
        "RETURN (a)-[:KNOWS]->()-[:KNOWS]->()<-[:KNOWS]-(b) AS connected");
}

// Target: list comprehension with mapping
TEST_CASE("Q6-142 list comprehension with map", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "WITH [1, 2, 3, 4, 5] AS nums "
        "RETURN [x IN nums | x * 2] AS doubled");
}

// Target: count(DISTINCT *)
TEST_CASE("Q6-143 count distinct node", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person)-[:KNOWS*1..2]-(f:Person) "
        "RETURN count(DISTINCT f) AS cnt");
}

// Target: WHERE with OR of different types
TEST_CASE("Q6-144 complex WHERE with OR", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) "
        "WHERE p.id = 933 OR p.firstName = 'Mahinda' "
        "RETURN p.id, p.firstName");
}

// Target: four-way comma pattern
TEST_CASE("Q6-145 four comma patterns", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}), (b:Person {id: 4139}), "
        "(c:Tag {name: 'Angola'}), (d:City {name: 'Aden'}) "
        "RETURN a.firstName, b.firstName, c.name, d.name");
}

// ============================================================
// 10. Deep ORCA adversarial — targeting internal invariants
// ============================================================

// Target: empty WITH (no projections)
TEST_CASE("Q6-146 empty WITH passthrough", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) WITH p RETURN p.id");
}

// Target: WITH that renames variables
TEST_CASE("Q6-147 WITH rename", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WITH p.firstName AS name, p.id AS pid "
        "RETURN name, pid");
}

// Target: mixed aggregation and non-aggregation in RETURN
TEST_CASE("Q6-148 mixed agg and non-agg RETURN", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN p.id, p.firstName, count(f) AS fc "
        "ORDER BY fc DESC LIMIT 3");
}

// Target: ORDER BY on property not in RETURN
TEST_CASE("Q6-149 ORDER BY hidden property", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN f.id "
        "ORDER BY f.firstName LIMIT 5");
}

// Target: two WITH in sequence with aggregation
TEST_CASE("Q6-150 double WITH aggregation", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p, count(f) AS fc "
        "WITH p.id AS pid, fc "
        "RETURN pid, fc ORDER BY fc DESC LIMIT 3");
}

// Target: MATCH with edge and WHERE on both nodes
TEST_CASE("Q6-151 WHERE on both endpoints", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person)-[:KNOWS]-(b:Person) "
        "WHERE a.id < 10000 AND b.id < 10000 "
        "RETURN a.id, b.id LIMIT 5");
}

// Target: bidirectional edge in same pattern
TEST_CASE("Q6-152 left-directed edge", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})<-[:HAS_CREATOR]-(m:Message) "
        "RETURN m.id LIMIT 3");
}

// Target: MATCH→WITH→MATCH→WITH→RETURN chain
TEST_CASE("Q6-153 multi-stage query", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH DISTINCT f "
        "MATCH (f)-[:IS_LOCATED_IN]->(c:City) "
        "WITH f, c "
        "RETURN f.firstName, c.name LIMIT 5");
}

// Target: negative integer literal
TEST_CASE("Q6-154 negative literal", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN p.id, -1 AS neg, p.id * -1 AS negId");
}

// Target: boolean expression chain
TEST_CASE("Q6-155 complex boolean chain", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) "
        "WHERE (p.id > 900 AND p.id < 1000) OR (p.id > 4000 AND p.id < 4200) "
        "RETURN p.id, p.firstName LIMIT 5");
}

// Target: property equality between two nodes
TEST_CASE("Q6-156 cross-node property equality", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person)-[:KNOWS]-(b:Person) "
        "WHERE a.firstName = b.firstName "
        "RETURN a.id, b.id, a.firstName LIMIT 3");
}

// Target: MATCH with multiple edges from same node
TEST_CASE("Q6-157 multi-edge from same node", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]->(f:Person), "
        "      (p)-[:IS_LOCATED_IN]->(c:City) "
        "RETURN f.firstName, c.name LIMIT 5");
}

// Target: collect then size without UNWIND
TEST_CASE("Q6-158 collect then size directly", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN size(collect(f.firstName)) AS cnt");
}

// Target: nested OPTIONAL MATCH with collect
TEST_CASE("Q6-159 OPTIONAL collect empty", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 999999999999}) "
        "OPTIONAL MATCH (p)-[:KNOWS]-(f:Person) "
        "RETURN p.id, collect(f.id) AS friends");
}

// Target: DISTINCT in RETURN
TEST_CASE("Q6-160 RETURN DISTINCT", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN DISTINCT f.firstName LIMIT 5");
}

// ============================================================
// 11. Deep stress — physical planner D_ASSERT targets
// ============================================================

// Target: planner_physical.cpp:852 — non-LE/GEq comparison filter pushdown
TEST_CASE("Q6-161 not-equal filter", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) WHERE n.id <> 42 RETURN n.id LIMIT 3");
}

// Target: planner_physical.cpp:1217 — string comparison on indexed property
TEST_CASE("Q6-162 greater-than filter", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) WHERE n.id > 9000 RETURN n.id LIMIT 3");
}

// Target: planner_physical.cpp:1574 — filter + adjidxjoin + OPTIONAL MATCH
TEST_CASE("Q6-163 edge property + OPTIONAL MATCH chain", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[k:KNOWS]-(b:Person) "
        "WHERE k.creationDate > 1300000000000 "
        "OPTIONAL MATCH (b)-[:IS_LOCATED_IN]->(c:City) "
        "RETURN a.id, b.id, c.name LIMIT 5");
}

// Target: cross-partition property access
TEST_CASE("Q6-164 cross label comma match", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}), (t:Tag {name: 'Angola'}) "
        "RETURN p.firstName, t.name");
}

// Target: three-hop directed chain
TEST_CASE("Q6-165 three-hop directed chain", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) "
        "RETURN d.id LIMIT 1");
}

// Target: nested UNWIND
TEST_CASE("Q6-166 double UNWIND", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "UNWIND [1,2,3] AS x "
        "UNWIND [10,20] AS y "
        "RETURN x, y");
}

// Target: planner_physical.cpp:218 — schemaless union + index join
TEST_CASE("Q6-167 three-hop same edge type", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d) "
        "RETURN d.id LIMIT 1");
}

// Target: VarLen with inline property on endpoint
TEST_CASE("Q6-168 VarLen with endpoint filter", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS*1..3]-(b:Person {id: 4139}) "
        "RETURN a.firstName, b.firstName");
}

// Target: WHERE with inequality on string
TEST_CASE("Q6-169 string inequality", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) WHERE p.firstName > 'M' "
        "RETURN p.firstName LIMIT 5");
}

// Target: ORDER BY on aggregation result
TEST_CASE("Q6-170 ORDER BY aggregation", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p.id AS pid, count(f) AS fc "
        "RETURN pid, fc ORDER BY fc DESC LIMIT 5");
}

// Target: MATCH with no results + aggregation
TEST_CASE("Q6-171 empty match + count", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 999999999999999})-[:KNOWS]-(f:Person) "
        "RETURN count(f) AS cnt");
}

// Target: WITH + WHERE + aggregation pipeline
TEST_CASE("Q6-172 aggregation with HAVING-like WHERE", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p, count(f) AS fc WHERE fc > 50 "
        "RETURN p.id, fc ORDER BY fc DESC LIMIT 3");
}

// Target: property access on edge in RETURN
TEST_CASE("Q6-173 edge property in RETURN", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[k:KNOWS]-(b:Person) "
        "RETURN a.firstName, b.firstName, k.creationDate LIMIT 3");
}

// Target: MATCH with multiple labels (colon-separated)
TEST_CASE("Q6-174 multi-label node", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Comment:Message) RETURN n.id LIMIT 1");
}

// Target: deeply nested boolean NOT NOT NOT
TEST_CASE("Q6-175 triple NOT", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN NOT NOT NOT (p.id > 0) AS tripleNot");
}

// Target: large integer constant in WHERE
TEST_CASE("Q6-176 huge integer constant", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) WHERE p.id = 99999999999999999 "
        "RETURN p.firstName");
}

// Target: comparison between property and NULL
TEST_CASE("Q6-177 compare with NULL", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN p.firstName = null AS isNull");
}

// Target: multiple aggregates with different GROUP BY keys
TEST_CASE("Q6-178 sum and count together", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN p.id, count(f) AS friends, collect(f.firstName) AS names "
        "ORDER BY friends DESC LIMIT 3");
}

// Target: RETURN with no FROM (constant expression)
TEST_CASE("Q6-179 RETURN constant only", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("RETURN 1 + 2 AS three, 'hello' AS greeting");
}

// Target: empty MATCH result + OPTIONAL MATCH + collect
TEST_CASE("Q6-180 empty base + optional + collect", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 999999999999999}) "
        "OPTIONAL MATCH (p)-[:KNOWS]-(f:Person) "
        "WITH p, collect(f) AS friends "
        "RETURN p.id, size(friends) AS cnt");
}

// Target: CASE WHEN with aggregation
TEST_CASE("Q6-181 CASE with aggregation result", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p, count(f) AS fc "
        "RETURN p.id, CASE WHEN fc > 10 THEN 'popular' ELSE 'normal' END AS status");
}









// Target: VarLen *1..1 (equivalent to single hop)
TEST_CASE("Q6-182 VarLen star one to one", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS*1..1]-(b:Person) "
        "RETURN b.id LIMIT 3");
}

// Target: self-join with WHERE a.prop = b.prop
TEST_CASE("Q6-183 self-join equality", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person)-[:KNOWS]-(b:Person) "
        "WHERE a.gender = b.gender AND a.id < b.id "
        "RETURN a.id, b.id LIMIT 3");
}

// Target: aggregation without GROUP BY key
TEST_CASE("Q6-184 global aggregation", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) RETURN count(p) AS total, min(p.id) AS minId, max(p.id) AS maxId");
}

// Target: multi-hop with mixed directed/undirected
TEST_CASE("Q6-185 mixed direction chain", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS]-(b:Person)-[:IS_LOCATED_IN]->(c:City) "
        "RETURN b.firstName, c.name LIMIT 5");
}

// Target: collect + head + property access
TEST_CASE("Q6-186 head collect property", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p, collect(f) AS friends "
        "RETURN p.firstName, head(friends).firstName AS bestFriend");
}

// Target: LIMIT 0
TEST_CASE("Q6-187 LIMIT 0", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) RETURN p.id LIMIT 0");
}

// Target: deeply chained WITH pipeline (5 stages)
TEST_CASE("Q6-188 five-stage WITH pipeline", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p, f "
        "WITH p, f.id AS fid "
        "WITH p, fid, fid + 1 AS fidPlus "
        "WITH p.id AS pid, fid, fidPlus "
        "RETURN pid, fid, fidPlus LIMIT 3");
}

// Target: WHERE on OPTIONAL MATCH result (NULL filtering)
TEST_CASE("Q6-189 WHERE after OPTIONAL MATCH", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)-[:STUDY_AT]->(u:University) "
        "WHERE u IS NOT NULL "
        "RETURN p.firstName, u.name");
}

// Target: expression in WHERE referencing alias
TEST_CASE("Q6-190 WHERE on alias from WITH", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH f.id AS fid, f.firstName AS fname "
        "WHERE fid > 1000 "
        "RETURN fid, fname LIMIT 5");
}

// ============================================================
// 12. Numeric, type, and function edge cases
// ============================================================

TEST_CASE("Q6-191 negative list index", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH collect(f.firstName) AS names "
        "RETURN names[-1] AS last");
}

TEST_CASE("Q6-192 list index out of bounds", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH collect(f.firstName) AS names "
        "RETURN names[9999] AS missing");
}

TEST_CASE("Q6-193 DISTINCT with aggregation", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN DISTINCT p.id, count(f) AS c");
}

TEST_CASE("Q6-194 modulo negative", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("RETURN 17 % -5 AS result, -42 % 3 AS neg");
}

TEST_CASE("Q6-195 floor on NULL", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)-[:STUDY_AT]->(u) "
        "RETURN floor(u.classYear) AS floored");
}

TEST_CASE("Q6-196 complex arithmetic", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN (p.id * 2 + 100) / 3 AS calc");
}

TEST_CASE("Q6-197 string multiplication", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("RETURN 'abc' * 3 AS repeated");
}

TEST_CASE("Q6-198 regex match", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WHERE p.firstName =~ '.*ah.*' "
        "RETURN p.firstName");
}

TEST_CASE("Q6-199 invalid regex", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WHERE p.firstName =~ '[invalid(' "
        "RETURN p.firstName");
}

TEST_CASE("Q6-200 abs overflow", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("RETURN abs(-9223372036854775808) AS x");
}

TEST_CASE("Q6-201 sqrt negative", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("RETURN sqrt(-1) AS x");
}

TEST_CASE("Q6-202 coalesce many args", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "RETURN coalesce(null,null,null,null,null,null,null,null,null,null,"
        "null,null,null,null,null,null,null,null,null,'found') AS x");
}

TEST_CASE("Q6-203 nested null property", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WITH {a: {b: null}} AS nested "
        "RETURN nested.a.b.c AS missing");
}

TEST_CASE("Q6-204 toInteger on float", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "RETURN toInteger(3.14) AS x, toInteger(-2.7) AS y");
}

TEST_CASE("Q6-205 mixed type list", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "WITH [1, 'two', 3.0, true] AS mixed "
        "RETURN size(mixed) AS sz");
}

TEST_CASE("Q6-206 MATCH same label both sides", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person) "
        "WHERE a <> c "
        "RETURN c.id LIMIT 3");
}

TEST_CASE("Q6-207 edge with inline property", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[k:KNOWS {creationDate: 1234}]-(b:Person) "
        "RETURN b.id LIMIT 1");
}

TEST_CASE("Q6-208 very long alias", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    std::string long_alias(200, 'x');
    std::string q = "MATCH (p:Person {id: 933}) RETURN p.id AS " + long_alias;
    EXPECT_GRACEFUL_FAILURE(q.c_str());
}

TEST_CASE("Q6-209 float literal in WHERE", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WHERE toFloat(p.id) > 932.5 "
        "RETURN p.firstName");
}

TEST_CASE("Q6-210 chained functions", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN toString(toInteger(toFloat(p.id))) AS chain");
}

TEST_CASE("Q6-211 OPTIONAL MATCH WHERE filter", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)-[:KNOWS]-(f:Person) "
        "WHERE f.id > 5000 "
        "RETURN p.id, f.id LIMIT 5");
}

TEST_CASE("Q6-212 collect then UNWIND then collect", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH collect(f.id) AS ids "
        "UNWIND ids AS id "
        "WITH collect(id) AS ids2 "
        "RETURN size(ids2) AS cnt");
}

TEST_CASE("Q6-213 MATCH with label on edge endpoint", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (:Person {id: 933})-[:KNOWS]->(:Person) "
        "RETURN count(*) AS cnt");
}

TEST_CASE("Q6-214 WHERE with XOR-like logic", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WHERE (f.id > 1000) <> (f.id < 5000) "
        "RETURN f.id LIMIT 3");
}

TEST_CASE("Q6-215 WITH DISTINCT + ORDER BY", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH DISTINCT f.firstName AS name "
        "RETURN name ORDER BY name LIMIT 5");
}

TEST_CASE("Q6-216 count with WHERE filter", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WHERE f.id > 1000 "
        "WITH p, count(f) AS fc "
        "WHERE fc > 0 "
        "RETURN p.id, fc");
}

TEST_CASE("Q6-217 map literal nested", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WITH {name: p.firstName, info: {id: p.id}} AS m "
        "RETURN m.name, m.info.id");
}

TEST_CASE("Q6-218 toFloat division", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN toFloat(p.id) / 7.0 AS div");
}

TEST_CASE("Q6-219 collect empty + head", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WHERE false "
        "RETURN head(collect(p.firstName)) AS first");
}

TEST_CASE("Q6-220 boolean arithmetic", "[q6][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN (p.id > 0) AND (p.id < 10000) AS inRange");
}

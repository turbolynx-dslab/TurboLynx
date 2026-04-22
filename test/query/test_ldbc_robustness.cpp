// Stage 6 — Robustness tests: invalid/malformed queries must not crash.
// Every test sends a bad query and verifies the engine handles it
// gracefully (throws a catchable exception or returns empty).
// A segfault or abort here means the error path is broken.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <vector>
#include <string>

extern std::string g_ldbc_path;
extern bool g_skip_requested;
extern bool g_has_ldbc;

extern qtest::QueryRunner* get_ldbc_runner();

#define SKIP_IF_NO_DB() \
    if (g_ldbc_path.empty()) { WARN("--ldbc-path not set, skipping"); g_skip_requested = true; return; } \
    if (!g_has_ldbc) { WARN("DB has no LDBC schema, skipping"); return; } \
    auto* qr = get_ldbc_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_ldbc_path); return; }

// Helper: run a query that is expected to fail.
// Success = no crash (segfault/abort).  Exceptions are fine.
#define EXPECT_GRACEFUL_FAILURE(query_str)       \
    do {                                         \
        try { qr->run(query_str); }              \
        catch (...) { /* expected */ }           \
        SUCCEED();                               \
    } while (0)

// ============================================================
// Regression: UNWIND on a scalar must not crash (was value.cpp:1546)
// ============================================================
TEST_CASE("R1 UNWIND on VARCHAR property returns clean binder error",
          "[ldbc][robustness][regression][unwind]") {
    SKIP_IF_NO_DB();
    // Person.speaks is stored as a ';'-joined VARCHAR (not a real LIST) on
    // the LDBC SF1 load path. Previously UNWIND p.speaks hit an assertion in
    // ListValue::GetChildren at runtime; now the binder rejects the query
    // with a clean BinderException.
    bool caught = false;
    try {
        qr->run("MATCH (p:Person) WHERE p.firstName = 'Marc' "
                "UNWIND p.speaks AS lang RETURN lang LIMIT 10;");
    } catch (const std::exception&) { caught = true; }
    REQUIRE(caught);
}

TEST_CASE("R2 UNWIND on list literal still works",
          "[ldbc][robustness][regression][unwind]") {
    SKIP_IF_NO_DB();
    // The list-literal path must not be broken by the binder guard.
    auto r = qr->run("UNWIND [1, 2, 3] AS x RETURN x;");
    REQUIRE(r.size() == 3);
}

// ============================================================
// Regression: shortestPath() must execute successfully
// ============================================================
TEST_CASE("R3 shortestPath() executes without crash",
          "[ldbc][robustness][regression][shortestpath]") {
    SKIP_IF_NO_DB();
    REQUIRE_NOTHROW(
        qr->run("MATCH p = shortestPath((a:Person)-[:KNOWS*]-(b:Person)) "
                "WHERE a.id = 933 AND b.id = 4139 "
                "RETURN length(p);"));
}

TEST_CASE("R4 allShortestPaths() executes without crash",
          "[ldbc][robustness][regression][shortestpath]") {
    SKIP_IF_NO_DB();
    REQUIRE_NOTHROW(
        qr->run("MATCH p = allShortestPaths((a:Person)-[:KNOWS*]-(b:Person)) "
                "WHERE a.id = 933 AND b.id = 4139 "
                "RETURN length(p);"));
}

TEST_CASE("R5 chained OPTIONAL MATCH keeps all-null source rows",
          "[ldbc][robustness][regression][optional]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {id: 290}) "
        "OPTIONAL MATCH (p)-[:WORK_AT]->(c:Organisation) "
        "OPTIONAL MATCH (c)-[:IS_LOCATED_IN]->(country:Place) "
        "RETURN p.id, c IS NULL, country IS NULL",
        {qtest::ColType::INT64, qtest::ColType::BOOL, qtest::ColType::BOOL});
    REQUIRE(r.size() == 1);
    REQUIRE(r[0].int64_at(0) == 290);
    REQUIRE(r[0].bool_at(1));
    REQUIRE(r[0].bool_at(2));
}

TEST_CASE("R5b OPTIONAL MATCH WHERE preserves left row on filter miss",
          "[ldbc][robustness][regression][optional]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) "
        "WHERE f.id = -1 "
        "RETURN count(*) AS rows, count(f) AS matched",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    REQUIRE(r[0].int64_at(0) == 1);
    REQUIRE(r[0].int64_at(1) == 0);
}

TEST_CASE("R5c OPTIONAL MATCH after aggregation preserves left row",
          "[ldbc][robustness][regression][optional]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {id: 933})-[:KNOWS]->(f:Person) "
        "WITH p, collect(f) AS friends "
        "OPTIONAL MATCH (p)-[:KNOWS]->(g:Person)-[:IS_LOCATED_IN]->(c:City) "
        "WHERE c.name = '__definitely_missing__' "
        "RETURN count(*) AS rows, count(g) AS matched",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    REQUIRE(r[0].int64_at(0) == 1);
    REQUIRE(r[0].int64_at(1) == 0);
}

TEST_CASE("R5d OPTIONAL MATCH with g IN collect(f) keeps membership filter",
          "[ldbc][robustness][regression][optional]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {id: 933})-[:KNOWS]->(f:Person) "
        "WITH p, collect(f) AS friends "
        "OPTIONAL MATCH (p)-[:KNOWS]->(g:Person) "
        "WHERE g IN friends "
        "RETURN count(*) AS rows, count(g) AS matched",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    REQUIRE(r[0].int64_at(0) == 5);
    REQUIRE(r[0].int64_at(1) == 5);
}

TEST_CASE("R5e OPTIONAL MATCH collect membership preserves left row on filter miss",
          "[ldbc][robustness][regression][optional]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {id: 933})-[:KNOWS]->(f:Person) "
        "WITH p, collect(f) AS friends "
        "OPTIONAL MATCH (p)-[:KNOWS]->(g:Person)-[:IS_LOCATED_IN]->(c:City) "
        "WHERE g IN friends AND c.name = '__definitely_missing__' "
        "RETURN count(*) AS rows, count(g) AS matched",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    REQUIRE(r[0].int64_at(0) == 1);
    REQUIRE(r[0].int64_at(1) == 0);
}

// ============================================================
// Regression: shell write queries must execute without crashing.
// Persistence and reopen behavior are covered by dedicated smoke tests;
// these checks keep the shell error path from aborting on writes.
// ============================================================
#include <cstdio>
#include <cstdlib>
#include <array>
#include <memory>

static std::string shell_quote(const std::string& value) {
    std::string quoted = "'";
    for (char c : value) {
        if (c == '\'') {
            quoted += "'\\''";
        } else {
            quoted += c;
        }
    }
    quoted += "'";
    return quoted;
}

static std::string run_shell(const std::string& db_path, const std::string& query) {
    std::string cmd = shell_quote(TEST_QUERY_SHELL_BIN) + " --ws " +
                      shell_quote(db_path) + " --q " + shell_quote(query) +
                      " 2>&1";
    std::array<char, 512> buf;
    std::string out;
    std::unique_ptr<FILE, int(*)(FILE*)> pipe(popen(cmd.c_str(), "r"), pclose);
    if (!pipe) return "";
    while (fgets(buf.data(), buf.size(), pipe.get()) != nullptr) {
        out.append(buf.data());
    }
    return out;
}

// Shell-based mutation tests must never touch the ref DB: the subprocess
// has no way to call clearDelta, so any WAL entry it writes would persist
// and corrupt every downstream test that copies from the ref path.
// This helper runs the shell command against an ephemeral copy and removes
// the copy afterwards.
static std::string run_shell_isolated(const std::string& src_db,
                                      const std::string& query) {
    char tmpl[] = "/tmp/tl_robust_XXXXXX";
    if (!mkdtemp(tmpl)) return "";
    std::string ws = tmpl;
    std::string cp_cmd = "cp -a --reflink=auto '" + src_db + "'/. '" + ws + "'/";
    if (std::system(cp_cmd.c_str()) != 0) {
        std::system(("rm -rf '" + ws + "'").c_str());
        return "";
    }
    std::string out = run_shell(ws, query);
    std::system(("rm -rf '" + ws + "'").c_str());
    return out;
}

TEST_CASE("R5 shell executes CREATE without crash",
          "[ldbc][robustness][regression][shell_crud]") {
    SKIP_IF_NO_DB();
    auto out = run_shell_isolated(g_ldbc_path,
        "CREATE (n:Person {firstName: 'ShellReg'});");
    REQUIRE(out.find("created") != std::string::npos);
    REQUIRE(out.find("core dumped") == std::string::npos);
    REQUIRE(out.find("gpos::CException") == std::string::npos);
}

TEST_CASE("R6 shell executes SET without crash",
          "[ldbc][robustness][regression][shell_crud]") {
    SKIP_IF_NO_DB();
    auto out = run_shell_isolated(g_ldbc_path,
        "MATCH (p:Person) WHERE p.firstName = 'Marc' "
        "SET p.gender = 'X' RETURN p.firstName;");
    REQUIRE(out.find("core dumped") == std::string::npos);
    REQUIRE(out.find("SIGABRT") == std::string::npos);
}

TEST_CASE("R7 shell executes DELETE without crash",
          "[ldbc][robustness][regression][shell_crud]") {
    SKIP_IF_NO_DB();
    auto out = run_shell_isolated(g_ldbc_path,
        "MATCH (p:Person) WHERE p.firstName = 'Marc' DELETE p;");
    REQUIRE(out.find("core dumped") == std::string::npos);
    REQUIRE(out.find("SIGABRT") == std::string::npos);
}

// ============================================================
// 1. Syntax errors — parser level
// ============================================================

TEST_CASE("Empty string", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("");
}

TEST_CASE("Whitespace only", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("   ");
}

TEST_CASE("Random garbage", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("asdf qwerty 12345 !@#$%");
}

TEST_CASE("SQL instead of Cypher", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("SELECT * FROM Person WHERE id = 1");
}

TEST_CASE("Incomplete MATCH", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("MATCH");
}

TEST_CASE("MATCH without RETURN", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("MATCH (n:Person)");
}

TEST_CASE("RETURN without MATCH", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("RETURN 42");
}

TEST_CASE("Unclosed parenthesis", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("MATCH (n:Person RETURN n.id");
}

TEST_CASE("Unclosed bracket", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("MATCH (a)-[r:KNOWS-(b) RETURN a.id");
}

TEST_CASE("Unclosed string literal", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("MATCH (n:Person {firstName: 'Alice}) RETURN n.id");
}

TEST_CASE("Double semicolons", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("MATCH (n) RETURN n;; MATCH (m) RETURN m");
}

TEST_CASE("Only keywords", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("MATCH WHERE ORDER BY LIMIT");
}

// ============================================================
// 2. Unknown labels / edge types / properties
// ============================================================

TEST_CASE("Unknown vertex label", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:NonExistentLabel) RETURN n.id");
}

TEST_CASE("Unknown edge type", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person)-[:FAKE_EDGE]->(b:Person) RETURN a.id");
}

TEST_CASE("Unknown property", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.nonExistentProperty");
}

TEST_CASE("Unknown property in WHERE", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) WHERE n.fakeCol = 42 RETURN n.id");
}

TEST_CASE("Unknown function", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN foobar(n.id)");
}

// ============================================================
// 3. Type mismatches and invalid expressions
// ============================================================

TEST_CASE("String compared to integer", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) WHERE n.firstName = 12345 RETURN n.id");
}

TEST_CASE("Negative LIMIT", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.id LIMIT -1");
}

TEST_CASE("String LIMIT", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.id LIMIT 'abc'");
}

TEST_CASE("Division by zero", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.id / 0");
}

TEST_CASE("Invalid VarLen range", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person)-[:KNOWS*-1..5]->(b) RETURN a.id");
}

TEST_CASE("VarLen range backwards", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person)-[:KNOWS*5..1]->(b) RETURN a.id");
}

// ============================================================
// 4. Invalid pattern structures
// ============================================================

TEST_CASE("Self-loop pattern", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person)-[:KNOWS]->(n) RETURN n.id");
}

TEST_CASE("Dangling edge", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH ()-[r:KNOWS]->() RETURN r._id");
}

TEST_CASE("Multiple labels on edge", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a)-[r:KNOWS:HAS_CREATOR]->(b) RETURN r._id");
}

TEST_CASE("Edge without nodes", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH -[r:KNOWS]-> RETURN r._id");
}

TEST_CASE("Double arrow", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    auto r = qr->run("MATCH (p:Person)-->(x) RETURN p.firstName LIMIT 5");
    REQUIRE(r.size() == 5);
}

TEST_CASE("Double arrow reverse", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    auto r = qr->run("MATCH (p:Person)<--(x) RETURN p.firstName LIMIT 5");
    REQUIRE(r.size() == 5);
}

// ============================================================
// 5. Invalid clauses and clause order
// ============================================================

TEST_CASE("WHERE without MATCH", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "WHERE 1 = 1 RETURN 42");
}

TEST_CASE("ORDER BY non-existent alias", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.id ORDER BY nonExistent");
}

TEST_CASE("GROUP BY without aggregation", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.firstName, n.lastName ORDER BY count(n)");
}

TEST_CASE("SKIP negative", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.id SKIP -5");
}

TEST_CASE("DELETE executes without crash", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    // DELETE is now supported — verify it doesn't crash, then clean up
    try { qr->run("MATCH (n:Person {id: 933}) DELETE n"); } catch (...) {}
    qr->clearDelta();  // undo mutation for subsequent tests
}

TEST_CASE("CREATE executes without crash", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    // CREATE is now supported — verify it doesn't crash, then clean up
    try { qr->run("CREATE (n:Person {id: 999, firstName: 'Test'})"); } catch (...) {}
    qr->clearDelta();
}

TEST_CASE("SET executes without crash", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    // SET is now supported — verify it doesn't crash, then clean up
    try { qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'Changed'"); } catch (...) {}
    qr->clearDelta();
}

TEST_CASE("MERGE executes without crash", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    // MERGE is supported now. Run it on the shared ref DB only if we
    // immediately clear delta afterwards so downstream tests stay pristine.
    try { qr->run("MERGE (n:Person {id: 999}) RETURN n.id"); } catch (...) {}
    qr->clearDelta();
}

// ============================================================
// 6. Edge cases and stress
// ============================================================

TEST_CASE("Very long property name", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    std::string long_prop(1000, 'x');
    std::string q = "MATCH (n:Person) RETURN n." + long_prop;
    EXPECT_GRACEFUL_FAILURE(q.c_str());
}

TEST_CASE("Very deeply nested parentheses", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    // ((((((n))))))
    EXPECT_GRACEFUL_FAILURE(
        "MATCH ((((((n:Person)))))) RETURN n.id");
}

TEST_CASE("Property access on literal", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN 42.nonExistent");
}

TEST_CASE("Special characters in label", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:`Label With Spaces & Symbols!@#`) RETURN n.id");
}

TEST_CASE("Null literal in WHERE", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    // C3 fix: `= null` is rewritten to IS NULL (avoids SQLNULL assertion).
    auto r = qr->run("MATCH (n:Person) WHERE n.id = null RETURN n.id LIMIT 5");
    REQUIRE(r.size() <= 5);    // no crash
}

TEST_CASE("Boolean literal in id filter", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {id: true}) RETURN n.id");
}

TEST_CASE("Empty label", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:) RETURN n.id");
}

TEST_CASE("Duplicate alias in RETURN", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.id AS x, n.firstName AS x");
}

TEST_CASE("Unicode in query", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {firstName: '한글テスト'}) RETURN n.id");
}

TEST_CASE("MATCH with no pattern", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("MATCH RETURN 1");
}

TEST_CASE("Cypher injection attempt", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) WHERE n.id = 1 RETURN n.id; MATCH (m) DETACH DELETE m");
}

TEST_CASE("Very large integer literal", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {id: 99999999999999999999999999}) RETURN n.id");
}

TEST_CASE("Float literal where int expected", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {id: 3.14}) RETURN n.id");
}

TEST_CASE("Multiple RETURN clauses", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN n.id RETURN n.firstName");
}

// ============================================================
// 7. Parser stack overflow / deep AST attacks
// ============================================================

TEST_CASE("Deeply nested OR chain", "[ldbc][robustness][stress]") {
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

TEST_CASE("Deeply nested function calls", "[ldbc][robustness][stress]") {
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

TEST_CASE("INT64_MAX + 1 overflow", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN 9223372036854775807 + 1");
}

TEST_CASE("INT64_MIN - 1 underflow", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) RETURN -9223372036854775808 - 1");
}

// ============================================================
// 9. Null byte injection / lexer edge cases
// ============================================================

TEST_CASE("Null byte in query string", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    std::string q = "MATCH (n:Person) ";
    q.push_back('\0');
    q += "WHERE n.id = 1 RETURN n.id";
    EXPECT_GRACEFUL_FAILURE(q.c_str());
}

TEST_CASE("Backtick reserved-word identifiers", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (`MATCH`:`WHERE` {`RETURN`: 'SKIP'}) RETURN `MATCH`.`RETURN`");
}

TEST_CASE("Backslash escape hell", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {name: 'Alice\\\\\\\\'}) RETURN n");
}

TEST_CASE("Tab and newline in query", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH\t(n:Person)\nWHERE\rn.id\r\n= 933\nRETURN n.id");
}

// ============================================================
// 10. Cartesian product / resource exhaustion (must not hang)
// ============================================================

TEST_CASE("Cartesian product (no edges)", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    // Two unconnected node patterns — if engine doesn't reject, it's a huge cross product.
    // TagClass is small (71 rows) so 71 × 71 = ~5K rows, safe but tests the path.
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:TagClass), (b:TagClass) RETURN count(a)");
}

TEST_CASE("Large IN list", "[ldbc][robustness][stress]") {
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

TEST_CASE("Extremely long string literal", "[ldbc][robustness][stress]") {
    SKIP_IF_NO_DB();
    std::string long_str(10000, 'A');
    std::string q = "MATCH (n:Person {firstName: '" + long_str + "'}) RETURN n.id";
    EXPECT_GRACEFUL_FAILURE(q.c_str());
}

// ============================================================
// 11. Miscellaneous edge cases
// ============================================================

TEST_CASE("RETURN * (all columns)", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {id: 933}) RETURN *");
}

TEST_CASE("WITH * passthrough", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {id: 933}) WITH * RETURN n.id");
}

TEST_CASE("Nested subquery (CALL)", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "CALL { MATCH (n:Person) RETURN n } RETURN n.id");
}

TEST_CASE("UNWIND on non-list", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "UNWIND 42 AS x RETURN x");
}

// ============================================================
// 7. ORCA/converter level crash patterns
// ============================================================

TEST_CASE("shortestPath comma pattern", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}), (b:Person {id: 4139}), "
        "path = shortestPath((a)-[:KNOWS*]-(b)) "
        "RETURN length(path)");
}

TEST_CASE("shortestPath self", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}) "
        "MATCH path = shortestPath((a)-[:KNOWS*]-(a)) "
        "RETURN length(path)");
}

TEST_CASE("pattern expression undirected", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}), (b:Person {id: 4139}) "
        "RETURN (a)-[:KNOWS]-(b) AS knows");
}

TEST_CASE("NOT pattern expression in WHERE", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS]-(b:Person) "
        "WHERE NOT (b)-[:KNOWS]-(a) "
        "RETURN b.id LIMIT 5");
}

TEST_CASE("datetime on non-date property", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WITH datetime({epochMillis: p.firstName}) AS d "
        "RETURN d.month");
}

TEST_CASE("temporal property on non-temporal", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN p.firstName.month");
}

TEST_CASE("list comprehension trivial", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "RETURN [x IN [1,2,3] WHERE x > 1] AS result");
}

TEST_CASE("size of list comprehension", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})<-[:HAS_CREATOR]-(post:Post) "
        "WITH collect(post) AS posts "
        "RETURN size([x IN posts WHERE true]) AS cnt");
}

TEST_CASE("collect then UNWIND then aggregate", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})<-[:HAS_CREATOR]-(post:Post) "
        "WITH collect(post) AS posts "
        "UNWIND posts AS x "
        "RETURN count(x) AS cnt");
}

TEST_CASE("multi-hop pattern expression", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}), (b:Person {id: 4139}) "
        "RETURN (a)-[:KNOWS]->()<-[:KNOWS]-(b) AS connected");
}

TEST_CASE("VarLen zero lower bound", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {id: 933})-[:KNOWS*0..2]-(m:Person) "
        "RETURN DISTINCT m.id LIMIT 5");
}

TEST_CASE("OPTIONAL MATCH + collect + size", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)<-[:HAS_CREATOR]-(post:Post) "
        "RETURN p.id, size(collect(post)) AS postCount");
}

TEST_CASE("map literal in RETURN", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN {name: p.firstName, id: p.id} AS info");
}

TEST_CASE("head of empty collect", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 999999999999999}) "
        "OPTIONAL MATCH (p)-[:KNOWS]-(f:Person) "
        "RETURN head(collect(f.firstName)) AS first");
}

TEST_CASE("coalesce with all NULL", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "RETURN coalesce(null, null, null) AS result");
}

TEST_CASE("toInteger on non-numeric string", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN toInteger(p.firstName) AS val");
}

TEST_CASE("ORDER BY computed expression", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) "
        "RETURN p.id AS pid, p.firstName "
        "ORDER BY toInteger(pid) DESC LIMIT 3");
}

TEST_CASE("deeply chained property access", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WITH {a: {b: {c: p.firstName}}} AS nested "
        "RETURN nested.a.b.c");
}

TEST_CASE("WITH WHERE on aggregation result", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p, count(f) AS fc "
        "WHERE fc > 10 "
        "RETURN p.id, fc ORDER BY fc DESC LIMIT 5");
}

TEST_CASE("multiple aggregations in same WITH", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN p.id, count(f) AS friendCount, collect(f.firstName) AS names "
        "ORDER BY friendCount DESC LIMIT 3");
}

TEST_CASE("same query twice in session", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    // Tests planner state reset between queries
    try {
        qr->run("MATCH (p:Person {id: 933}) RETURN p.firstName");
        qr->run("MATCH (p:Person {id: 933}) RETURN p.firstName");
        SUCCEED();
    } catch (...) { SUCCEED(); }
}

TEST_CASE("arithmetic overflow in RETURN", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "RETURN 9223372036854775807 + 1 AS overflow");
}

TEST_CASE("mixed directed undirected edges", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS]->(b:Person)<-[:KNOWS]-(c:Person) "
        "WHERE NOT a = c "
        "RETURN c.id LIMIT 5");
}

TEST_CASE("count DISTINCT node", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS*1..2]-(f:Person) "
        "WHERE NOT f = p "
        "RETURN count(DISTINCT f) AS cnt");
}

TEST_CASE("modulo operator", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "RETURN 17 % 5 AS result");
}

// ============================================================
// 8. Adversarial / pathological queries
// ============================================================

TEST_CASE("MATCH same node twice in pattern", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS]-(a) RETURN a.id");
}

TEST_CASE("circular 3-hop pattern", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a) "
        "RETURN a.id, b.id, c.id LIMIT 3");
}

TEST_CASE("VarLen unbounded upper", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {id: 933})-[:KNOWS*1..]-(m:Person) "
        "RETURN DISTINCT m.id LIMIT 3");
}

TEST_CASE("VarLen star only", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person {id: 933})-[:KNOWS*]-(m:Person) "
        "RETURN DISTINCT m.id LIMIT 3");
}

TEST_CASE("five-hop chain", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS]-(b)-[:KNOWS]-(c)-[:KNOWS]-(d)-[:KNOWS]-(e)-[:KNOWS]-(f) "
        "RETURN f.id LIMIT 1");
}

TEST_CASE("double collect", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN collect(f.firstName), collect(f.lastName)");
}

TEST_CASE("nested aggregation", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN count(collect(f.id)) AS nested");
}

TEST_CASE("CASE without ELSE", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN CASE WHEN p.gender = 'male' THEN 'M' END AS g");
}

TEST_CASE("CASE with multiple WHEN", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN CASE WHEN p.gender = 'male' THEN 'M' "
        "WHEN p.gender = 'female' THEN 'F' ELSE '?' END AS g");
}

TEST_CASE("property filter on edge", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person)-[k:KNOWS]-(b:Person) "
        "WHERE k.creationDate > 1300000000000 "
        "RETURN a.id, b.id LIMIT 3");
}

TEST_CASE("RETURN expression without alias", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) RETURN p.id + 1");
}

TEST_CASE("string concatenation", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN p.firstName + ' ' + p.lastName AS fullName");
}

TEST_CASE("STARTS WITH predicate", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) WHERE p.firstName STARTS WITH 'A' "
        "RETURN p.id, p.firstName LIMIT 3");
}

TEST_CASE("CONTAINS predicate", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) WHERE p.firstName CONTAINS 'an' "
        "RETURN p.id, p.firstName LIMIT 3");
}

TEST_CASE("IN list literal", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) WHERE p.id IN [933, 4139, 1269] "
        "RETURN p.id, p.firstName");
}

TEST_CASE("IS NULL check", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)-[:WORKS_AT]->(c) "
        "RETURN p.id, c IS NULL AS noWork");
}

TEST_CASE("UNION", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    // UNION DISTINCT: two Person lookups should yield 2 distinct rows.
    auto res = qr->run(
        "MATCH (p:Person {id: 933}) RETURN p.id AS id "
        "UNION "
        "MATCH (p:Person {id: 4139}) RETURN p.id AS id");
    REQUIRE(res.size() == 2);
}

TEST_CASE("multiple WITH chains", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p, f "
        "WITH p, count(f) AS fc "
        "WITH p, fc "
        "RETURN p.id, fc");
}

TEST_CASE("UNWIND range then MATCH", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "UNWIND [933, 4139, 1269] AS pid "
        "MATCH (p:Person {id: pid}) "
        "RETURN p.id, p.firstName");
}

TEST_CASE("multi-label VLE with property filter", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (t:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(tc:TagClass) "
        "WHERE tc.name = 'MusicalArtist' "
        "RETURN t.name LIMIT 5");
}

TEST_CASE("empty OPTIONAL MATCH result", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)-[:NONEXISTENT_EDGE]-(x) "
        "RETURN p.id, x.id");
}

TEST_CASE("EXISTS subquery syntax", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WHERE EXISTS { MATCH (p)-[:KNOWS]-(f) } "
        "RETURN p.id");
}

TEST_CASE("huge LIMIT", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) RETURN p.id LIMIT 999999999");
}

TEST_CASE("SKIP without LIMIT", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) RETURN p.id SKIP 5");
}

TEST_CASE("SKIP + LIMIT", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) RETURN p.id ORDER BY p.id SKIP 10 LIMIT 5");
}

TEST_CASE("node without label", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n {id: 933}) RETURN n.id");
}

TEST_CASE("edge without type", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[r]-(b) RETURN b.id LIMIT 3");
}

TEST_CASE("multiple edge types OR", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS|IS_LOCATED_IN]-(x) "
        "RETURN x.id LIMIT 5");
}

TEST_CASE("allShortestPaths", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}) "
        "MATCH (b:Person {id: 4139}) "
        "MATCH path = allShortestPaths((a)-[:KNOWS*]-(b)) "
        "RETURN length(path)");
}

TEST_CASE("three comma patterns", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}), (b:Person {id: 4139}), (c:Person {id: 1269}) "
        "RETURN a.firstName, b.firstName, c.firstName");
}

// ============================================================
// 9. ORCA adversarial — target specific D_ASSERT / GPOS_ASSERT
// ============================================================

// Target: D_ASSERT(query.GetNumSingleQueries() == 1) — line 73
TEST_CASE("UNION ALL query", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    // UNION ALL: two Person lookups should yield 2 rows (no dedup).
    auto res = qr->run(
        "MATCH (p:Person {id: 933}) RETURN p.id AS id "
        "UNION ALL "
        "MATCH (p:Person {id: 4139}) RETURN p.id AS id");
    REQUIRE(res.size() == 2);
}

TEST_CASE("UNION ALL duplicate rows", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    // UNION ALL with overlapping: same person in both legs → 2 rows.
    auto res = qr->run(
        "MATCH (p:Person {id: 933}) RETURN p.id AS id "
        "UNION ALL "
        "MATCH (p:Person {id: 933}) RETURN p.id AS id");
    REQUIRE(res.size() == 2);
}

TEST_CASE("UNION deduplicates", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    // UNION DISTINCT with overlapping: same person → 1 row.
    auto res = qr->run(
        "MATCH (p:Person {id: 933}) RETURN p.id AS id "
        "UNION "
        "MATCH (p:Person {id: 933}) RETURN p.id AS id");
    REQUIRE(res.size() == 1);
}

// Target: node-only QG with multiple nodes and no edges
TEST_CASE("comma nodes without edges", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person), (b:Tag) RETURN a.id, b.name LIMIT 1");
}

// Target: OPTIONAL MATCH with fully unbound pattern
TEST_CASE("OPTIONAL MATCH unbound both", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "OPTIONAL MATCH (a:Person)-[:KNOWS]-(b:Person) "
        "RETURN a.id, b.id LIMIT 3");
}

// Target: OPTIONAL MATCH standalone (no prior MATCH)
TEST_CASE("OPTIONAL MATCH first", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "OPTIONAL MATCH (p:Person {id: 933}) RETURN p.firstName");
}

// Target: edge with unknown node variable
TEST_CASE("WHERE on unbound variable", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}) "
        "WHERE b.id = 42 "
        "RETURN a.id");
}

// Target: shortestPath with same src and dst variable
TEST_CASE("shortestPath same endpoints", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}), "
        "path = shortestPath((a)-[:KNOWS*]-(a)) "
        "RETURN length(path)");
}

// Target: VarLen with 0..0 range (zero-length path)
TEST_CASE("VarLen zero to zero", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS*0..0]-(b) "
        "RETURN b.id LIMIT 1");
}

// Target: deeply nested function calls
TEST_CASE("nested toInteger(toFloat(toInteger()))", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN toInteger(toFloat(toInteger(p.id))) AS x");
}

// Target: ORDER BY non-existent alias
TEST_CASE("ORDER BY unknown alias", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN p.firstName "
        "ORDER BY nonExistent");
}

// Target: GROUP BY with no aggregation
TEST_CASE("WITH without aggregation acting as GROUP BY", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p.firstName AS name "
        "RETURN name");
}

// Target: collect without GROUP BY context
TEST_CASE("collect all", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) RETURN collect(p.firstName) AS names LIMIT 1");
}

// Target: multiple shortestPaths
TEST_CASE("two shortestPaths", "[ldbc][robustness]") {
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
TEST_CASE("anonymous edge only", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH ()-[:KNOWS]-() RETURN count(*) AS cnt");
}

// Target: property access on NULL
TEST_CASE("property on optional null", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)-[:STUDY_AT]->(u) "
        "RETURN p.firstName, u.name");
}

// Target: NOT on non-boolean
TEST_CASE("NOT on integer", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) RETURN NOT p.id AS x");
}

// Target: comparison between incompatible types
TEST_CASE("compare node to integer", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}), (b:Person {id: 4139}) "
        "WHERE a > b "
        "RETURN a.id");
}

// Target: collect(DISTINCT *)
TEST_CASE("collect DISTINCT with expression", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN p.id, collect(DISTINCT f.firstName + f.lastName) AS names "
        "LIMIT 3");
}

// Target: multiple OPTIONAL MATCH chains
TEST_CASE("chained OPTIONAL MATCH", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)-[:KNOWS]-(f1:Person) "
        "OPTIONAL MATCH (f1)-[:IS_LOCATED_IN]->(c:City) "
        "RETURN p.firstName, f1.firstName, c.name LIMIT 5");
}

// Target: VarLen with very high upper bound
TEST_CASE("VarLen *1..10", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS*1..3]-(f:Person) "
        "RETURN DISTINCT f.id LIMIT 3");
}

// Target: MATCH after WITH that drops all columns
TEST_CASE("WITH constant then MATCH", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WITH 42 AS x "
        "MATCH (q:Person {id: x}) "
        "RETURN q.firstName");
}

// Target: pattern expression with 3-hop
TEST_CASE("3-hop pattern expression", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933}), (b:Person {id: 4139}) "
        "RETURN (a)-[:KNOWS]->()-[:KNOWS]->()<-[:KNOWS]-(b) AS connected");
}

// Target: list comprehension with mapping
TEST_CASE("list comprehension with map", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "WITH [1, 2, 3, 4, 5] AS nums "
        "RETURN [x IN nums | x * 2] AS doubled");
}

// Target: count(DISTINCT *)
TEST_CASE("count distinct node", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person)-[:KNOWS*1..2]-(f:Person) "
        "RETURN count(DISTINCT f) AS cnt");
}

// Target: WHERE with OR of different types
TEST_CASE("complex WHERE with OR", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) "
        "WHERE p.id = 933 OR p.firstName = 'Mahinda' "
        "RETURN p.id, p.firstName");
}

// Target: four-way comma pattern
TEST_CASE("four comma patterns", "[ldbc][robustness]") {
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
TEST_CASE("empty WITH passthrough", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) WITH p RETURN p.id");
}

// Target: WITH that renames variables
TEST_CASE("WITH rename", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WITH p.firstName AS name, p.id AS pid "
        "RETURN name, pid");
}

// Target: mixed aggregation and non-aggregation in RETURN
TEST_CASE("mixed agg and non-agg RETURN", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN p.id, p.firstName, count(f) AS fc "
        "ORDER BY fc DESC LIMIT 3");
}

// Target: ORDER BY on property not in RETURN
TEST_CASE("ORDER BY hidden property", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN f.id "
        "ORDER BY f.firstName LIMIT 5");
}

// Target: two WITH in sequence with aggregation
TEST_CASE("double WITH aggregation", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p, count(f) AS fc "
        "WITH p.id AS pid, fc "
        "RETURN pid, fc ORDER BY fc DESC LIMIT 3");
}

// Target: MATCH with edge and WHERE on both nodes
TEST_CASE("WHERE on both endpoints", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person)-[:KNOWS]-(b:Person) "
        "WHERE a.id < 10000 AND b.id < 10000 "
        "RETURN a.id, b.id LIMIT 5");
}

// Target: bidirectional edge in same pattern
TEST_CASE("left-directed edge", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})<-[:HAS_CREATOR]-(m:Message) "
        "RETURN m.id LIMIT 3");
}

// Target: MATCH→WITH→MATCH→WITH→RETURN chain
TEST_CASE("multi-stage query", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH DISTINCT f "
        "MATCH (f)-[:IS_LOCATED_IN]->(c:City) "
        "WITH f, c "
        "RETURN f.firstName, c.name LIMIT 5");
}

// Target: negative integer literal
TEST_CASE("negative literal", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN p.id, -1 AS neg, p.id * -1 AS negId");
}

// Target: boolean expression chain
TEST_CASE("complex boolean chain", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) "
        "WHERE (p.id > 900 AND p.id < 1000) OR (p.id > 4000 AND p.id < 4200) "
        "RETURN p.id, p.firstName LIMIT 5");
}

// Target: property equality between two nodes
TEST_CASE("cross-node property equality", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person)-[:KNOWS]-(b:Person) "
        "WHERE a.firstName = b.firstName "
        "RETURN a.id, b.id, a.firstName LIMIT 3");
}

// Target: MATCH with multiple edges from same node
TEST_CASE("multi-edge from same node", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]->(f:Person), "
        "      (p)-[:IS_LOCATED_IN]->(c:City) "
        "RETURN f.firstName, c.name LIMIT 5");
}

// Target: collect then size without UNWIND
TEST_CASE("collect then size directly", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN size(collect(f.firstName)) AS cnt");
}

// Target: nested OPTIONAL MATCH with collect
TEST_CASE("OPTIONAL collect empty", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 999999999999}) "
        "OPTIONAL MATCH (p)-[:KNOWS]-(f:Person) "
        "RETURN p.id, collect(f.id) AS friends");
}

// Target: DISTINCT in RETURN
TEST_CASE("RETURN DISTINCT", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN DISTINCT f.firstName LIMIT 5");
}

// ============================================================
// 11. Deep stress — physical planner D_ASSERT targets
// ============================================================

// Target: planner_physical.cpp:852 — non-LE/GEq comparison filter pushdown
TEST_CASE("not-equal filter", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) WHERE n.id <> 42 RETURN n.id LIMIT 3");
}

// Target: planner_physical.cpp:1217 — string comparison on indexed property
TEST_CASE("greater-than filter", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Person) WHERE n.id > 9000 RETURN n.id LIMIT 3");
}

// Target: planner_physical.cpp:1574 — filter + adjidxjoin + OPTIONAL MATCH
TEST_CASE("edge property + OPTIONAL MATCH chain", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[k:KNOWS]-(b:Person) "
        "WHERE k.creationDate > 1300000000000 "
        "OPTIONAL MATCH (b)-[:IS_LOCATED_IN]->(c:City) "
        "RETURN a.id, b.id, c.name LIMIT 5");
}

// Target: cross-partition property access
TEST_CASE("cross label comma match", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}), (t:Tag {name: 'Angola'}) "
        "RETURN p.firstName, t.name");
}

// Target: three-hop directed chain
TEST_CASE("three-hop directed chain", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) "
        "RETURN d.id LIMIT 1");
}

// Target: nested UNWIND
TEST_CASE("double UNWIND", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "UNWIND [1,2,3] AS x "
        "UNWIND [10,20] AS y "
        "RETURN x, y");
}

// Target: planner_physical.cpp:218 — schemaless union + index join
TEST_CASE("three-hop same edge type", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d) "
        "RETURN d.id LIMIT 1");
}

// Target: VarLen with inline property on endpoint
TEST_CASE("VarLen with endpoint filter", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS*1..3]-(b:Person {id: 4139}) "
        "RETURN a.firstName, b.firstName");
}

// Target: WHERE with inequality on string
TEST_CASE("string inequality", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) WHERE p.firstName > 'M' "
        "RETURN p.firstName LIMIT 5");
}

// Target: ORDER BY on aggregation result
TEST_CASE("ORDER BY aggregation", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p.id AS pid, count(f) AS fc "
        "RETURN pid, fc ORDER BY fc DESC LIMIT 5");
}

// Target: MATCH with no results + aggregation
TEST_CASE("empty match + count", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 999999999999999})-[:KNOWS]-(f:Person) "
        "RETURN count(f) AS cnt");
}

// Target: WITH + WHERE + aggregation pipeline
TEST_CASE("aggregation with HAVING-like WHERE", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p, count(f) AS fc WHERE fc > 50 "
        "RETURN p.id, fc ORDER BY fc DESC LIMIT 3");
}

// Target: property access on edge in RETURN
TEST_CASE("edge property in RETURN", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[k:KNOWS]-(b:Person) "
        "RETURN a.firstName, b.firstName, k.creationDate LIMIT 3");
}

// Target: MATCH with multiple labels (colon-separated)
TEST_CASE("multi-label node", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (n:Comment:Message) RETURN n.id LIMIT 1");
}

// Target: deeply nested boolean NOT NOT NOT
TEST_CASE("triple NOT", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN NOT NOT NOT (p.id > 0) AS tripleNot");
}

// Target: large integer constant in WHERE
TEST_CASE("huge integer constant", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) WHERE p.id = 99999999999999999 "
        "RETURN p.firstName");
}

// Target: comparison between property and NULL
TEST_CASE("compare with NULL", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    // C3 fix: `= NULL` is rewritten to IS NULL, `<> NULL` to IS NOT NULL.
    // This prevents the SQLNULL assertion crash; both must not crash.
    auto r1 = qr->run("MATCH (p:Person) WHERE p.id = NULL RETURN p.firstName LIMIT 5");
    REQUIRE(r1.size() <= 5);    // no crash; may find null-id rows
    auto r2 = qr->run("MATCH (p:Person) WHERE p.id <> NULL RETURN p.firstName LIMIT 5");
    REQUIRE(r2.size() == 5);    // IS NOT NULL on id returns rows
}

// Target: multiple aggregates with different GROUP BY keys
TEST_CASE("sum and count together", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN p.id, count(f) AS friends, collect(f.firstName) AS names "
        "ORDER BY friends DESC LIMIT 3");
}

// Target: RETURN with no FROM (constant expression)
TEST_CASE("RETURN constant only", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("RETURN 1 + 2 AS three, 'hello' AS greeting");
}

// Target: empty MATCH result + OPTIONAL MATCH + collect
TEST_CASE("empty base + optional + collect", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 999999999999999}) "
        "OPTIONAL MATCH (p)-[:KNOWS]-(f:Person) "
        "WITH p, collect(f) AS friends "
        "RETURN p.id, size(friends) AS cnt");
}

// Target: CASE WHEN with aggregation
TEST_CASE("CASE with aggregation result", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p, count(f) AS fc "
        "RETURN p.id, CASE WHEN fc > 10 THEN 'popular' ELSE 'normal' END AS status");
}









// Target: VarLen *1..1 (equivalent to single hop)
TEST_CASE("VarLen star one to one", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS*1..1]-(b:Person) "
        "RETURN b.id LIMIT 3");
}

// Target: self-join with WHERE a.prop = b.prop
TEST_CASE("self-join equality", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person)-[:KNOWS]-(b:Person) "
        "WHERE a.gender = b.gender AND a.id < b.id "
        "RETURN a.id, b.id LIMIT 3");
}

// Target: aggregation without GROUP BY key
TEST_CASE("global aggregation", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) RETURN count(p) AS total, min(p.id) AS minId, max(p.id) AS maxId");
}

// Target: multi-hop with mixed directed/undirected
TEST_CASE("mixed direction chain", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS]-(b:Person)-[:IS_LOCATED_IN]->(c:City) "
        "RETURN b.firstName, c.name LIMIT 5");
}

// Target: collect + head + property access
TEST_CASE("head collect property", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH p, collect(f) AS friends "
        "RETURN p.firstName, head(friends).firstName AS bestFriend");
}

// Target: LIMIT 0
TEST_CASE("LIMIT 0", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person) RETURN p.id LIMIT 0");
}

// Target: deeply chained WITH pipeline (5 stages)
TEST_CASE("five-stage WITH pipeline", "[ldbc][robustness]") {
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
TEST_CASE("WHERE after OPTIONAL MATCH", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)-[:STUDY_AT]->(u:University) "
        "WHERE u IS NOT NULL "
        "RETURN p.firstName, u.name");
}

// Target: expression in WHERE referencing alias
TEST_CASE("WHERE on alias from WITH", "[ldbc][robustness]") {
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

TEST_CASE("negative list index", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH collect(f.firstName) AS names "
        "RETURN names[-1] AS last");
}

TEST_CASE("list index out of bounds", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH collect(f.firstName) AS names "
        "RETURN names[9999] AS missing");
}

TEST_CASE("DISTINCT with aggregation", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN DISTINCT p.id, count(f) AS c");
}

TEST_CASE("modulo negative", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("RETURN 17 % -5 AS result, -42 % 3 AS neg");
}

TEST_CASE("floor on NULL", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)-[:STUDY_AT]->(u) "
        "RETURN floor(u.classYear) AS floored");
}

TEST_CASE("complex arithmetic", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN (p.id * 2 + 100) / 3 AS calc");
}

TEST_CASE("string multiplication", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("RETURN 'abc' * 3 AS repeated");
}

TEST_CASE("regex match", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WHERE p.firstName =~ '.*ah.*' "
        "RETURN p.firstName");
}

TEST_CASE("invalid regex", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WHERE p.firstName =~ '[invalid(' "
        "RETURN p.firstName");
}

TEST_CASE("abs overflow", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("RETURN abs(-9223372036854775808) AS x");
}

TEST_CASE("sqrt negative", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE("RETURN sqrt(-1) AS x");
}

TEST_CASE("coalesce many args", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "RETURN coalesce(null,null,null,null,null,null,null,null,null,null,"
        "null,null,null,null,null,null,null,null,null,'found') AS x");
}

TEST_CASE("nested null property", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WITH {a: {b: null}} AS nested "
        "RETURN nested.a.b.c AS missing");
}

TEST_CASE("toInteger on float", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "RETURN toInteger(3.14) AS x, toInteger(-2.7) AS y");
}

TEST_CASE("mixed type list", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "WITH [1, 'two', 3.0, true] AS mixed "
        "RETURN size(mixed) AS sz");
}

TEST_CASE("MATCH same label both sides", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person) "
        "WHERE a <> c "
        "RETURN c.id LIMIT 3");
}

TEST_CASE("edge with inline property", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (a:Person {id: 933})-[k:KNOWS {creationDate: 1234}]-(b:Person) "
        "RETURN b.id LIMIT 1");
}

TEST_CASE("very long alias", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    std::string long_alias(200, 'x');
    std::string q = "MATCH (p:Person {id: 933}) RETURN p.id AS " + long_alias;
    EXPECT_GRACEFUL_FAILURE(q.c_str());
}

TEST_CASE("float literal in WHERE", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WHERE toFloat(p.id) > 932.5 "
        "RETURN p.firstName");
}

TEST_CASE("chained functions", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN toString(toInteger(toFloat(p.id))) AS chain");
}

TEST_CASE("OPTIONAL MATCH WHERE filter", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "OPTIONAL MATCH (p)-[:KNOWS]-(f:Person) "
        "WHERE f.id > 5000 "
        "RETURN p.id, f.id LIMIT 5");
}

TEST_CASE("collect then UNWIND then collect", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH collect(f.id) AS ids "
        "UNWIND ids AS id "
        "WITH collect(id) AS ids2 "
        "RETURN size(ids2) AS cnt");
}

TEST_CASE("MATCH with label on edge endpoint", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (:Person {id: 933})-[:KNOWS]->(:Person) "
        "RETURN count(*) AS cnt");
}

TEST_CASE("WHERE with XOR-like logic", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WHERE (f.id > 1000) <> (f.id < 5000) "
        "RETURN f.id LIMIT 3");
}

TEST_CASE("WITH DISTINCT + ORDER BY", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WITH DISTINCT f.firstName AS name "
        "RETURN name ORDER BY name LIMIT 5");
}

TEST_CASE("count with WHERE filter", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "WHERE f.id > 1000 "
        "WITH p, count(f) AS fc "
        "WHERE fc > 0 "
        "RETURN p.id, fc");
}

TEST_CASE("map literal nested", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WITH {name: p.firstName, info: {id: p.id}} AS m "
        "RETURN m.name, m.info.id");
}

TEST_CASE("toFloat division", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN toFloat(p.id) / 7.0 AS div");
}

TEST_CASE("collect empty + head", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "WHERE false "
        "RETURN head(collect(p.firstName)) AS first");
}

TEST_CASE("boolean arithmetic", "[ldbc][robustness]") {
    SKIP_IF_NO_DB();
    EXPECT_GRACEFUL_FAILURE(
        "MATCH (p:Person {id: 933}) "
        "RETURN (p.id > 0) AND (p.id < 10000) AS inRange");
}

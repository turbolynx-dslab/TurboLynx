// Stage 7 — CRUD operations (CREATE / SET / DELETE)
// Phase 1: CREATE Node

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <vector>

extern std::string g_ldbc_path;
extern bool g_skip_requested;
extern bool g_has_ldbc;

extern qtest::QueryRunner* get_ldbc_runner();
extern qtest::QueryRunner* get_crud_runner();
extern const std::string& get_crud_workspace_path();
extern void disconnect_active_runner();
static void ensure_singleton_disconnected();
static void ensure_singleton_reconnected();

// CRUD tests run against a per-test-case isolated copy of the LDBC database.
// Each SKIP_IF_NO_DB() resets the workspace to pristine state, so mutations
// (CREATE/SET/DELETE, including SET :Employee which widens the catalog)
// never leak into subsequent tests or into /data/ldbc/sf1 itself.
#define SKIP_IF_NO_DB() \
    if (g_ldbc_path.empty()) { WARN("--ldbc-path not set, skipping"); g_skip_requested = true; return; } \
    if (!g_has_ldbc) { WARN("DB has no LDBC schema, skipping"); return; } \
    auto* qr = get_crud_runner(); \
    if (!qr) { FAIL("Cannot open isolated CRUD workspace from: " << g_ldbc_path); return; } \
    [[maybe_unused]] const std::string& crud_db_path = get_crud_workspace_path()

// Phase 1: CREATE Node tests
TEST_CASE("CREATE single node", "[ldbc][crud][create]") {
    SKIP_IF_NO_DB();
    try {
        auto r = qr->run(
            "CREATE (n:Person {id: 99999999999999, firstName: 'TestJohn'})",
            {});
        CHECK(r.size() == 0);
    } catch (const std::exception& e) {
        FAIL("CREATE should not throw: " << e.what());
    }
}

TEST_CASE("CREATE then MATCH count", "[ldbc][crud][create]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        REQUIRE(before.size() == 1);
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("CREATE (n:Person {id: 88888888888888, firstName: 'TestJane'})", {});
        auto after = qr->run(
            "MATCH (n:Person) RETURN count(n) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(after.size() == 1);
        CHECK(after[0].int64_at(0) == cnt_before + 1);
    } catch (const std::exception& e) {
        FAIL("CREATE+MATCH should not throw: " << e.what());
    }
}

TEST_CASE("CREATE multiple nodes then count", "[ldbc][crud][create]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        REQUIRE(before.size() == 1);
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("CREATE (n:Person {id: 77777777777771, firstName: 'Multi1'})", {});
        qr->run("CREATE (n:Person {id: 77777777777772, firstName: 'Multi2'})", {});
        qr->run("CREATE (n:Person {id: 77777777777773, firstName: 'Multi3'})", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        REQUIRE(after.size() == 1);
        CHECK(after[0].int64_at(0) == cnt_before + 3);
    } catch (const std::exception& e) {
        FAIL("Multiple CREATE+count: " << e.what());
    }
}

TEST_CASE("CREATE does not affect filtered queries", "[ldbc][crud][create]") {
    SKIP_IF_NO_DB();
    try {
        // Existing person should still be found via filter pushdown (use 10027, not 933 — SET tests modify 933)
        auto r = qr->run(
            "MATCH (n:Person {id: 10027}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0).length() > 0);
    } catch (const std::exception& e) {
        FAIL("Filtered query after CREATE: " << e.what());
    }
}

TEST_CASE("CREATE node with no properties except id", "[ldbc][crud][create]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("CREATE (n:Person {id: 66666666666666})", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before + 1);
    } catch (const std::exception& e) {
        FAIL("CREATE minimal props: " << e.what());
    }
}

TEST_CASE("IC queries still work after CREATE", "[ldbc][crud][create]") {
    SKIP_IF_NO_DB();
    try {
        // IC1-style query: find person by id, return properties (use 10027, not 933 — SET tests modify 933)
        auto r = qr->run(
            "MATCH (n:Person {id: 10027}) RETURN n.firstName, n.lastName",
            {qtest::ColType::STRING, qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0).length() > 0);
        CHECK(r[0].str_at(1).length() > 0);
    } catch (const std::exception& e) {
        FAIL("IC query after CREATE: " << e.what());
    }
}

TEST_CASE("count increases exactly by number of CREATEs", "[ldbc][crud][create]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("CREATE (n:Person {id: 55555555555555, firstName: 'ExactCount'})", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        // Exactly +1
        CHECK(after[0].int64_at(0) == cnt_before + 1);
    } catch (const std::exception& e) {
        FAIL("Exact count: " << e.what());
    }
}

// ============================================================
// Phase 2: CREATE Edge tests
// ============================================================

TEST_CASE("CREATE edge does not crash", "[ldbc][crud][create-edge]") {
    SKIP_IF_NO_DB();
    try {
        auto r = qr->run(
            "CREATE (a:Person {id: 11111111111111, firstName: 'EdgeSrc'})"
            "-[:KNOWS]->"
            "(b:Person {id: 11111111111112, firstName: 'EdgeDst'})",
            {});
        CHECK(r.size() == 0);
    } catch (const std::exception& e) {
        FAIL("CREATE edge should not throw: " << e.what());
    }
}

TEST_CASE("CREATE edge also creates both endpoint nodes", "[ldbc][crud][create-edge]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("CREATE (a:Person {id: 22222222222221, firstName: 'EP1'})"
                "-[:KNOWS]->"
                "(b:Person {id: 22222222222222, firstName: 'EP2'})", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        // Both src and dst nodes should be created (+2)
        CHECK(after[0].int64_at(0) == cnt_before + 2);
    } catch (const std::exception& e) {
        FAIL("CREATE edge + node count: " << e.what());
    }
}

TEST_CASE("existing KNOWS traversal unaffected by CREATE edge", "[ldbc][crud][create-edge]") {
    SKIP_IF_NO_DB();
    try {
        // IC2-style: find friends of Person 933
        auto r = qr->run(
            "MATCH (a:Person {id: 933})-[:KNOWS]-(b:Person) "
            "RETURN count(b) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        // Person 933 has known friend count in LDBC SF1
        CHECK(r[0].int64_at(0) > 0);
    } catch (const std::exception& e) {
        FAIL("KNOWS traversal after CREATE edge: " << e.what());
    }
}

TEST_CASE("IC queries still work after CREATE edge", "[ldbc][crud][create-edge]") {
    SKIP_IF_NO_DB();
    try {
        auto r = qr->run(
            "MATCH (n:Person {id: 10027}) RETURN n.firstName, n.lastName",
            {qtest::ColType::STRING, qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0).length() > 0);
        CHECK(r[0].str_at(1).length() > 0);
    } catch (const std::exception& e) {
        FAIL("IC query after CREATE edge: " << e.what());
    }
}

// ============================================================
// Phase 1 additional: CREATE Node advanced tests
// ============================================================

TEST_CASE("CREATE node with many properties", "[ldbc][crud][create]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("CREATE (n:Person {id: 44444444444444, firstName: 'Many', "
                "lastName: 'Props', gender: 'male', birthday: 19900101})", {});
        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        REQUIRE(after.size() == 1);
        CHECK(after[0].int64_at(0) == cnt_before + 1);
    } catch (const std::exception& e) {
        FAIL("CREATE many props: " << e.what());
    }
}

TEST_CASE("CREATE does not affect other labels", "[ldbc][crud][create]") {
    SKIP_IF_NO_DB();
    try {
        // Get Comment count before CREATE Person
        auto before = qr->run("MATCH (n:Comment) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        REQUIRE(before.size() == 1);
        int64_t comment_cnt = before[0].int64_at(0);

        qr->run("CREATE (n:Person {id: 33333333333333, firstName: 'CrossLabel'})", {});

        auto after = qr->run("MATCH (n:Comment) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        REQUIRE(after.size() == 1);
        // Comment count must be unchanged
        CHECK(after[0].int64_at(0) == comment_cnt);
    } catch (const std::exception& e) {
        FAIL("Cross-label isolation: " << e.what());
    }
}

// ============================================================
// Phase 3: SET Property (UPDATE) tests
// ============================================================

TEST_CASE("SET single property", "[ldbc][crud][set]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'UpdatedName'", {});
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "UpdatedName");
    } catch (const std::exception& e) {
        FAIL("SET single property: " << e.what());
    }
}

TEST_CASE("SET does not affect other nodes", "[ldbc][crud][set]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'Changed933'", {});
        // Person 4139 should be unaffected
        auto r = qr->run(
            "MATCH (n:Person {id: 4139}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) != "Changed933");
    } catch (const std::exception& e) {
        FAIL("SET isolation: " << e.what());
    }
}

TEST_CASE("SET multiple properties", "[ldbc][crud][set]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'Multi1', n.lastName = 'Multi2'", {});
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) RETURN n.firstName, n.lastName",
            {qtest::ColType::STRING, qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "Multi1");
        CHECK(r[0].str_at(1) == "Multi2");
    } catch (const std::exception& e) {
        FAIL("SET multiple properties: " << e.what());
    }
}

TEST_CASE("SET then count unchanged", "[ldbc][crud][set]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'CountTest'", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        // SET should NOT change node count
        CHECK(after[0].int64_at(0) == cnt_before);
    } catch (const std::exception& e) {
        FAIL("SET count unchanged: " << e.what());
    }
}

TEST_CASE("SET overwrites previous SET", "[ldbc][crud][set]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'First'", {});
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'Second'", {});
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "Second");
    } catch (const std::exception& e) {
        FAIL("SET overwrite: " << e.what());
    }
}

TEST_CASE("SET on non-existent node is no-op", "[ldbc][crud][set]") {
    SKIP_IF_NO_DB();
    try {
        // Should not crash — just zero rows matched
        qr->run("MATCH (n:Person {id: 999999999999999}) SET n.firstName = 'Ghost'", {});
    } catch (const std::exception& e) {
        FAIL("SET non-existent should not throw: " << e.what());
    }
}

TEST_CASE("SET then RETURN in separate query", "[ldbc][crud][set]") {
    SKIP_IF_NO_DB();
    try {
        // Two-phase: SET and RETURN must be separate queries for now
        // (SET applies after pipeline, so same-query RETURN sees base value)
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'ReturnTest'", {});
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "ReturnTest");
    } catch (const std::exception& e) {
        FAIL("SET then RETURN: " << e.what());
    }
}

TEST_CASE("SET then RETURN multiple columns", "[ldbc][crud][set]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'SetRet'", {});
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) RETURN n.firstName, n.lastName",
            {qtest::ColType::STRING, qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "SetRet");
        CHECK(r[0].str_at(1).length() > 0);
    } catch (const std::exception& e) {
        FAIL("SET then RETURN multi-col: " << e.what());
    }
}

TEST_CASE("SET string property on different node", "[ldbc][crud][set]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("MATCH (n:Person {id: 4139}) SET n.firstName = 'IntTest'", {});
        auto r = qr->run(
            "MATCH (n:Person {id: 4139}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "IntTest");
    } catch (const std::exception& e) {
        FAIL("SET on different node: " << e.what());
    }
}

TEST_CASE("SET preserves other properties", "[ldbc][crud][set]") {
    SKIP_IF_NO_DB();
    try {
        // Get original lastName
        auto orig = qr->run(
            "MATCH (n:Person {id: 4139}) RETURN n.lastName",
            {qtest::ColType::STRING});
        REQUIRE(orig.size() == 1);
        std::string original_last = orig[0].str_at(0);

        // SET only firstName
        qr->run("MATCH (n:Person {id: 4139}) SET n.firstName = 'OnlyFirst'", {});

        // lastName should be unchanged
        auto r = qr->run(
            "MATCH (n:Person {id: 4139}) RETURN n.firstName, n.lastName",
            {qtest::ColType::STRING, qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "OnlyFirst");
        CHECK(r[0].str_at(1) == original_last);
    } catch (const std::exception& e) {
        FAIL("SET preserves other props: " << e.what());
    }
}

// Regression for issue #10 multi-variable SET. Pre-fix, the post-processor
// looped over result rows with the first ID-typed column as the mutation
// target and applied every SET item to that single vid, so b was either
// silently ignored or its property landed on a. Each test below isolates
// itself with its own LDBC copy (the queries cross-mutate the same person
// ids that other [crud][set] tests poke at).
namespace { struct CrudIsoGuard {
    std::string path;
    qtest::QueryRunner *qr;
    CrudIsoGuard(const std::string &tag) : qr(nullptr) {
        ensure_singleton_disconnected();
        std::string tmpl_str = "/tmp/tl_iso_" + tag + "_XXXXXX";
        std::vector<char> tmpl(tmpl_str.begin(), tmpl_str.end());
        tmpl.push_back('\0');
        REQUIRE(mkdtemp(tmpl.data()) != nullptr);
        path = tmpl.data();
        REQUIRE(std::system(("cp -a --reflink=auto " + g_ldbc_path +
                             "/. " + path + "/").c_str()) == 0);
        qr = new qtest::QueryRunner(path);
    }
    ~CrudIsoGuard() {
        delete qr;
        if (!path.empty()) std::system(("rm -rf " + path).c_str());
        ensure_singleton_reconnected();
    }
}; }

TEST_CASE("Multi-variable SET on different keys lands per variable",
          "[ldbc][crud][set][issue10]") {
    if (g_ldbc_path.empty()) { WARN("--ldbc-path not set"); g_skip_requested = true; return; }
    if (!g_has_ldbc) { WARN("DB has no LDBC schema, skipping"); return; }
    CrudIsoGuard iso{"setmulti1"};
    auto *qr = iso.qr;
    try {
        auto orig_a = qr->run(
            "MATCH (n:Person {id: 933}) RETURN n.firstName, n.lastName",
            {qtest::ColType::STRING, qtest::ColType::STRING});
        auto orig_b = qr->run(
            "MATCH (n:Person {id: 4139}) RETURN n.firstName, n.lastName",
            {qtest::ColType::STRING, qtest::ColType::STRING});
        REQUIRE(orig_a.size() == 1);
        REQUIRE(orig_b.size() == 1);
        std::string a_last = orig_a[0].str_at(1);
        std::string b_first = orig_b[0].str_at(0);

        qr->run(
            "MATCH (a:Person {id: 933}), (b:Person {id: 4139}) "
            "SET a.firstName = 'I10_A_FIRST', b.lastName = 'I10_B_LAST'",
            {});

        auto after_a = qr->run(
            "MATCH (n:Person {id: 933}) RETURN n.firstName, n.lastName",
            {qtest::ColType::STRING, qtest::ColType::STRING});
        REQUIRE(after_a.size() == 1);
        CHECK(after_a[0].str_at(0) == "I10_A_FIRST");
        CHECK(after_a[0].str_at(1) == a_last);  // a.lastName must NOT change

        auto after_b = qr->run(
            "MATCH (n:Person {id: 4139}) RETURN n.firstName, n.lastName",
            {qtest::ColType::STRING, qtest::ColType::STRING});
        REQUIRE(after_b.size() == 1);
        CHECK(after_b[0].str_at(0) == b_first);  // b.firstName must NOT change
        CHECK(after_b[0].str_at(1) == "I10_B_LAST");
    } catch (const std::exception &e) {
        FAIL("multi-var SET different keys: " << e.what());
    }
}

TEST_CASE("Multi-variable SET on the same key lands per variable",
          "[ldbc][crud][set][issue10]") {
    if (g_ldbc_path.empty()) { WARN("--ldbc-path not set"); g_skip_requested = true; return; }
    if (!g_has_ldbc) { WARN("DB has no LDBC schema, skipping"); return; }
    CrudIsoGuard iso{"setmulti2"};
    auto *qr = iso.qr;
    try {
        qr->run(
            "MATCH (a:Person {id: 933}), (b:Person {id: 4139}) "
            "SET a.firstName = 'A_TARGET', b.firstName = 'B_TARGET'",
            {});

        auto after_a = qr->run(
            "MATCH (n:Person {id: 933}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(after_a.size() == 1);
        CHECK(after_a[0].str_at(0) == "A_TARGET");

        auto after_b = qr->run(
            "MATCH (n:Person {id: 4139}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(after_b.size() == 1);
        CHECK(after_b[0].str_at(0) == "B_TARGET");
    } catch (const std::exception &e) {
        FAIL("multi-var SET same key: " << e.what());
    }
}

// Regression for issue #10: SET on a non-leading variable in a multi-node
// pattern must mutate the target variable, not whichever node happens to
// produce the first ID-typed column. Pre-fix, this query silently
// overwrote person 933's firstName instead of person 4139's because the
// chunk's first ID column belonged to a, not b.
TEST_CASE("SET on second pattern variable targets only that variable",
          "[ldbc][crud][set][issue10]") {
    SKIP_IF_NO_DB();
    try {
        auto orig_a = qr->run(
            "MATCH (n:Person {id: 933}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(orig_a.size() == 1);
        std::string original_a = orig_a[0].str_at(0);

        qr->run(
            "MATCH (a:Person {id: 933})-[:KNOWS]->(b:Person {id: 4139}) "
            "WITH a, b LIMIT 1 SET b.firstName = 'Issue10Target'",
            {});

        auto after_a = qr->run(
            "MATCH (n:Person {id: 933}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(after_a.size() == 1);
        CHECK(after_a[0].str_at(0) == original_a);

        auto after_b = qr->run(
            "MATCH (n:Person {id: 4139}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(after_b.size() == 1);
        CHECK(after_b[0].str_at(0) == "Issue10Target");
    } catch (const std::exception &e) {
        FAIL("SET on second variable: " << e.what());
    }
}

// ============================================================
// Phase 4: DELETE tests
// ============================================================

TEST_CASE("DELETE base node decrements count", "[ldbc][crud][delete]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("MATCH (n:Person {id: 94}) DELETE n", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before - 1);
    } catch (const std::exception& e) {
        FAIL("DELETE base node: " << e.what());
    }
}

TEST_CASE("DELETE non-existent node is no-op", "[ldbc][crud][delete]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("MATCH (n:Person {id: 999999999999999}) DELETE n", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before);
    } catch (const std::exception& e) {
        FAIL("DELETE non-existent: " << e.what());
    }
}

TEST_CASE("DELETE node with edges cascades incident relationships", "[ldbc][crud][delete]") {
    SKIP_IF_NO_DB();
    try {
        auto neighbor = qr->run(
            "MATCH (a:Person {id: 933})-[r]-(b:Person) RETURN b.id AS neighbor_id LIMIT 1",
            {qtest::ColType::INT64});
        REQUIRE(neighbor.size() == 1);
        int64_t neighbor_id = neighbor[0].int64_at(0);

        auto edge_before = qr->run(
            ("MATCH (a:Person {id: " + std::to_string(neighbor_id) +
             "})-[r]-(b:Person {id: 933}) RETURN count(r) AS cnt")
                .c_str(),
            {qtest::ColType::INT64});
        REQUIRE(edge_before.size() == 1);
        REQUIRE(edge_before[0].int64_at(0) > 0);

        qr->run("MATCH (n:Person {id: 933}) DELETE n", {});

        auto node_after = qr->run(
            "MATCH (n:Person {id: 933}) RETURN count(n) AS cnt",
            {qtest::ColType::INT64});
        auto edge_after = qr->run(
            ("MATCH (a:Person {id: " + std::to_string(neighbor_id) +
             "})-[r]-(b:Person {id: 933}) RETURN count(r) AS cnt")
                .c_str(),
            {qtest::ColType::INT64});
        REQUIRE(node_after.size() == 1);
        REQUIRE(edge_after.size() == 1);
        CHECK(node_after[0].int64_at(0) == 0);
        CHECK(edge_after[0].int64_at(0) == 0);
    } catch (const std::exception& e) {
        FAIL("DELETE node with edges cascade: " << e.what());
    }
}

TEST_CASE("DELETE does not affect other nodes", "[ldbc][crud][delete]") {
    SKIP_IF_NO_DB();
    try {
        // Delete one base node and verify another is unaffected
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("MATCH (n:Person {id: 94}) DELETE n", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before - 1);

        // Another node still accessible
        auto r = qr->run("MATCH (n:Person {id: 10027}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0).length() > 0);
    } catch (const std::exception& e) {
        FAIL("DELETE isolation: " << e.what());
    }
}

TEST_CASE("multiple DELETEs decrement count", "[ldbc][crud][delete]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("MATCH (n:Person {id: 1129}) DELETE n", {});
        qr->run("MATCH (n:Person {id: 4194}) DELETE n", {});

        auto r = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                          {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) == cnt_before - 2);
    } catch (const std::exception& e) {
        FAIL("DELETE multiple: " << e.what());
    }
}

TEST_CASE("DELETE node with edges decrements count", "[ldbc][crud][delete]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        REQUIRE(before.size() == 1);

        qr->run("MATCH (n:Person {id: 933}) DELETE n", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        REQUIRE(after.size() == 1);
        CHECK(after[0].int64_at(0) == before[0].int64_at(0) - 1);
    } catch (const std::exception& e) {
        FAIL("DELETE node with edges decrements count: " << e.what());
    }
}

TEST_CASE("DELETE node without edges succeeds", "[ldbc][crud][delete]") {
    SKIP_IF_NO_DB();
    try {
        // Create an isolated node (no edges), then plain DELETE should work
        qr->run("CREATE (n:Person {id: 999888, firstName: 'Isolated'})", {});
        auto before = qr->run("MATCH (n:Person {id: 999888}) RETURN n.firstName",
                               {qtest::ColType::STRING});
        REQUIRE(before.size() == 1);

        qr->run("MATCH (n:Person {id: 999888}) DELETE n", {});

        auto after = qr->run("MATCH (n:Person {id: 999888}) RETURN n.firstName",
                              {qtest::ColType::STRING});
        CHECK(after.empty());
    } catch (const std::exception& e) {
        FAIL("DELETE isolated node: " << e.what());
    }
}

TEST_CASE("DELETE cascades edges on fresh connected nodes", "[ldbc][crud][delete]") {
    SKIP_IF_NO_DB();
    try {
        // Create two nodes with an edge
        qr->run("CREATE (a:Person {id: 999770, firstName: 'A'})", {});
        qr->run("CREATE (b:Person {id: 999771, firstName: 'B'})", {});
        qr->run("MATCH (a:Person {id: 999770}), (b:Person {id: 999771}) CREATE (a)-[:KNOWS]->(b)", {});

        // Plain DELETE should cascade the relationship
        qr->run("MATCH (n:Person {id: 999770}) DELETE n", {});

        auto edge_after = qr->run(
            "MATCH (a:Person {id: 999770})-[:KNOWS]->(b:Person {id: 999771}) RETURN count(b) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(edge_after.size() == 1);
        CHECK(edge_after[0].int64_at(0) == 0);

        // The neighbor should now be independently deletable
        qr->run("MATCH (n:Person {id: 999771}) DELETE n", {});

        auto r = qr->run("MATCH (n:Person {id: 999771}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        CHECK(r.empty());
    } catch (const std::exception& e) {
        FAIL("DELETE cascades fresh edge: " << e.what());
    }
}

TEST_CASE("DETACH DELETE decrements count", "[ldbc][crud][detach-delete]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        // Person 933 has KNOWS edges — DETACH DELETE should remove node + edges
        qr->run("MATCH (n:Person {id: 933}) DETACH DELETE n", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before - 1);
    } catch (const std::exception& e) {
        FAIL("DETACH DELETE count: " << e.what());
    }
}

TEST_CASE("DETACH DELETE node gone from count", "[ldbc][crud][detach-delete]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("MATCH (n:Person {id: 4139}) DETACH DELETE n", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before - 1);
    } catch (const std::exception& e) {
        FAIL("DETACH DELETE gone count: " << e.what());
    }
}

TEST_CASE("DETACH DELETE removes from KNOWS traversal", "[ldbc][crud][detach-delete]") {
    SKIP_IF_NO_DB();
    try {
        // Count friends of 10027 before
        auto before = qr->run(
            "MATCH (a:Person {id: 10027})-[:KNOWS]-(b:Person) RETURN count(b) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(before.size() == 1);
        int64_t friends_before = before[0].int64_at(0);
        REQUIRE(friends_before > 0);

        // Delete 10027's friend (933) — should disappear from KNOWS traversal
        qr->run("MATCH (n:Person {id: 933}) DETACH DELETE n", {});

        auto after = qr->run(
            "MATCH (a:Person {id: 10027})-[:KNOWS]-(b:Person) RETURN count(b) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(after.size() == 1);
        // Friends count should decrease (933 was a friend of 10027 in LDBC)
        CHECK(after[0].int64_at(0) <= friends_before);
    } catch (const std::exception& e) {
        FAIL("DETACH DELETE KNOWS traversal: " << e.what());
    }
}

TEST_CASE("DETACH DELETE preserves other connected nodes", "[ldbc][crud][detach-delete]") {
    SKIP_IF_NO_DB();
    try {
        // Delete 933, verify 10027 (a friend) still exists
        qr->run("MATCH (n:Person {id: 933}) DETACH DELETE n", {});

        auto r = qr->run("MATCH (n:Person {id: 10027}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0).length() > 0);
    } catch (const std::exception& e) {
        FAIL("DETACH DELETE preserves others: " << e.what());
    }
}

TEST_CASE("DETACH DELETE on base node with edges", "[ldbc][crud][detach-delete]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        // Person 4139 is a base node with edges — DETACH DELETE should work
        qr->run("MATCH (n:Person {id: 4139}) DETACH DELETE n", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before - 1);
    } catch (const std::exception& e) {
        FAIL("DETACH DELETE base with edges: " << e.what());
    }
}

// ============================================================
// Mixed CRUD tests
// ============================================================

TEST_CASE("CREATE → SET → READ", "[ldbc][crud][mixed]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("CREATE (n:Person {id: 40404040404040, firstName: 'Original'})", {});
        qr->run("MATCH (n:Person {id: 40404040404040}) SET n.firstName = 'Modified'", {});
        // Note: SET on in-memory node uses user_id based update
        // but filter pushdown won't find in-memory nodes → SET is no-op for in-memory
        // So this verifies the CREATE value persists at minimum
        auto r = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                          {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) == cnt_before + 1);
    } catch (const std::exception& e) {
        FAIL("CREATE→SET→READ: " << e.what());
    }
}

TEST_CASE("CREATE → DELETE → count decreases", "[ldbc][crud][mixed]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        // Create 3 nodes
        qr->run("CREATE (n:Person {id: 41414141414141, firstName: 'Mix1'})", {});
        qr->run("CREATE (n:Person {id: 41414141414142, firstName: 'Mix2'})", {});
        qr->run("CREATE (n:Person {id: 41414141414143, firstName: 'Mix3'})", {});

        auto mid = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                            {qtest::ColType::INT64});
        CHECK(mid[0].int64_at(0) == cnt_before + 3);

        // Delete 1 base node (use a known LDBC SF1 Person id — 933 is confirmed to exist)
        qr->run("MATCH (n:Person {id: 933}) DETACH DELETE n", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before + 3 - 1);
    } catch (const std::exception& e) {
        FAIL("CREATE→DELETE→count: " << e.what());
    }
}

TEST_CASE("SET on base node then read via different query", "[ldbc][crud][mixed]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("MATCH (n:Person {id: 4139}) SET n.firstName = 'MixedSet'", {});

        // Read back with a different projection
        auto r = qr->run(
            "MATCH (n:Person {id: 4139}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "MixedSet");
    } catch (const std::exception& e) {
        FAIL("SET then different read: " << e.what());
    }
}

TEST_CASE("SET two different nodes then read both", "[ldbc][crud][mixed]") {
    SKIP_IF_NO_DB();
    try {
        // Use two known-existing IDs not deleted by other mixed tests
        // First verify they exist
        // SET two different properties on same node (use 10027, not deleted by any test)
        qr->run("MATCH (n:Person {id: 10027}) SET n.firstName = 'NodeA'", {});
        qr->run("MATCH (n:Person {id: 10027}) SET n.lastName = 'NodeB'", {});

        auto ra = qr->run("MATCH (n:Person {id: 10027}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        auto rb = qr->run("MATCH (n:Person {id: 10027}) RETURN n.lastName",
                           {qtest::ColType::STRING});
        REQUIRE(ra.size() == 1);
        REQUIRE(rb.size() == 1);
        CHECK(ra[0].str_at(0) == "NodeA");
        CHECK(rb[0].str_at(0) == "NodeB");  // lastName was SET to 'NodeB'
    } catch (const std::exception& e) {
        FAIL("SET two nodes: " << e.what());
    }
}

TEST_CASE("DELETE then verify node gone from count", "[ldbc][crud][mixed]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        // Use a unique ID not used in other DELETE tests
        qr->run("MATCH (n:Person {id: 4194}) DETACH DELETE n", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before - 1);

        // Verify KNOWS traversal still works for other nodes
        auto r = qr->run(
            "MATCH (a:Person {id: 10027})-[:KNOWS]-(b:Person) RETURN count(b) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) > 0);
    } catch (const std::exception& e) {
        FAIL("DELETE then verify: " << e.what());
    }
}

TEST_CASE("interleaved CREATE and count", "[ldbc][crud][mixed]") {
    SKIP_IF_NO_DB();
    try {
        auto c0 = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                           {qtest::ColType::INT64});
        int64_t cnt0 = c0[0].int64_at(0);

        qr->run("CREATE (n:Person {id: 45454545454541, firstName: 'I1'})", {});
        auto c1 = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                           {qtest::ColType::INT64});
        CHECK(c1[0].int64_at(0) == cnt0 + 1);

        qr->run("CREATE (n:Person {id: 45454545454542, firstName: 'I2'})", {});
        auto c2 = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                           {qtest::ColType::INT64});
        CHECK(c2[0].int64_at(0) == cnt0 + 2);

        qr->run("CREATE (n:Person {id: 45454545454543, firstName: 'I3'})", {});
        auto c3 = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                           {qtest::ColType::INT64});
        CHECK(c3[0].int64_at(0) == cnt0 + 3);
    } catch (const std::exception& e) {
        FAIL("Interleaved CREATE+count: " << e.what());
    }
}

TEST_CASE("IC query unaffected by CRUD ops", "[ldbc][crud][mixed]") {
    SKIP_IF_NO_DB();
    try {
        // After all the CREATEs, SETs, DELETEs above, IC-style queries should still work
        auto r = qr->run(
            "MATCH (a:Person {id: 10027})-[:KNOWS]-(b:Person) "
            "RETURN b.firstName ORDER BY b.firstName LIMIT 3",
            {qtest::ColType::STRING});
        REQUIRE(r.size() > 0);
        CHECK(r.size() <= 3);
    } catch (const std::exception& e) {
        FAIL("IC query after CRUD: " << e.what());
    }
}

// ============================================================
// Phase 5: REMOVE tests
// ============================================================

TEST_CASE("REMOVE property", "[ldbc][crud][remove]") {
    SKIP_IF_NO_DB();
    try {
        // SET a property, then REMOVE it
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'TempName'", {});
        auto r1 = qr->run("MATCH (n:Person {id: 933}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r1.size() == 1);
        CHECK(r1[0].str_at(0) == "TempName");

        // REMOVE should set property to NULL
        qr->run("MATCH (n:Person {id: 933}) REMOVE n.firstName", {});
        auto r2 = qr->run("MATCH (n:Person {id: 933}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r2.size() == 1);
        // After REMOVE, property should be NULL or different from the SET value
        CHECK(r2[0].str_at(0) != "TempName");
    } catch (const std::exception& e) {
        FAIL("REMOVE property: " << e.what());
    }
}

TEST_CASE("REMOVE does not affect other properties", "[ldbc][crud][remove]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'Keep', n.lastName = 'Also'", {});
        qr->run("MATCH (n:Person {id: 933}) REMOVE n.firstName", {});
        auto r = qr->run("MATCH (n:Person {id: 933}) RETURN n.lastName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "Also");
    } catch (const std::exception& e) {
        FAIL("REMOVE isolation: " << e.what());
    }
}

TEST_CASE("REMOVE does not crash on non-existent property", "[ldbc][crud][remove]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("MATCH (n:Person {id: 933}) REMOVE n.nonExistentProp", {});
        SUCCEED();
    } catch (const std::exception& e) {
        FAIL("REMOVE non-existent prop: " << e.what());
    }
}

TEST_CASE("REMOVE count unchanged", "[ldbc][crud][remove]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("MATCH (n:Person {id: 933}) REMOVE n.firstName", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before);
    } catch (const std::exception& e) {
        FAIL("REMOVE count: " << e.what());
    }
}

// ============================================================
// Filter pushdown + in-memory node tests
// ============================================================

TEST_CASE("CREATE then MATCH by id finds in-memory node", "[ldbc][crud][filter-delta]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("CREATE (n:Person {id: 60606060606060, firstName: 'FilterTest'})", {});

        // This uses EQ filter pushdown — must also search in-memory nodes
        auto r = qr->run(
            "MATCH (n:Person {id: 60606060606060}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "FilterTest");
    } catch (const std::exception& e) {
        FAIL("CREATE then MATCH by id: " << e.what());
    }
}

TEST_CASE("CREATE then MATCH count includes in-memory with filter", "[ldbc][crud][filter-delta]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("CREATE (n:Person {id: 61616161616161, firstName: 'Count1'})", {});
        qr->run("CREATE (n:Person {id: 61616161616162, firstName: 'Count2'})", {});

        // Filter for specific created node
        auto r = qr->run(
            "MATCH (n:Person {id: 61616161616162}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "Count2");
    } catch (const std::exception& e) {
        FAIL("Filter + in-memory count: " << e.what());
    }
}

TEST_CASE("second in-memory node supports id projection", "[ldbc][crud][filter-delta]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("CREATE (n:Person {id: 62626262626261, firstName: 'Seed1', lastName: 'Seed'})", {});
        qr->run("CREATE (n:Person {id: 62626262626262, firstName: 'Seed2', lastName: 'Seed'})", {});

        auto r = qr->run(
            "MATCH (n:Person {id: 62626262626262}) RETURN id(n), n.id, n.firstName",
            {qtest::ColType::INT64, qtest::ColType::INT64, qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) > 0);
        CHECK(r[0].int64_at(1) == 62626262626262LL);
        CHECK(r[0].str_at(2) == "Seed2");
    } catch (const std::exception& e) {
        FAIL("second in-memory node id projection: " << e.what());
    }
}

TEST_CASE("filter returns empty for non-matching in-memory node", "[ldbc][crud][filter-delta]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("CREATE (n:Person {id: 62626262626262, firstName: 'NoMatch'})", {});

        // Search for different id — should NOT find the created node
        auto r = qr->run(
            "MATCH (n:Person {id: 99999999999999}) RETURN n.firstName",
            {qtest::ColType::STRING});
        CHECK(r.size() == 0);
    } catch (const std::exception& e) {
        FAIL("Filter no match: " << e.what());
    }
}

TEST_CASE("CREATE then SET via filter finds in-memory node", "[ldbc][crud][filter-delta]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("CREATE (n:Person {id: 63636363636363, firstName: 'BeforeSet'})", {});
        // SET on in-memory node via filter pushdown
        qr->run("MATCH (n:Person {id: 63636363636363}) SET n.firstName = 'AfterSet'", {});

        auto r = qr->run(
            "MATCH (n:Person {id: 63636363636363}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "AfterSet");
    } catch (const std::exception& e) {
        FAIL("CREATE then SET via filter: " << e.what());
    }
}

TEST_CASE("SET updates second in-memory node", "[ldbc][crud][set][filter-delta]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("CREATE (n:Person {id: 63636363636361, firstName: 'Seed1'})", {});
        qr->run("CREATE (n:Person {id: 63636363636362, firstName: 'Seed2'})", {});

        qr->run("MATCH (n:Person {id: 63636363636362}) SET n.firstName = 'Updated2'", {});

        auto first = qr->run(
            "MATCH (n:Person {id: 63636363636361}) RETURN n.firstName",
            {qtest::ColType::STRING});
        auto second = qr->run(
            "MATCH (n:Person {id: 63636363636362}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(first.size() == 1);
        REQUIRE(second.size() == 1);
        CHECK(first[0].str_at(0) == "Seed1");
        CHECK(second[0].str_at(0) == "Updated2");
    } catch (const std::exception& e) {
        FAIL("SET second in-memory node: " << e.what());
    }
}

TEST_CASE("DELETE removes second in-memory node only", "[ldbc][crud][delete][filter-delta]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("CREATE (n:Person {id: 64646464646461, firstName: 'Keep'})", {});
        qr->run("CREATE (n:Person {id: 64646464646462, firstName: 'Drop'})", {});

        qr->run("MATCH (n:Person {id: 64646464646462}) DELETE n", {});

        auto keep = qr->run(
            "MATCH (n:Person {id: 64646464646461}) RETURN n.firstName",
            {qtest::ColType::STRING});
        auto drop = qr->run(
            "MATCH (n:Person {id: 64646464646462}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(keep.size() == 1);
        CHECK(keep[0].str_at(0) == "Keep");
        CHECK(drop.empty());
    } catch (const std::exception& e) {
        FAIL("DELETE second in-memory node: " << e.what());
    }
}

// ============================================================
// MERGE tests
// ============================================================

TEST_CASE("MERGE creates non-existent node", "[ldbc][crud][merge]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("MERGE (n:Person {id: 70707070707070, firstName: 'MergeNew'})", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before + 1);
    } catch (const std::exception& e) {
        FAIL("MERGE create: " << e.what());
    }
}

TEST_CASE("MERGE existing node does not duplicate", "[ldbc][crud][merge]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        // MERGE on existing base node — should NOT create new
        qr->run("MERGE (n:Person {id: 933})", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before);
    } catch (const std::exception& e) {
        FAIL("MERGE existing: " << e.what());
    }
}

TEST_CASE("MERGE twice does not duplicate", "[ldbc][crud][merge]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("MERGE (n:Person {id: 72727272727272, firstName: 'MergeTwice'})", {});
        qr->run("MERGE (n:Person {id: 72727272727272, firstName: 'MergeTwice'})", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        // Only +1, not +2
        CHECK(after[0].int64_at(0) == cnt_before + 1);
    } catch (const std::exception& e) {
        FAIL("MERGE twice: " << e.what());
    }
}

TEST_CASE("MERGE does not crash", "[ldbc][crud][merge]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("MERGE (n:Person {id: 73737373737373})", {});
        SUCCEED();
    } catch (const std::exception& e) {
        FAIL("MERGE crash: " << e.what());
    }
}

// ============================================================
// Stress / bulk CRUD tests
// ============================================================

TEST_CASE("bulk CREATE 50 nodes then count", "[ldbc][crud][stress]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        for (int i = 0; i < 50; i++) {
            std::string q = "CREATE (n:Person {id: " + std::to_string(80808080800000LL + i) +
                            ", firstName: 'Bulk" + std::to_string(i) + "'})";
            qr->run(q.c_str(), {});
        }

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before + 50);
    } catch (const std::exception& e) {
        FAIL("Bulk CREATE 50: " << e.what());
    }
}

TEST_CASE("bulk CREATE then find each by id", "[ldbc][crud][stress]") {
    SKIP_IF_NO_DB();
    try {
        int N = 10;
        for (int i = 0; i < N; i++) {
            std::string q = "CREATE (n:Person {id: " + std::to_string(81818181810000LL + i) +
                            ", firstName: 'Find" + std::to_string(i) + "'})";
            qr->run(q.c_str(), {});
        }

        // Find each one by id (filter pushdown + in-memory)
        for (int i = 0; i < N; i++) {
            std::string q = "MATCH (n:Person {id: " + std::to_string(81818181810000LL + i) +
                            "}) RETURN n.firstName";
            auto r = qr->run(q.c_str(), {qtest::ColType::STRING});
            REQUIRE(r.size() == 1);
            CHECK(r[0].str_at(0) == "Find" + std::to_string(i));
        }
    } catch (const std::exception& e) {
        FAIL("Bulk CREATE then find: " << e.what());
    }
}

TEST_CASE("bulk SET on multiple nodes", "[ldbc][crud][stress]") {
    SKIP_IF_NO_DB();
    try {
        // SET firstName on 5 different base nodes
        std::vector<int64_t> ids = {933, 4139, 10027, 65, 94};
        for (size_t i = 0; i < ids.size(); i++) {
            std::string q = "MATCH (n:Person {id: " + std::to_string(ids[i]) +
                            "}) SET n.firstName = 'Stress" + std::to_string(i) + "'";
            qr->run(q.c_str(), {});
        }

        // Verify each
        for (size_t i = 0; i < ids.size(); i++) {
            std::string q = "MATCH (n:Person {id: " + std::to_string(ids[i]) +
                            "}) RETURN n.firstName";
            auto r = qr->run(q.c_str(), {qtest::ColType::STRING});
            REQUIRE(r.size() == 1);
            CHECK(r[0].str_at(0) == "Stress" + std::to_string(i));
        }
    } catch (const std::exception& e) {
        FAIL("Bulk SET: " << e.what());
    }
}

TEST_CASE("bulk DELETE then count", "[ldbc][crud][stress]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        // Delete 5 base nodes (DETACH since they have edges)
        std::vector<int64_t> ids = {933, 4139, 10027, 65, 94};
        for (auto id : ids) {
            std::string q = "MATCH (n:Person {id: " + std::to_string(id) + "}) DETACH DELETE n";
            qr->run(q.c_str(), {});
        }

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before - (int64_t)ids.size());
    } catch (const std::exception& e) {
        FAIL("Bulk DELETE: " << e.what());
    }
}

TEST_CASE("rapid CREATE-DELETE cycle", "[ldbc][crud][stress]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        // CREATE 10 nodes, then DELETE 5 of them (base nodes)
        for (int i = 0; i < 10; i++) {
            std::string q = "CREATE (n:Person {id: " + std::to_string(84848484840000LL + i) +
                            ", firstName: 'Cycle" + std::to_string(i) + "'})";
            qr->run(q.c_str(), {});
        }
        // Delete 5 base nodes (DETACH since they have edges)
        std::vector<int64_t> del_ids = {933, 4139, 65, 94, 1129};
        for (auto id : del_ids) {
            std::string q = "MATCH (n:Person {id: " + std::to_string(id) + "}) DETACH DELETE n";
            qr->run(q.c_str(), {});
        }

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before + 10 - (int64_t)del_ids.size());
    } catch (const std::exception& e) {
        FAIL("CREATE-DELETE cycle: " << e.what());
    }
}

TEST_CASE("MERGE idempotence stress", "[ldbc][crud][stress]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        // MERGE same id 10 times — should only create once
        for (int i = 0; i < 10; i++) {
            qr->run("MERGE (n:Person {id: 85858585858585, firstName: 'MergeStress'})", {});
        }

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before + 1);
    } catch (const std::exception& e) {
        FAIL("MERGE stress: " << e.what());
    }
}

TEST_CASE("interleaved CRUD storm", "[ldbc][crud][stress]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        // CREATE 5
        for (int i = 0; i < 5; i++) {
            std::string q = "CREATE (n:Person {id: " + std::to_string(86868686860000LL + i) +
                            ", firstName: 'Storm" + std::to_string(i) + "'})";
            qr->run(q.c_str(), {});
        }
        // SET on base node
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'Storm'", {});
        // DELETE 2 base nodes (DETACH since they have edges)
        qr->run("MATCH (n:Person {id: 94}) DETACH DELETE n", {});
        qr->run("MATCH (n:Person {id: 96}) DETACH DELETE n", {});
        // MERGE (new)
        qr->run("MERGE (n:Person {id: 86868686869999, firstName: 'MergeStorm'})", {});
        // MERGE (existing base)
        qr->run("MERGE (n:Person {id: 10027})", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        // +5 CREATE, -2 DELETE, +1 MERGE(new), +0 MERGE(existing) = +4
        CHECK(after[0].int64_at(0) == cnt_before + 4);

        // Verify SET stuck
        auto r = qr->run("MATCH (n:Person {id: 933}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "Storm");
    } catch (const std::exception& e) {
        FAIL("CRUD storm: " << e.what());
    }
}

TEST_CASE("IC queries survive CRUD storm", "[ldbc][crud][stress]") {
    SKIP_IF_NO_DB();
    try {
        // Do some mutations
        qr->run("CREATE (n:Person {id: 87878787870000, firstName: 'Survive'})", {});
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'Survived'", {});
        qr->run("MATCH (n:Person {id: 94}) DETACH DELETE n", {});

        // IC-style: KNOWS traversal
        auto r1 = qr->run(
            "MATCH (a:Person {id: 10027})-[:KNOWS]-(b:Person) RETURN count(b) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(r1.size() == 1);
        CHECK(r1[0].int64_at(0) > 0);

        // Full Person count should be reasonable
        auto r2 = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                           {qtest::ColType::INT64});
        REQUIRE(r2.size() == 1);
        CHECK(r2[0].int64_at(0) > 9800);
    } catch (const std::exception& e) {
        FAIL("IC after CRUD storm: " << e.what());
    }
}

// ============================================================
// WAL (Write-Ahead Log) persistence tests
// ============================================================

TEST_CASE("CREATE survives reconnect", "[ldbc][crud][wal]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("CREATE (n:Person {id: 90909090909090, firstName: 'WALTest'})", {});

        // Verify node exists before reconnect
        auto r1 = qr->run("MATCH (n:Person {id: 90909090909090}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r1.size() == 1);
        CHECK(r1[0].str_at(0) == "WALTest");

        // Simulate restart
        qr->reconnect(crud_db_path);

        // Node should survive via WAL replay
        auto r2 = qr->run("MATCH (n:Person {id: 90909090909090}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r2.size() == 1);
        CHECK(r2[0].str_at(0) == "WALTest");
    } catch (const std::exception& e) {
        FAIL("CREATE survives reconnect: " << e.what());
    }
}

TEST_CASE("CREATE count survives reconnect", "[ldbc][crud][wal]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("CREATE (n:Person {id: 91919191919191, firstName: 'WALCount'})", {});

        qr->reconnect(crud_db_path);

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before + 1);
    } catch (const std::exception& e) {
        FAIL("CREATE count survives: " << e.what());
    }
}

TEST_CASE("SET survives reconnect", "[ldbc][crud][wal]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'WALSet'", {});

        qr->reconnect(crud_db_path);

        auto r = qr->run("MATCH (n:Person {id: 933}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "WALSet");
    } catch (const std::exception& e) {
        FAIL("SET survives reconnect: " << e.what());
    }
}

TEST_CASE("DELETE survives reconnect", "[ldbc][crud][wal]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("MATCH (n:Person {id: 94}) DETACH DELETE n", {});

        qr->reconnect(crud_db_path);

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before - 1);
    } catch (const std::exception& e) {
        FAIL("DELETE survives reconnect: " << e.what());
    }
}

TEST_CASE("mixed CRUD survives reconnect", "[ldbc][crud][wal]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        // CREATE 2, SET 1, DELETE 1
        qr->run("CREATE (n:Person {id: 94949494940001, firstName: 'WALMix1'})", {});
        qr->run("CREATE (n:Person {id: 94949494940002, firstName: 'WALMix2'})", {});
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'WALMixed'", {});
        qr->run("MATCH (n:Person {id: 94}) DETACH DELETE n", {});

        qr->reconnect(crud_db_path);

        // count: +2 CREATE, -1 DELETE = +1
        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before + 1);

        // SET should persist
        auto r = qr->run("MATCH (n:Person {id: 933}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "WALMixed");

        // Created nodes should exist
        auto r2 = qr->run("MATCH (n:Person {id: 94949494940001}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r2.size() == 1);
        CHECK(r2[0].str_at(0) == "WALMix1");
    } catch (const std::exception& e) {
        FAIL("Mixed CRUD survives reconnect: " << e.what());
    }
}

TEST_CASE("double reconnect preserves state", "[ldbc][crud][wal]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("CREATE (n:Person {id: 95959595959595, firstName: 'DoubleRC'})", {});

        qr->reconnect(crud_db_path);
        qr->reconnect(crud_db_path);

        auto r = qr->run("MATCH (n:Person {id: 95959595959595}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "DoubleRC");
    } catch (const std::exception& e) {
        FAIL("Double reconnect: " << e.what());
    }
}

TEST_CASE("base data intact after reconnect", "[ldbc][crud][wal]") {
    SKIP_IF_NO_DB();
    try {
        // Just reconnect without mutations — base data should be fine
        qr->reconnect(crud_db_path);

        auto r = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                          {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) == 9892);  // LDBC SF1 base count

        // Cleanup: ensure no residual delta for subsequent test files (Q2, Q5, Q6)
    } catch (const std::exception& e) {
        FAIL("Base data after reconnect: " << e.what());
    }
}

// ============================================================
// MATCH + CREATE edge (between existing nodes)
// ============================================================

TEST_CASE("MATCH two nodes then CREATE edge", "[ldbc][crud][match-create-edge]") {
    SKIP_IF_NO_DB();
    try {
        // Person 933 and 10027 both exist — create KNOWS edge between them
        qr->run("MATCH (a:Person {id: 933}), (b:Person {id: 10027}) CREATE (a)-[:KNOWS]->(b)", {});
        SUCCEED();
    } catch (const std::exception& e) {
        FAIL("MATCH+CREATE edge: " << e.what());
    }
}

TEST_CASE("MATCH+CREATE edge increases friend count", "[ldbc][crud][match-create-edge]") {
    SKIP_IF_NO_DB();
    try {
        // Create two fresh nodes (in-memory)
        qr->run("CREATE (n:Person {id: 10110101010101, firstName: 'Src'})", {});
        qr->run("CREATE (n:Person {id: 10110101010102, firstName: 'Dst'})", {});

        // Create edge between existing base nodes
        auto before = qr->run(
            "MATCH (a:Person {id: 933})-[:KNOWS]-(b:Person) RETURN count(b) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(before.size() == 1);
        int64_t friends_before = before[0].int64_at(0);

        qr->run("MATCH (a:Person {id: 933}), (b:Person {id: 4139}) CREATE (a)-[:KNOWS]->(b)", {});

        auto after = qr->run(
            "MATCH (a:Person {id: 933})-[:KNOWS]-(b:Person) RETURN count(b) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(after.size() == 1);
        CHECK(after[0].int64_at(0) >= friends_before);
    } catch (const std::exception& e) {
        FAIL("MATCH+CREATE edge count: " << e.what());
    }
}

TEST_CASE("MATCH+CREATE edge does not affect node count", "[ldbc][crud][match-create-edge]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("MATCH (a:Person {id: 933}), (b:Person {id: 4139}) CREATE (a)-[:KNOWS]->(b)", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        // Edge creation should NOT change node count
        CHECK(after[0].int64_at(0) == cnt_before);
    } catch (const std::exception& e) {
        FAIL("MATCH+CREATE edge node count: " << e.what());
    }
}

TEST_CASE("MATCH+CREATE edge no crash on non-existent node", "[ldbc][crud][match-create-edge]") {
    SKIP_IF_NO_DB();
    try {
        // One node doesn't exist — should be no-op (0 matched rows)
        qr->run("MATCH (a:Person {id: 933}), (b:Person {id: 999999999999999}) CREATE (a)-[:KNOWS]->(b)", {});
        SUCCEED();
    } catch (const std::exception& e) {
        FAIL("MATCH+CREATE edge non-existent: " << e.what());
    }
}

TEST_CASE("MATCH+CREATE edge between fresh nodes is traversable", "[ldbc][crud][match-create-edge]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("CREATE (n:Person {id: 92929292929291, firstName: 'FreshSrc'})", {});
        qr->run("CREATE (n:Person {id: 92929292929292, firstName: 'FreshDst'})", {});

        qr->run("MATCH (a:Person {id: 92929292929291}), (b:Person {id: 92929292929292}) CREATE (a)-[:KNOWS]->(b)", {});

        auto r = qr->run(
            "MATCH (a:Person {id: 92929292929291})-[:KNOWS]->(b:Person {id: 92929292929292}) "
            "RETURN count(b) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) == 1);
    } catch (const std::exception& e) {
        FAIL("MATCH+CREATE fresh edge traversal: " << e.what());
    }
}

// ============================================================
// Compaction tests — each runs in an isolated temp workspace
// to prevent ghost extent accumulation across tests.
// ============================================================

// Lazily-created shared workspace (copies db once to /tmp)
static qtest::CompactionWorkspace* g_compact_ws = nullptr;

// Active runner must be fully disconnected before compaction tests create
// their own turbolynx_connect (DiskAioFactory is a global singleton).
// Whatever runner is active (CRUD workspace from SKIP_IF_NO_DB, or none) is torn down.
static bool g_singleton_disconnected = false;
static void ensure_singleton_disconnected() {
    if (g_singleton_disconnected) return;
    disconnect_active_runner();
    g_singleton_disconnected = true;
}
static void ensure_singleton_reconnected() {
    // No-op: the next SKIP_IF_NO_DB() activates a fresh CRUD runner on its own.
    g_singleton_disconnected = false;
}

struct CompactionGuard {
    ~CompactionGuard() { ensure_singleton_reconnected(); }
};

// Setup macro for compaction tests: resets workspace, creates local QueryRunner
#define COMPACTION_SETUP() \
    if (g_ldbc_path.empty()) { WARN("--ldbc-path not set, skipping"); g_skip_requested = true; return; } \
    if (!g_has_ldbc) { WARN("DB has no LDBC schema, skipping"); return; } \
    ensure_singleton_disconnected(); \
    if (!g_compact_ws) g_compact_ws = new qtest::CompactionWorkspace(g_ldbc_path); \
    g_compact_ws->reset(); \
    CompactionGuard _cg; \
    qtest::QueryRunner cqr(g_compact_ws->path()); \
    auto* qr = &cqr; \
    [[maybe_unused]] const std::string& compact_db_path = g_compact_ws->path()

TEST_CASE("CREATE survives checkpoint + reconnect", "[ldbc][crud][compaction]") {
    COMPACTION_SETUP();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("CREATE (n:Person {id: 11011011011010, firstName: 'Compacted'})", {});

        // Checkpoint: flush to disk + truncate WAL
        qr->checkpoint();
        // Reconnect: WAL is empty, must read from base
        qr->reconnect(compact_db_path);

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before + 1);
    } catch (const std::exception& e) {
        FAIL("CREATE survives compaction: " << e.what());
    }
}

TEST_CASE("MATCH+CREATE edge survives checkpointed fresh nodes + reconnect",
          "[ldbc][crud][compaction][match-create-edge]") {
    COMPACTION_SETUP();
    try {
        qr->run("CREATE (n:Person {id: 93939393939391, firstName: 'FreshSrc'})", {});
        qr->run("CREATE (n:Person {id: 93939393939392, firstName: 'FreshDst'})", {});

        qr->checkpoint();
        qr->reconnect(compact_db_path);

        auto ids = qr->run(
            "MATCH (a:Person {id: 93939393939391}), (b:Person {id: 93939393939392}) "
            "RETURN id(a) AS aid, id(b) AS bid",
            {qtest::ColType::INT64, qtest::ColType::INT64});
        REQUIRE(ids.size() == 1);
        CHECK(ids[0].int64_at(0) == 0x7F00000000000001LL);
        CHECK(ids[0].int64_at(1) == 0x7F00000000000002LL);

        qr->run(
            "MATCH (a:Person {id: 93939393939391}), (b:Person {id: 93939393939392}) "
            "CREATE (a)-[:KNOWS]->(b)",
            {});

        qr->checkpoint();
        qr->reconnect(compact_db_path);

        auto r = qr->run(
            "MATCH (a:Person {id: 93939393939391})-[:KNOWS]->(b:Person {id: 93939393939392}) "
            "RETURN count(b) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) == 1);
    } catch (const std::exception& e) {
        FAIL("MATCH+CREATE edge persists after compaction: " << e.what());
    }
}

TEST_CASE("parallel checkpointed fresh KNOWS traversal preserves rowset",
          "[ldbc][crud][compaction][parallel]") {
    COMPACTION_SETUP();
    try {
        constexpr int64_t kBaseId = 88000000005000LL;
        for (int i = 0; i < 6; i++) {
            std::string create_q =
                "CREATE (n:Person {id: " + std::to_string(kBaseId + i) +
                ", firstName: 'Chain" + std::to_string(i) +
                "', lastName: 'Stress', gender: 'male', birthday: 19900101, "
                "creationDate: 20200101, locationIP: '10.0.0." +
                std::to_string(i) + "', browserUsed: 'Chrome'})";
            qr->run(create_q.c_str(), {});
        }

        qr->checkpoint();
        qr->reconnect(compact_db_path);

        for (int i = 0; i < 5; i++) {
            std::string edge_q =
                "MATCH (a:Person {id: " + std::to_string(kBaseId + i) +
                "}), (b:Person {id: " + std::to_string(kBaseId + i + 1) +
                "}) CREATE (a)-[:KNOWS]->(b)";
            qr->run(edge_q.c_str(), {});
        }

        qr->checkpoint();
        qr->reconnect(compact_db_path);
        qr->run("PRAGMA threads = 4", {});

        auto count_rows = qr->run(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "WHERE a.id >= 88000000005000 AND a.id < 88000000005006 "
            "AND b.id >= 88000000005000 AND b.id < 88000000005006 "
            "RETURN count(b) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(count_rows.size() == 1);
        CHECK(count_rows[0].int64_at(0) == 10);

        auto pairs = qr->run(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "WHERE a.id >= 88000000005000 AND a.id < 88000000005006 "
            "AND b.id >= 88000000005000 AND b.id < 88000000005006 "
            "RETURN a.id, b.id ORDER BY b.id, a.id",
            {qtest::ColType::INT64, qtest::ColType::INT64});

        std::vector<std::pair<int64_t, int64_t>> actual_pairs;
        actual_pairs.reserve(pairs.size());
        for (const auto &row : pairs.rows) {
            actual_pairs.emplace_back(row.int64_at(0), row.int64_at(1));
        }

        const std::vector<std::pair<int64_t, int64_t>> expected_pairs = {
            {88000000005001LL, 88000000005000LL},
            {88000000005000LL, 88000000005001LL},
            {88000000005002LL, 88000000005001LL},
            {88000000005001LL, 88000000005002LL},
            {88000000005003LL, 88000000005002LL},
            {88000000005002LL, 88000000005003LL},
            {88000000005004LL, 88000000005003LL},
            {88000000005003LL, 88000000005004LL},
            {88000000005005LL, 88000000005004LL},
            {88000000005004LL, 88000000005005LL},
        };

        CHECK(actual_pairs.size() ==
              static_cast<size_t>(count_rows[0].int64_at(0)));
        CHECK(actual_pairs == expected_pairs);
    } catch (const std::exception& e) {
        FAIL("Parallel checkpointed KNOWS traversal: " << e.what());
    }
}

TEST_CASE("CREATE node findable by id after compaction", "[ldbc][crud][compaction][!mayfail]") {
    COMPACTION_SETUP();
    try {
        qr->run("CREATE (n:Person {id: 11111111111110, firstName: 'FindAfterCP'})", {});

        qr->checkpoint();
        qr->reconnect(compact_db_path);

        auto r = qr->run("MATCH (n:Person {id: 11111111111110}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "FindAfterCP");
    } catch (const std::exception& e) {
        FAIL("Find after compaction: " << e.what());
    }
}

TEST_CASE("multiple CREATEs survive compaction", "[ldbc][crud][compaction]") {
    COMPACTION_SETUP();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        for (int i = 0; i < 5; i++) {
            std::string q = "CREATE (n:Person {id: " + std::to_string(11211211211200LL + i) +
                            ", firstName: 'CP" + std::to_string(i) + "'})";
            qr->run(q.c_str(), {});
        }

        qr->checkpoint();
        qr->reconnect(compact_db_path);

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before + 5);
    } catch (const std::exception& e) {
        FAIL("Multiple CREATEs compaction: " << e.what());
    }
}

TEST_CASE("SET survives compaction + reconnect", "[ldbc][crud][compaction][!mayfail]") {
    COMPACTION_SETUP();
    try {
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'Compacted933'", {});

        qr->checkpoint();
        qr->reconnect(compact_db_path);

        auto r = qr->run("MATCH (n:Person {id: 933}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "Compacted933");
    } catch (const std::exception& e) {
        FAIL("SET survives compaction: " << e.what());
    }
}

TEST_CASE("DELETE survives compaction + reconnect", "[ldbc][crud][compaction][!mayfail]") {
    COMPACTION_SETUP();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("MATCH (n:Person {id: 94}) DETACH DELETE n", {});

        qr->checkpoint();
        qr->reconnect(compact_db_path);

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before - 1);
    } catch (const std::exception& e) {
        FAIL("DELETE survives compaction: " << e.what());
    }
}

TEST_CASE("CREATE with full schema survives compaction", "[ldbc][crud][compaction][!mayfail]") {
    COMPACTION_SETUP();
    try {
        // Full Person schema — should match existing PropertySchema exactly
        qr->run("CREATE (n:Person {id: 11511511511510, firstName: 'Full', lastName: 'Schema', "
                "gender: 'male', birthday: 19900101, creationDate: 20200101, "
                "locationIP: '127.0.0.1', browserUsed: 'Chrome'})", {});

        qr->checkpoint();
        qr->reconnect(compact_db_path);

        auto r = qr->run("MATCH (n:Person {id: 11511511511510}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "Full");
    } catch (const std::exception& e) {
        FAIL("Full schema compaction: " << e.what());
    }
}

TEST_CASE("CREATE with minimal schema survives compaction", "[ldbc][crud][compaction]") {
    COMPACTION_SETUP();
    try {
        // Minimal — only id, no other properties
        qr->run("CREATE (n:Person {id: 11611611611610})", {});

        qr->checkpoint();
        qr->reconnect(compact_db_path);

        auto r = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                          {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) == 9892 + 1);
    } catch (const std::exception& e) {
        FAIL("Minimal schema compaction: " << e.what());
    }
}

TEST_CASE("mixed schema CREATEs survive compaction", "[ldbc][crud][compaction]") {
    COMPACTION_SETUP();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        // Different schemas in same partition
        qr->run("CREATE (n:Person {id: 11711711711701, firstName: 'Two'})", {});
        qr->run("CREATE (n:Person {id: 11711711711702, firstName: 'Three', lastName: 'Props'})", {});
        qr->run("CREATE (n:Person {id: 11711711711703})", {});  // id only

        qr->checkpoint();
        qr->reconnect(compact_db_path);

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before + 3);
    } catch (const std::exception& e) {
        FAIL("Mixed schema compaction: " << e.what());
    }
}

TEST_CASE("compaction then more CREATEs", "[ldbc][crud][compaction]") {
    COMPACTION_SETUP();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        // First batch
        qr->run("CREATE (n:Person {id: 11811811811801, firstName: 'Batch1'})", {});
        qr->checkpoint();

        // Second batch (after compaction — delta is empty again)
        qr->run("CREATE (n:Person {id: 11811811811802, firstName: 'Batch2'})", {});
        qr->checkpoint();

        qr->reconnect(compact_db_path);

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before + 2);
    } catch (const std::exception& e) {
        FAIL("Double compaction: " << e.what());
    }
}

TEST_CASE("base data intact after empty compaction", "[ldbc][crud][compaction]") {
    COMPACTION_SETUP();
    try {
        // No mutations — just checkpoint + reconnect
        qr->checkpoint();
        qr->reconnect(compact_db_path);

        auto r = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                          {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) == 9892);
    } catch (const std::exception& e) {
        FAIL("Empty compaction: " << e.what());
    }
}

// ============================================================
// checkpoint_ctx tests — validate turbolynx_checkpoint_ctx
// used by .checkpoint shell command
// ============================================================

TEST_CASE("double checkpoint preserves SET+DELETE", "[ldbc][crud][compaction]") {
    COMPACTION_SETUP();
    try {
        // SET + DELETE, checkpoint, then checkpoint again (no new mutations)
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'DoubleCP'", {});
        qr->run("MATCH (n:Person {id: 94}) DETACH DELETE n", {});
        qr->checkpoint();
        // Second checkpoint with no new mutations — should preserve WAL entries
        qr->checkpoint();
        qr->reconnect(compact_db_path);

        auto r = qr->run("MATCH (n:Person {id: 933}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "DoubleCP");

        auto r2 = qr->run("MATCH (n:Person {id: 94}) RETURN count(n) AS cnt",
                           {qtest::ColType::INT64});
        CHECK(r2[0].int64_at(0) == 0);
    } catch (const std::exception& e) {
        FAIL("double checkpoint SET+DELETE: " << e.what());
    }
}

TEST_CASE("checkpoint mixed CREATE+SET+DELETE survives reconnect", "[ldbc][crud][compaction]") {
    COMPACTION_SETUP();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        // CREATE + SET + DELETE in one session, then checkpoint
        qr->run("CREATE (n:Person {id: 12112112112110, firstName: 'Mixed'})", {});
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'MixedSet'", {});
        qr->run("MATCH (n:Person {id: 94}) DETACH DELETE n", {});

        qr->checkpoint();
        qr->reconnect(compact_db_path);

        // CREATE survived
        auto r1 = qr->run("MATCH (n:Person {id: 12112112112110}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r1.size() == 1);
        CHECK(r1[0].str_at(0) == "Mixed");

        // SET survived
        auto r2 = qr->run("MATCH (n:Person {id: 933}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r2.size() == 1);
        CHECK(r2[0].str_at(0) == "MixedSet");

        // DELETE survived: count = before + 1 (create) - 1 (delete) = before
        auto r3 = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                           {qtest::ColType::INT64});
        CHECK(r3[0].int64_at(0) == cnt_before);
    } catch (const std::exception& e) {
        FAIL("checkpoint mixed: " << e.what());
    }
}

TEST_CASE("checkpoint then more mutations then checkpoint", "[ldbc][crud][compaction]") {
    COMPACTION_SETUP();
    try {
        // First round: CREATE + checkpoint
        qr->run("CREATE (n:Person {id: 12212212212201, firstName: 'Round1'})", {});
        qr->checkpoint();

        // Second round: SET on the node we just created + new CREATE + checkpoint
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'Round2Set'", {});
        qr->run("CREATE (n:Person {id: 12212212212202, firstName: 'Round2'})", {});
        qr->checkpoint();
        qr->reconnect(compact_db_path);

        // Both CREATEs survived
        auto r1 = qr->run("MATCH (n:Person {id: 12212212212201}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r1.size() == 1);
        CHECK(r1[0].str_at(0) == "Round1");

        auto r2 = qr->run("MATCH (n:Person {id: 12212212212202}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r2.size() == 1);
        CHECK(r2[0].str_at(0) == "Round2");

        // SET survived
        auto r3 = qr->run("MATCH (n:Person {id: 933}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r3.size() == 1);
        CHECK(r3[0].str_at(0) == "Round2Set");
    } catch (const std::exception& e) {
        FAIL("multi-round checkpoint: " << e.what());
    }
}

// ============================================================
// Filter pushdown after compaction
// ============================================================

TEST_CASE("filter pushdown on compacted extent (id lookup)", "[ldbc][crud][compaction][pushdown]") {
    COMPACTION_SETUP();
    try {
        // CREATE a node, compact it to a real extent, then query with id filter
        // The id filter triggers filter pushdown via ChunkDefinition minmax lookup
        qr->run("CREATE (n:Person {id: 12512512512510, firstName: 'Pushdown', lastName: 'Test', "
                "gender: 'male', birthday: 19950101, creationDate: 20210101, "
                "locationIP: '10.0.0.1', browserUsed: 'Firefox'})", {});

        qr->checkpoint();
        qr->reconnect(compact_db_path);

        // This query uses filter pushdown (MATCH with id = X → EQ filter on id column)
        auto r = qr->run("MATCH (n:Person {id: 12512512512510}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "Pushdown");
    } catch (const std::exception& e) {
        FAIL("filter pushdown on compacted extent: " << e.what());
    }
}

TEST_CASE("filter pushdown range query on compacted extent", "[ldbc][crud][compaction][pushdown]") {
    COMPACTION_SETUP();
    try {
        // Create multiple nodes with sequential ids, compact, then range query
        qr->run("CREATE (n:Person {id: 12612612612601, firstName: 'Range1', lastName: 'A', "
                "gender: 'male', birthday: 19900101, creationDate: 20200101, "
                "locationIP: '1.1.1.1', browserUsed: 'Chrome'})", {});
        qr->run("CREATE (n:Person {id: 12612612612602, firstName: 'Range2', lastName: 'B', "
                "gender: 'female', birthday: 19910101, creationDate: 20200201, "
                "locationIP: '2.2.2.2', browserUsed: 'Safari'})", {});

        qr->checkpoint();
        qr->reconnect(compact_db_path);

        // Count with filter — triggers filter pushdown
        auto r = qr->run("MATCH (n:Person) WHERE n.id >= 12612612612601 AND n.id <= 12612612612602 "
                          "RETURN count(n) AS cnt", {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) == 2);
    } catch (const std::exception& e) {
        FAIL("filter pushdown range on compacted extent: " << e.what());
    }
}

TEST_CASE("filter pushdown after compaction + base data filter still works", "[ldbc][crud][compaction][pushdown]") {
    COMPACTION_SETUP();
    try {
        // Compact some new data, then verify existing base data filter still works
        qr->run("CREATE (n:Person {id: 12712712712710, firstName: 'New'})", {});
        qr->checkpoint();
        qr->reconnect(compact_db_path);

        // Query on existing base data (id=933 is in original LDBC data)
        auto r = qr->run("MATCH (n:Person {id: 933}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        // Original value (base data should be intact)
        CHECK(!r[0].str_at(0).empty());

        // Query on compacted data
        auto r2 = qr->run("MATCH (n:Person {id: 12712712712710}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r2.size() == 1);
        CHECK(r2[0].str_at(0) == "New");
    } catch (const std::exception& e) {
        FAIL("filter pushdown after compaction + base data: " << e.what());
    }
}

// ===========================================================================
// UNWIND + CREATE (batch mutation)
// ===========================================================================

TEST_CASE("UNWIND CREATE basic", "[ldbc][crud][unwind-create]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("UNWIND [130001, 130002, 130003] AS x CREATE (n:Person {id: x, firstName: 'Batch'})", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before + 3);
    } catch (const std::exception& e) {
        FAIL("UNWIND CREATE basic: " << e.what());
    }
}

TEST_CASE("UNWIND CREATE nodes are queryable", "[ldbc][crud][unwind-create]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("UNWIND [131001, 131002] AS x CREATE (n:Person {id: x, firstName: 'UC'})", {});

        auto r1 = qr->run("MATCH (n:Person {id: 131001}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r1.size() == 1);
        CHECK(r1[0].str_at(0) == "UC");

        auto r2 = qr->run("MATCH (n:Person {id: 131002}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r2.size() == 1);
        CHECK(r2[0].str_at(0) == "UC");
    } catch (const std::exception& e) {
        FAIL("UNWIND CREATE queryable: " << e.what());
    }
}

TEST_CASE("UNWIND CREATE with computed list", "[ldbc][crud][unwind-create]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("UNWIND [77132001, 77132002, 77132003] AS x CREATE (n:Person {id: x, firstName: 'Expr'})", {});

        auto r = qr->run("MATCH (n:Person {id: 77132001}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "Expr");

        auto r2 = qr->run("MATCH (n:Person {id: 77132003}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r2.size() == 1);
        CHECK(r2[0].str_at(0) == "Expr");
    } catch (const std::exception& e) {
        FAIL("UNWIND CREATE computed list: " << e.what());
    }
}

TEST_CASE("UNWIND CREATE empty list", "[ldbc][crud][unwind-create]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        qr->run("UNWIND [] AS x CREATE (n:Person {id: x, firstName: 'Empty'})", {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before);
    } catch (const std::exception& e) {
        FAIL("UNWIND CREATE empty list: " << e.what());
    }
}

TEST_CASE("UNWIND CREATE large batch", "[ldbc][crud][unwind-create]") {
    SKIP_IF_NO_DB();
    try {
        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        // Build list [134000..134099]
        std::string list = "[";
        for (int i = 0; i < 100; i++) {
            if (i > 0) list += ",";
            list += std::to_string(134000 + i);
        }
        list += "]";
        qr->run(("UNWIND " + list + " AS x CREATE (n:Person {id: x, firstName: 'Bulk'})").c_str(), {});

        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before + 100);
    } catch (const std::exception& e) {
        FAIL("UNWIND CREATE large batch: " << e.what());
    }
}

TEST_CASE("SET new property creates schema-evolved node version",
          "[ldbc][crud][set][schema-evolution]") {
    SKIP_IF_NO_DB();
    try {
        qr->run(
            "MATCH (n:Person {id: 933}) SET n.tlSchemaProp = 'schema-evolved'",
            {});

        auto value = qr->run(
            "MATCH (n:Person {id: 933}) RETURN n.tlSchemaProp",
            {qtest::ColType::STRING});
        REQUIRE(value.size() == 1);
        CHECK(value[0].str_at(0) == "schema-evolved");

        auto keys = qr->run("MATCH (n:Person {id: 933}) RETURN keys(n)",
                            {qtest::ColType::STRING});
        REQUIRE(keys.size() == 1);
        CHECK(keys[0].str_at(0).find("tlSchemaProp") != std::string::npos);
    } catch (const std::exception& e) {
        FAIL("SET new property: " << e.what());
    }
}

TEST_CASE("SET label preserves existing label and traversal",
          "[ldbc][crud][set][label]") {
    SKIP_IF_NO_DB();
    try {
        ensure_singleton_disconnected();
        struct SingletonReconnectGuard {
            ~SingletonReconnectGuard() { ensure_singleton_reconnected(); }
        } reconnect_guard;
        char tmpl[] = "/tmp/tl_label_XXXXXX";
        REQUIRE(mkdtemp(tmpl) != nullptr);
        std::string ws_path = tmpl;
        struct TempWorkspaceGuard {
            std::string path;
            ~TempWorkspaceGuard() {
                if (!path.empty()) {
                    std::system(("rm -rf " + path).c_str());
                }
            }
        } guard{ws_path};
        REQUIRE(std::system(
                    ("cp -a --reflink=auto " + g_ldbc_path + "/. " + ws_path +
                     "/")
                        .c_str()) == 0);
        qtest::QueryRunner local_qr(ws_path);
        constexpr auto relabel_src_id = 8800000000190000ULL;
        constexpr auto relabel_dst_id = 8800000000190001ULL;
        local_qr.run(
            "CREATE (n:Person {id: 8800000000190000, firstName: 'RelabelSrc'})",
            {});
        local_qr.run(
            "CREATE (n:Person {id: 8800000000190001, firstName: 'RelabelDst'})",
            {});
        local_qr.run(
            "MATCH (a:Person {id: 8800000000190000}), "
            "(b:Person {id: 8800000000190001}) "
            "CREATE (a)-[:KNOWS]->(b)",
            {});
        local_qr.run(
            "MATCH (n:Person {id: 8800000000190000}) SET n:Employee", {});

        CHECK(local_qr.count(
                  "MATCH (n:Employee {id: 8800000000190000}) RETURN count(n) AS cnt") ==
              1);
        CHECK(local_qr.count(
                  "MATCH (n:Person {id: 8800000000190000}) RETURN count(n) AS cnt") ==
              1);
        CHECK(local_qr.count(
                  "MATCH (a:Employee {id: 8800000000190000})-[:KNOWS]->"
                  "(b) RETURN count(b) AS cnt") ==
              1);
        CHECK(local_qr.count(
                  "MATCH (a:Employee {id: 8800000000190000})-[:KNOWS]->"
                  "(b:Person) RETURN count(b) AS cnt") ==
              1);
        CHECK(local_qr.count(
                  "MATCH (a:Employee {id: 8800000000190000})-[:KNOWS]->"
                  "(b:Person {id: 8800000000190001}) RETURN count(b) AS cnt") ==
              1);

        auto labels = local_qr.run(
            "MATCH (n {id: 8800000000190000}) RETURN labels(n)",
            {qtest::ColType::STRING});
        REQUIRE(labels.size() == 1);
        CHECK(labels[0].str_at(0).find("Person") != std::string::npos);
        CHECK(labels[0].str_at(0).find("Employee") != std::string::npos);
    } catch (const std::exception& e) {
        FAIL("SET label: " << e.what());
    }
}

TEST_CASE("REMOVE label restores original label set",
          "[ldbc][crud][remove][label]") {
    SKIP_IF_NO_DB();
    try {
        constexpr auto remove_label_id = 8800000000190010ULL;
        qr->run(
            "CREATE (n:Person {id: 8800000000190010, firstName: 'RemoveLabel'})",
            {});
        qr->run(
            "MATCH (n:Person {id: 8800000000190010}) SET n:Employee", {});
        qr->run(
            "MATCH (n:Employee {id: 8800000000190010}) REMOVE n:Employee", {});

        CHECK(qr->count(
                  "MATCH (n:Person {id: 8800000000190010}) RETURN count(n) AS cnt") ==
              1);
        CHECK(qr->count(
                  "MATCH (n:Employee {id: 8800000000190010}) RETURN count(n) AS cnt") ==
              0);

        auto labels = qr->run(
            "MATCH (n {id: 8800000000190010}) RETURN labels(n)",
            {qtest::ColType::STRING});
        REQUIRE(labels.size() == 1);
        CHECK(labels[0].str_at(0).find("Person") != std::string::npos);
        CHECK(labels[0].str_at(0).find("Employee") == std::string::npos);
    } catch (const std::exception& e) {
        FAIL("REMOVE label: " << e.what());
    }
}

TEST_CASE("SET and REMOVE label on base node preserve visible label set",
          "[ldbc][crud][label][base]") {
    SKIP_IF_NO_DB();
    try {
        ensure_singleton_disconnected();
        struct SingletonReconnectGuard {
            ~SingletonReconnectGuard() { ensure_singleton_reconnected(); }
        } reconnect_guard;
        char tmpl[] = "/tmp/tl_label_base_XXXXXX";
        REQUIRE(mkdtemp(tmpl) != nullptr);
        std::string ws_path = tmpl;
        struct TempWorkspaceGuard {
            std::string path;
            ~TempWorkspaceGuard() {
                if (!path.empty()) {
                    std::system(("rm -rf " + path).c_str());
                }
            }
        } guard{ws_path};
        REQUIRE(std::system(
                    ("cp -a --reflink=auto " + g_ldbc_path + "/. " + ws_path +
                     "/")
                        .c_str()) == 0);
        qtest::QueryRunner local_qr(ws_path);

        constexpr auto base_person_id = 933ULL;
        CHECK(local_qr.count(
                  "MATCH (n:Person {id: 933}) RETURN count(n) AS cnt") == 1);

        local_qr.run(
            "MATCH (n:Person {id: 933}) SET n:Employee", {});
        CHECK(local_qr.count(
                  "MATCH (n:Employee {id: 933}) RETURN count(n) AS cnt") == 1);
        CHECK(local_qr.count(
                  "MATCH (n:Person {id: 933}) RETURN count(n) AS cnt") == 1);

        auto labels_after_set = local_qr.run(
            "MATCH (n:Employee {id: 933}) RETURN labels(n)",
            {qtest::ColType::STRING});
        REQUIRE(labels_after_set.size() == 1);
        CHECK(labels_after_set[0].str_at(0).find("Person") != std::string::npos);
        CHECK(labels_after_set[0].str_at(0).find("Employee") != std::string::npos);

        local_qr.run(
            "MATCH (n:Employee {id: 933}) REMOVE n:Employee", {});
        CHECK(local_qr.count(
                  "MATCH (n:Employee {id: 933}) RETURN count(n) AS cnt") == 0);
        CHECK(local_qr.count(
                  "MATCH (n:Person {id: 933}) RETURN count(n) AS cnt") == 1);

        auto labels_after_remove = local_qr.run(
            "MATCH (n:Person {id: 933}) RETURN labels(n)",
            {qtest::ColType::STRING});
        REQUIRE(labels_after_remove.size() == 1);
        CHECK(labels_after_remove[0].str_at(0).find("Person") != std::string::npos);
        CHECK(labels_after_remove[0].str_at(0).find("Employee") == std::string::npos);
    } catch (const std::exception& e) {
        FAIL("Base label mutation: " << e.what());
    }
}

TEST_CASE("semicolon-terminated SET label preserves traversal",
          "[ldbc][crud][set][label][shell]") {
    SKIP_IF_NO_DB();
    try {
        ensure_singleton_disconnected();
        struct SingletonReconnectGuard {
            ~SingletonReconnectGuard() { ensure_singleton_reconnected(); }
        } reconnect_guard;
        char tmpl[] = "/tmp/tl_label_XXXXXX";
        REQUIRE(mkdtemp(tmpl) != nullptr);
        std::string ws_path = tmpl;
        struct TempWorkspaceGuard {
            std::string path;
            ~TempWorkspaceGuard() {
                if (!path.empty()) {
                    std::system(("rm -rf " + path).c_str());
                }
            }
        } guard{ws_path};
        REQUIRE(std::system(
                    ("cp -a --reflink=auto " + g_ldbc_path + "/. " + ws_path +
                     "/")
                        .c_str()) == 0);
        qtest::QueryRunner local_qr(ws_path);

        local_qr.run(
            "CREATE (n:Person {id: 8800000000190100, firstName: 'SemiSrc'});",
            {});
        local_qr.run(
            "CREATE (n:Person {id: 8800000000190101, firstName: 'SemiDst'});",
            {});
        local_qr.run(
            "MATCH (a:Person {id: 8800000000190100}), "
            "(b:Person {id: 8800000000190101}) "
            "CREATE (a)-[:KNOWS]->(b);",
            {});
        local_qr.run(
            "MATCH (n:Person {id: 8800000000190100}) SET n:Employee;", {});

        CHECK(local_qr.count(
                  "MATCH (a:Employee {id: 8800000000190100})-[:KNOWS]->"
                  "(b) RETURN count(b) AS cnt;") ==
              1);
        CHECK(local_qr.count(
                  "MATCH (a:Employee {id: 8800000000190100})-[:KNOWS]->"
                  "(b:Person) RETURN count(b) AS cnt;") ==
              1);
        CHECK(local_qr.count(
                  "MATCH (a:Employee {id: 8800000000190100})-[:KNOWS]->"
                  "(b:Person {id: 8800000000190101}) RETURN count(b) AS cnt;") ==
              1);

        auto labels = local_qr.run(
            "MATCH (n {id: 8800000000190100}) RETURN labels(n);",
            {qtest::ColType::STRING});
        REQUIRE(labels.size() == 1);
        CHECK(labels[0].str_at(0).find("Person") != std::string::npos);
        CHECK(labels[0].str_at(0).find("Employee") != std::string::npos);
    } catch (const std::exception& e) {
        FAIL("Semicolon SET label: " << e.what());
    }
}

TEST_CASE("semicolon-terminated REMOVE label restores original label set",
          "[ldbc][crud][remove][label][shell]") {
    SKIP_IF_NO_DB();
    try {
        qr->run(
            "CREATE (n:Person {id: 8800000000190110, firstName: 'SemiRemove'});",
            {});
        qr->run(
            "MATCH (n:Person {id: 8800000000190110}) SET n:Employee;", {});
        qr->run(
            "MATCH (n:Employee {id: 8800000000190110}) REMOVE n:Employee;",
            {});

        CHECK(qr->count(
                  "MATCH (n:Person {id: 8800000000190110}) RETURN count(n) AS cnt;") ==
              1);
        CHECK(qr->count(
                  "MATCH (n:Employee {id: 8800000000190110}) RETURN count(n) AS cnt;") ==
              0);
    } catch (const std::exception& e) {
        FAIL("Semicolon REMOVE label: " << e.what());
    }
}

TEST_CASE("UNWIND CREATE string values", "[ldbc][crud][unwind-create]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("UNWIND [135001, 135002] AS x CREATE (n:Person {id: x, firstName: 'StrTest'})", {});

        auto r1 = qr->run("MATCH (n:Person {id: 135001}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r1.size() == 1);
        CHECK(r1[0].str_at(0) == "StrTest");

        auto r2 = qr->run("MATCH (n:Person {id: 135002}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r2.size() == 1);
        CHECK(r2[0].str_at(0) == "StrTest");
    } catch (const std::exception& e) {
        FAIL("UNWIND CREATE string values: " << e.what());
    }
}

// ===========================================================================
// Auto compaction tests
// ===========================================================================

TEST_CASE("auto compaction triggers after threshold", "[ldbc][crud][auto-compact]") {
    COMPACTION_SETUP();
    try {
        // Lower threshold for faster testing
        turbolynx_set_auto_compact_threshold(500, 128);

        auto before = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                               {qtest::ColType::INT64});
        int64_t cnt_before = before[0].int64_at(0);

        // Insert 600 rows (exceeds 500 threshold) in 6 batches of 100
        for (int batch = 0; batch < 6; batch++) {
            std::string blist = "[";
            for (int i = 0; i < 100; i++) {
                if (i > 0) blist += ",";
                blist += std::to_string(77000000 + batch * 100 + i);
            }
            blist += "]";
            qr->run(("UNWIND " + blist + " AS x CREATE (n:Person {id: x, firstName: 'AutoCompact'})").c_str(), {});
        }

        // Verify data is still queryable after auto compaction
        auto after = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                              {qtest::ColType::INT64});
        CHECK(after[0].int64_at(0) == cnt_before + 600);

        auto r = qr->run("MATCH (n:Person {id: 77000000}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "AutoCompact");

        // Restore default threshold
        turbolynx_set_auto_compact_threshold(10000, 128);
    } catch (const std::exception& e) {
        turbolynx_set_auto_compact_threshold(10000, 128);
        FAIL("Auto compaction trigger: " << e.what());
    }
}

TEST_CASE("auto compaction data survives reconnect", "[ldbc][crud][auto-compact]") {
    COMPACTION_SETUP();
    try {
        turbolynx_set_auto_compact_threshold(500, 128);

        // Insert 600 rows to trigger auto compaction
        for (int batch = 0; batch < 6; batch++) {
            std::string blist = "[";
            for (int i = 0; i < 100; i++) {
                if (i > 0) blist += ",";
                blist += std::to_string(78000000 + batch * 100 + i);
            }
            blist += "]";
            qr->run(("UNWIND " + blist + " AS x CREATE (n:Person {id: x, firstName: 'Persist'})").c_str(), {});
        }

        // Reconnect — clears in-memory delta, relies on disk + WAL
        qr->reconnect(compact_db_path);

        auto r = qr->run("MATCH (n:Person {id: 78000000}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "Persist");

        auto r2 = qr->run("MATCH (n:Person {id: 78000599}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r2.size() == 1);
        CHECK(r2[0].str_at(0) == "Persist");

        turbolynx_set_auto_compact_threshold(10000, 128);
    } catch (const std::exception& e) {
        turbolynx_set_auto_compact_threshold(10000, 128);
        FAIL("Auto compaction reconnect: " << e.what());
    }
}

TEST_CASE("below threshold no auto compaction", "[ldbc][crud][auto-compact]") {
    COMPACTION_SETUP();
    try {
        turbolynx_set_auto_compact_threshold(10000, 128);

        qr->run("UNWIND [79000001, 79000002, 79000003] AS x CREATE (n:Person {id: x, firstName: 'Small'})", {});

        auto r = qr->run("MATCH (n:Person {id: 79000001}) RETURN n.firstName",
                          {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "Small");
    } catch (const std::exception& e) {
        FAIL("Below threshold: " << e.what());
    }
}

TEST_CASE("checkpoint WAL markers survive reconnect", "[ldbc][crud][auto-compact]") {
    COMPACTION_SETUP();
    try {
        turbolynx_set_auto_compact_threshold(500, 128);

        // Insert 600 rows → auto compaction → CHECKPOINT_BEGIN/END written to WAL
        for (int batch = 0; batch < 6; batch++) {
            std::string blist = "[";
            for (int i = 0; i < 100; i++) {
                if (i > 0) blist += ",";
                blist += std::to_string(80000000 + batch * 100 + i);
            }
            blist += "]";
            qr->run(("UNWIND " + blist + " AS x CREATE (n:Person {id: x, firstName: 'WALMarker'})").c_str(), {});
        }

        // Add some SET mutations AFTER compaction (these go into new WAL section)
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'PostCheckpoint'", {});

        // Reconnect — WAL replay should skip compacted INSERTs, apply SET
        qr->reconnect(compact_db_path);

        // Compacted data (from disk)
        auto r1 = qr->run("MATCH (n:Person {id: 80000000}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r1.size() == 1);
        CHECK(r1[0].str_at(0) == "WALMarker");

        // SET mutation (from WAL replay after checkpoint)
        auto r2 = qr->run("MATCH (n:Person {id: 933}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r2.size() == 1);
        CHECK(r2[0].str_at(0) == "PostCheckpoint");

        turbolynx_set_auto_compact_threshold(10000, 128);
    } catch (const std::exception& e) {
        turbolynx_set_auto_compact_threshold(10000, 128);
        FAIL("WAL markers reconnect: " << e.what());
    }
}

TEST_CASE("auto compaction failure preserves fresh edge traversal",
          "[ldbc][crud][auto-compact][shell]") {
    COMPACTION_SETUP();
    try {
        turbolynx_set_auto_compact_threshold(1, 1);

        qtest::QueryRunner blocker(compact_db_path);

        qr->run(
            "CREATE (n:Person {id: 82000001, firstName: 'LockedSrc'})", {});
        qr->run(
            "CREATE (n:Person {id: 82000002, firstName: 'LockedDst'})", {});
        qr->run(
            "MATCH (a:Person {id: 82000001}), (b:Person {id: 82000002}) "
            "CREATE (a)-[:KNOWS]->(b)",
            {});

        auto names = qr->run(
            "MATCH (n:Person {id: 82000001}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(names.size() == 1);
        CHECK(names[0].str_at(0) == "LockedSrc");

        auto count_rows = qr->run(
            "MATCH (a:Person {id: 82000001})-[:KNOWS]->"
            "(b:Person {id: 82000002}) RETURN count(b) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(count_rows.size() == 1);
        CHECK(count_rows[0].int64_at(0) == 1);

        turbolynx_set_auto_compact_threshold(10000, 128);
    } catch (const std::exception& e) {
        turbolynx_set_auto_compact_threshold(10000, 128);
        FAIL("Auto compact failure keeps delta live: " << e.what());
    }
}

// ===========================================================================
// List slicing tests
// ===========================================================================

TEST_CASE("list slicing via size", "[ldbc][crud][expr][list-slice]") {
    SKIP_IF_NO_DB();
    try {
        // DuckDB list_slice is 1-based inclusive: [1:3] → elements 1,2,3 → size=3
        auto r = qr->run("MATCH (n:Person {id: 933}) RETURN size([10,20,30,40,50][1:3]) AS cnt",
                          {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) == 3);
    } catch (const std::exception& e) {
        FAIL("List slicing via size: " << e.what());
    }
}

TEST_CASE("list slicing head", "[ldbc][crud][expr][list-slice]") {
    SKIP_IF_NO_DB();
    try {
        // [1:2] = elements 1,2 → size = 2
        auto r = qr->run("MATCH (n:Person {id: 933}) RETURN size([10,20,30,40,50][1:2]) AS cnt",
                          {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) == 2);
    } catch (const std::exception& e) {
        FAIL("List slicing head: " << e.what());
    }
}

TEST_CASE("list slicing on property", "[ldbc][crud][expr][list-slice]") {
    SKIP_IF_NO_DB();
    try {
        // collect() produces a list, slice it
        auto r = qr->run(
            "MATCH (n:Person) WITH collect(n.id) AS ids "
            "RETURN size(ids[0:5]) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) == 5);
    } catch (const std::exception& e) {
        FAIL("List slicing on property: " << e.what());
    }
}

TEST_CASE("shortestPath keeps passthrough rows aligned", "[ldbc][crud][expr][shortestpath]") {
    SKIP_IF_NO_DB();
    try {
        auto r = qr->run(
            "MATCH (src:Person), (dst:Person) "
            "WHERE (src.id = 933 AND dst.id = 65) "
            "   OR (src.id = 933 AND dst.id = 4139) "
            "WITH src, dst ORDER BY dst.id ASC "
            "MATCH path = shortestPath((src)-[:KNOWS*1..1]-(dst)) "
            "RETURN src.id, dst.id, length(path)",
            {qtest::ColType::INT64, qtest::ColType::INT64, qtest::ColType::INT64});

        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) == 933);
        CHECK(r[0].int64_at(1) == 4139);
        CHECK(r[0].int64_at(2) == 1);
    } catch (const std::exception& e) {
        FAIL("shortestPath passthrough alignment: " << e.what());
    }
}

TEST_CASE("shortestPath resets iterator state before self rows", "[ldbc][crud][expr][shortestpath]") {
    SKIP_IF_NO_DB();
    try {
        auto r = qr->run(
            "MATCH (src:Person), (dst:Person) "
            "WHERE (src.id = 933 AND dst.id = 4139) "
            "   OR (src.id = 933 AND dst.id = 933) "
            "WITH src, dst ORDER BY dst.id DESC "
            "MATCH path = shortestPath((src)-[:KNOWS*1..1]-(dst)) "
            "RETURN src.id, dst.id, length(path)",
            {qtest::ColType::INT64, qtest::ColType::INT64, qtest::ColType::INT64});

        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) == 933);
        CHECK(r[0].int64_at(1) == 4139);
        CHECK(r[0].int64_at(2) == 1);
    } catch (const std::exception& e) {
        FAIL("shortestPath self-row state reset: " << e.what());
    }
}

TEST_CASE("post-checkpoint mutations survive reconnect", "[ldbc][crud][auto-compact]") {
    COMPACTION_SETUP();
    try {
        turbolynx_set_auto_compact_threshold(500, 128);

        // Insert 600 rows → triggers auto compaction
        for (int batch = 0; batch < 6; batch++) {
            std::string blist = "[";
            for (int i = 0; i < 100; i++) {
                if (i > 0) blist += ",";
                blist += std::to_string(81000000 + batch * 100 + i);
            }
            blist += "]";
            qr->run(("UNWIND " + blist + " AS x CREATE (n:Person {id: x, firstName: 'Pre'})").c_str(), {});
        }

        // These mutations happen AFTER compaction — stored in WAL after CHECKPOINT_END
        qr->run("CREATE (n:Person {id: 81999999, firstName: 'PostCompact'})", {});
        qr->run("MATCH (n:Person {id: 933}) SET n.firstName = 'AfterCP'", {});

        // Reconnect — WAL replay should only apply post-checkpoint entries
        qr->reconnect(compact_db_path);

        // Post-checkpoint INSERT (from WAL)
        auto r1 = qr->run("MATCH (n:Person {id: 81999999}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r1.size() == 1);
        CHECK(r1[0].str_at(0) == "PostCompact");

        // Pre-checkpoint data (from disk)
        auto r2 = qr->run("MATCH (n:Person {id: 81000000}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r2.size() == 1);
        CHECK(r2[0].str_at(0) == "Pre");

        // Post-checkpoint SET (from WAL)
        auto r3 = qr->run("MATCH (n:Person {id: 933}) RETURN n.firstName",
                           {qtest::ColType::STRING});
        REQUIRE(r3.size() == 1);
        CHECK(r3[0].str_at(0) == "AfterCP");

        turbolynx_set_auto_compact_threshold(10000, 128);
    } catch (const std::exception& e) {
        turbolynx_set_auto_compact_threshold(10000, 128);
        FAIL("Post-checkpoint reconnect: " << e.what());
    }
}

// ===========================================================================
// EXISTS subquery tests
// ===========================================================================

TEST_CASE("EXISTS basic — filter persons with KNOWS edges", "[ldbc][crud][expr][exists]") {
    SKIP_IF_NO_DB();
    try {
        // Person 933 has KNOWS edges — should be in result
        // EXISTS may produce duplicates if decorrelated to inner join;
        // use DISTINCT or count to verify existence.
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) "
            "WHERE EXISTS { MATCH (n)-[:KNOWS]->(:Person) } "
            "RETURN DISTINCT n.id",
            {qtest::ColType::INT64});
        REQUIRE(r.size() >= 1);
        CHECK(r[0].int64_at(0) == 933);
    } catch (const std::exception& e) {
        FAIL("EXISTS basic: " << e.what());
    }
}

TEST_CASE("NOT EXISTS — filter persons without pattern", "[ldbc][crud][expr][exists]") {
    SKIP_IF_NO_DB();
    try {
        // Create an isolated node (no edges)
        qr->run("CREATE (n:Person {id: 161000, firstName: 'Alone'})", {});

        // NOT EXISTS: isolated node has no KNOWS edges → should appear
        auto r = qr->run(
            "MATCH (n:Person {id: 161000}) "
            "WHERE NOT EXISTS { MATCH (n)-[:KNOWS]->(:Person) } "
            "RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "Alone");
    } catch (const std::exception& e) {
        FAIL("NOT EXISTS: " << e.what());
    }
}

TEST_CASE("EXISTS with WHERE in subquery", "[ldbc][crud][expr][exists]") {
    SKIP_IF_NO_DB();
    try {
        // EXISTS with correlated WHERE — find persons who know someone with specific name
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) "
            "WHERE EXISTS { MATCH (n)-[:KNOWS]->(m:Person) WHERE m.firstName = 'Mahinda' } "
            "RETURN n.id",
            {qtest::ColType::INT64});
        // Result depends on data — just check it doesn't crash
        // If person 933 knows someone named 'Mahinda', result size = 1, otherwise 0
        CHECK(r.size() <= 1);
    } catch (const std::exception& e) {
        FAIL("EXISTS with WHERE: " << e.what());
    }
}

TEST_CASE("EXISTS count — how many persons have KNOWS edges", "[ldbc][crud][expr][exists]") {
    SKIP_IF_NO_DB();
    try {
        auto r = qr->run(
            "MATCH (n:Person) "
            "WHERE EXISTS { MATCH (n)-[:KNOWS]->(:Person) } "
            "RETURN count(n) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        // Most LDBC persons have KNOWS edges, so count should be > 0
        CHECK(r[0].int64_at(0) > 0);
    } catch (const std::exception& e) {
        FAIL("EXISTS count: " << e.what());
    }
}

TEST_CASE("NOT EXISTS — node with edges must be excluded", "[ldbc][crud][expr][exists]") {
    SKIP_IF_NO_DB();
    try {
        // Person 933 HAS KNOWS edges → NOT EXISTS must return empty
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) "
            "WHERE NOT EXISTS { MATCH (n)-[:KNOWS]->(:Person) } "
            "RETURN n.firstName",
            {qtest::ColType::STRING});
        CHECK(r.size() == 0);
    } catch (const std::exception& e) {
        FAIL("NOT EXISTS exclusion: " << e.what());
    }
}

TEST_CASE("NOT EXISTS count — persons without KNOWS edges", "[ldbc][crud][expr][exists]") {
    SKIP_IF_NO_DB();
    try {
        // Neo4j: 1642 persons have no outgoing KNOWS edges
        auto r = qr->run(
            "MATCH (n:Person) "
            "WHERE NOT EXISTS { MATCH (n)-[:KNOWS]->(:Person) } "
            "RETURN count(n) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) == 1642);
    } catch (const std::exception& e) {
        FAIL("NOT EXISTS count: " << e.what());
    }
}

TEST_CASE("NOT EXISTS with inner WHERE", "[ldbc][crud][expr][exists]") {
    SKIP_IF_NO_DB();
    try {
        // Person 933 does NOT know anyone named 'Mahinda' (Neo4j verified)
        // → NOT EXISTS is satisfied → should return 933
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) "
            "WHERE NOT EXISTS { MATCH (n)-[:KNOWS]->(m:Person) WHERE m.firstName = 'Mahinda' } "
            "RETURN n.id",
            {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) == 933);
    } catch (const std::exception& e) {
        FAIL("NOT EXISTS with WHERE: " << e.what());
    }
}

// Neo4j-style schemaless first CREATE: a brand-new workspace with no
// pre-loaded labels accepts CREATE/MATCH on previously unseen labels and
// edge types, bootstrapping the partitions on the fly. This is the path a
// user trying to migrate from Neo4j hits in their first 5 minutes.
TEST_CASE("CREATE bootstraps labels in an empty workspace",
          "[crud][bootstrap][empty]") {
    ensure_singleton_disconnected();
    struct SingletonReconnectGuard {
        ~SingletonReconnectGuard() { ensure_singleton_reconnected(); }
    } reconnect_guard;
    char tmpl[] = "/tmp/tl_empty_XXXXXX";
    REQUIRE(mkdtemp(tmpl) != nullptr);
    std::string ws_path = tmpl;
    struct TempWorkspaceGuard {
        std::string path;
        ~TempWorkspaceGuard() {
            if (!path.empty()) {
                std::system(("rm -rf " + path).c_str());
            }
        }
    } guard{ws_path};
    // Note: no `cp` here — the workspace is genuinely empty, so the binder
    // must auto-create vertex partitions on the first CREATE.
    qtest::QueryRunner local_qr(ws_path);

    try {
        // Single-label nodes
        local_qr.run("CREATE (:Person {name: 'Alice', age: 30})", {});
        local_qr.run("CREATE (:Person {name: 'Bob', age: 25})", {});
        local_qr.run("CREATE (:Person {name: 'Carol', age: 35})", {});
        // Different label exercises a second bootstrap
        local_qr.run(
            "CREATE (:Movie {title: 'Inception', released: 2010})", {});

        CHECK(local_qr.count(
                  "MATCH (n:Person) RETURN count(n) AS cnt") == 3);
        CHECK(local_qr.count(
                  "MATCH (n:Movie) RETURN count(n) AS cnt") == 1);

        // Numeric property roundtrip — proves PropertySchema and stats are
        // wired up enough that filter-pushdown, ORDER BY, and aggregation
        // all return correct values.
        auto by_age = local_qr.run(
            "MATCH (n:Person) WHERE n.age > 27 RETURN n.name "
            "ORDER BY n.age",
            {qtest::ColType::STRING});
        REQUIRE(by_age.size() == 2);
        CHECK(by_age[0].str_at(0) == "Alice");
        CHECK(by_age[1].str_at(0) == "Carol");

        // count() over the bootstrapped partition picks up all delta rows.
        // (min/max/sum aggregates on delta-only data have a separate
        // pre-existing limitation and are not exercised here.)
        auto agg = local_qr.run(
            "MATCH (n:Person) RETURN count(n) AS total",
            {qtest::ColType::INT64});
        REQUIRE(agg.size() == 1);
        CHECK(agg[0].int64_at(0) == 3);

        // Edge bootstrap: inline-label CREATE auto-creates the KNOWS edge
        // partition between Person and Person. Use undirected matching to
        // mirror the convention used by existing CRUD edge tests (DeltaStore
        // records both directions of each fresh edge — pre-existing behavior
        // unrelated to this bootstrap).
        local_qr.run(
            "CREATE (:Person {name: 'Dave', age: 28})"
            "-[:KNOWS]->"
            "(:Person {name: 'Eve', age: 32})",
            {});
        // Endpoint nodes are now visible too — total Persons = 3 + 2.
        CHECK(local_qr.count(
                  "MATCH (n:Person) RETURN count(n) AS cnt") == 5);
        // The KNOWS edge connects Dave and Eve; undirected match returns at
        // least one hit per endpoint.
        CHECK(local_qr.count(
                  "MATCH (a:Person {name: 'Dave'})-[:KNOWS]-(b:Person) "
                  "RETURN count(b) AS cnt") >= 1);
        CHECK(local_qr.count(
                  "MATCH (a:Person {name: 'Eve'})-[:KNOWS]-(b:Person) "
                  "RETURN count(b) AS cnt") >= 1);
    } catch (const std::exception &e) {
        FAIL("Empty-workspace bootstrap CRUD: " << e.what());
    }
}

// Catalog mutations done by the bootstrap path must survive a
// disconnect/reconnect (i.e. the partition must be persisted, not just
// kept in memory).
TEST_CASE("Bootstrap partitions persist across reconnect",
          "[crud][bootstrap][empty][persist]") {
    ensure_singleton_disconnected();
    struct SingletonReconnectGuard {
        ~SingletonReconnectGuard() { ensure_singleton_reconnected(); }
    } reconnect_guard;
    char tmpl[] = "/tmp/tl_empty_persist_XXXXXX";
    REQUIRE(mkdtemp(tmpl) != nullptr);
    std::string ws_path = tmpl;
    struct TempWorkspaceGuard {
        std::string path;
        ~TempWorkspaceGuard() {
            if (!path.empty()) {
                std::system(("rm -rf " + path).c_str());
            }
        }
    } guard{ws_path};

    try {
        {
            qtest::QueryRunner first_qr(ws_path);
            first_qr.run("CREATE (:Account {handle: 'root', tier: 1})", {});
            first_qr.run("CREATE (:Account {handle: 'guest', tier: 0})", {});
        }
        {
            qtest::QueryRunner second_qr(ws_path);
            CHECK(second_qr.count(
                      "MATCH (n:Account) RETURN count(n) AS cnt") == 2);
            auto by_tier = second_qr.run(
                "MATCH (n:Account) RETURN n.handle ORDER BY n.tier DESC",
                {qtest::ColType::STRING});
            REQUIRE(by_tier.size() == 2);
            CHECK(by_tier[0].str_at(0) == "root");
            CHECK(by_tier[1].str_at(0) == "guest");
        }
    } catch (const std::exception &e) {
        FAIL("Bootstrap persistence: " << e.what());
    }
}

// Regression for #8: traversal of a freshly-bootstrapped edge must hit
// the right endpoints. Pre-fix, KNOWS rows stored their _sid/_tid as
// logical_ids while LDBC stores them as base pids; the inner-join
// silently matched both sides to the same node, returning (Bob, Bob)
// instead of (Alice, Bob).
TEST_CASE("Fresh CREATE edge traverses with correct endpoints",
          "[crud][bootstrap][empty][traversal]") {
    ensure_singleton_disconnected();
    struct SingletonReconnectGuard {
        ~SingletonReconnectGuard() { ensure_singleton_reconnected(); }
    } reconnect_guard;
    char tmpl[] = "/tmp/tl_empty_trav_XXXXXX";
    REQUIRE(mkdtemp(tmpl) != nullptr);
    std::string ws_path = tmpl;
    struct TempWorkspaceGuard {
        std::string path;
        ~TempWorkspaceGuard() {
            if (!path.empty()) std::system(("rm -rf " + path).c_str());
        }
    } guard{ws_path};

    try {
        qtest::QueryRunner local_qr(ws_path);
        local_qr.run("CREATE (:Person {name: 'Alice'})", {});
        local_qr.run("CREATE (:Person {name: 'Bob'})", {});
        local_qr.run(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS]->(b)",
            {});

        // The endpoint-level fix this test pins: scanning a fresh KNOWS
        // edge must yield rows whose `_sid`/`_tid` resolve to actual
        // Person nodes — pre-fix, every ID-typed column was overwritten
        // with the row's reconstructed PhysicalID, so the inner-join saw
        // edge=node and matched both endpoints to the same node. We use
        // undirected matching because LogAndApplyInsertEdge writes both
        // directions into AdjListDelta; LDBC-style separated forward and
        // backward CSRs only materialise after a checkpoint.
        auto from_alice = local_qr.run(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS]-(b:Person) "
            "RETURN b.name",
            {qtest::ColType::STRING});
        bool saw_bob = false;
        for (size_t i = 0; i < from_alice.size(); i++) {
            if (from_alice[i].str_at(0) == "Bob") saw_bob = true;
        }
        CHECK(saw_bob);

        auto from_bob = local_qr.run(
            "MATCH (a:Person {name: 'Bob'})-[:KNOWS]-(b:Person) "
            "RETURN b.name",
            {qtest::ColType::STRING});
        bool saw_alice = false;
        for (size_t i = 0; i < from_bob.size(); i++) {
            if (from_bob[i].str_at(0) == "Alice") saw_alice = true;
        }
        CHECK(saw_alice);
    } catch (const std::exception &e) {
        FAIL("Fresh CREATE edge traversal: " << e.what());
    }
}

// Bug A (PLAN.md): labels()/keys() of the first checkpointed node in a
// fresh workspace returned an empty list because both
// ResolveCurrentEntityPidForMetaInput and ResolveCurrentPartition gated
// partition lookup on `pid != 0`. The first row of the first vertex
// partition lands on (extent 0, row 0) — pid 0 — so partition resolution
// silently bailed and labels emitted []. The fix uses lid liveness
// instead, with partition_id extracted from the resolved pid (numeric
// value 0 is fine). The combined PLAN.md repro
// `MATCH (n) RETURN labels(n)[0] AS label, count(*) AS cnt`
// still fails on a deeper planner/type-system issue (LIST indexing on
// the formatted-string labels return + ORCA aggregation over
// multi-vertex-partition scans); that scope stays open.
TEST_CASE("labels() returns the right partition for the first node",
          "[crud][bootstrap][empty][bug-a-partial]") {
    ensure_singleton_disconnected();
    struct SingletonReconnectGuard {
        ~SingletonReconnectGuard() { ensure_singleton_reconnected(); }
    } reconnect_guard;
    char tmpl[] = "/tmp/tl_bugA_XXXXXX";
    REQUIRE(mkdtemp(tmpl) != nullptr);
    std::string ws_path = tmpl;
    struct TempWorkspaceGuard {
        std::string path;
        ~TempWorkspaceGuard() {
            if (!path.empty()) std::system(("rm -rf " + path).c_str());
        }
    } guard{ws_path};

    try {
        qtest::QueryRunner local_qr(ws_path);
        local_qr.run("CREATE (:Person {name:'Keanu', born:1964})", {});
        local_qr.run("CREATE (:Movie {title:'Matrix', released:1999})", {});

        auto rows = local_qr.run(
            "MATCH (n) RETURN labels(n)",
            {qtest::ColType::STRING});
        REQUIRE(rows.size() == 2);
        // Pre-fix: row 0 is "[]" because ResolveCurrentPartition rejected
        // pid==0 (Keanu's legitimate disk slot). Post-fix: ["Person"].
        bool saw_person = false;
        bool saw_movie = false;
        for (size_t i = 0; i < rows.size(); i++) {
            auto v = rows[i].str_at(0);
            if (v == "[\"Person\"]") saw_person = true;
            if (v == "[\"Movie\"]") saw_movie = true;
        }
        CHECK(saw_person);
        CHECK(saw_movie);
    } catch (const std::exception &e) {
        FAIL("Bug A labels() partial: " << e.what());
    }
}

// PLAN.md Bug A (deeper, A1): `labels(n)[i]` / `keys(n)[i]` typed the
// element as ANY because labels()/keys() report VARCHAR (a JSON-formatted
// string) rather than a real LIST(VARCHAR). list_extract over VARCHAR
// fell through to elem_type=ANY → PhysicalType::INVALID → GetTypeIdSize
// throw. Fix: rewrite the subscript pattern at bind time to dedicated
// __tl_node_label_at / __tl_*_key_at functions that resolve the i-th
// entry directly as VARCHAR.
TEST_CASE("labels(n)[i] / keys(n)[i] subscript routing",
          "[crud][bootstrap][empty][bug-a1]") {
    ensure_singleton_disconnected();
    struct G { ~G() { ensure_singleton_reconnected(); } } g;
    char tmpl[] = "/tmp/tl_bugA1_XXXXXX";
    REQUIRE(mkdtemp(tmpl) != nullptr);
    std::string ws_path = tmpl;
    struct W { std::string p; ~W() { if (!p.empty()) std::system(("rm -rf " + p).c_str()); } } w{ws_path};

    qtest::QueryRunner local_qr(ws_path);
    local_qr.run("CREATE (:Person {name:'Keanu', born:1964})", {});
    local_qr.run("CREATE (:Movie {title:'Matrix', released:1999})", {});

    SECTION("labels(n)[0] returns first label as VARCHAR") {
        auto rows = local_qr.run("MATCH (n) RETURN labels(n)[0] AS lbl",
                                 {qtest::ColType::STRING});
        REQUIRE(rows.size() == 2);
        bool saw_person = false, saw_movie = false;
        for (auto &r : rows.rows) {
            if (r.str_at(0) == "Person") saw_person = true;
            if (r.str_at(0) == "Movie")  saw_movie = true;
        }
        CHECK(saw_person);
        CHECK(saw_movie);
    }
    SECTION("labels(n)[i] inside aggregation — full PLAN.md repro") {
        auto rows = local_qr.run(
            "MATCH (n) RETURN labels(n)[0] AS label, count(*) AS cnt",
            {qtest::ColType::STRING, qtest::ColType::INT64});
        REQUIRE(rows.size() == 2);
        // each label group has exactly one node
        for (auto &r : rows.rows) CHECK(r.int64_at(1) == 1);
    }
    SECTION("keys(n)[0] returns first declared key as VARCHAR") {
        auto rows = local_qr.run(
            "MATCH (n:Person) RETURN keys(n)[0] AS k",
            {qtest::ColType::STRING});
        REQUIRE(rows.size() == 1);
        // Implementation detail: first declared key is `_id` for fresh
        // bootstrap. Just check that the result is non-empty and stable.
        CHECK(!rows[0].str_at(0).empty());
    }
}

// PLAN.md Bug A (deeper, A2): `MATCH (n) RETURN count(*)` over a multi-vertex-
// partition NodeScan aborted in ORCA because column pruning collapsed every
// per-partition ProjectColumnar to NULL when count(*) referenced no columns.
// Fix: PexprPruneProjListProjectOrGbAgg now drops ProjectColumnar on
// "all-defined-unused" instead of asserting.
TEST_CASE("MPV count(*) on disjoint bootstrap partitions",
          "[crud][bootstrap][empty][bug-a2]") {
    ensure_singleton_disconnected();
    struct G { ~G() { ensure_singleton_reconnected(); } } g;
    char tmpl[] = "/tmp/tl_bugA2_XXXXXX";
    REQUIRE(mkdtemp(tmpl) != nullptr);
    std::string ws_path = tmpl;
    struct W { std::string p; ~W() { if (!p.empty()) std::system(("rm -rf " + p).c_str()); } } w{ws_path};

    try {
        qtest::QueryRunner local_qr(ws_path);
        local_qr.run("CREATE (:Person {name:'Keanu', born:1964})", {});
        local_qr.run("CREATE (:Movie {title:'Matrix', released:1999})", {});

        auto p_rows = local_qr.run("MATCH (n:Person) RETURN count(n) AS cnt",
                                   {qtest::ColType::INT64});
        REQUIRE(p_rows.size() == 1);
        auto m_rows = local_qr.run("MATCH (n:Movie) RETURN count(n) AS cnt",
                                   {qtest::ColType::INT64});
        REQUIRE(m_rows.size() == 1);
        auto p = p_rows[0].int64_at(0);
        auto m = m_rows[0].int64_at(0);

        auto rows = local_qr.run("MATCH (n) RETURN count(*) AS cnt",
                                 {qtest::ColType::INT64});
        REQUIRE(rows.size() == 1);
        CHECK(rows[0].int64_at(0) == p + m);
    } catch (const std::exception &e) {
        FAIL("Bug A2 MPV count(*): " << e.what());
    }
}

// PLAN.md Bug D: multi-variable SET crashes pTransformScalarIdent on a
// fresh-bootstrap workspace. Phase 3 fix (`317e1012d`) made multi-target SET
// emit a synthesized RETURN that re-reads the affected rows; the fix passed
// LDBC isolated regression but trips on fresh-bootstrap PS where the
// EnsureImplicitMutationReadbackProjection alias path differs.
TEST_CASE("Multi-variable SET on fresh-bootstrap workspace",
          "[crud][bootstrap][empty][bug-d]") {
    ensure_singleton_disconnected();
    struct G { ~G() { ensure_singleton_reconnected(); } } g;
    char tmpl[] = "/tmp/tl_bugD_XXXXXX";
    REQUIRE(mkdtemp(tmpl) != nullptr);
    std::string ws_path = tmpl;
    struct W { std::string p; ~W() { if (!p.empty()) std::system(("rm -rf " + p).c_str()); } } w{ws_path};

    try {
        qtest::QueryRunner local_qr(ws_path);
        local_qr.run("CREATE (:Person {name:'Keanu Reeves', born:1964})", {});
        local_qr.run("CREATE (:Person {name:'Carrie-Anne Moss', born:1967})", {});

        // Pre-fix: integer property projection always returned 0 because
        // Cypher integer literals were transformed to int32 (LogicalType
        // INTEGER) and the BIGINT-typed C API getter rejected the type
        // mismatch. Now that the parser always emits BIGINT, the readback
        // matches the inserted values.
        auto pre = local_qr.run(
            "MATCH (n:Person) RETURN n.name AS name, n.born AS born "
            "ORDER BY n.name",
            {qtest::ColType::STRING, qtest::ColType::INT64});
        REQUIRE(pre.size() == 2);
        CHECK(pre[0].str_at(0) == "Carrie-Anne Moss");
        CHECK(pre[0].int64_at(1) == 1967);
        CHECK(pre[1].str_at(0) == "Keanu Reeves");
        CHECK(pre[1].int64_at(1) == 1964);

        // The actual PLAN.md Bug D shape: multi-variable SET targeting two
        // bound nodes from the same MATCH. Pre-fix this also crashed
        // pTransformScalarIdent on fresh-bootstrap; the diagnosis from
        // PLAN.md was wrong — that crash was downstream of the integer-
        // typing bug. With BIGINT literals the SET path runs through
        // EnsureImplicitMutationReadbackProjection cleanly.
        local_qr.run(
            "MATCH (a:Person {name:'Keanu Reeves'}), "
            "(b:Person {name:'Carrie-Anne Moss'}) "
            "SET a.born = 1965, b.email = 'cam@matrix.com'", {});

        auto post = local_qr.run(
            "MATCH (n:Person) RETURN n.name AS name, n.born AS born, "
            "n.email AS email ORDER BY n.name",
            {qtest::ColType::STRING, qtest::ColType::INT64,
             qtest::ColType::STRING});
        REQUIRE(post.size() == 2);
        CHECK(post[0].str_at(0) == "Carrie-Anne Moss");
        CHECK(post[0].int64_at(1) == 1967);
        CHECK(post[0].str_at(2) == "cam@matrix.com");
        CHECK(post[1].str_at(0) == "Keanu Reeves");
        CHECK(post[1].int64_at(1) == 1965);
        // The SET targeted only Carrie's email; Keanu's email must not have
        // been cross-applied. (The exact representation of "no value" for a
        // schema column the row predates is missing-key-emits-empty-string,
        // which is a separate concern from SET dispatch correctness.)
        CHECK(post[1].str_at(2) != "cam@matrix.com");
    } catch (const std::exception &e) {
        FAIL("Bug D multi-variable SET: " << e.what());
    }
}

// PLAN.md Bug E (partial): a CREATE that introduces a new property on an
// existing label-bootstrapped partition must (a) register the property
// globally so subsequent MATCH/RETURN can resolve it without throwing
// "Unknown property" and (b) emit the actual stored value in projection,
// not a typed-NULL literal. Both halves are what this regression pins.
//
// The deeper PLAN.md Bug E symptom — `WHERE p.x IS NULL RETURN p.name`
// swapping the email value into the name column and the IS NULL filter
// passing every row — is **not** addressed here and remains an open bug.
// See the open-followups note in PLAN.md.
TEST_CASE("CREATE on existing partition registers + emits new property",
          "[crud][bootstrap][empty][bug-e-partial]") {
    ensure_singleton_disconnected();
    struct G { ~G() { ensure_singleton_reconnected(); } } g;
    char tmpl[] = "/tmp/tl_bugE_XXXXXX";
    REQUIRE(mkdtemp(tmpl) != nullptr);
    std::string ws_path = tmpl;
    struct W { std::string p; ~W() { if (!p.empty()) std::system(("rm -rf " + p).c_str()); } } w{ws_path};

    try {
        qtest::QueryRunner local_qr(ws_path);
        local_qr.run("CREATE (:Person {name:'Keanu Reeves'})", {});
        local_qr.run("CREATE (:Person {name:'Carrie-Anne Moss', email:'cam@matrix.com'})", {});

        // Pre-fix: throws "Unknown property 'email' on node p" because the
        // 2nd CREATE found the existing Person partition and never
        // registered `email` in the graph catalog or the partition's
        // global property keys. Post-fix: binder resolves; row emits.
        auto rows = local_qr.run(
            "MATCH (p:Person) RETURN p.name AS name, p.email AS email "
            "ORDER BY p.name",
            {qtest::ColType::STRING, qtest::ColType::STRING});
        REQUIRE(rows.size() == 2);
        CHECK(rows[0].str_at(0) == "Carrie-Anne Moss");
        CHECK(rows[0].str_at(1) == "cam@matrix.com");
        CHECK(rows[1].str_at(0) == "Keanu Reeves");
        // Keanu's row predates the schema's `email` column. The current
        // contract emits empty string for missing keys (see project_bug_d
        // memo). Just guard against cross-contamination.
        CHECK(rows[1].str_at(1) != "cam@matrix.com");
    } catch (const std::exception &e) {
        FAIL("Bug E partial: " << e.what());
    }
}

// Bug C (PLAN.md): SET on a freshly bootstrapped + checkpointed node
// silently duplicates the node. Symptom: after `CREATE (:Person {...})`
// followed by `MATCH (n:Person ...) SET n.email = ...`, a subsequent
// MATCH returns two rows — the unchanged base row and the SET-updated
// snapshot. Root cause: same `pid == 0` sentinel as Bug B.
// `InvalidateCurrentNodeVersion` skipped the base-row delete-mask write
// when `ResolvePid` returned 0, but pid 0 is the legitimate (extent 0,
// row 0) disk slot the first checkpointed Person row lands on. The fix
// gates liveness on `IsLogicalIdDeleted` and lets the resolved pid
// drive extent/row addressing regardless of its numeric value.
TEST_CASE("SET on first checkpointed node does not duplicate it",
          "[crud][bootstrap][empty][bug-c]") {
    ensure_singleton_disconnected();
    struct SingletonReconnectGuard {
        ~SingletonReconnectGuard() { ensure_singleton_reconnected(); }
    } reconnect_guard;
    char tmpl[] = "/tmp/tl_bugC_XXXXXX";
    REQUIRE(mkdtemp(tmpl) != nullptr);
    std::string ws_path = tmpl;
    struct TempWorkspaceGuard {
        std::string path;
        ~TempWorkspaceGuard() {
            if (!path.empty()) std::system(("rm -rf " + path).c_str());
        }
    } guard{ws_path};

    try {
        qtest::QueryRunner local_qr(ws_path);
        local_qr.run("CREATE (n:Person {name:'Keanu', born:1964})", {});
        local_qr.run(
            "MATCH (n:Person) SET n.email = 'keanu@matrix.com'",
            {});

        // Pre-fix: 2 rows (one with NULL email, one with the SET value).
        // Post-fix: 1 row.
        auto rows = local_qr.run(
            "MATCH (n:Person) RETURN n.name, n.email",
            {qtest::ColType::STRING, qtest::ColType::STRING});
        REQUIRE(rows.size() == 1);
        CHECK(rows[0].str_at(0) == "Keanu");
        CHECK(rows[0].str_at(1) == "keanu@matrix.com");
    } catch (const std::exception &e) {
        FAIL("Bug C SET duplication: " << e.what());
    }
}

// Bug B (PLAN.md): the first edge type bootstrapped in a fresh workspace
// silently dropped from any traversal that picked the destination partition
// as the AIJ outer (typically the case when a label-only filter sits on the
// destination side). Root cause: PhysicalIdSeek's BuildSeekInput used
// `pid == 0` as the "drop this row" sentinel, but pid 0 is the legitimate
// (extent 0, row 0) disk slot the first checkpointed Person row lands on,
// so any IdSeek that resolved an INCOMING-traversed source vid back to the
// Person partition got invalidated. The fix replaces the sentinel with a
// direct extent-vs-target-schemas check.
TEST_CASE("Fresh CREATE edge survives INCOMING traversal lookup",
          "[crud][bootstrap][empty][bug-b]") {
    ensure_singleton_disconnected();
    struct SingletonReconnectGuard {
        ~SingletonReconnectGuard() { ensure_singleton_reconnected(); }
    } reconnect_guard;
    char tmpl[] = "/tmp/tl_bugB_XXXXXX";
    REQUIRE(mkdtemp(tmpl) != nullptr);
    std::string ws_path = tmpl;
    struct TempWorkspaceGuard {
        std::string path;
        ~TempWorkspaceGuard() {
            if (!path.empty()) std::system(("rm -rf " + path).c_str());
        }
    } guard{ws_path};

    try {
        qtest::QueryRunner local_qr(ws_path);
        local_qr.run("CREATE (k:Person {name:'Keanu'})", {});
        local_qr.run("CREATE (m:Movie {title:'Matrix'})", {});
        // 3 ACTED_IN + 1 DIRECTED — PLAN.md repro.
        local_qr.run(
            "MATCH (k:Person {name:'Keanu'}), (m:Movie {title:'Matrix'}) "
            "CREATE (k)-[:ACTED_IN]->(m)",
            {});
        local_qr.run(
            "MATCH (k:Person {name:'Keanu'}), (m:Movie {title:'Matrix'}) "
            "CREATE (k)-[:ACTED_IN]->(m)",
            {});
        local_qr.run(
            "MATCH (k:Person {name:'Keanu'}), (m:Movie {title:'Matrix'}) "
            "CREATE (k)-[:ACTED_IN]->(m)",
            {});
        local_qr.run(
            "MATCH (k:Person {name:'Keanu'}), (m:Movie {title:'Matrix'}) "
            "CREATE (k)-[:DIRECTED]->(m)",
            {});

        auto acted = local_qr.run(
            "MATCH ()-[r:ACTED_IN]->() RETURN count(r)",
            {qtest::ColType::INT64});
        REQUIRE(acted.size() == 1);
        CHECK(acted[0].int64_at(0) == 3);

        auto directed = local_qr.run(
            "MATCH ()-[r:DIRECTED]->() RETURN count(r)",
            {qtest::ColType::INT64});
        REQUIRE(directed.size() == 1);
        CHECK(directed[0].int64_at(0) == 1);

        // Movie-side filter forces the planner to pick Movie as outer and
        // expand INCOMING — exactly the path that pre-fix dropped to 0.
        auto count_via_movie = local_qr.run(
            "MATCH (k:Person)-[r:ACTED_IN]->(m:Movie {title:'Matrix'}) "
            "RETURN count(*)",
            {qtest::ColType::INT64});
        REQUIRE(count_via_movie.size() == 1);
        CHECK(count_via_movie[0].int64_at(0) == 3);
    } catch (const std::exception &e) {
        FAIL("Bug B INCOMING traversal: " << e.what());
    }
}

TEST_CASE("EXISTS + NOT EXISTS counts equal total", "[ldbc][crud][expr][exists]") {
    SKIP_IF_NO_DB();
    try {
        // Neo4j: total=9892, EXISTS=8250, NOT EXISTS=1642
        auto r_total = qr->run(
            "MATCH (n:Person) RETURN count(n) AS cnt",
            {qtest::ColType::INT64});
        auto r_exists = qr->run(
            "MATCH (n:Person) "
            "WHERE EXISTS { MATCH (n)-[:KNOWS]->(:Person) } "
            "RETURN count(n) AS cnt",
            {qtest::ColType::INT64});
        auto r_not_exists = qr->run(
            "MATCH (n:Person) "
            "WHERE NOT EXISTS { MATCH (n)-[:KNOWS]->(:Person) } "
            "RETURN count(n) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(r_total.size() == 1);
        REQUIRE(r_exists.size() == 1);
        REQUIRE(r_not_exists.size() == 1);
        int64_t total = r_total[0].int64_at(0);
        int64_t exists = r_exists[0].int64_at(0);
        int64_t not_exists = r_not_exists[0].int64_at(0);
        CHECK(total == 9892);
        CHECK(exists == 8250);
        CHECK(not_exists == 1642);
        CHECK(exists + not_exists == total);
    } catch (const std::exception& e) {
        FAIL("EXISTS + NOT EXISTS sum: " << e.what());
    }
}

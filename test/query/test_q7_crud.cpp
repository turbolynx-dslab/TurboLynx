// Stage 7 — CRUD operations (CREATE / SET / DELETE)
// Phase 1: CREATE Node

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <vector>

extern std::string g_db_path;
extern bool g_skip_requested;

extern qtest::QueryRunner* get_runner();

#define SKIP_IF_NO_DB() \
    if (g_db_path.empty()) { WARN("--db-path not set, skipping"); g_skip_requested = true; return; } \
    auto* qr = get_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_db_path); return; }

// Phase 1: CREATE Node tests
TEST_CASE("Q7-01 CREATE single node", "[q7][crud][create]") {
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

TEST_CASE("Q7-02 CREATE then MATCH count", "[q7][crud][create]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("CREATE (n:Person {id: 88888888888888, firstName: 'TestJane'})", {});
        auto r = qr->run(
            "MATCH (n:Person) RETURN count(n) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        // Base Person count (9892) + 2 created nodes (Q7-01 + Q7-02)
        CHECK(r[0].int64_at(0) >= 9894);
    } catch (const std::exception& e) {
        FAIL("CREATE+MATCH should not throw: " << e.what());
    }
}

TEST_CASE("Q7-03 CREATE multiple nodes then count", "[q7][crud][create]") {
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

TEST_CASE("Q7-04 CREATE does not affect filtered queries", "[q7][crud][create]") {
    SKIP_IF_NO_DB();
    try {
        // Existing person should still be found via filter pushdown
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "Mahinda");
    } catch (const std::exception& e) {
        FAIL("Filtered query after CREATE: " << e.what());
    }
}

TEST_CASE("Q7-05 CREATE node with no properties except id", "[q7][crud][create]") {
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

TEST_CASE("Q7-06 IC queries still work after CREATE", "[q7][crud][create]") {
    SKIP_IF_NO_DB();
    try {
        // IC1-style query: find person by id, return properties
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) RETURN n.firstName, n.lastName",
            {qtest::ColType::STRING, qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "Mahinda");
        CHECK(r[0].str_at(1) == "Perera");
    } catch (const std::exception& e) {
        FAIL("IC query after CREATE: " << e.what());
    }
}

TEST_CASE("Q7-07 count increases exactly by number of CREATEs", "[q7][crud][create]") {
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

TEST_CASE("Q7-10 CREATE edge does not crash", "[q7][crud][create-edge]") {
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

TEST_CASE("Q7-11 CREATE edge also creates both endpoint nodes", "[q7][crud][create-edge]") {
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

TEST_CASE("Q7-12 existing KNOWS traversal unaffected by CREATE edge", "[q7][crud][create-edge]") {
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

TEST_CASE("Q7-13 IC queries still work after CREATE edge", "[q7][crud][create-edge]") {
    SKIP_IF_NO_DB();
    try {
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) RETURN n.firstName, n.lastName",
            {qtest::ColType::STRING, qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "Mahinda");
        CHECK(r[0].str_at(1) == "Perera");
    } catch (const std::exception& e) {
        FAIL("IC query after CREATE edge: " << e.what());
    }
}

// ============================================================
// Phase 1 additional: CREATE Node advanced tests
// ============================================================

TEST_CASE("Q7-08 CREATE node with many properties", "[q7][crud][create]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("CREATE (n:Person {id: 44444444444444, firstName: 'Many', "
                "lastName: 'Props', gender: 'male', birthday: 19900101})", {});
        auto r = qr->run("MATCH (n:Person) RETURN count(n) AS cnt",
                          {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) > 9892);
    } catch (const std::exception& e) {
        FAIL("CREATE many props: " << e.what());
    }
}

TEST_CASE("Q7-09 CREATE does not affect other labels", "[q7][crud][create]") {
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

TEST_CASE("Q7-20 SET single property", "[q7][crud][set]") {
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

TEST_CASE("Q7-21 SET does not affect other nodes", "[q7][crud][set]") {
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

TEST_CASE("Q7-22 SET multiple properties", "[q7][crud][set]") {
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

TEST_CASE("Q7-23 SET then count unchanged", "[q7][crud][set]") {
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

TEST_CASE("Q7-24 SET overwrites previous SET", "[q7][crud][set]") {
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

TEST_CASE("Q7-25 SET on non-existent node is no-op", "[q7][crud][set]") {
    SKIP_IF_NO_DB();
    try {
        // Should not crash — just zero rows matched
        qr->run("MATCH (n:Person {id: 999999999999999}) SET n.firstName = 'Ghost'", {});
    } catch (const std::exception& e) {
        FAIL("SET non-existent should not throw: " << e.what());
    }
}

TEST_CASE("Q7-26 SET with RETURN", "[q7][crud][set]") {
    SKIP_IF_NO_DB();
    try {
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) SET n.firstName = 'ReturnTest' RETURN n.firstName",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "ReturnTest");
    } catch (const std::exception& e) {
        FAIL("SET with RETURN: " << e.what());
    }
}

TEST_CASE("Q7-27 SET with RETURN multiple columns", "[q7][crud][set]") {
    SKIP_IF_NO_DB();
    try {
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) SET n.firstName = 'SetRet' "
            "RETURN n.id, n.firstName, n.lastName",
            {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(1) == "SetRet");
        // lastName should be unchanged
        CHECK(r[0].str_at(2).length() > 0);
    } catch (const std::exception& e) {
        FAIL("SET with RETURN multi-col: " << e.what());
    }
}

TEST_CASE("Q7-28 SET integer property", "[q7][crud][set]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("MATCH (n:Person {id: 933}) SET n.birthday = 20000101", {});
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) RETURN n.birthday",
            {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        CHECK(r[0].int64_at(0) == 20000101);
    } catch (const std::exception& e) {
        FAIL("SET integer property: " << e.what());
    }
}

TEST_CASE("Q7-29 SET preserves other properties", "[q7][crud][set]") {
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

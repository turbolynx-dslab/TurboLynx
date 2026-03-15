// Stage 3 — Multi-hop traversal + aggregation
// All expected values verified against Neo4j 5.24.0 with LDBC SF1.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <vector>
#include <utility>

extern std::string g_db_path;
extern bool g_skip_requested;

extern qtest::QueryRunner* get_runner();

#define SKIP_IF_NO_DB() \
    if (g_db_path.empty()) { WARN("--db-path not set, skipping"); g_skip_requested = true; return; } \
    auto* qr = get_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_db_path); return; }

TEST_CASE("Q3-01 FoF count (Person 933)", "[q3][multihop]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933})-[:KNOWS]->(f:Person)-[:KNOWS]->(fof:Person) "
        "WHERE fof <> p RETURN count(DISTINCT fof)") == 1506);
}

TEST_CASE("Q3-02 Top 10 persons by Comment count", "[q3][multihop]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person)<-[:HAS_CREATOR]-(c:Comment) "
        "RETURN p.id, count(c) AS cnt "
        "ORDER BY cnt DESC, p.id ASC LIMIT 10",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 10);

    // Verify top result (personId, cnt)
    using P = std::pair<int64_t, int64_t>;
    std::vector<P> expected = {
        {2199023262543LL, 8915}, {4139,  7896}, {2199023259756LL, 7694},
        {2783,            7530}, {4398046513018LL, 7423}, {7725, 6612},
        {6597069777240LL, 6565}, {9116,  6135}, {4398046519372LL, 5894},
        {8796093029267LL, 5640}
    };
    for (size_t i = 0; i < 10; ++i) {
        INFO("row " << i);
        CHECK(r[i].int64_at(0) == expected[i].first);
        CHECK(r[i].int64_at(1) == expected[i].second);
    }
}

TEST_CASE("Q3-03 Top 5 Forums by Post count", "[q3][multihop]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post) "
        "RETURN f.id, count(p) AS cnt "
        "ORDER BY cnt DESC, f.id ASC LIMIT 5",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 5);

    using P = std::pair<int64_t, int64_t>;
    std::vector<P> expected = {
        {77644,         1208}, {87312,  1032},
        {137439023186LL, 1001}, {412317916558LL, 891}, {55025, 810}
    };
    for (size_t i = 0; i < 5; ++i) {
        INFO("row " << i);
        CHECK(r[i].int64_at(0) == expected[i].first);
        CHECK(r[i].int64_at(1) == expected[i].second);
    }
}

TEST_CASE("Q3-04 Distinct Comment creators liked by Person 933", "[q3][multihop]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933})-[:LIKES]->(c:Comment)-[:HAS_CREATOR]->(creator:Person) "
        "RETURN count(DISTINCT creator)") == 12);
}

TEST_CASE("Q3-05 Top 5 TagClasses by Tag count", "[q3][multihop]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (t:Tag)-[:HAS_TYPE]->(tc:TagClass) "
        "RETURN tc.name, count(t) AS cnt "
        "ORDER BY cnt DESC, tc.name ASC LIMIT 5",
        {qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 5);

    using P = std::pair<std::string, int64_t>;
    std::vector<P> expected = {
        {"Album", 5061}, {"Single", 4311}, {"Person", 1530},
        {"Country", 1000}, {"MusicalArtist", 899}
    };
    for (size_t i = 0; i < 5; ++i) {
        INFO("row " << i);
        CHECK(r[i].str_at(0) == expected[i].first);
        CHECK(r[i].int64_at(1) == expected[i].second);
    }
}

TEST_CASE("Q3-06 Top 5 Tags by Post count", "[q3][multihop]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Post)-[:HAS_TAG]->(t:Tag) "
        "RETURN t.name, count(p) AS cnt "
        "ORDER BY cnt DESC, t.name ASC LIMIT 5",
        {qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 5);

    using P = std::pair<std::string, int64_t>;
    std::vector<P> expected = {
        {"Augustine_of_Hippo", 10817}, {"Adolf_Hitler", 5227},
        {"Muammar_Gaddafi", 5120}, {"Imelda_Marcos", 4412}, {"Sammy_Sosa", 4059}
    };
    for (size_t i = 0; i < 5; ++i) {
        INFO("row " << i);
        CHECK(r[i].str_at(0) == expected[i].first);
        CHECK(r[i].int64_at(1) == expected[i].second);
    }
}

// ---------------------------------------------------------------------------
// Multi-partition vertex/edge tests (M28 — :Message = Comment + Post)
// ---------------------------------------------------------------------------

TEST_CASE("Q3-07 Top 10 persons by Message count", "[q3][multihop][mpe]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person)<-[:HAS_CREATOR]-(m:Message) "
        "RETURN p.id, count(m) AS cnt "
        "ORDER BY cnt DESC, p.id ASC LIMIT 10",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 10);

    using P = std::pair<int64_t, int64_t>;
    std::vector<P> expected = {
        {2199023262543LL, 9936}, {2783,             8754},
        {2199023259756LL, 8667}, {4139,             8558},
        {7725,            7833}, {4398046513018LL,  7682},
        {6597069777240LL, 7491}, {9116,             7189},
        {4398046519372LL, 6535}, {8796093029267LL,  6294}
    };
    for (size_t i = 0; i < 10; ++i) {
        INFO("row " << i);
        CHECK(r[i].int64_at(0) == expected[i].first);
        CHECK(r[i].int64_at(1) == expected[i].second);
    }
}

TEST_CASE("Q3-08 Top 5 Tags by Message count", "[q3][multihop][mpe]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (m:Message)-[:HAS_TAG]->(t:Tag) "
        "RETURN t.name, count(m) AS cnt "
        "ORDER BY cnt DESC, t.name ASC LIMIT 5",
        {qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 5);

    using P = std::pair<std::string, int64_t>;
    std::vector<P> expected = {
        {"Augustine_of_Hippo", 24299}, {"Adolf_Hitler", 12326},
        {"Muammar_Gaddafi", 12003}, {"Imelda_Marcos", 9571}, {"Genghis_Khan", 8982}
    };
    for (size_t i = 0; i < 5; ++i) {
        INFO("row " << i);
        CHECK(r[i].str_at(0) == expected[i].first);
        CHECK(r[i].int64_at(1) == expected[i].second);
    }
}

TEST_CASE("Q3-09 Distinct Message creators liked by Person 933", "[q3][multihop][mpe]") {
    SKIP_IF_NO_DB();
    // LIKES -> Message -> HAS_CREATOR -> Person
    // 12 comment creators + 5 post creators, 14 distinct persons
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933})-[:LIKES]->(m:Message)-[:HAS_CREATOR]->(creator:Person) "
        "RETURN count(DISTINCT creator)") == 14);
}

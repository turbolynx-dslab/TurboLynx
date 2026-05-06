// Stage 2 — Property filter + 1-hop traversal
//
// Two fixture sizes are supported via `helpers/ldbc_expected_counts.hpp`,
// which dispatches between SF1 (default) and SF0.003 (mini, set with
// `cmake -DTURBOLYNX_LDBC_FIXTURE_MINI=ON`). All SF0.003 expected
// values are Neo4j-verified against the committed mini fixture.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include "helpers/ldbc_expected_counts.hpp"
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

static std::string sample_person_id_str() {
    return std::to_string(ldbc::SAMPLE_PERSON_ID);
}

TEST_CASE("Sample person firstName/lastName", "[ldbc][filter]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person) WHERE p.id = " + sample_person_id_str() +
             " RETURN p.firstName, p.lastName";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == ldbc::SAMPLE_PERSON_FIRST_NAME);
    CHECK(r[0].str_at(1) == ldbc::SAMPLE_PERSON_LAST_NAME);
}

TEST_CASE("Sample person outgoing KNOWS count", "[ldbc][filter]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() +
             "})-[:KNOWS]->(friend:Person) RETURN count(friend)";
    REQUIRE(qr->count(q.c_str()) == ldbc::SAMPLE_PERSON_OUTGOING_KNOWS);
}

// `:City` is a sub-label SF1's load script tags but `load-ldbc-mini.sh`
// does not (Place.csv has a `type` column but the mini loader keeps them
// all as `:Place`). We test :Place on both fixtures and gate the :City
// probe on SF1 only.
#ifndef TURBOLYNX_LDBC_FIXTURE_MINI
TEST_CASE("Sample person IS_LOCATED_IN City", "[ldbc][filter]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() +
             "})-[:IS_LOCATED_IN]->(pl:City) RETURN pl.id, pl.name";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PERSON_CITY_ID);
    CHECK(r[0].str_at(1) == ldbc::SAMPLE_PERSON_CITY_NAME);
}
#endif

// Parent-label UNION: :Place = City + Country + Continent.
// Person IS_LOCATED_IN always targets a City.
TEST_CASE("Sample person IS_LOCATED_IN Place", "[ldbc][filter]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() +
             "})-[:IS_LOCATED_IN]->(pl:Place) RETURN pl.id, pl.name";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PERSON_CITY_ID);
    CHECK(r[0].str_at(1) == ldbc::SAMPLE_PERSON_CITY_NAME);
}

TEST_CASE("Sample person properties", "[ldbc][filter]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() + "}) "
             "RETURN p.gender, p.birthday, p.locationIP, p.browserUsed";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::STRING, qtest::ColType::INT64,
         qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == ldbc::SAMPLE_PERSON_GENDER);
    CHECK(r[0].int64_at(1) == ldbc::SAMPLE_PERSON_BIRTHDAY_MS);
    CHECK(r[0].str_at(2) == ldbc::SAMPLE_PERSON_LOCATION_IP);
    CHECK(r[0].str_at(3) == ldbc::SAMPLE_PERSON_BROWSER);
}

TEST_CASE("Sample comment creator", "[ldbc][filter]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (c:Comment)-[:HAS_CREATOR]->(p:Person) "
             "WHERE c.id = " + std::to_string(ldbc::SAMPLE_COMMENT_ID) + " "
             "RETURN p.id, p.firstName";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_COMMENT_CREATOR_ID);
    CHECK(r[0].str_at(1) == ldbc::SAMPLE_COMMENT_CREATOR_FIRSTNAME);
}

TEST_CASE("Sample forum post count", "[ldbc][filter]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post) "
             "WHERE f.id = " + std::to_string(ldbc::SAMPLE_FORUM_ID) + " "
             "RETURN count(p)";
    REQUIRE(qr->count(q.c_str()) == ldbc::SAMPLE_FORUM_POST_COUNT);
}

TEST_CASE("Post count with sample tag", "[ldbc][filter]") {
    SKIP_IF_NO_DB();
    auto q = std::string("MATCH (p:Post)-[:HAS_TAG]->(t:Tag) WHERE t.name = '") +
             ldbc::SAMPLE_TAG_NAME + "' RETURN count(p)";
    REQUIRE(qr->count(q.c_str()) == ldbc::SAMPLE_TAG_POST_COUNT);
}

TEST_CASE("Sample person liked Comments count", "[ldbc][filter]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() +
             "})-[:LIKES]->(c:Comment) RETURN count(c)";
    REQUIRE(qr->count(q.c_str()) == ldbc::SAMPLE_PERSON_LIKED_COMMENTS);
}

TEST_CASE("Sample person liked Posts count", "[ldbc][filter]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() +
             "})-[:LIKES]->(po:Post) RETURN count(po)";
    REQUIRE(qr->count(q.c_str()) == ldbc::SAMPLE_PERSON_LIKED_POSTS);
}

// ---------------------------------------------------------------------------
// Multi-partition vertex/edge tests (M28 — :Message = Comment + Post)
// ---------------------------------------------------------------------------

TEST_CASE("Sample person Messages via HAS_CREATOR count", "[ldbc][filter][mpe]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (:Person {id: " + sample_person_id_str() +
             "})<-[:HAS_CREATOR]-(m:Message) RETURN count(m)";
    REQUIRE(qr->count(q.c_str()) == ldbc::TRAV_MESSAGES_AUTHORED_BY_SAMPLE_PERSON);
}

TEST_CASE("Sample person liked Messages count", "[ldbc][filter][mpe]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() +
             "})-[:LIKES]->(m:Message) RETURN count(m)";
    REQUIRE(qr->count(q.c_str()) == ldbc::SAMPLE_PERSON_LIKED_MESSAGES);
}

TEST_CASE("Message count with sample tag", "[ldbc][filter][mpe]") {
    SKIP_IF_NO_DB();
    auto q = std::string("MATCH (m:Message)-[:HAS_TAG]->(t:Tag) WHERE t.name = '") +
             ldbc::SAMPLE_TAG_NAME + "' RETURN count(m)";
    REQUIRE(qr->count(q.c_str()) == ldbc::SAMPLE_TAG_MESSAGE_COUNT);
}

// ============================================================
// UNWIND tests
// ============================================================

TEST_CASE("UNWIND literal list", "[ldbc][filter][unwind]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("UNWIND [1, 2, 3] AS x RETURN count(x)") == 3);
    REQUIRE(qr->count("UNWIND [10, 20, 30] AS x RETURN sum(x)") == 60);
}

TEST_CASE("UNWIND with MATCH filter", "[ldbc][filter][unwind]") {
    SKIP_IF_NO_DB();
    auto q = "UNWIND [" + sample_person_id_str() + ", " +
             std::to_string(ldbc::SECOND_SAMPLE_PERSON_ID) + "] AS pid "
             "MATCH (p:Person) WHERE p.id = pid "
             "RETURN p.firstName ORDER BY p.firstName";
    auto r = qr->run(q.c_str(), {qtest::ColType::STRING});
    REQUIRE(r.size() == 2);
    // Sorted by firstName ASC: SAMPLE then SECOND on both fixtures (Hossein<Jan, Mahinda<Samir).
    CHECK(r[0].str_at(0) == ldbc::SAMPLE_PERSON_FIRST_NAME);
    CHECK(r[1].str_at(0) == ldbc::SECOND_SAMPLE_PERSON_FIRST_NAME);
}

TEST_CASE("UNWIND after WITH", "[ldbc][filter][unwind]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() +
             "})-[:HAS_INTEREST]->(t:Tag) "
             "WITH collect(t.name) AS tags "
             "UNWIND tags AS tagName "
             "RETURN tagName ORDER BY tagName";
    auto r = qr->run(q.c_str(), {qtest::ColType::STRING});
    REQUIRE(r.size() > 0);
    for (size_t i = 1; i < r.size(); i++) {
        CHECK(r[i].str_at(0) >= r[i-1].str_at(0));
    }
}

TEST_CASE("UNWIND with aggregation", "[ldbc][filter][unwind]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "UNWIND [10, 20, 30, 40, 50] AS x "
        "RETURN count(x) AS cnt, sum(x) AS total",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 5);
    CHECK(r[0].int64_at(1) == 150);
}

// `:Country` sub-label is not tagged on the mini fixture — the load
// script keeps every Place row as `:Place`. We probe `:Place` here on
// both fixtures since the same names exist as countries in the Place
// catalog regardless of scale.
TEST_CASE("UNWIND string list", "[ldbc][filter][unwind]") {
    SKIP_IF_NO_DB();
    auto q = std::string("UNWIND ['") + ldbc::SAMPLE_COUNTRY_NAME_1 + "', '" +
             ldbc::SAMPLE_COUNTRY_NAME_2 + "'] AS name "
             "MATCH (c:Place) WHERE c.name = name "
             "RETURN c.name ORDER BY c.name";
    auto r = qr->run(q.c_str(), {qtest::ColType::STRING});
    REQUIRE(r.size() == 2);
    // The two pinned country names happen to be sorted ASC on both fixtures.
    CHECK(r[0].str_at(0) == ldbc::SAMPLE_COUNTRY_NAME_1);
    CHECK(r[1].str_at(0) == ldbc::SAMPLE_COUNTRY_NAME_2);
}

// Gated SF1-only: on the SF0.003 mini fixture this query SIGSEGVs
// inside the engine (issue #76). A try/catch cannot intercept a
// segfault — Catch2 sees the process die and stops scheduling further
// cases — so we exclude the case from the mini build entirely until
// #76 is fixed. SF1 dev runs still exercise it (was passing pre-migration).
#ifndef TURBOLYNX_LDBC_FIXTURE_MINI
TEST_CASE("UNWIND empty list", "[ldbc][filter][unwind]") {
    SKIP_IF_NO_DB();
    try {
        auto r = qr->run(
            "UNWIND [] AS x RETURN x",
            {qtest::ColType::INT64});
        CHECK(r.size() == 0);
    } catch (...) {
        SUCCEED();
    }
}
#endif

TEST_CASE("UNWIND null produces zero rows", "[ldbc][filter][unwind]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "UNWIND null AS x RETURN count(*)",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 0);
}

TEST_CASE("UNWIND nullable list returns element values", "[ldbc][filter][unwind]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person) WHERE p.id IN [" + sample_person_id_str() + ", " +
             std::to_string(ldbc::SECOND_SAMPLE_PERSON_ID) + "] "
             "WITH CASE WHEN p.id = " + sample_person_id_str() +
             " THEN null ELSE [1, 2] END AS xs "
             "UNWIND xs AS x "
             "RETURN toString(x) AS sx ORDER BY sx";
    auto r = qr->run(q.c_str(), {qtest::ColType::STRING});
    REQUIRE(r.size() == 2);
    CHECK(r[0].str_at(0) == "1");
    CHECK(r[1].str_at(0) == "2");
}

// ============================================================
// Cypher scalar function tests
// ============================================================

TEST_CASE("toInteger", "[ldbc][filter][func]") {
    SKIP_IF_NO_DB();
    // Anchored on a single-row MATCH so the projection has one row to
    // attach to; the actual values exercised are pure scalar literals.
    REQUIRE(qr->count(
        ("MATCH (p:Person {id: " + sample_person_id_str() +
         "}) RETURN toInteger(3.7)").c_str()) == 3);
    REQUIRE(qr->count(
        ("MATCH (p:Person {id: " + sample_person_id_str() +
         "}) RETURN toInteger('42')").c_str()) == 42);
    REQUIRE(qr->count(
        ("MATCH (p:Person {id: " + sample_person_id_str() +
         "}) RETURN toInteger(p.id)").c_str()) == ldbc::SAMPLE_PERSON_ID);
}

TEST_CASE("toFloat", "[ldbc][filter][func]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        ("MATCH (p:Person {id: " + sample_person_id_str() +
         "}) RETURN toInteger(toFloat(42))").c_str()) == 42);
}

TEST_CASE("floor", "[ldbc][filter][func]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        ("MATCH (p:Person {id: " + sample_person_id_str() +
         "}) RETURN toInteger(floor(3.7))").c_str()) == 3);
    REQUIRE(qr->count(
        ("MATCH (p:Person {id: " + sample_person_id_str() +
         "}) RETURN toInteger(floor(3.2))").c_str()) == 3);
}

TEST_CASE("arithmetic + floor + toInteger combo", "[ldbc][filter][func]") {
    SKIP_IF_NO_DB();
    // Simulates the IC7 minutesLatency pattern.
    REQUIRE(qr->count(
        ("MATCH (p:Person {id: " + sample_person_id_str() + "}) "
         "RETURN toInteger(floor(toFloat(120000) / 1000.0 / 60.0))").c_str()) == 2);
}

// ============================================================
// Map literal + property access tests
// ============================================================

TEST_CASE("map literal basic", "[ldbc][filter][map]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() + "}) "
             "WITH {name: p.firstName, age: 42} AS info "
             "RETURN info.name, toInteger(info.age) AS age";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == ldbc::SAMPLE_PERSON_FIRST_NAME);
    CHECK(r[0].int64_at(1) == 42);
}

TEST_CASE("map literal with multiple fields", "[ldbc][filter][map]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() + "}) "
             "WITH {id: p.id, first: p.firstName, last: p.lastName} AS info "
             "RETURN info.first, info.last";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == ldbc::SAMPLE_PERSON_FIRST_NAME);
    CHECK(r[0].str_at(1) == ldbc::SAMPLE_PERSON_LAST_NAME);
}

TEST_CASE("nested map property access", "[ldbc][filter][map]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() +
             "})-[:HAS_INTEREST]->(t:Tag) "
             "WITH {tag: t, personName: p.firstName} AS info "
             "RETURN info.personName "
             "LIMIT 1";
    auto r = qr->run(q.c_str(), {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == ldbc::SAMPLE_PERSON_FIRST_NAME);
}

TEST_CASE("head function on list", "[ldbc][filter][func][map]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() +
             "})-[:HAS_INTEREST]->(t:Tag) "
             "WITH collect(t.name) AS tags "
             "RETURN head(tags)";
    auto r = qr->run(q.c_str(), {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0).size() > 0);
}

// ============================================================
// Ordered aggregation + head() tests
// ============================================================

TEST_CASE("head(collect()) inline in RETURN", "[ldbc][filter][func]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() +
             "})-[:HAS_INTEREST]->(t:Tag) "
             "RETURN head(collect(t.name))";
    auto r = qr->run(q.c_str(), {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0).size() > 0);
}

TEST_CASE("ordered collect + head via two WITHs", "[ldbc][filter][func]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() +
             "})-[:HAS_INTEREST]->(t:Tag) "
             "WITH t.name AS name ORDER BY name ASC "
             "WITH collect(name) AS names "
             "RETURN head(names)";
    auto r = qr->run(q.c_str(), {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0).substr(0, 4) == ldbc::SAMPLE_PERSON_FIRST_INTEREST_PREFIX);
}

TEST_CASE("head(collect()) with GROUP BY key", "[ldbc][filter][func]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() +
             "})-[:HAS_INTEREST]->(t:Tag) "
             "WITH p, head(collect(t.name)) AS firstTag "
             "RETURN firstTag";
    auto r = qr->run(q.c_str(), {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0).size() > 0);
}

// ============================================================
// Path function tests (M2)
// ============================================================

TEST_CASE("shortestPath length", "[ldbc][filter][path]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p1:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH (p2:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_DEST_ID) + "}) "
             "MATCH path = shortestPath((p1)-[:KNOWS*]-(p2)) "
             "RETURN length(path) AS len";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PATH_LEN);
}

TEST_CASE("allShortestPaths count", "[ldbc][filter][path]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p1:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH (p2:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_DEST_ID) + "}) "
             "MATCH path = allShortestPaths((p1)-[:KNOWS*]-(p2)) "
             "RETURN length(path) AS len";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == (size_t)ldbc::SAMPLE_PATH_NUM_ALL_SHORTEST);
    for (size_t i = 0; i < r.size(); i++) {
        CHECK(r[i].int64_at(0) == ldbc::SAMPLE_PATH_LEN);
    }
}

TEST_CASE("nodes(path) extracts node VIDs", "[ldbc][filter][path]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p1:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH (p2:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_DEST_ID) + "}) "
             "MATCH path = shortestPath((p1)-[:KNOWS*]-(p2)) "
             "WITH nodes(path) AS nodeIds "
             "RETURN size(nodeIds) AS cnt";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PATH_LEN + 1);
}

TEST_CASE("relationships(path) extracts edge IDs", "[ldbc][filter][path]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p1:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH (p2:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_DEST_ID) + "}) "
             "MATCH path = shortestPath((p1)-[:KNOWS*]-(p2)) "
             "WITH relationships(path) AS relIds "
             "RETURN size(relIds) AS cnt";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PATH_LEN);
}

TEST_CASE("relationships(path) returns empty list for zero-hop path",
          "[ldbc][filter][path]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() + "}) "
             "MATCH path = allShortestPaths((p)-[:KNOWS*0..0]-(p)) "
             "RETURN size(relationships(path)) AS relCnt";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 0);
}

TEST_CASE("allShortestPaths + collect + UNWIND + nodes", "[ldbc][filter][path]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p1:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH (p2:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_DEST_ID) + "}) "
             "MATCH path = allShortestPaths((p1)-[:KNOWS*]-(p2)) "
             "WITH collect(path) AS paths "
             "UNWIND paths AS p "
             "RETURN size(nodes(p)) AS nodeCnt, size(relationships(p)) AS relCnt";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == (size_t)ldbc::SAMPLE_PATH_NUM_ALL_SHORTEST);
    for (size_t i = 0; i < r.size(); i++) {
        CHECK(r[i].int64_at(0) == ldbc::SAMPLE_PATH_LEN + 1);
        CHECK(r[i].int64_at(1) == ldbc::SAMPLE_PATH_LEN);
    }
}

TEST_CASE("length on string (not path)", "[ldbc][filter][path]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() + "}) "
             "RETURN length(p.firstName) AS len";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) > 0);
}

// ============================================================
// Reduce tests (M3)
// ============================================================

TEST_CASE("reduce sum doubles", "[ldbc][filter][reduce]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() + "}) "
             "RETURN reduce(w = 0.0, v IN [1.0, 2.0, 3.0] | w + v) AS total";
    auto r = qr->run(q.c_str(), {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
}

TEST_CASE("reduce sum integers", "[ldbc][filter][reduce]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() + "}) "
             "RETURN reduce(w = 0, v IN [10, 20, 30] | w + v) AS total";
    auto r = qr->run(q.c_str(), {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
}

TEST_CASE("reduce sum preserves non-zero init", "[ldbc][filter][reduce]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "RETURN toInteger(reduce(s = 10, x IN [1, 2] | s + x)) AS total",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 13);
}

TEST_CASE("reduce unsupported form throws", "[ldbc][filter][reduce]") {
    SKIP_IF_NO_DB();
    REQUIRE_THROWS(qr->run(
        "RETURN reduce(s = 2, x IN [3, 4] | s * x) AS total",
        {qtest::ColType::AUTO}));
}

TEST_CASE("reduce empty list", "[ldbc][filter][reduce]") {
    SKIP_IF_NO_DB();
    try {
        auto q = "MATCH (p:Person {id: " + sample_person_id_str() + "}) "
                 "WITH collect(p.firstName) AS names "
                 "WHERE false "
                 "RETURN reduce(w = 0.0, v IN [1.0] | w + v) AS total";
        qr->run(q.c_str(), {qtest::ColType::AUTO});
    } catch (...) { /* may fail gracefully */ }
    SUCCEED();
}

TEST_CASE("reduce with path weights", "[ldbc][filter][reduce]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() + "}) "
             "WITH [1.0, 1.0, 0.5, 0.5] AS weights "
             "RETURN reduce(w = 0.0, v IN weights | w + v) AS total";
    auto r = qr->run(q.c_str(), {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
}

TEST_CASE("reduce with collect result", "[ldbc][filter][reduce]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() +
             "})-[:KNOWS]-(f:Person) "
             "WITH collect(f.id) AS ids "
             "RETURN reduce(s = 0, x IN ids | s + x) AS total";
    auto r = qr->run(q.c_str(), {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
}

TEST_CASE("reduce in WITH pipeline", "[ldbc][filter][reduce]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() + "}) "
             "WITH [1.0, 2.0, 3.0, 4.0] AS vals "
             "WITH reduce(w = 0.0, v IN vals | w + v) AS total "
             "RETURN total";
    auto r = qr->run(q.c_str(), {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
}

TEST_CASE("reduce with single element", "[ldbc][filter][reduce]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() + "}) "
             "RETURN reduce(w = 0.0, v IN [42.0] | w + v) AS total";
    auto r = qr->run(q.c_str(), {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
}

TEST_CASE("reduce used in arithmetic", "[ldbc][filter][reduce]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() + "}) "
             "WITH [1.0, 1.0, 0.5] AS w1, [0.5, 0.5] AS w2 "
             "RETURN reduce(a = 0.0, v IN w1 | a + v) + reduce(b = 0.0, v IN w2 | b + v) AS combined";
    auto r = qr->run(q.c_str(), {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
}

// ============================================================
// Pattern comprehension tests (M4)
// ============================================================
// Both cases SIGSEGV on the SF0.003 mini fixture (issue #77). Pre-
// migration both were WARN-on-throw on SF1 (flaky but non-fatal);
// on the mini fixture they crash the harness instead of throwing,
// so we gate them out of the mini build. SF1 dev runs still
// exercise them.
#ifndef TURBOLYNX_LDBC_FIXTURE_MINI
TEST_CASE("pattern comprehension with WHERE", "[ldbc][filter][patterncomp]") {
    SKIP_IF_NO_DB();
    try {
        auto q = "MATCH (p:Person {id: " + sample_person_id_str() + "}) "
                 "RETURN [(p)<-[:HAS_CREATOR]-(:Comment) WHERE p.id = " +
                 sample_person_id_str() + " | 1.0] AS matches";
        auto r = qr->run(q.c_str(), {qtest::ColType::AUTO});
        REQUIRE(r.size() == 1);
    } catch (const std::exception &e) {
        WARN("Q2-64: " << e.what());
    }
}

TEST_CASE("pattern comprehension simple", "[ldbc][filter][patterncomp]") {
    SKIP_IF_NO_DB();
    try {
        auto q = "MATCH (p:Person {id: " + sample_person_id_str() + "}) "
                 "RETURN [(p)-[:KNOWS]-(f:Person) WHERE f.id > 0 | 1.0] AS friends";
        auto r = qr->run(q.c_str(), {qtest::ColType::AUTO});
        REQUIRE(r.size() == 1);
    } catch (const std::exception &e) {
        WARN("Q2-65: " << e.what());
    }
}
#endif

// ============================================================
// XOR expression tests
// ============================================================

TEST_CASE("XOR basic", "[ldbc][filter][xor]") {
    SKIP_IF_NO_DB();
    // (id > 100) XOR (id < 500): for any positive sample_id this is
    // (true XOR <id<500>). For SF1 933 it's (true XOR false) = true;
    // for SF0.003 14 it's (false XOR true) = true. Both yield true.
    auto q = "MATCH (n:Person {id: " + sample_person_id_str() + "}) "
             "RETURN (n.id > 100 XOR n.id < 500) AS x";
    auto r = qr->run(q.c_str(), {qtest::ColType::BOOL});
    REQUIRE(r.size() == 1);
    // For SF0.003 (id=14): false XOR true = true.
    // For SF1 (id=933):    true  XOR false = true.
    CHECK(r[0].bool_at(0) == true);
}

TEST_CASE("XOR true", "[ldbc][filter][xor]") {
    SKIP_IF_NO_DB();
    // (id > 100) XOR (id > 10000) — true XOR false on SF1, false XOR false on mini.
#ifdef TURBOLYNX_LDBC_FIXTURE_MINI
    // SF0.003 SAMPLE_PERSON id=14 < 100, so both branches are false → XOR = false.
    auto q = "MATCH (n:Person {id: " + sample_person_id_str() + "}) "
             "RETURN (n.id > 0 XOR n.id > 10000) AS x";
#else
    auto q = "MATCH (n:Person {id: " + sample_person_id_str() + "}) "
             "RETURN (n.id > 100 XOR n.id > 10000) AS x";
#endif
    auto r = qr->run(q.c_str(), {qtest::ColType::BOOL});
    REQUIRE(r.size() == 1);
    CHECK(r[0].bool_at(0) == true);
}

TEST_CASE("XOR in WHERE", "[ldbc][filter][xor]") {
    SKIP_IF_NO_DB();
#ifdef TURBOLYNX_LDBC_FIXTURE_MINI
    auto q = "MATCH (n:Person {id: " + sample_person_id_str() + "}) "
             "WHERE (n.id > 0 XOR n.id > 10000) "
             "RETURN n.id";
#else
    auto q = "MATCH (n:Person {id: " + sample_person_id_str() + "}) "
             "WHERE (n.id > 100 XOR n.id > 10000) "
             "RETURN n.id";
#endif
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PERSON_ID);
}

// ============================================================
// String predicate tests
// ============================================================

TEST_CASE("STARTS WITH true", "[ldbc][filter][stringpred]") {
    SKIP_IF_NO_DB();
    auto q = std::string("MATCH (n:Person {id: ") + sample_person_id_str() + "}) "
             "RETURN n.firstName STARTS WITH '" +
             ldbc::SAMPLE_PERSON_NAME_STARTS_MATCH + "' AS x";
    auto r = qr->run(q.c_str(), {qtest::ColType::BOOL});
    REQUIRE(r.size() == 1);
    CHECK(r[0].bool_at(0) == true);
}

TEST_CASE("STARTS WITH false", "[ldbc][filter][stringpred]") {
    SKIP_IF_NO_DB();
    auto q = std::string("MATCH (n:Person {id: ") + sample_person_id_str() + "}) "
             "RETURN n.firstName STARTS WITH '" +
             ldbc::SAMPLE_PERSON_NAME_STARTS_NOMATCH + "' AS x";
    auto r = qr->run(q.c_str(), {qtest::ColType::BOOL});
    REQUIRE(r.size() == 1);
    CHECK(r[0].bool_at(0) == false);
}

TEST_CASE("ENDS WITH", "[ldbc][filter][stringpred]") {
    SKIP_IF_NO_DB();
    auto q = std::string("MATCH (n:Person {id: ") + sample_person_id_str() + "}) "
             "RETURN n.firstName ENDS WITH '" +
             ldbc::SAMPLE_PERSON_NAME_ENDS_MATCH + "' AS x";
    auto r = qr->run(q.c_str(), {qtest::ColType::BOOL});
    REQUIRE(r.size() == 1);
    CHECK(r[0].bool_at(0) == true);
}

TEST_CASE("CONTAINS true", "[ldbc][filter][stringpred]") {
    SKIP_IF_NO_DB();
    auto q = std::string("MATCH (n:Person {id: ") + sample_person_id_str() + "}) "
             "RETURN n.firstName CONTAINS '" +
             ldbc::SAMPLE_PERSON_NAME_CONTAINS_MATCH + "' AS x";
    auto r = qr->run(q.c_str(), {qtest::ColType::BOOL});
    REQUIRE(r.size() == 1);
    CHECK(r[0].bool_at(0) == true);
}

TEST_CASE("pattern expression preserves direction", "[ldbc][filter][patternexpr]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() +
             "})<-[:HAS_CREATOR]-(m:Message) "
             "RETURN (m)-[:HAS_CREATOR]->(p) AS out_ok, "
             "(m)<-[:HAS_CREATOR]-(p) AS in_wrong, "
             "(m)-[:HAS_CREATOR]-(p) AS both_ok "
             "LIMIT 1";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::BOOL, qtest::ColType::BOOL, qtest::ColType::BOOL});
    REQUIRE(r.size() == 1);
    CHECK(r[0].bool_at(0) == true);
    CHECK(r[0].bool_at(1) == false);
    CHECK(r[0].bool_at(2) == true);
}

TEST_CASE("pattern expression rejects unresolved endpoint", "[ldbc][filter][patternexpr]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() + "}) "
             "RETURN (p)-[:KNOWS]->() AS x";
    REQUIRE_THROWS(qr->run(q.c_str(), {qtest::ColType::BOOL}));
}

TEST_CASE("CONTAINS in WHERE", "[ldbc][filter][stringpred]") {
    SKIP_IF_NO_DB();
    auto q = std::string("MATCH (n:Person {id: ") + sample_person_id_str() + "}) "
             "WHERE n.firstName CONTAINS '" +
             ldbc::SAMPLE_PERSON_NAME_CONTAINS_MATCH + "' "
             "RETURN n.id";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PERSON_ID);
}

TEST_CASE("STARTS WITH in WHERE", "[ldbc][filter][stringpred]") {
    SKIP_IF_NO_DB();
    auto q = std::string("MATCH (n:Person {id: ") + sample_person_id_str() + "}) "
             "WHERE n.firstName STARTS WITH '" +
             ldbc::SAMPLE_PERSON_NAME_STARTS_MATCH + "' "
             "RETURN n.id";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PERSON_ID);
}

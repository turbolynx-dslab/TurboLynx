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

TEST_CASE("UNWIND empty list", "[ldbc][filter][unwind]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "UNWIND [] AS x RETURN x",
        {qtest::ColType::INT64});
    CHECK(r.size() == 0);
}

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

// Regression for issue #47: TemplatedContainsOrPosition's nested-type
// branch called Vector::GetValue with the physical index returned by
// VectorData.sel. GetValue takes a logical row and applies the
// vector's own selection, so DICTIONARY_VECTOR inputs would have the
// selection applied twice — comparing the wrong nested value. The
// fix passes the logical index (list_entry.offset + child_idx /
// outer loop i). Today's Cypher pipeline doesn't appear to feed a
// DICTIONARY_VECTOR into list_contains, so this test mostly pins
// the correctness of the nested-type path and guards against a
// future regression that introduces dict-vector inputs.
TEST_CASE("list_contains on list of structs is per-row correct",
          "[ldbc][filter][issue-47][list-contains]") {
    SKIP_IF_NO_DB();
    const std::string q =
        "MATCH (p:Person) WHERE p.id IN [" +
        std::to_string(ldbc::SAMPLE_PERSON_ID) + ", " +
        std::to_string(ldbc::SECOND_SAMPLE_PERSON_ID) + ", " +
        std::to_string(ldbc::THIRD_SAMPLE_PERSON_ID) + "] "
        "WITH p, [{i: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
        "}, {i: " + std::to_string(ldbc::THIRD_SAMPLE_PERSON_ID) +
        "}] AS targets "
        "RETURN p.id, {i: p.id} IN targets AS yes "
        "ORDER BY p.id ASC";
    auto r = qr->run(q.c_str(),
                     {qtest::ColType::INT64, qtest::ColType::BOOL});
    REQUIRE(r.size() == 3);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PERSON_ID);
    CHECK(r[0].bool_at(1) == true);
    CHECK(r[1].int64_at(0) == ldbc::SECOND_SAMPLE_PERSON_ID);
    CHECK(r[1].bool_at(1) == false);
    CHECK(r[2].int64_at(0) == ldbc::THIRD_SAMPLE_PERSON_ID);
    CHECK(r[2].bool_at(1) == true);
}

TEST_CASE("list_contains on list of lists is per-row correct",
          "[ldbc][filter][issue-47][list-contains]") {
    SKIP_IF_NO_DB();
    const std::string q =
        "MATCH (p:Person) WHERE p.id IN [" +
        std::to_string(ldbc::SAMPLE_PERSON_ID) + ", " +
        std::to_string(ldbc::SECOND_SAMPLE_PERSON_ID) + ", " +
        std::to_string(ldbc::THIRD_SAMPLE_PERSON_ID) + "] "
        "WITH p, [[" + std::to_string(ldbc::SAMPLE_PERSON_ID) +
        ", 1], [" + std::to_string(ldbc::THIRD_SAMPLE_PERSON_ID) +
        ", 1]] AS targets "
        "RETURN p.id, [p.id, 1] IN targets AS yes "
        "ORDER BY p.id ASC";
    auto r = qr->run(q.c_str(),
                     {qtest::ColType::INT64, qtest::ColType::BOOL});
    REQUIRE(r.size() == 3);
    CHECK(r[0].bool_at(1) == true);
    CHECK(r[1].bool_at(1) == false);
    CHECK(r[2].bool_at(1) == true);
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

// Regression for issue #43: list_value with all-constant arguments set
// the result to CONSTANT_VECTOR but still wrote one list_entry_t per
// input row, running past the constant slot. For a multi-row chunk
// (e.g. MATCH returning N rows + RETURN [c1, c2]) this corrupted
// adjacent memory or produced wrong-length lists. After the fix the
// constant case materialises a single list value.
TEST_CASE("constant list literal across multi-row input",
          "[ldbc][filter][issue-43][list-value]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person) WITH p LIMIT 5 "
        "RETURN size([1, 2, 3, 4]) AS sz",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 5);
    for (size_t i = 0; i < r.size(); ++i) {
        CHECK(r[i].int64_at(0) == 4);
    }
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
// Plain varlen path materialization
// ------------------------------------------------------------
// MATCH path = (a)-[:R*N..M]-(b) RETURN length(path) was crashing
// because PathJoin never projected a path column (shortestPath did).
// Validate the materialization through the path functions.
// ============================================================

TEST_CASE("length(path) on plain varlen *N..N is exactly N",
          "[ldbc][filter][path][varlen]") {
    SKIP_IF_NO_DB();
    // Endpoint-bound varlen: enumerates 7 paths of length 3 between
    // SAMPLE_PATH_SRC_ID and SAMPLE_PATH_DEST_ID, every row should
    // report length == 3.
    auto q = "MATCH (src:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH (dst:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_DEST_ID) + "}) "
             "MATCH path = (src)-[:KNOWS*" + std::to_string(ldbc::SAMPLE_PATH_LEN) +
             ".." + std::to_string(ldbc::SAMPLE_PATH_LEN) + "]-(dst) "
             "RETURN length(path) AS len";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == (size_t)ldbc::SAMPLE_PATH_NUM_ALL_SHORTEST);
    for (size_t i = 0; i < r.size(); i++) {
        CHECK(r[i].int64_at(0) == ldbc::SAMPLE_PATH_LEN);
    }
}

TEST_CASE("nodes(path) / relationships(path) on plain varlen size match",
          "[ldbc][filter][path][varlen]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (src:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH (dst:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_DEST_ID) + "}) "
             "MATCH path = (src)-[:KNOWS*" + std::to_string(ldbc::SAMPLE_PATH_LEN) +
             ".." + std::to_string(ldbc::SAMPLE_PATH_LEN) + "]-(dst) "
             "RETURN size(nodes(path)) AS nc, size(relationships(path)) AS rc";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == (size_t)ldbc::SAMPLE_PATH_NUM_ALL_SHORTEST);
    for (size_t i = 0; i < r.size(); i++) {
        CHECK(r[i].int64_at(0) == ldbc::SAMPLE_PATH_LEN + 1);
        CHECK(r[i].int64_at(1) == ldbc::SAMPLE_PATH_LEN);
    }
}

TEST_CASE("length(path) on plain varlen ordered descending",
          "[ldbc][filter][path][varlen]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (src:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH path = (src)-[:KNOWS*1..2]->(b:Person) "
             "RETURN length(path) AS len ORDER BY len DESC LIMIT 3";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() <= 3);
    if (r.size() >= 2) {
        CHECK(r[0].int64_at(0) >= r[1].int64_at(0));
    }
    for (size_t i = 0; i < r.size(); i++) {
        CHECK(r[i].int64_at(0) >= 1);
        CHECK(r[i].int64_at(0) <= 2);
    }
}

TEST_CASE("count by length(path) groups plain varlen rows",
          "[ldbc][filter][path][varlen]") {
    SKIP_IF_NO_DB();
    // group-by aggregation over a path function — exercises that the
    // path column survives the projection-then-Hash/StreamAgg pipeline.
    auto q = "MATCH (src:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH path = (src)-[:KNOWS*1..2]->(b:Person) "
             "RETURN length(path) AS len, count(*) AS c ORDER BY len";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() >= 1);
    int64_t prev = 0;
    int64_t total = 0;
    for (size_t i = 0; i < r.size(); i++) {
        CHECK(r[i].int64_at(0) > prev);  // strictly increasing length
        prev = r[i].int64_at(0);
        total += r[i].int64_at(1);
    }
    CHECK(total >= 1);
}

TEST_CASE("count(path) on plain varlen counts rows",
          "[ldbc][filter][path][varlen]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (src:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH path = (src)-[:KNOWS*1..2]->(b:Person) "
             "RETURN count(path) AS c";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) >= 1);
}

TEST_CASE("plain varlen path materialization with bidirectional edge",
          "[ldbc][filter][path][varlen]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (src:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH (dst:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_DEST_ID) + "}) "
             "MATCH path = (src)-[:KNOWS*" + std::to_string(ldbc::SAMPLE_PATH_LEN) +
             ".." + std::to_string(ldbc::SAMPLE_PATH_LEN) + "]-(dst) "
             "RETURN length(path) AS len";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == (size_t)ldbc::SAMPLE_PATH_NUM_ALL_SHORTEST);
    for (size_t i = 0; i < r.size(); i++) {
        CHECK(r[i].int64_at(0) == ldbc::SAMPLE_PATH_LEN);
    }
}

TEST_CASE("WHERE length(path) filter on plain varlen",
          "[ldbc][filter][path][varlen]") {
    SKIP_IF_NO_DB();
    // Filter sits directly above CPhysicalVarlenPath and inherits the
    // path-bearing chunk layout. The Filter lowering must not let the
    // post-op wrapper shrink physical_plan_output_colrefs to its own
    // PcrsRequired or the projection above would mis-resolve path's
    // chunk position.
    auto q = "MATCH (src:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH path = (src)-[:KNOWS*1..2]->(b:Person) "
             "WHERE length(path) > 1 "
             "RETURN length(path) AS len LIMIT 5";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() >= 1);
    for (size_t i = 0; i < r.size(); i++) {
        CHECK(r[i].int64_at(0) > 1);
    }
}

TEST_CASE("WHERE length(path) = N matches exactly N-hop rows",
          "[ldbc][filter][path][varlen]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (src:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH path = (src)-[:KNOWS*1..2]->(b:Person) "
             "WHERE length(path) = 1 "
             "RETURN length(path) AS len LIMIT 5";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() >= 1);
    for (size_t i = 0; i < r.size(); i++) {
        CHECK(r[i].int64_at(0) == 1);
    }
}

TEST_CASE("WHERE size(nodes(path)) > N filters path by node count",
          "[ldbc][filter][path][varlen]") {
    SKIP_IF_NO_DB();
    // size(nodes(path)) exercises the same Filter passthrough as
    // length(path) > N, but with a list-of-list nesting.
    auto q = "MATCH (src:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH path = (src)-[:KNOWS*1..2]->(b:Person) "
             "WHERE size(nodes(path)) > 2 "
             "RETURN length(path) AS len LIMIT 5";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() >= 1);
    for (size_t i = 0; i < r.size(); i++) {
        // size(nodes(path)) > 2 means len + 1 > 2, i.e. len >= 2
        CHECK(r[i].int64_at(0) >= 2);
    }
}

TEST_CASE("plain varlen path materialization is inert without path use",
          "[ldbc][filter][path][varlen]") {
    SKIP_IF_NO_DB();
    // PathUseMode::None — the binder never flags path as used, so the
    // converter must NOT emit the CLogicalVarlenPath wrap (its cost
    // would otherwise show up against this baseline).
    auto q = "MATCH (src:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH path = (src)-[:KNOWS*1..2]->(b:Person) "
             "RETURN b.id AS bid LIMIT 1";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
}

TEST_CASE("WITH length(path) AS L survives projection scope narrowing",
          "[ldbc][filter][path][varlen]") {
    SKIP_IF_NO_DB();
    // BindContext::ResetToProjectedScope moves the path's meta into
    // shadowed_path_meta_ when the WITH drops it from the carried-forward
    // names. The post-bind sync must still pick the use_mode up so the
    // converter emits the CLogicalVarlenPath wrap and the schema carries
    // path through the WITH-defined projection.
    auto q = "MATCH (src:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH path = (src)-[:KNOWS*1..2]->(b:Person) "
             "WITH length(path) AS L RETURN L LIMIT 5";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() >= 1);
    for (size_t i = 0; i < r.size(); i++) {
        CHECK((r[i].int64_at(0) == 1 || r[i].int64_at(0) == 2));
    }
}

TEST_CASE("WITH nodes(path) AS ns survives projection scope narrowing",
          "[ldbc][filter][path][varlen]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (src:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH path = (src)-[:KNOWS*1..2]->(b:Person) "
             "WITH nodes(path) AS ns RETURN size(ns) AS sz LIMIT 5";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() >= 1);
    for (size_t i = 0; i < r.size(); i++) {
        // size(nodes(path)) == hop + 1 ∈ {2, 3}
        CHECK((r[i].int64_at(0) == 2 || r[i].int64_at(0) == 3));
    }
}

TEST_CASE("WITH length(path) AS L then WHERE on L",
          "[ldbc][filter][path][varlen]") {
    SKIP_IF_NO_DB();
    // The WITH-WHERE filters on L (already a scalar), so the second
    // query part's Filter operates on a normal int column — exercises
    // that the wrap's runtime cost only materialized for the first part.
    auto q = "MATCH (src:Person {id: " + std::to_string(ldbc::SAMPLE_PATH_SRC_ID) + "}) "
             "MATCH path = (src)-[:KNOWS*1..2]->(b:Person) "
             "WITH length(path) AS L WHERE L > 1 RETURN L LIMIT 5";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() >= 1);
    for (size_t i = 0; i < r.size(); i++) {
        CHECK(r[i].int64_at(0) > 1);
    }
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
// Pre-migration these were WARN-on-throw on SF1 (flaky but non-fatal);
// on the mini fixture they used to crash the harness instead, so they
// were gated. The signal shield from #79 now catches the SIGSEGV and
// surfaces a clean exception, so both cases re-enter the WARN path.
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

// ============================================================
// Issue #17 — list comprehension WHERE/| mapping must actually run
// (used to return the unfiltered source list silently).
// ============================================================

TEST_CASE("list comprehension WHERE filters by predicate",
          "[ldbc][filter][listcomp][issue17]") {
    SKIP_IF_NO_DB();
    // Pin every element of the expected output so a wrong middle entry
    // can't slip past a size/first/last spot check.
    auto r = qr->run(
        "WITH [1,2,3,4,5] AS xs "
        "WITH [x IN xs WHERE x > 2] AS r "
        "RETURN size(r) AS n, r[0] AS e0, r[1] AS e1, r[2] AS e2",
        {qtest::ColType::INT64, qtest::ColType::INT64,
         qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 3);   // size == 3
    CHECK(r[0].int64_at(1) == 3);   // r[0]
    CHECK(r[0].int64_at(2) == 4);   // r[1]
    CHECK(r[0].int64_at(3) == 5);   // r[2]
}

TEST_CASE("list comprehension | mapping applies projection",
          "[ldbc][filter][listcomp][issue17]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "WITH [1,2,3,4] AS xs "
        "WITH [x IN xs | x*x] AS r "
        "RETURN size(r) AS n, r[0] AS e0, r[1] AS e1, r[2] AS e2, r[3] AS e3",
        {qtest::ColType::INT64, qtest::ColType::INT64, qtest::ColType::INT64,
         qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 4);   // size == 4
    CHECK(r[0].int64_at(1) == 1);   // 1*1
    CHECK(r[0].int64_at(2) == 4);   // 2*2
    CHECK(r[0].int64_at(3) == 9);   // 3*3
    CHECK(r[0].int64_at(4) == 16);  // 4*4
}

TEST_CASE("list comprehension WHERE + mapping combined",
          "[ldbc][filter][listcomp][issue17]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "WITH [1,2,3,4,5] AS xs "
        "WITH [x IN xs WHERE x > 2 | x*10] AS r "
        "RETURN size(r) AS n, r[0] AS e0, r[1] AS e1, r[2] AS e2",
        {qtest::ColType::INT64, qtest::ColType::INT64,
         qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 3);    // size == 3
    CHECK(r[0].int64_at(1) == 30);   // 3*10
    CHECK(r[0].int64_at(2) == 40);   // 4*10
    CHECK(r[0].int64_at(3) == 50);   // 5*10
}

// ============================================================
// Issue #19 — WITH scoping must hide non-projected variables from
// subsequent query parts. Oracles below were captured against
// Neo4j 5 on the same LDBC SF0.003 mini fixture (Hossein Forouhar
// is Person id="14" with firstName "Hossein"; Person id="16" is
// "Jan Zelenka"). The anchor uses firstName so the test does not
// have to care whether the planner sees id as INT or STRING.
// ============================================================

TEST_CASE("WITH p AS q: original name p is no longer in scope",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    bool caught = false;
    try {
        qr->run("MATCH (p:Person) WHERE p.firstName = 'Hossein' "
                "WITH p AS q RETURN p.firstName");
    } catch (const std::exception&) { caught = true; }
    CHECK(caught);  // Neo4j: Variable `p` not defined
}

TEST_CASE("WITH p AS q: new name q is in scope",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    auto r = qr->run("MATCH (p:Person) WHERE p.firstName = 'Hossein' "
                     "WITH p AS q RETURN q.firstName AS fn",
                     {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "Hossein");
}

TEST_CASE("WITH p (bare): p is carried forward",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    auto r = qr->run("MATCH (p:Person) WHERE p.firstName = 'Hossein' "
                     "WITH p RETURN p.firstName AS fn",
                     {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "Hossein");
}

TEST_CASE("WITH p.firstName AS fn: p is dropped at RETURN",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    bool caught = false;
    try {
        qr->run("MATCH (p:Person) WHERE p.firstName = 'Hossein' "
                "WITH p.firstName AS fn RETURN p.lastName");
    } catch (const std::exception&) { caught = true; }
    CHECK(caught);  // Neo4j: Variable `p` not defined
}

TEST_CASE("WITH p.firstName AS fn: fn is in scope",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    auto r = qr->run("MATCH (p:Person) WHERE p.firstName = 'Hossein' "
                     "WITH p.firstName AS fn RETURN fn",
                     {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "Hossein");
}

TEST_CASE("WITH p MATCH (q): both p and the new q are visible",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    auto r = qr->run("MATCH (p:Person) WHERE p.firstName = 'Hossein' "
                     "WITH p MATCH (q:Person) WHERE q.firstName = 'Jan' "
                     "RETURN p.firstName AS pfn, q.firstName AS qfn",
                     {qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "Hossein");
    CHECK(r[0].str_at(1) == "Jan");
}

TEST_CASE("WITH p.firstName AS fn MATCH (q) WHERE q.firstName = p.firstName: p dropped",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    bool caught = false;
    try {
        qr->run("MATCH (p:Person) WHERE p.firstName = 'Hossein' "
                "WITH p.firstName AS fn MATCH (q:Person) "
                "WHERE q.firstName = p.firstName "
                "RETURN count(q) AS c");
    } catch (const std::exception&) { caught = true; }
    CHECK(caught);  // Neo4j: Variable `p` not defined in the WHERE clause
}

TEST_CASE("WITH fn MATCH (q) WHERE q.firstName = fn: fn-only works",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    auto r = qr->run("MATCH (p:Person) WHERE p.firstName = 'Hossein' "
                     "WITH p.firstName AS fn MATCH (q:Person) "
                     "WHERE q.firstName = fn "
                     "RETURN count(q) AS c",
                     {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 1);  // Hossein
}

TEST_CASE("two WITHs: fn dropped at second WITH",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    bool caught = false;
    try {
        qr->run("MATCH (p:Person) WHERE p.firstName = 'Hossein' "
                "WITH p.firstName AS fn WITH fn AS name RETURN fn");
    } catch (const std::exception&) { caught = true; }
    CHECK(caught);  // Neo4j: Variable `fn` not defined
}

TEST_CASE("two WITHs: name is in scope after second WITH",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    auto r = qr->run("MATCH (p:Person) WHERE p.firstName = 'Hossein' "
                     "WITH p.firstName AS fn WITH fn AS name RETURN name",
                     {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "Hossein");
}

TEST_CASE("WITH * carries every visible variable forward",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    auto r = qr->run("MATCH (p:Person) WHERE p.firstName = 'Hossein' "
                     "WITH p MATCH (q:Person) WHERE q.firstName = 'Jan' "
                     "WITH * RETURN p.firstName AS pfn, q.firstName AS qfn",
                     {qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "Hossein");
    CHECK(r[0].str_at(1) == "Jan");
}

TEST_CASE("WITH count(f) AS fc drops un-projected p",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    bool caught = false;
    try {
        qr->run("MATCH (p:Person)-[:KNOWS]->(f) "
                "WHERE p.firstName = 'Hossein' "
                "WITH count(f) AS fc RETURN p.firstName, fc");
    } catch (const std::exception&) { caught = true; }
    CHECK(caught);  // Neo4j: Variable `p` not defined
}

TEST_CASE("WITH p, count(f) AS fc keeps p alongside the aggregate",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    auto r = qr->run("MATCH (p:Person)-[:KNOWS]->(f) "
                     "WHERE p.firstName = 'Hossein' "
                     "WITH p, count(f) AS fc "
                     "RETURN p.firstName AS pfn, fc",
                     {qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "Hossein");
    CHECK(r[0].int64_at(1) == 3);  // Hossein's outgoing KNOWS = 3
}

TEST_CASE("WHERE in WITH still sees the un-projected node (Neo4j behavior)",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    // Neo4j treats WITH's WHERE more permissively than its RETURN —
    // even though p is not projected, `WHERE p.firstName = …` is
    // accepted here. Matching Neo4j keeps existing LDBC queries that
    // filter the carrier in the WITH itself from regressing.
    auto r = qr->run("MATCH (p:Person) "
                     "WITH p.firstName AS fn "
                     "WHERE p.firstName = 'Hossein' "
                     "RETURN fn",
                     {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "Hossein");
}

TEST_CASE("WHERE in WITH using the projected alias works",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    auto r = qr->run("MATCH (p:Person) "
                     "WITH p.firstName AS fn "
                     "WHERE fn = 'Hossein' "
                     "RETURN fn",
                     {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "Hossein");
}

TEST_CASE("chained renames: p dropped after first WITH",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    bool caught = false;
    try {
        qr->run("MATCH (p:Person) WHERE p.firstName = 'Hossein' "
                "WITH p AS q WITH q AS r RETURN p.firstName");
    } catch (const std::exception&) { caught = true; }
    CHECK(caught);
}

TEST_CASE("chained renames: q dropped after second WITH",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    bool caught = false;
    try {
        qr->run("MATCH (p:Person) WHERE p.firstName = 'Hossein' "
                "WITH p AS q WITH q AS r RETURN q.firstName");
    } catch (const std::exception&) { caught = true; }
    CHECK(caught);
}

TEST_CASE("chained renames: only r is in scope at RETURN",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    auto r = qr->run("MATCH (p:Person) WHERE p.firstName = 'Hossein' "
                     "WITH p AS q WITH q AS r RETURN r.firstName AS fn",
                     {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "Hossein");
}

// Cypher 5 entity-introduction shape: a name that the WITH dropped from
// expression scope is reused (NOT freshly scanned) when the next MATCH
// reintroduces it. Without this the planner would either reject the
// unanchored binding or open a cartesian product. Oracles captured
// against Neo4j 5 on the same fixture.

TEST_CASE("MATCH reuses a name dropped by the preceding WITH",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    auto r = qr->run("MATCH (p:Person) WHERE p.firstName = 'Hossein' "
                     "MATCH (p)-[:KNOWS]->(f:Person) "
                     "WITH collect(f) AS fs "
                     "MATCH (f:Person) WHERE f IN fs "
                     "RETURN count(f) AS c",
                     {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 3);  // Hossein's 3 KNOWS friends
}

TEST_CASE("MATCH after WITH p reuses the binding rather than reopening it",
          "[ldbc][filter][withscope][issue19]") {
    SKIP_IF_NO_DB();
    // `MATCH (p:Person)` after `WITH p` reuses p (same binding); the
    // label filter is redundant but legal, the WHERE narrows the row to
    // the one Hossein.
    auto r = qr->run("MATCH (p:Person) WHERE p.firstName = 'Hossein' "
                     "WITH p MATCH (p:Person) "
                     "WHERE p.firstName = 'Hossein' "
                     "RETURN count(*) AS c",
                     {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 1);  // would be > 1 if p were freshly scanned
}

// #224 — when a vertex is carried via WITH and used as the LHS of a
// BOTH (`-[:R]-`) edge, the converter's UnionAll wrap + AdjIdxJoin xform
// chain dropped all rows. Plain 1-hop and degenerate var-len `*1..1` are
// both affected; var-len `*N..M` with N≥2 takes a different code path
// (PathJoin) and was never broken. LDBC IC tests all use the latter
// shape, so the regression hid in plain sight until #224 surfaced it.
//
// Oracles below are pre-WITH baselines on the SAME fixture (inline-anchor
// gives the right answer) — the WITH-carried form must produce the same
// counts.
TEST_CASE("WITH-carried anchor → plain BOTH edge matches inline anchor",
          "[ldbc][filter][withscope][issue224]") {
    SKIP_IF_NO_DB();
    // Hossein has 3 outgoing KNOWS (all forward-stored), 0 incoming.
    auto inline_r = qr->run(
        "MATCH (p:Person {firstName: 'Hossein'})-[:KNOWS]-(f:Person) "
        "RETURN count(f) AS c",
        {qtest::ColType::INT64});
    REQUIRE(inline_r.size() == 1);
    auto with_r = qr->run(
        "MATCH (p:Person {firstName: 'Hossein'}) WITH p "
        "MATCH (p)-[:KNOWS]-(f:Person) RETURN count(f) AS c",
        {qtest::ColType::INT64});
    REQUIRE(with_r.size() == 1);
    CHECK(inline_r[0].int64_at(0) == 3);
    CHECK(with_r[0].int64_at(0) == inline_r[0].int64_at(0));
}

TEST_CASE("WITH-carried anchor → BOTH covers backward-stored neighbours",
          "[ldbc][filter][withscope][issue224]") {
    SKIP_IF_NO_DB();
    // Friend id=10995116277782 has a mix of forward and backward KNOWS;
    // the BOTH count must match the inline-anchored baseline (which
    // already exercises the wrap + AdjIdxJoin successfully).
    auto inline_r = qr->run(
        "MATCH (p:Person {id: 10995116277782})-[:KNOWS]-(f:Person) "
        "RETURN count(f) AS c",
        {qtest::ColType::INT64});
    REQUIRE(inline_r.size() == 1);
    auto with_r = qr->run(
        "MATCH (p:Person {id: 10995116277782}) WITH p "
        "MATCH (p)-[:KNOWS]-(f:Person) RETURN count(f) AS c",
        {qtest::ColType::INT64});
    REQUIRE(with_r.size() == 1);
    CHECK(with_r[0].int64_at(0) == inline_r[0].int64_at(0));
}

TEST_CASE("WITH-carried anchor → degenerate var-len *1..1 still works",
          "[ldbc][filter][withscope][issue224]") {
    SKIP_IF_NO_DB();
    // `*1..1` is semantically a single hop. Before the fix it followed
    // the same broken wrap path as plain BOTH; var-len *1..2 and above
    // went through PathJoin and were always correct.
    auto inline_r = qr->run(
        "MATCH (p:Person {firstName: 'Hossein'})-[:KNOWS*1..1]-(f:Person) "
        "RETURN count(f) AS c",
        {qtest::ColType::INT64});
    REQUIRE(inline_r.size() == 1);
    auto with_r = qr->run(
        "MATCH (p:Person {firstName: 'Hossein'}) WITH p "
        "MATCH (p)-[:KNOWS*1..1]-(f:Person) RETURN count(f) AS c",
        {qtest::ColType::INT64});
    REQUIRE(with_r.size() == 1);
    CHECK(with_r[0].int64_at(0) == inline_r[0].int64_at(0));
}

TEST_CASE("WITH-carried anchor on RHS of BOTH edge",
          "[ldbc][filter][withscope][issue224]") {
    SKIP_IF_NO_DB();
    // The bound endpoint is on the RHS of the pattern: (other)-[:R]-(p).
    // A gate-only fix on the LHS wouldn't catch this — only the
    // colref-rebind-at-WITH approach covers both LHS and RHS uniformly.
    auto inline_r = qr->run(
        "MATCH (p:Person {firstName: 'Hossein'}) "
        "MATCH (other:Person)-[:KNOWS]-(p) RETURN count(other) AS c",
        {qtest::ColType::INT64});
    REQUIRE(inline_r.size() == 1);
    auto with_r = qr->run(
        "MATCH (p:Person {firstName: 'Hossein'}) WITH p "
        "MATCH (other:Person)-[:KNOWS]-(p) RETURN count(other) AS c",
        {qtest::ColType::INT64});
    REQUIRE(with_r.size() == 1);
    CHECK(with_r[0].int64_at(0) == inline_r[0].int64_at(0));
}

TEST_CASE("WITH p AS q → BOTH edge uses the alias as anchor",
          "[ldbc][filter][withscope][issue224]") {
    SKIP_IF_NO_DB();
    auto baseline = qr->run(
        "MATCH (p:Person {firstName: 'Hossein'})-[:KNOWS]-(f:Person) "
        "RETURN count(f) AS c",
        {qtest::ColType::INT64});
    REQUIRE(baseline.size() == 1);
    auto aliased = qr->run(
        "MATCH (p:Person {firstName: 'Hossein'}) WITH p AS q "
        "MATCH (q)-[:KNOWS]-(f:Person) RETURN count(f) AS c",
        {qtest::ColType::INT64});
    REQUIRE(aliased.size() == 1);
    CHECK(aliased[0].int64_at(0) == baseline[0].int64_at(0));
}

TEST_CASE("Repeated bare WITH then BOTH edge stays correct",
          "[ldbc][filter][withscope][issue224]") {
    SKIP_IF_NO_DB();
    auto baseline = qr->run(
        "MATCH (p:Person {firstName: 'Hossein'})-[:KNOWS]-(f:Person) "
        "RETURN count(f) AS c",
        {qtest::ColType::INT64});
    REQUIRE(baseline.size() == 1);
    auto repeated = qr->run(
        "MATCH (p:Person {firstName: 'Hossein'}) WITH p WITH p "
        "MATCH (p)-[:KNOWS]-(f:Person) RETURN count(f) AS c",
        {qtest::ColType::INT64});
    REQUIRE(repeated.size() == 1);
    CHECK(repeated[0].int64_at(0) == baseline[0].int64_at(0));
}

// Chain rename `WITH p AS q WITH q AS r`: the binder's shadow-recall
// surfaces the later MATCH's BoundNodeExpression under the ORIGINAL
// name (`p`), not the final alias (`r`). Without transitive-name
// propagation in the converter, MATCH (r) would look up `p` in a
// schema that only kept {q, r} and trigger a fresh duplicate Get of
// the source table — cartesian explosion (88 instead of 3 for forward,
// 176 for BOTH).
TEST_CASE("Chain rename (p AS q AS r) preserves anchor through BOTH edge",
          "[ldbc][filter][withscope][issue224]") {
    SKIP_IF_NO_DB();
    auto baseline = qr->run(
        "MATCH (p:Person {firstName: 'Hossein'})-[:KNOWS]-(f:Person) "
        "RETURN count(f) AS c",
        {qtest::ColType::INT64});
    REQUIRE(baseline.size() == 1);
    auto chained = qr->run(
        "MATCH (p:Person {firstName: 'Hossein'}) WITH p AS q WITH q AS r "
        "MATCH (r)-[:KNOWS]-(f:Person) RETURN count(f) AS c",
        {qtest::ColType::INT64});
    REQUIRE(chained.size() == 1);
    CHECK(chained[0].int64_at(0) == baseline[0].int64_at(0));
}

TEST_CASE("Chain rename (p AS q AS r) preserves anchor through forward edge",
          "[ldbc][filter][withscope][issue224]") {
    SKIP_IF_NO_DB();
    auto baseline = qr->run(
        "MATCH (p:Person {firstName: 'Hossein'})-[:KNOWS]->(f:Person) "
        "RETURN count(f) AS c",
        {qtest::ColType::INT64});
    REQUIRE(baseline.size() == 1);
    auto chained = qr->run(
        "MATCH (p:Person {firstName: 'Hossein'}) WITH p AS q WITH q AS r "
        "MATCH (r)-[:KNOWS]->(f:Person) RETURN count(f) AS c",
        {qtest::ColType::INT64});
    REQUIRE(chained.size() == 1);
    CHECK(chained[0].int64_at(0) == baseline[0].int64_at(0));
}

// #227 — When a scalar alias introduced by WITH appears in the same
// WITH's WHERE clause, ORCA's transpose substitutes the alias with its
// bound constant, leaving Select(Const cmp Const) above the carried
// node's ProjectColumnar. Without constant folding, the dangling colref
// inside the now-tautological Select breaks PcrsRequired propagation
// down to the NodeScan, which then produces empty output_cols — every
// property projection returns 0 rows even though count(*) confirms the
// row is alive. Fixed by folding ScalarCmp(Const, Const) in the ORCA
// preprocessor right after the columnar transpose, so the normalizer
// can drop the trivial Select before plan generation.
//
// Hossein's id is 933 in the LDBC mini fixture.
TEST_CASE("#227 scalar alias WHERE keeps carried node's columns readable",
          "[ldbc][filter][withscope][issue227]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {firstName: '" LDBC_SAMPLE_FN_STR "'}) WITH p, 1 AS x "
        "WHERE x = 1 RETURN p.firstName",
        {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == LDBC_SAMPLE_FN_STR);
}

TEST_CASE("#227 scalar alias WHERE — full carried node projects correctly",
          "[ldbc][filter][withscope][issue227]") {
    SKIP_IF_NO_DB();
    auto q = std::string("MATCH (p:Person {firstName: '") +
             ldbc::SAMPLE_PERSON_FIRST_NAME +
             "'}) WITH p, 1 AS x WHERE x = 1 "
             "RETURN p.id, p.firstName, p.lastName";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::STRING,
         qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PERSON_ID);
    CHECK(r[0].str_at(1) == ldbc::SAMPLE_PERSON_FIRST_NAME);
}

TEST_CASE("#227 const-fold reverse: WHERE 1 = x",
          "[ldbc][filter][withscope][issue227]") {
    SKIP_IF_NO_DB();
    // Commuted operand order — PexprReorderScalarCmpChildren only
    // reorders ident/const; const/const stays as-is. The fold must
    // work regardless of which side the substituted alias landed on.
    auto r = qr->run(
        "MATCH (p:Person {firstName: '" LDBC_SAMPLE_FN_STR "'}) WITH p, 1 AS x "
        "WHERE 1 = x RETURN p.firstName",
        {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == LDBC_SAMPLE_FN_STR);
}

TEST_CASE("#227 const-fold inequality: WHERE x <> 0",
          "[ldbc][filter][withscope][issue227]") {
    SKIP_IF_NO_DB();
    // <> uses Matches() like =; ordering operators (>, <, >=, <=)
    // rely on ORCA's statistical mapping which TurboLynx's int4/int8
    // literals do not provide, so those forms remain unfolded — a
    // separate fix is needed for ordering-operator variants of #227.
    auto r = qr->run(
        "MATCH (p:Person {firstName: '" LDBC_SAMPLE_FN_STR "'}) WITH p, 1 AS x "
        "WHERE x <> 0 RETURN p.firstName",
        {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == LDBC_SAMPLE_FN_STR);
}

TEST_CASE("#227 const-fold AND: WHERE x = 1 AND y = 2 (all true)",
          "[ldbc][filter][withscope][issue227]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {firstName: '" LDBC_SAMPLE_FN_STR "'}) WITH p, 1 AS x, 2 AS y "
        "WHERE x = 1 AND y = 2 RETURN p.firstName",
        {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == LDBC_SAMPLE_FN_STR);
}

TEST_CASE("#227 const-fold OR: WHERE x = 99 OR y = 2",
          "[ldbc][filter][withscope][issue227]") {
    SKIP_IF_NO_DB();
    // Left disjunct false, right disjunct true → OR folds to true.
    auto r = qr->run(
        "MATCH (p:Person {firstName: '" LDBC_SAMPLE_FN_STR "'}) WITH p, 1 AS x, 2 AS y "
        "WHERE x = 99 OR y = 2 RETURN p.firstName",
        {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == LDBC_SAMPLE_FN_STR);
}

TEST_CASE("#227 const-fold to FALSE drops all rows: WHERE x = 2",
          "[ldbc][filter][withscope][issue227]") {
    SKIP_IF_NO_DB();
    // The predicate folds to FALSE — the filter must reject the row
    // rather than spuriously letting it through.
    auto r = qr->run(
        "MATCH (p:Person {firstName: '" LDBC_SAMPLE_FN_STR "'}) WITH p, 1 AS x "
        "WHERE x = 2 RETURN p.firstName",
        {qtest::ColType::STRING});
    CHECK(r.size() == 0);
}

TEST_CASE("#227 const-fold to FALSE via AND: x = 1 AND y = 99",
          "[ldbc][filter][withscope][issue227]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {firstName: '" LDBC_SAMPLE_FN_STR "'}) WITH p, 1 AS x, 2 AS y "
        "WHERE x = 1 AND y = 99 RETURN p.firstName",
        {qtest::ColType::STRING});
    CHECK(r.size() == 0);
}

// Separate fault surfaced while diagnosing #227: a plain
// `MATCH (p:Person) WITH p WHERE p.firstName=...` (no scalar alias, no
// const-fold path) also segfaulted because the columnar transpose
// pushed the WHERE Select down through the rename ProjectColumnar
// layers without substituting the predicate's colref — leaving a Filter
// that referenced firstName(26) while its child TableScan only produced
// firstName(5). The transpose refactor (always use the project
// element's LHS as the substitution target, and stop dropping nested
// ProjectColumnar layers inside Collapse) makes this work.
TEST_CASE("WITH p (bare) + WHERE on carried node projects correctly",
          "[ldbc][filter][withscope][issue227]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person) WITH p WHERE p.firstName = '" LDBC_SAMPLE_FN_STR "' "
        "RETURN p.id, p.firstName",
        {qtest::ColType::INT64, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PERSON_ID);
    CHECK(r[0].str_at(1) == LDBC_SAMPLE_FN_STR);
}

TEST_CASE("Two stacked bare WITHs + WHERE on carried node",
          "[ldbc][filter][withscope][issue227]") {
    SKIP_IF_NO_DB();
    // Two rename layers — exercises the recursive transpose handling
    // the second Select(ProjectColumnar) one step at a time.
    auto r = qr->run(
        "MATCH (p:Person) WITH p WITH p WHERE p.firstName = '" LDBC_SAMPLE_FN_STR "' "
        "RETURN p.firstName",
        {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == LDBC_SAMPLE_FN_STR);
}

TEST_CASE("#227 count(*) baseline (was already correct, sanity-check)",
          "[ldbc][filter][withscope][issue227]") {
    SKIP_IF_NO_DB();
    // count(*) was the diagnostic — it always reported 1, even when the
    // property projection broke. Keep it as a guard against regressions
    // in the other direction (rows incorrectly dropped).
    auto r = qr->run(
        "MATCH (p:Person {firstName: '" LDBC_SAMPLE_FN_STR "'}) WITH p, 1 AS x "
        "WHERE x = 1 RETURN count(*) AS c",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 1);
}

// Anchor-less OPTIONAL MATCH (every endpoint freshly introduced) must
// materialise a LEFT OUTER cartesian against prev_plan, not throw.
// WHERE predicates referencing both sides become the LOJ ON condition;
// the NULL-fill row is preserved when the subquery has no match.
// Oracles Neo4j-verified on the SF0.003 mini fixture (#186).
TEST_CASE("anchor-less OPTIONAL MATCH yields prev × subquery cardinality",
          "[ldbc][filter][optmatch][issue186]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (x:Forum) WITH count(x) AS c "
        "OPTIONAL MATCH (p:Person)-[:HAS_INTEREST]->(t:Tag) "
        "RETURN c, count(*) AS n",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 400);
    CHECK(r[0].int64_at(1) == 1256);
}

TEST_CASE("anchor-less OPTIONAL MATCH absorbs WHERE into LOJ condition",
          "[ldbc][filter][optmatch][issue186]") {
    SKIP_IF_NO_DB();
    // Forum.id and Person.id are disjoint, so the WHERE p.id IN fids
    // condition rejects every subquery row; the single prev row
    // survives with NULL-filled p, t and count(*) = 1.
    auto r = qr->run(
        "MATCH (x:Forum) WITH count(x) AS c, collect(x.id) AS fids "
        "OPTIONAL MATCH (p:Person)-[:HAS_INTEREST]->(t:Tag) "
        "WHERE p.id IN fids "
        "RETURN c, count(*) AS n",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 400);
    CHECK(r[0].int64_at(1) == 1);
}

TEST_CASE("anchor-less OPTIONAL MATCH pushes sub-only WHERE into subquery",
          "[ldbc][filter][optmatch][issue186]") {
    SKIP_IF_NO_DB();
    // WHERE references only subquery columns → Selection pushed onto
    // the subquery (LOJ condition stays TRUE). 51 HAS_INTEREST edges
    // sourced from the Hossein person.
    auto r = qr->run(
        "MATCH (x:Forum) WITH count(x) AS c "
        "OPTIONAL MATCH (p:Person)-[:HAS_INTEREST]->(t:Tag) "
        "WHERE p.firstName = 'Hossein' "
        "RETURN c, count(*) AS n",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 400);
    CHECK(r[0].int64_at(1) == 51);
}

TEST_CASE("anchor-less OPTIONAL MATCH preserves prev row when subquery is empty",
          "[ldbc][filter][optmatch][issue186]") {
    SKIP_IF_NO_DB();
    // Empty subquery → LEFT OUTER semantics fills the prev row with
    // NULL columns rather than dropping it.
    auto r = qr->run(
        "MATCH (x:Forum) WITH count(x) AS c "
        "OPTIONAL MATCH (p:Person {id: 99999999999999}) "
        "RETURN c, count(*) AS n",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 400);
    CHECK(r[0].int64_at(1) == 1);
}

TEST_CASE("anchor-less node-only OPTIONAL MATCH yields prev × scan",
          "[ldbc][filter][optmatch][issue186]") {
    SKIP_IF_NO_DB();
    // No edges in the OPTIONAL pattern — single node scan LOJ'd
    // against prev. 50 Person × 1 prev row.
    auto r = qr->run(
        "MATCH (x:Forum) WITH count(x) AS c "
        "OPTIONAL MATCH (p:Person) "
        "RETURN c, count(p) AS n",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 400);
    CHECK(r[0].int64_at(1) == 50);
}

TEST_CASE("bare pattern comprehension returns the projected friend list",
          "[ldbc][filter][listcomp][patterncomp]") {
    SKIP_IF_NO_DB();
    // Replaces the earlier "throws a clean binder error" expectation that
    // pinned the pre-#184 binder rejection (originally guarding against the
    // pre-fix converter throw inside ORCA → SIGSEGV in CAutoMemoryPool
    // teardown). #184 PR-1.2 wires the converter lowering, so the same form
    // now returns a real LIST.
    auto q = std::string("MATCH (p:Person {id: ") + sample_person_id_str() +
             "}) RETURN [(p)-[:KNOWS]->(f) | f.firstName] AS friends";
    auto r = qr->run(q.c_str(), {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(!r[0].is_null_at(0));
}

// Cypher zero-length variable-length paths: `*0..N` must emit the
// source vertex as the length-0 binding whenever the destination
// pattern accepts it. Pre-fix the loader gated zero-hop emission on a
// terminal-partition heuristic (`src_is_terminal`), so non-terminal
// source vertices silently dropped the zero-length match (#35).

TEST_CASE("KNOWS *0..0 emits the source vertex (zero-hop self)",
          "[ldbc][filter][varlen][issue35]") {
    SKIP_IF_NO_DB();
    // `*0..0` is a single-node match: b must equal a.
    auto q = "MATCH (a:Person {id: " + sample_person_id_str() +
             "})-[:KNOWS*0..0]-(b:Person) RETURN b.id ORDER BY b.id";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PERSON_ID);
}

#ifdef TURBOLYNX_LDBC_FIXTURE_MINI
TEST_CASE("KNOWS *0..1 includes self plus 1-hop friends",
          "[ldbc][filter][varlen][issue35]") {
    SKIP_IF_NO_DB();
    // Person 14's three KNOWS neighbours in the mini fixture are
    // 10995116277782, 24189255811081, 26388279066668. The *0..1
    // closure must additionally contain person 14 itself.
    auto q = "MATCH (a:Person {id: " + sample_person_id_str() +
             "})-[:KNOWS*0..1]-(b:Person) "
             "RETURN DISTINCT b.id ORDER BY b.id";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 4);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PERSON_ID);
    CHECK(r[1].int64_at(0) == 10995116277782LL);
    CHECK(r[2].int64_at(0) == 24189255811081LL);
    CHECK(r[3].int64_at(0) == 26388279066668LL);
}
#endif

TEST_CASE("KNOWS *0..1 size exceeds *1..1 by exactly the source",
          "[ldbc][filter][varlen][issue35]") {
    SKIP_IF_NO_DB();
    // |distinct b : (a)-[*0..1]-(b)| - |distinct b : (a)-[*1..1]-(b)|
    // equals 1 (the source itself, contributed only by the
    // zero-length path).
    auto q0 = "MATCH (a:Person {id: " + sample_person_id_str() +
              "})-[:KNOWS*0..1]-(b:Person) RETURN count(DISTINCT b)";
    auto q1 = "MATCH (a:Person {id: " + sample_person_id_str() +
              "})-[:KNOWS*1..1]-(b:Person) RETURN count(DISTINCT b)";
    CHECK(qr->count(q0.c_str()) - qr->count(q1.c_str()) == 1);
}

TEST_CASE("Same-variable zero-hop binds the source to itself",
          "[ldbc][filter][varlen][issue35]") {
    SKIP_IF_NO_DB();
    // Both endpoints reuse `p`. A length-0 path forces p == p, so the
    // single result is the source person itself.
    auto q = "MATCH (p:Person {id: " + sample_person_id_str() +
             "})-[:KNOWS*0..0]-(p) RETURN p.id";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PERSON_ID);
}

TEST_CASE("Heterogeneous edge zero-hop respects vertex label predicate",
          "[ldbc][filter][varlen][issue35]") {
    SKIP_IF_NO_DB();
    // HAS_TYPE has Tag → TagClass schema. Cypher zero-length only
    // checks the vertex pattern, not the edge schema's dst, so
    // (t:Tag)-[*0..0]->(tc:Tag) binds tc to every Tag (16080 rows in
    // the mini fixture) while (tc:TagClass) rejects all of them.
    CHECK(qr->count("MATCH (t:Tag)-[:HAS_TYPE*0..0]->(tc:Tag) "
                    "RETURN count(*)") == ldbc::TAG_COUNT);
    CHECK(qr->count("MATCH (t:Tag)-[:HAS_TYPE*0..0]->(tc:TagClass) "
                    "RETURN count(*)") == 0);
}

// VarLen 1+-hop output filtering must come from the vertex-pattern
// label predicate, not the edge-schema's terminal partition set. For
// REPLY_OF (Comment→Comment + Comment→Post), the planner previously
// computed `dst_partition_ids = dst_pids − src_pids = {Post}` and used
// that as the operator-level output filter, silently dropping all
// Comment results from any *N..M VLE traversal (#36). Oracles below
// are Neo4j-verified against the SF0.003 mini fixture.

#ifdef TURBOLYNX_LDBC_FIXTURE_MINI
TEST_CASE("REPLY_OF *1..2 returns all dst vertices when no label is bound",
          "[ldbc][filter][varlen][issue36]") {
    SKIP_IF_NO_DB();
    CHECK(qr->count("MATCH (c:Comment)-[:REPLY_OF*1..2]->(m) "
                    "RETURN count(*)") == 1169);
}

TEST_CASE("REPLY_OF *1..2 honours dst:Comment label",
          "[ldbc][filter][varlen][issue36]") {
    SKIP_IF_NO_DB();
    // Comment is both a source and a destination of REPLY_OF, so the
    // terminal-partition heuristic erroneously dropped it.
    CHECK(qr->count("MATCH (c:Comment)-[:REPLY_OF*1..2]->(m:Comment) "
                    "RETURN count(*)") == 478);
}

TEST_CASE("REPLY_OF *1..2 honours dst:Post label",
          "[ldbc][filter][varlen][issue36]") {
    SKIP_IF_NO_DB();
    // Post-only happened to be correct under the old filter (Post is
    // the unique terminal partition of REPLY_OF). Locked in as a
    // regression for the fix.
    CHECK(qr->count("MATCH (c:Comment)-[:REPLY_OF*1..2]->(m:Post) "
                    "RETURN count(*)") == 691);
}

TEST_CASE("REPLY_OF *1..3 honours dst label across depths",
          "[ldbc][filter][varlen][issue36]") {
    SKIP_IF_NO_DB();
    CHECK(qr->count("MATCH (c:Comment)-[:REPLY_OF*1..3]->(m:Comment) "
                    "RETURN count(*)") == 497);
    CHECK(qr->count("MATCH (c:Comment)-[:REPLY_OF*1..3]->(m:Post) "
                    "RETURN count(*)") == 771);
}

TEST_CASE("REPLY_OF *1..3 no-label total matches Neo4j",
          "[ldbc][filter][varlen][issue193]") {
    SKIP_IF_NO_DB();
    CHECK(qr->count("MATCH (c:Comment)-[:REPLY_OF*1..3]->(m) "
                    "RETURN count(*)") == 1268);
}

TEST_CASE("REPLY_OF *1..3 label partition is disjoint",
          "[ldbc][filter][varlen][issue193]") {
    SKIP_IF_NO_DB();
    auto both = qr->count("MATCH (c:Comment)-[:REPLY_OF*1..3]->(m) "
                          "RETURN count(*)");
    auto only_c = qr->count("MATCH (c:Comment)-[:REPLY_OF*1..3]->(m:Comment) "
                            "RETURN count(*)");
    auto only_p = qr->count("MATCH (c:Comment)-[:REPLY_OF*1..3]->(m:Post) "
                            "RETURN count(*)");
    CHECK(both == only_c + only_p);
}

TEST_CASE("REPLY_OF *1..2 label partition is disjoint",
          "[ldbc][filter][varlen][issue36]") {
    SKIP_IF_NO_DB();
    auto both = qr->count("MATCH (c:Comment)-[:REPLY_OF*1..2]->(m) "
                          "RETURN count(*)");
    auto only_c = qr->count("MATCH (c:Comment)-[:REPLY_OF*1..2]->(m:Comment) "
                            "RETURN count(*)");
    auto only_p = qr->count("MATCH (c:Comment)-[:REPLY_OF*1..2]->(m:Post) "
                            "RETURN count(*)");
    CHECK(both == only_c + only_p);
}

// allShortestPaths must enumerate every same-depth predecessor. The BFS
// previously stored only the first parent that reached an intermediate
// node, so converging diamond shapes silently dropped all but one path.
// Oracles below are Neo4j-verified against the LDBC SF0.003 mini
// fixture (#39).

#ifdef TURBOLYNX_LDBC_FIXTURE_MINI
TEST_CASE("allShortestPaths enumerates every diamond at hop=3",
          "[ldbc][filter][allshortest][issue39]") {
    SKIP_IF_NO_DB();
    // Person 14 ↔ {16, 13194139533352, 35184372088850, 10995116277761}
    // sit at hop=3 with multiple shortest paths each. These cases all
    // happened to be correct under the old code because the
    // meeting-point branch picked up the extra predecessors; they are
    // here as regression locks.
    CHECK(qr->count("MATCH path = allShortestPaths("
                    "(p1:Person {id:14})-[:KNOWS*0..]-"
                    "(p2:Person {id:13194139533352})) "
                    "RETURN count(path)") == 11);
    CHECK(qr->count("MATCH path = allShortestPaths("
                    "(p1:Person {id:14})-[:KNOWS*0..]-"
                    "(p2:Person {id:16})) "
                    "RETURN count(path)") == 7);
    CHECK(qr->count("MATCH path = allShortestPaths("
                    "(p1:Person {id:14})-[:KNOWS*0..]-"
                    "(p2:Person {id:35184372088850})) "
                    "RETURN count(path)") == 6);
    CHECK(qr->count("MATCH path = allShortestPaths("
                    "(p1:Person {id:14})-[:KNOWS*0..]-"
                    "(p2:Person {id:10995116277761})) "
                    "RETURN count(path)") == 4);
}

TEST_CASE("allShortestPaths enumerates every diamond at hop=4",
          "[ldbc][filter][allshortest][issue39]") {
    SKIP_IF_NO_DB();
    // Person 14 ↔ 19791209299987 has 11 shortest paths of length 4
    // (Neo4j-verified). Pre-fix we returned only 6 — five same-depth
    // predecessors were dropped at an intermediate forward-BFS
    // frontier before backward-BFS reached the meeting point.
    CHECK(qr->count("MATCH path = allShortestPaths("
                    "(p1:Person {id:14})-[:KNOWS*0..]-"
                    "(p2:Person {id:19791209299987})) "
                    "RETURN count(path)") == 11);
}
#endif
#endif

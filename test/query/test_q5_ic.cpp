// Stage 5 — LDBC Interactive Complex (IC) queries
// All expected values verified against Neo4j 5.24.0 with LDBC SF1.

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

// IC1 simplified — shortestPath distance + friend info
// Tests shortestPath with bidirectional BFS on KNOWS.
TEST_CASE("Q5-IC1-basic shortestPath with friend properties", "[q5][ic]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {id: 30786325583618}), (friend:Person {firstName: 'Chau'}) "
        "WHERE NOT p=friend "
        "WITH p, friend "
        "MATCH path = shortestPath((p)-[:KNOWS*1..3]-(friend)) "
        "RETURN friend.id AS friendId, friend.lastName AS friendLastName, "
        "       path_length(path) AS distance "
        "ORDER BY distance ASC, friendLastName ASC, friendId ASC "
        "LIMIT 20",
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() >= 18);

    // All distance-2 friends should come first
    // row 0: Do (9101), distance=2
    CHECK(r[0].int64_at(0) == 9101);
    CHECK(r[0].str_at(1) == "Do");
    CHECK(r[0].int64_at(2) == 2);

    // row 1: Ha (15393162793500), distance=2
    CHECK(r[1].int64_at(0) == 15393162793500LL);
    CHECK(r[1].str_at(1) == "Ha");
    CHECK(r[1].int64_at(2) == 2);

    // row 2: Ho (10995116285282), distance=2
    CHECK(r[2].int64_at(0) == 10995116285282LL);
    CHECK(r[2].str_at(1) == "Ho");
    CHECK(r[2].int64_at(2) == 2);

    // row 3: Ho (19791209303405), distance=2
    CHECK(r[3].int64_at(0) == 19791209303405LL);
    CHECK(r[3].str_at(1) == "Ho");
    CHECK(r[3].int64_at(2) == 2);

    // row 4: Ho (28587302323020), distance=2
    CHECK(r[4].int64_at(0) == 28587302323020LL);
    CHECK(r[4].str_at(1) == "Ho");
    CHECK(r[4].int64_at(2) == 2);

    // row 5: Ho (32985348842021), distance=2
    CHECK(r[5].int64_at(0) == 32985348842021LL);
    CHECK(r[5].str_at(1) == "Ho");
    CHECK(r[5].int64_at(2) == 2);

    // row 6: Loan (6597069771031), distance=2
    CHECK(r[6].int64_at(0) == 6597069771031LL);
    CHECK(r[6].str_at(1) == "Loan");
    CHECK(r[6].int64_at(2) == 2);

    // row 7: Loan (13194139544258), distance=2
    CHECK(r[7].int64_at(0) == 13194139544258LL);
    CHECK(r[7].str_at(1) == "Loan");
    CHECK(r[7].int64_at(2) == 2);

    // row 8: Loan (26388279076217), distance=2
    CHECK(r[8].int64_at(0) == 26388279076217LL);
    CHECK(r[8].str_at(1) == "Loan");
    CHECK(r[8].int64_at(2) == 2);

    // row 9: Nguyen (4848), distance=2
    CHECK(r[9].int64_at(0) == 4848);
    CHECK(r[9].str_at(1) == "Nguyen");
    CHECK(r[9].int64_at(2) == 2);

    // row 10: Nguyen (2199023265573), distance=2
    CHECK(r[10].int64_at(0) == 2199023265573LL);
    CHECK(r[10].str_at(1) == "Nguyen");
    CHECK(r[10].int64_at(2) == 2);

    // row 11: Nguyen (8796093031224), distance=2
    CHECK(r[11].int64_at(0) == 8796093031224LL);
    CHECK(r[11].str_at(1) == "Nguyen");
    CHECK(r[11].int64_at(2) == 2);

    // row 12: Nguyen (26388279068635), distance=2
    CHECK(r[12].int64_at(0) == 26388279068635LL);
    CHECK(r[12].str_at(1) == "Nguyen");
    CHECK(r[12].int64_at(2) == 2);

    // row 13: Nguyen (28587302322743), distance=2
    CHECK(r[13].int64_at(0) == 28587302322743LL);
    CHECK(r[13].str_at(1) == "Nguyen");
    CHECK(r[13].int64_at(2) == 2);

    // rows 14-17: distance=3
    CHECK(r[14].int64_at(2) == 3);
    CHECK(r[15].int64_at(2) == 3);
    CHECK(r[16].int64_at(2) == 3);
    CHECK(r[17].int64_at(2) == 3);
}

// IC1 full — multi-stage WITH, shortestPath + min aggregation,
// OPTIONAL MATCH, collect, IS_LOCATED_IN, STUDY_AT, WORK_AT
TEST_CASE("Q5-IC1-full shortestPath with collect and OPTIONAL MATCH", "[q5][ic]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {id: 30786325583618}), (friend:Person {firstName: 'Chau'}) "
        "WHERE NOT p=friend "
        "WITH p, friend "
        "MATCH path = shortestPath((p)-[:KNOWS*1..3]-(friend)) "
        "WITH min(path_length(path)) AS distance, friend "
        "ORDER BY distance ASC, friend.lastName ASC, friend.id ASC "
        "LIMIT 20 "
        "MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:City) "
        "OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:Organisation)"
        "  -[:IS_LOCATED_IN]->(uniCity:Place) "
        "WITH friend, collect(uni.name) AS unis, friendCity, distance "
        "OPTIONAL MATCH (friend)-[workAt:WORK_AT]->(company:Organisation)"
        "  -[:IS_LOCATED_IN]->(companyCountry:Place) "
        "WITH friend, collect(company.name) AS companies, unis, friendCity, distance "
        "RETURN "
        "  friend.id AS friendId, "
        "  friend.lastName AS friendLastName, "
        "  distance AS distanceFromPerson, "
        "  friendCity.name AS friendCityName, "
        "  unis AS friendUniversities, "
        "  companies AS friendCompanies "
        "ORDER BY distanceFromPerson ASC, friendLastName ASC, friendId ASC "
        "LIMIT 20",
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::INT64,
         qtest::ColType::STRING, qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 18);

    // row 0: Do (9101), distance=2, city=Quảng_Ngãi
    CHECK(r[0].int64_at(0) == 9101);
    CHECK(r[0].str_at(1) == "Do");
    CHECK(r[0].int64_at(2) == 2);

    // row 6: Loan (6597069771031), distance=2
    CHECK(r[6].int64_at(0) == 6597069771031LL);
    CHECK(r[6].str_at(1) == "Loan");
    CHECK(r[6].int64_at(2) == 2);

    // row 9: Nguyen (4848), distance=2
    CHECK(r[9].int64_at(0) == 4848);
    CHECK(r[9].str_at(1) == "Nguyen");
    CHECK(r[9].int64_at(2) == 2);

    // row 14: Ha (15393162789090), distance=3
    CHECK(r[14].int64_at(0) == 15393162789090LL);
    CHECK(r[14].str_at(1) == "Ha");
    CHECK(r[14].int64_at(2) == 3);

    // row 17: Nguyen (26388279072379), distance=3
    CHECK(r[17].int64_at(0) == 26388279072379LL);
    CHECK(r[17].str_at(1) == "Nguyen");
    CHECK(r[17].int64_at(2) == 3);
}

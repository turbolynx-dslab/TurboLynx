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

// IC2 — recent messages of friends
// Tests: 2-hop traversal, WHERE <=, coalesce, ORDER BY DESC+ASC, Message union label
TEST_CASE("Q5-IC2 recent messages of friends", "[q5][ic]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (n:Person {id: 17592186052613})-[:KNOWS]-(friend:Person)"
        "  <-[:HAS_CREATOR]-(message:Message) "
        "WHERE message.creationDate <= 1354060800000 "
        "RETURN "
        "  friend.id AS personId, "
        "  friend.firstName AS personFirstName, "
        "  friend.lastName AS personLastName, "
        "  message.id AS postOrCommentId, "
        "  coalesce(message.content, message.imageFile) AS postOrCommentContent, "
        "  message.creationDate AS postOrCommentCreationDate "
        "ORDER BY postOrCommentCreationDate DESC, postOrCommentId ASC "
        "LIMIT 20",
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING,
         qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 20);

    // row 0: Bill Moore, message 2199023411115, date 1347527459367
    CHECK(r[0].int64_at(0) == 6597069773262LL);
    CHECK(r[0].str_at(1) == "Bill");
    CHECK(r[0].str_at(2) == "Moore");
    CHECK(r[0].int64_at(3) == 2199023411115LL);
    CHECK(r[0].int64_at(5) == 1347527459367LL);

    // row 1: Hamani Diori, message 2199029688182
    CHECK(r[1].int64_at(0) == 13194139535625LL);
    CHECK(r[1].str_at(1) == "Hamani");
    CHECK(r[1].int64_at(3) == 2199029688182LL);
    CHECK(r[1].int64_at(5) == 1347525845869LL);

    // row 2: Gheorghe Popescu, "no way!"
    CHECK(r[2].int64_at(0) == 8796093031506LL);
    CHECK(r[2].str_at(4) == "no way!");
    CHECK(r[2].int64_at(5) == 1347505401921LL);

    // row 5: Chengdong Li, "roflol"
    CHECK(r[5].int64_at(0) == 2199023261325LL);
    CHECK(r[5].str_at(4) == "roflol");

    // row 10: Benhalima Ferrer, "thanks"
    CHECK(r[10].int64_at(0) == 13194139533574LL);
    CHECK(r[10].str_at(4) == "thanks");

    // row 19 (last): Zaenal Budjana, about Botswana
    CHECK(r[19].int64_at(0) == 8796093029854LL);
    CHECK(r[19].int64_at(5) == 1347396666367LL);

    // Verify DESC ordering: dates should be non-increasing
    for (size_t i = 1; i < r.size(); i++) {
        CHECK(r[i].int64_at(5) <= r[i-1].int64_at(5));
    }
}

// IC3 — original LDBC IC3 query (friends-of-friends messaging in two countries)
// Tests: Country/City sub-labels, IN operator, chained comparison (a > x >= b),
//        collect() aggregation, NOT x IN list, CASE WHEN with node variable comparison,
//        sum(alias) from WITH, WHERE after WITH aggregation, arithmetic on aliases,
//        KNOWS*1..2 undirected, multi-stage WITH, multiple MATCH clauses
TEST_CASE("Q5-IC3 friends in countries", "[q5][ic][ic3]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (countryX:Country {name: 'Laos'}), "
        "      (countryY:Country {name: 'Scotland'}), "
        "      (person:Person {id: 17592186055119}) "
        "WITH person, countryX, countryY "
        "LIMIT 1 "
        "MATCH (city:City)-[:IS_PART_OF]->(country:Country) "
        "WHERE country IN [countryX, countryY] "
        "WITH person, countryX, countryY, collect(city) AS cities "
        "MATCH (person)-[:KNOWS*1..2]-(friend)-[:IS_LOCATED_IN]->(city) "
        "WHERE NOT person=friend AND NOT city IN cities "
        "WITH DISTINCT friend, countryX, countryY "
        "MATCH (friend)<-[:HAS_CREATOR]-(message), "
        "      (message)-[:IS_LOCATED_IN]->(country) "
        "WHERE 1310515200000 > message.creationDate >= 1306886400000 AND "
        "      country IN [countryX, countryY] "
        "WITH friend, "
        "     CASE WHEN country=countryX THEN 1 ELSE 0 END AS messageX, "
        "     CASE WHEN country=countryY THEN 1 ELSE 0 END AS messageY "
        "WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount "
        "WHERE xCount>0 AND yCount>0 "
        "RETURN friend.id AS friendId, "
        "       friend.firstName AS friendFirstName, "
        "       friend.lastName AS friendLastName, "
        "       xCount, "
        "       yCount, "
        "       xCount + yCount AS xyCount "
        "ORDER BY xyCount DESC, friendId ASC "
        "LIMIT 20",
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING,
         qtest::ColType::INT64, qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 2);

    // row 0: Francisco Gonzalez, 1 Laos msg, 2 Scotland msgs, 3 total
    CHECK(r[0].int64_at(0) == 26388279070362LL);
    CHECK(r[0].str_at(1) == "Francisco");
    CHECK(r[0].str_at(2) == "Gonzalez");
    CHECK(r[0].int64_at(3) == 1);
    CHECK(r[0].int64_at(4) == 2);
    CHECK(r[0].int64_at(5) == 3);

    // row 1: Frank Jones, 1 Laos msg, 1 Scotland msg, 2 total
    CHECK(r[1].int64_at(0) == 6597069770562LL);
    CHECK(r[1].str_at(1) == "Frank");
    CHECK(r[1].str_at(2) == "Jones");
    CHECK(r[1].int64_at(3) == 1);
    CHECK(r[1].int64_at(4) == 1);
    CHECK(r[1].int64_at(5) == 2);

    // Verify DESC ordering: xyCount should be non-increasing
    for (size_t i = 1; i < r.size(); i++) {
        CHECK(r[i].int64_at(5) <= r[i-1].int64_at(5));
    }
}

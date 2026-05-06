// LDBC SNB expected vertex / edge counts.
//
// Two fixture sizes are supported by the same test sources:
//
//   * SF1     — the legacy benchmark fixture, loaded externally via
//               `scripts/load-ldbc.sh`. This is the default selection.
//               Values are the originals previously hard-coded in
//               `test_ldbc_count.cpp` ("verified against Neo4j 5.24.0").
//   * SF0.003 — the committed mini fixture under `test/data/ldbc-mini/`,
//               loaded via `scripts/load-ldbc-mini.sh` and used by CI.
//               Selected by `cmake -DTURBOLYNX_LDBC_FIXTURE_MINI=ON`.
//               Values were verified against Neo4j 5 with the same
//               fixture loaded via `neo4j-admin database import`.

#pragma once
#include <cstdint>

namespace ldbc {

#ifdef TURBOLYNX_LDBC_FIXTURE_MINI
// SF0.003 mini fixture — Neo4j-verified.
inline constexpr int64_t PERSON_COUNT       = 50;
inline constexpr int64_t COMMENT_COUNT      = 790;
inline constexpr int64_t POST_COUNT         = 3385;
inline constexpr int64_t FORUM_COUNT        = 400;
inline constexpr int64_t TAG_COUNT          = 16080;
inline constexpr int64_t TAGCLASS_COUNT     = 71;
inline constexpr int64_t PLACE_COUNT        = 1460;
inline constexpr int64_t ORGANISATION_COUNT = 7955;

inline constexpr int64_t KNOWS_COUNT                = 88;
inline constexpr int64_t HAS_CREATOR_COMMENT_COUNT  = 790;
inline constexpr int64_t HAS_CREATOR_POST_COUNT     = 3385;
inline constexpr int64_t LIKES_COMMENT_COUNT        = 228;
inline constexpr int64_t LIKES_POST_COUNT           = 421;
inline constexpr int64_t CONTAINER_OF_COUNT         = 3385;
inline constexpr int64_t REPLY_OF_POST_COUNT        = 411;
inline constexpr int64_t REPLY_OF_COMMENT_COUNT     = 379;
inline constexpr int64_t HAS_TAG_COMMENT_COUNT      = 847;
inline constexpr int64_t HAS_TAG_POST_COUNT         = 214;
inline constexpr int64_t HAS_TAG_FORUM_COUNT        = 1606;
inline constexpr int64_t HAS_TYPE_COUNT             = 16080;
inline constexpr int64_t IS_PART_OF_COUNT           = 1454;

// Multi-partition vertex / edge counts (Comment + Post both have the
// :Message label per the load script's `--nodes Comment:Message` /
// `--nodes Post:Message` flags).
inline constexpr int64_t MESSAGE_COUNT               = 4175;
inline constexpr int64_t HAS_CREATOR_MESSAGE_COUNT   = 4175;
inline constexpr int64_t LIKES_MESSAGE_COUNT         = 649;
inline constexpr int64_t HAS_TAG_MESSAGE_COUNT       = 1061;
inline constexpr int64_t IS_LOCATED_IN_MESSAGE_COUNT = 4175;
inline constexpr int64_t IS_LOCATED_IN_TOTAL_COUNT   = 12180;

// Sample Person used by `[ldbc][func]` function tests — picked from the
// mini fixture (test/data/ldbc-mini/dynamic/Person.csv, first row id=14).
// Has outgoing KNOWS edges and a non-empty firstName/lastName whose
// concatenation we exact-match in the string-+ test. Values come from
// the CSV itself (CSV is ground truth for property reads).
inline constexpr int64_t     SAMPLE_PERSON_ID        = 14;
inline constexpr const char* SAMPLE_PERSON_FULL_NAME = "Hossein Forouhar";

// ---- Traversal expected values (Neo4j-verified on SF0.003) ----
// Person 14 (= SAMPLE_PERSON_ID) relative counts.
inline constexpr int64_t TRAV_FOF_COUNT                          = 9;
inline constexpr int64_t TRAV_DISTINCT_COMMENT_CREATORS_LIKED    = 4;
inline constexpr int64_t TRAV_DISTINCT_MESSAGE_CREATORS_LIKED    = 5;
inline constexpr int64_t TRAV_MESSAGES_AUTHORED_BY_SAMPLE_PERSON = 386;
inline constexpr int64_t TRAV_COMMENTS_AUTHORED_BY_SAMPLE_PERSON = 17;
// Undirected `(p)-[:KNOWS]-(f)` distinct friends of SAMPLE_PERSON.
// Used both as a `count(...)` expectation and as the row count for
// queries that expand the full friend list.
inline constexpr int64_t TRAV_KNOWS_FRIENDS_OF_SAMPLE_PERSON     = 3;

// Sample Comment + creator for the [both] HAS_CREATOR test. Picked
// as `MIN(c.id)` over Comment-HAS_CREATOR-Person, then resolved to
// the creator's first name. The creator happens to be a different
// Person than SAMPLE_PERSON, which is fine — this test only needs a
// (Comment, Person) pair where the directional edge resolves cleanly.
inline constexpr int64_t     SAMPLE_COMMENT_ID                = 481036339217LL;
inline constexpr int64_t     SAMPLE_COMMENT_CREATOR_ID        = 2199023255594LL;
inline constexpr const char* SAMPLE_COMMENT_CREATOR_FIRSTNAME = "Ali";

// Top-N rankings. Each entry holds the keying property and the count.
struct TravPersonCount { int64_t pid; int64_t cnt; };
struct TravForumCount  { int64_t fid; int64_t cnt; };
struct TravTagCount    { const char* name; int64_t cnt; };

inline constexpr TravPersonCount TRAV_TOP10_PERSON_BY_COMMENT[] = {
    {26388279066658LL, 64}, {24189255811081LL, 39}, {2199023255594LL, 34},
    {28587302322180LL, 34}, {8796093022244LL,  31}, {32985348833329LL, 29},
    {17592186044461LL, 28}, {2199023255557LL,  27}, {26388279066641LL, 27},
    {35184372088856LL, 27}
};

// All five forums tie at cnt=20 on this fixture, so the ranking
// degenerates to ORDER BY fid ASC. Keep the ordering pinned anyway —
// it's still a regression check on the GROUP BY + ORDER BY pipeline.
inline constexpr TravForumCount TRAV_TOP5_FORUM_BY_POST[] = {
    {137438953609LL, 20}, {206158430310LL, 20}, {206158430319LL, 20},
    {206158430320LL, 20}, {274877906991LL, 20}
};

// SF0.003 happens to share the same SF1 top-5 here, but pin it
// separately so the value isn't accidentally tied to the SF1 source.
inline constexpr TravTagCount TRAV_TOP5_TAGCLASS_BY_TAG[] = {
    {"Album",         5061}, {"Single", 4311}, {"Person", 1530},
    {"Country",       1000}, {"MusicalArtist",  899}
};

inline constexpr TravTagCount TRAV_TOP5_TAG_BY_POST[] = {
    {"Hannibal",                17}, {"Nat_King_Cole",          9},
    {"Saint_George",             7}, {"Wolfgang_Amadeus_Mozart",7},
    {"Jacques_Chirac",           5}
};

inline constexpr TravPersonCount TRAV_TOP10_PERSON_BY_MESSAGE[] = {
    {14,                386}, {2199023255573LL, 373}, {2199023255594LL, 370},
    {6597069766702LL,   325}, {4398046511139LL, 263}, {8796093022237LL, 245},
    {17592186044461LL,  210}, {15393162788877LL,194}, {26388279066658LL, 159},
    {24189255811109LL,  150}
};

inline constexpr TravTagCount TRAV_TOP5_TAG_BY_MESSAGE[] = {
    {"Hannibal",       33}, {"Nat_King_Cole", 17}, {"Saint_George", 15},
    {"Franz_Kafka",    14}, {"Jacques_Chirac", 11}
};

// ---- Filter test expected values (SAMPLE_PERSON 14 properties) ----
inline constexpr const char* SAMPLE_PERSON_FIRST_NAME    = "Hossein";
inline constexpr const char* SAMPLE_PERSON_LAST_NAME     = "Forouhar";
inline constexpr const char* SAMPLE_PERSON_GENDER        = "male";
inline constexpr int64_t     SAMPLE_PERSON_BIRTHDAY_MS   = 447811200000LL;
inline constexpr const char* SAMPLE_PERSON_LOCATION_IP   = "77.245.239.11";
inline constexpr const char* SAMPLE_PERSON_BROWSER       = "Firefox";
inline constexpr int64_t     SAMPLE_PERSON_CITY_ID       = 1166;
inline constexpr const char* SAMPLE_PERSON_CITY_NAME     = "Tehran";

// SAMPLE_PERSON outgoing/likes counts (separate from
// TRAV_KNOWS_FRIENDS_OF_SAMPLE_PERSON: that one is the *undirected*
// distinct friend count; this is the one-way outgoing edge count).
inline constexpr int64_t SAMPLE_PERSON_OUTGOING_KNOWS  = 3;
inline constexpr int64_t SAMPLE_PERSON_LIKED_COMMENTS  = 4;
inline constexpr int64_t SAMPLE_PERSON_LIKED_POSTS     = 8;
inline constexpr int64_t SAMPLE_PERSON_LIKED_MESSAGES  = 12;

// SAMPLE_PERSON's first interest tag in alphabetical order.
// We pin only the first 4 chars because the full tag name varies and
// the test only needs to verify ordered head() correctness.
inline constexpr const char* SAMPLE_PERSON_FIRST_INTEREST_PREFIX = "2_Be";

// String predicate fragments derived from SAMPLE_PERSON_FIRST_NAME ("Hossein").
inline constexpr const char* SAMPLE_PERSON_NAME_STARTS_MATCH   = "Ho";   // STARTS WITH true
inline constexpr const char* SAMPLE_PERSON_NAME_STARTS_NOMATCH = "Jo";   // STARTS WITH false
inline constexpr const char* SAMPLE_PERSON_NAME_ENDS_MATCH     = "in";   // ENDS WITH true
inline constexpr const char* SAMPLE_PERSON_NAME_CONTAINS_MATCH = "ss";   // CONTAINS true

// Sample Forum: first row of TRAV_TOP5_FORUM_BY_POST (all five tie at
// cnt=20 on this fixture, see PR #75 caveat).
inline constexpr int64_t SAMPLE_FORUM_ID         = 137438953609LL;
inline constexpr int64_t SAMPLE_FORUM_POST_COUNT = 20;

// Sample Tag for HAS_TAG count tests: top tag by post/message count
// on this fixture (Genghis_Khan has zero connections at SF0.003).
inline constexpr const char* SAMPLE_TAG_NAME              = "Hannibal";
inline constexpr int64_t     SAMPLE_TAG_POST_COUNT        = 17;
inline constexpr int64_t     SAMPLE_TAG_MESSAGE_COUNT     = 33;

// Sample second Person used by UNWIND-with-MATCH lookup (paired with
// SAMPLE_PERSON_ID). Ordered by firstName ASC the row order is
// "Hossein" (id=14) then "Jan" (id=16).
inline constexpr int64_t     SECOND_SAMPLE_PERSON_ID         = 16;
inline constexpr const char* SECOND_SAMPLE_PERSON_FIRST_NAME = "Jan";

// Sample Country names that exist in SF0.003 (Place catalog is shared
// across scales, but the load script does not tag :Country sub-labels,
// so we keep the test on the parent :Place label).
inline constexpr const char* SAMPLE_COUNTRY_NAME_1 = "Laos";
inline constexpr const char* SAMPLE_COUNTRY_NAME_2 = "Scotland";

// Path test endpoints — Person 14 -> Person 16 has 7 shortest paths
// of length 3. allShortestPaths returning exactly 7 makes a sharp
// regression check on the path enumeration.
inline constexpr int64_t SAMPLE_PATH_SRC_ID        = 14;
inline constexpr int64_t SAMPLE_PATH_DEST_ID       = 16;
inline constexpr int64_t SAMPLE_PATH_LEN           = 3;
inline constexpr int64_t SAMPLE_PATH_NUM_ALL_SHORTEST = 7;

// Sample Person with **no** WORK_AT relationships, used by the
// chained-OPTIONAL-MATCH preservation test (R5). On SF1 the original
// fixture used Person 290 because Marc-pre-migration didn't have
// WORK_AT — pick any equivalent Person here.
inline constexpr int64_t SAMPLE_PERSON_NO_WORK_ID = 2199023255557LL;

#else
// SF1 (full) — original values, Neo4j 5.24.0 verified.
inline constexpr int64_t PERSON_COUNT       = 9892;
inline constexpr int64_t COMMENT_COUNT      = 2052169;
inline constexpr int64_t POST_COUNT         = 1003605;
inline constexpr int64_t FORUM_COUNT        = 90492;
inline constexpr int64_t TAG_COUNT          = 16080;
inline constexpr int64_t TAGCLASS_COUNT     = 71;
inline constexpr int64_t PLACE_COUNT        = 1460;
inline constexpr int64_t ORGANISATION_COUNT = 7955;

inline constexpr int64_t KNOWS_COUNT                = 180623;
inline constexpr int64_t HAS_CREATOR_COMMENT_COUNT  = 2052169;
inline constexpr int64_t HAS_CREATOR_POST_COUNT     = 1003605;
inline constexpr int64_t LIKES_COMMENT_COUNT        = 1438418;
inline constexpr int64_t LIKES_POST_COUNT           = 751677;
inline constexpr int64_t CONTAINER_OF_COUNT         = 1003605;
inline constexpr int64_t REPLY_OF_POST_COUNT        = 1011420;
inline constexpr int64_t REPLY_OF_COMMENT_COUNT     = 1040749;
inline constexpr int64_t HAS_TAG_COMMENT_COUNT      = 2698393;
inline constexpr int64_t HAS_TAG_POST_COUNT         = 713258;
inline constexpr int64_t HAS_TAG_FORUM_COUNT        = 309766;
inline constexpr int64_t HAS_TYPE_COUNT             = 16080;
inline constexpr int64_t IS_PART_OF_COUNT           = 1454;

inline constexpr int64_t MESSAGE_COUNT               = 3055774;
inline constexpr int64_t HAS_CREATOR_MESSAGE_COUNT   = 3055774;
inline constexpr int64_t LIKES_MESSAGE_COUNT         = 2190095;
inline constexpr int64_t HAS_TAG_MESSAGE_COUNT       = 3411651;
inline constexpr int64_t IS_LOCATED_IN_MESSAGE_COUNT = 3055774;
inline constexpr int64_t IS_LOCATED_IN_TOTAL_COUNT   = 3073621;

// Sample Person — original 933 id used pre-migration. Only ID and
// "has KNOWS edges + non-empty firstName/lastName" are guaranteed at
// SF1 scale; per-person literals (firstName, lastName) are not pinned
// here, so SF1 function tests fall back to substring/structural checks.
inline constexpr int64_t SAMPLE_PERSON_ID = 933;

// ---- Traversal expected values (SF1, Neo4j 5.24.0 verified) ----
inline constexpr int64_t TRAV_FOF_COUNT                          = 1506;
inline constexpr int64_t TRAV_DISTINCT_COMMENT_CREATORS_LIKED    = 12;
inline constexpr int64_t TRAV_DISTINCT_MESSAGE_CREATORS_LIKED    = 14;
inline constexpr int64_t TRAV_MESSAGES_AUTHORED_BY_SAMPLE_PERSON = 370;
inline constexpr int64_t TRAV_COMMENTS_AUTHORED_BY_SAMPLE_PERSON = 57;
inline constexpr int64_t TRAV_KNOWS_FRIENDS_OF_SAMPLE_PERSON     = 5;

inline constexpr int64_t     SAMPLE_COMMENT_ID                = 824635044686LL;
inline constexpr int64_t     SAMPLE_COMMENT_CREATOR_ID        = 933;
inline constexpr const char* SAMPLE_COMMENT_CREATOR_FIRSTNAME = "Mahinda";

struct TravPersonCount { int64_t pid; int64_t cnt; };
struct TravForumCount  { int64_t fid; int64_t cnt; };
struct TravTagCount    { const char* name; int64_t cnt; };

inline constexpr TravPersonCount TRAV_TOP10_PERSON_BY_COMMENT[] = {
    {2199023262543LL, 8915}, {4139,            7896},
    {2199023259756LL, 7694}, {2783,            7530},
    {4398046513018LL, 7423}, {7725,            6612},
    {6597069777240LL, 6565}, {9116,            6135},
    {4398046519372LL, 5894}, {8796093029267LL, 5640}
};

inline constexpr TravForumCount TRAV_TOP5_FORUM_BY_POST[] = {
    {77644,         1208}, {87312,          1032},
    {137439023186LL,1001}, {412317916558LL,  891},
    {55025,          810}
};

inline constexpr TravTagCount TRAV_TOP5_TAGCLASS_BY_TAG[] = {
    {"Album",   5061}, {"Single",        4311}, {"Person",  1530},
    {"Country", 1000}, {"MusicalArtist",  899}
};

inline constexpr TravTagCount TRAV_TOP5_TAG_BY_POST[] = {
    {"Augustine_of_Hippo",10817}, {"Adolf_Hitler", 5227},
    {"Muammar_Gaddafi",    5120}, {"Imelda_Marcos",4412},
    {"Sammy_Sosa",         4059}
};

inline constexpr TravPersonCount TRAV_TOP10_PERSON_BY_MESSAGE[] = {
    {2199023262543LL, 9936}, {2783,            8754},
    {2199023259756LL, 8667}, {4139,            8558},
    {7725,            7833}, {4398046513018LL, 7682},
    {6597069777240LL, 7491}, {9116,            7189},
    {4398046519372LL, 6535}, {8796093029267LL, 6294}
};

inline constexpr TravTagCount TRAV_TOP5_TAG_BY_MESSAGE[] = {
    {"Augustine_of_Hippo", 24299}, {"Adolf_Hitler",  12326},
    {"Muammar_Gaddafi",    12003}, {"Imelda_Marcos",  9571},
    {"Genghis_Khan",        8982}
};

// ---- Filter test expected values (SF1, Neo4j 5.24.0 verified) ----
inline constexpr const char* SAMPLE_PERSON_FIRST_NAME    = "Mahinda";
inline constexpr const char* SAMPLE_PERSON_LAST_NAME     = "Perera";
inline constexpr const char* SAMPLE_PERSON_GENDER        = "male";
inline constexpr int64_t     SAMPLE_PERSON_BIRTHDAY_MS   = 628646400000LL;
inline constexpr const char* SAMPLE_PERSON_LOCATION_IP   = "119.235.7.103";
inline constexpr const char* SAMPLE_PERSON_BROWSER       = "Firefox";
inline constexpr int64_t     SAMPLE_PERSON_CITY_ID       = 1353;
inline constexpr const char* SAMPLE_PERSON_CITY_NAME     = "Kelaniya";

inline constexpr int64_t SAMPLE_PERSON_OUTGOING_KNOWS  = 5;
inline constexpr int64_t SAMPLE_PERSON_LIKED_COMMENTS  = 12;
inline constexpr int64_t SAMPLE_PERSON_LIKED_POSTS     = 5;
inline constexpr int64_t SAMPLE_PERSON_LIKED_MESSAGES  = 17;

inline constexpr const char* SAMPLE_PERSON_FIRST_INTEREST_PREFIX = "1962";

inline constexpr const char* SAMPLE_PERSON_NAME_STARTS_MATCH   = "Ma";   // "Mahinda" STARTS WITH "Ma"
inline constexpr const char* SAMPLE_PERSON_NAME_STARTS_NOMATCH = "Jo";
inline constexpr const char* SAMPLE_PERSON_NAME_ENDS_MATCH     = "da";   // "Mahinda" ENDS WITH "da"
inline constexpr const char* SAMPLE_PERSON_NAME_CONTAINS_MATCH = "ah";   // "Mahinda" CONTAINS "ah"

inline constexpr int64_t SAMPLE_FORUM_ID         = 77644;
inline constexpr int64_t SAMPLE_FORUM_POST_COUNT = 1208;

inline constexpr const char* SAMPLE_TAG_NAME          = "Genghis_Khan";
inline constexpr int64_t     SAMPLE_TAG_POST_COUNT    = 3715;
inline constexpr int64_t     SAMPLE_TAG_MESSAGE_COUNT = 8982;

inline constexpr int64_t     SECOND_SAMPLE_PERSON_ID         = 2199023262543LL;
inline constexpr const char* SECOND_SAMPLE_PERSON_FIRST_NAME = "Samir";

inline constexpr const char* SAMPLE_COUNTRY_NAME_1 = "Laos";
inline constexpr const char* SAMPLE_COUNTRY_NAME_2 = "Scotland";

// Path endpoints kept from the legacy SF1 test source. SF1 uses two
// dest persons (one with a single shortest path, one with seven) — we
// pin the seven-path dest here since both shortestPath and
// allShortestPaths probes work against it.
inline constexpr int64_t SAMPLE_PATH_SRC_ID           = 17592186055119LL;
inline constexpr int64_t SAMPLE_PATH_DEST_ID          = 10995116282665LL;
inline constexpr int64_t SAMPLE_PATH_LEN              = 3;
inline constexpr int64_t SAMPLE_PATH_NUM_ALL_SHORTEST = 7;

// Sample Person without WORK_AT — pre-migration the legacy code used
// Person 290 here.
inline constexpr int64_t SAMPLE_PERSON_NO_WORK_ID = 290;

#endif

// Strict lower bound for the [bug-a2] count(*) regression: count(*) on
// a no-label MATCH must exceed the largest single-label count we already
// pin (Person). Same expression on both fixtures.
inline constexpr int64_t COUNT_STAR_LOWER_BOUND = PERSON_COUNT;

}  // namespace ldbc

// String-literal forms of SAMPLE_PERSON_ID / SECOND_SAMPLE_PERSON_ID for
// callers (mostly test_ldbc_robustness.cpp) that build their query as a
// single C++ string literal and use adjacent-string-literal concatenation
// (`"…id: " LDBC_SAMPLE_PID_STR "…"`). The values are duplicates of the
// `int64_t` / `const char*` constants above — keep them in sync.
#ifdef TURBOLYNX_LDBC_FIXTURE_MINI
#define LDBC_SAMPLE_PID_STR        "14"
#define LDBC_SECOND_PID_STR        "16"
#define LDBC_NO_WORK_PID_STR       "2199023255557"
#define SAMPLE_FN_MATCH_LITERAL    "Hossein"
#else
#define LDBC_SAMPLE_PID_STR        "933"
#define LDBC_SECOND_PID_STR        "4139"
#define LDBC_NO_WORK_PID_STR       "290"
#define SAMPLE_FN_MATCH_LITERAL    "Marc"
#endif

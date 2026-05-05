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
#endif

// Strict lower bound for the [bug-a2] count(*) regression: count(*) on
// a no-label MATCH must exceed the largest single-label count we already
// pin (Person). Same expression on both fixtures.
inline constexpr int64_t COUNT_STAR_LOWER_BOUND = PERSON_COUNT;

}  // namespace ldbc

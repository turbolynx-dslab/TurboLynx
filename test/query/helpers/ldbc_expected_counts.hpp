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

// ---- IS (Interactive Short) expected values (Neo4j-verified on SF0.003) ----
//
// IS1: Person basic info. Mini reuses SAMPLE_PERSON_ID; SF1 used a
// separate Changpeng/Wei anchor pre-migration so we expose IS1_* directly
// even though all of these are duplicates of SAMPLE_PERSON_* on mini.
inline constexpr int64_t     IS1_PERSON_ID         = SAMPLE_PERSON_ID;
inline constexpr const char* IS1_FIRST_NAME        = SAMPLE_PERSON_FIRST_NAME;
inline constexpr const char* IS1_LAST_NAME         = SAMPLE_PERSON_LAST_NAME;
inline constexpr int64_t     IS1_BIRTHDAY_MS       = SAMPLE_PERSON_BIRTHDAY_MS;
inline constexpr const char* IS1_LOCATION_IP       = SAMPLE_PERSON_LOCATION_IP;
inline constexpr const char* IS1_BROWSER_USED      = SAMPLE_PERSON_BROWSER;
inline constexpr int64_t     IS1_CITY_ID           = SAMPLE_PERSON_CITY_ID;
inline constexpr const char* IS1_GENDER            = SAMPLE_PERSON_GENDER;
inline constexpr int64_t     IS1_CREATION_DATE_MS  = 1262531431499LL;

// IS2: SAMPLE_PERSON's 10 most recent Comment-with-original-Post info,
// ordered by messageCreationDate DESC, messageId ASC.
struct IsRecentComment {
    int64_t     message_id;
    const char* content;
    int64_t     message_creation_ms;
    int64_t     post_id;
    int64_t     person_id;
    const char* first_name;
    const char* last_name;
};
inline constexpr IsRecentComment IS2_RECENT_COMMENTS[] = {
    {1168231108486LL, "thanks",                                                                                                       1356562996399LL, 1168231108477LL,  8796093022234LL, "Rahul",            "Sharma"},
    {1168231108516LL, "About Estonia, ed to Finnish. One distinctive feature that has caused a great",                                 1356547280755LL, 1168231108506LL, 10995116277808LL, "Adje van den Berg", "Vries"},
    {1168231108514LL, "great",                                                                                                        1356542235020LL, 1168231108506LL, 10995116277808LL, "Adje van den Berg", "Vries"},
    {1168231107550LL, "About Aq Qoyunlu,  parts of present-day Eastern Turkey, AAbout Drive-In Satur",                                 1355579276206LL, 1168231107539LL, 30786325577740LL, "Jose",             "Alonso"},
    {1168231106604LL, "ok",                                                                                                           1353902374659LL,  962072676387LL, 10995116277782LL, "Ken",              "Yamada"},
    {1168231106601LL, "About Woodrow Wilson, d has been a contentioAbout Robert F. Kennedy, officially named a",                       1353830401258LL,  962072676387LL, 10995116277782LL, "Ken",              "Yamada"},
    {1168231106594LL, "About Nicholas II of Russia, on of political opponents, and his pursuit ofAbout Second Spanish Republic, f the Second Spanish Republic", 1353730304489LL, 481036339222LL, 10995116277782LL, "Ken", "Yamada"},
    {1168231106633LL, "yes",                                                                                                          1352846306943LL,  755914246211LL, 10995116277782LL, "Ken",              "Yamada"},
    {1168231106625LL, "About Sarah McLachlan, nown as a highly vAbout Simón Bolívar, , he played a key About Por",                     1352846199009LL,  481036339251LL, 10995116277782LL, "Ken",              "Yamada"},
    {1168231106634LL, "yes",                                                                                                          1352824016064LL,  755914246211LL, 10995116277782LL, "Ken",              "Yamada"},
};

// IS3: SAMPLE_PERSON's friends list, ordered by friendshipCreationDate DESC,
// personId ASC.
struct IsFriend {
    int64_t     person_id;
    const char* first_name;
    const char* last_name;
    int64_t     friendship_ms;
};
inline constexpr IsFriend IS3_FRIENDS[] = {
    {26388279066668LL, "Alexei", "Kahnovich", 1353883521004LL},
    {10995116277782LL, "Ken",    "Yamada",    1349551480381LL},
    {24189255811081LL, "Alim",   "Guliyev",   1341736032264LL},
};

// IS4: a Post with a non-empty imageFile, authored by SAMPLE_PERSON.
inline constexpr int64_t     IS4_POST_ID            = 68719476848LL;
inline constexpr int64_t     IS4_CREATION_MS        = 1269114863092LL;
inline constexpr const char* IS4_IMAGE_FILE         = "photo68719476848.jpg";

// IS5: SAMPLE_COMMENT creator's last name (id+firstName already pinned above).
inline constexpr const char* SAMPLE_COMMENT_CREATOR_LASTNAME = "Achiou";

// IS6: forum + moderator that contains SAMPLE_COMMENT (via REPLY_OF*0..->Post).
inline constexpr int64_t     IS6_FORUM_ID    = 343597383879LL;
inline constexpr const char* IS6_FORUM_TITLE = "Wall of Evangelos Alkaios";
inline constexpr int64_t     IS6_MOD_ID      = 10995116277761LL;
inline constexpr const char* IS6_MOD_FIRST   = "Evangelos";
inline constexpr const char* IS6_MOD_LAST    = "Alkaios";

// IS7: a Message with exactly two replies, where the replies' author is a
// KNOWS-friend of the original message author. Picked to mirror the SF1 IS7
// shape (author != reply author, knows-author = true on both rows).
inline constexpr int64_t IS7_MESSAGE_ID = 618475290624LL;
struct IsReply {
    int64_t     comment_id;
    const char* content;
    int64_t     creation_ms;
    int64_t     reply_author_id;
    const char* reply_author_first;
    const char* reply_author_last;
    bool        reply_author_knows_orig;
};
inline constexpr IsReply IS7_REPLIES[] = {
    {962072674305LL, "yes",    1341766121630LL, 24189255811081LL, "Alim", "Guliyev", true},
    {962072674306LL, "thanks", 1341754323239LL, 24189255811081LL, "Alim", "Guliyev", true},
};

// ---- IC (Interactive Complex) expected values (Neo4j-verified on SF0.003) ----
//
// IC anchor: Ali Lala (id=2199023255594) — the most KNOWS-connected
// Person on this fixture (17 direct KNOWS), giving non-trivial 1-2 hop
// neighbourhoods for the IC queries that need a populated friend graph.

// IC1 — shortestPath((anchor)-[:KNOWS*1..3]-(friend by firstName)).
// 'John' is the most common firstName among Ali's 3-hop neighbours
// (3 reachable Johns at distances 1 and 2).
inline constexpr int64_t     IC1_ANCHOR_PERSON_ID  = 2199023255594LL;
inline constexpr const char* IC1_FRIEND_FIRST_NAME = "John";

struct IcShortestPathRow {
    int64_t     friend_id;
    const char* friend_last_name;
    int64_t     distance;
};
inline constexpr IcShortestPathRow IC1_BASIC_RESULTS[] = {
    {8796093022244LL,  "Reddy", 1},
    {19791209299968LL, "Khan",  2},
    {8796093022249LL,  "Kumar", 2},
};

// IC6 — tag co-occurrence on posts authored by (anchor)'s 1-2 hop friends
// that also carry the known tag. `Wolfgang_Amadeus_Mozart` chosen because
// it appears on multi-tag posts within Ali's friends-of-friends; the more
// common SF1 anchor `Hannibal` only appears on single-tag mini posts and
// would yield zero co-occurrence rows.
inline constexpr int64_t     IC6_ANCHOR_PERSON_ID = IC1_ANCHOR_PERSON_ID;
inline constexpr const char* IC6_KNOWN_TAG_NAME   = "Wolfgang_Amadeus_Mozart";

struct IcTagCount {
    const char* tag_name;
    int64_t     post_count;
};
inline constexpr IcTagCount IC6_RESULTS[] = {
    {"Alexander_Hamilton", 2},
    {"Bob_Dylan",          2},
    {"Martin_Luther",      2},
    {"2_Become_1",         1},
    {"Barack_Obama",       1},
    {"Daniel_Nestor",      1},
    {"George_Lucas",       1},
    {"Howard_Stern",       1},
    {"Hugo_Chávez",        1},
    {"Humphrey_Bogart",    1},
};

// IC7 — recent likers of (anchor)'s messages, grouped per liker via
// head(collect()) over the liker's likes ordered by likeTime DESC.
// Anchor reuses IC1_ANCHOR_PERSON_ID (Ali, 89 likes on their messages).
// Result is 17 rows (LIMIT 20 not saturated). Most rows happen to point
// at the same Post 1168231105519 (a photo authored by Ali that drew the
// most likes); content uses startswith semantics for the long
// "About …" fragment on the final row.
inline constexpr int64_t IC7_ANCHOR_PERSON_ID = IC1_ANCHOR_PERSON_ID;
struct IcRecentLiker {
    int64_t     person_id;
    const char* first_name;
    const char* last_name;
    int64_t     like_creation_ms;
    int64_t     comment_or_post_id;
    const char* content;       // startswith semantics
    int64_t     minutes_latency;
};
inline constexpr IcRecentLiker IC7_RESULTS[] = {
    { 8796093022244LL, "John",                 "Reddy",      1353140416076LL, 1168231105519LL, "photo1168231105519.jpg", 9265},
    {35184372088856LL, "Jie",                  "Yang",       1353113918830LL, 1168231105519LL, "photo1168231105519.jpg", 8823},
    {10995116277761LL, "Evangelos",            "Alkaios",    1353112879293LL, 1168231105519LL, "photo1168231105519.jpg", 8806},
    {26388279066641LL, "Almira",               "Patras",     1353036641761LL, 1168231105519LL, "photo1168231105519.jpg", 7535},
    {28587302322196LL, "Yahya Ould Ahmed El",  "Abdallahi",  1353008629543LL, 1168231105519LL, "photo1168231105519.jpg", 7068},
    {30786325577740LL, "Jose",                 "Alonso",     1352968995146LL, 1168231105519LL, "photo1168231105519.jpg", 6408},
    {24189255811081LL, "Alim",                 "Guliyev",    1352918953864LL, 1168231105519LL, "photo1168231105519.jpg", 5574},
    {              16, "Jan",                  "Zakrzewski", 1352792396846LL, 1168231105519LL, "photo1168231105519.jpg", 3465},
    {17592186044461LL, "Ali",                  "Abouba",     1352732461090LL, 1168231105519LL, "photo1168231105519.jpg", 2466},
    {13194139533342LL, "Joakim",               "Larsson",    1352620688664LL, 1168231105519LL, "photo1168231105519.jpg", 603},
    {28587302322180LL, "Bryn",                 "Davies",     1352607642589LL, 1168231105519LL, "photo1168231105519.jpg", 385},
    {26388279066658LL, "Roberto",              "Diaz",       1352588667275LL, 1168231105519LL, "photo1168231105519.jpg", 69},
    {26388279066668LL, "Alexei",               "Kahnovich",  1352432488356LL, 1099511628808LL, "photo1099511628808.jpg", 8950},
    {15393162788877LL, "Mehmet",               "Koksal",     1345656333589LL, 1030792151962LL, "photo1030792151962.jpg", 9169},
    {              32, "Miguel",               "Gonzalez",   1345265808320LL, 1030792151966LL, "photo1030792151966.jpg", 2661},
    {13194139533352LL, "Celso",                "Oliveira",   1345230184082LL, 1030792151966LL, "photo1030792151966.jpg", 2067},
    { 2199023255594LL, "Ali",                  "Achiou",     1318033024148LL,  687194767825LL, "About Niandra Lades",     810},
};

// IC8 — recent replies to (anchor)'s messages. Anchor reuses Ali (54
// reply comments on Ali's messages). 20 rows ordered by replyCreationDate
// DESC, replyId ASC. Content uses startswith semantics for the long
// "About …" fragments.
inline constexpr int64_t IC8_ANCHOR_PERSON_ID = IC1_ANCHOR_PERSON_ID;
struct IcRecentReply {
    int64_t     person_id;
    const char* first_name;
    const char* last_name;
    int64_t     comment_creation_ms;
    int64_t     comment_id;
    const char* content;       // startswith semantics
};
// IC12 — friends with comment-replies on Posts that have a Tag in
// the (HAS_TYPE|IS_SUBCLASS_OF*0..) closure of a base TagClass.
// Anchor = Ali; base TagClass = "MusicalArtist" yields 15 rows on
// SF0.003 (the SF1 anchor "BasketballPlayer" returns 0 rows on mini).
inline constexpr int64_t     IC12_ANCHOR_PERSON_ID = IC1_ANCHOR_PERSON_ID;
inline constexpr const char* IC12_BASE_TAGCLASS    = "MusicalArtist";
inline constexpr int64_t     IC12_NUM_ROWS         = 15;
inline constexpr int64_t     IC12_TOP_PERSON_ID    = 24189255811081LL;
inline constexpr const char* IC12_TOP_FIRST_NAME   = "Alim";
inline constexpr int64_t     IC12_TOP_REPLY_COUNT  = 7;
inline constexpr int64_t     IC12_LAST_PERSON_ID   = 15393162788877LL;
inline constexpr const char* IC12_LAST_FIRST_NAME  = "Mehmet";
inline constexpr int64_t     IC12_LAST_REPLY_COUNT = 1;

// IC13 — shortestPath((p1)-[:KNOWS*]-(p2)). Endpoints chosen so Ali
// (= IC anchor, most connected) reaches SAMPLE_PERSON_ID (= 14, Hossein)
// in 2 KNOWS hops. allShortestPaths-style enumeration is bounded by
// shortestPath's "first match" semantics, so the unbounded `*` does
// not hit issue #86.
inline constexpr int64_t IC13_SRC_PERSON_ID = IC1_ANCHOR_PERSON_ID;
inline constexpr int64_t IC13_DST_PERSON_ID = SAMPLE_PERSON_ID;
inline constexpr int64_t IC13_EXPECTED_LENGTH = 2;

// IC14 — allShortestPaths((p1)-[:KNOWS*0..]-(p2)) reused endpoints.
// Same pair as IC13 yields 2 shortest paths of length 2 between Ali and
// Hossein on SF0.003 (via Alim and Alexei), verified on Neo4j 5.
// pathWeight values are 7.5 and 3.5 (two HAS_CREATOR/REPLY_OF/HAS_CREATOR
// triangles on the Alim hop, one on the Alexei hop).
inline constexpr int64_t IC14_SRC_PERSON_ID = IC1_ANCHOR_PERSON_ID;
inline constexpr int64_t IC14_DST_PERSON_ID = SAMPLE_PERSON_ID;
inline constexpr int64_t IC14_NUM_SHORTEST_PATHS = 2;
inline constexpr const char* IC14_TOP_PATH_WEIGHT_STR = "7.500000";
inline constexpr const char* IC14_SECOND_PATH_WEIGHT_STR = "3.500000";

inline constexpr IcRecentReply IC8_RESULTS[] = {
    {17592186044443LL, "Wojciech",  "Ciesla",          1356685362927LL, 1168231108517LL, "About Czechoslovakia,"},
    {35184372088856LL, "Jie",       "Yang",            1356559048186LL, 1168231105398LL, "roflol"},
    {10995116277761LL, "Evangelos", "Alkaios",         1355982540324LL, 1168231108515LL, "About Jacques Chirac,"},
    {32985348833291LL, "Cheng",     "Wei",             1355455394169LL, 1168231108522LL, "LOL"},
    {26388279066668LL, "Alexei",    "Kahnovich",       1355442165096LL, 1168231108512LL, "thx"},
    {35184372088871LL, "Alexei",    "Feltsman",        1355433943901LL, 1168231108521LL, "fine"},
    {13194139533352LL, "Celso",     "Oliveira",        1355179183342LL, 1168231104903LL, "About Jonathan Swift,"},
    {13194139533352LL, "Celso",     "Oliveira",        1355173569970LL, 1168231104902LL, "fine"},
    {              16, "Jan",       "Zakrzewski",      1355058521183LL, 1168231108551LL, "About Diesel and Dust,"},
    {26388279066641LL, "Almira",    "Patras",          1354260071338LL, 1168231105356LL, "great"},
    {26388279066641LL, "Almira",    "Patras",          1354227602453LL, 1168231105373LL, "About Fidel Castro,"},
    {35184372088850LL, "Neil",      "Murray",          1353360359791LL, 1168231106676LL, "About Albert Einstein,"},
    {17592186044461LL, "Ali",       "Abouba",          1352774880661LL, 1168231104954LL, "About Diana, Princess of Wales,"},
    {17592186044461LL, "Ali",       "Abouba",          1352739862257LL, 1168231104927LL, "maybe"},
    {26388279066658LL, "Roberto",   "Diaz",            1352295637125LL, 1099511629934LL, "About Ban Ki-moon,"},
    {19791209299968LL, "John",      "Khan",            1350652957308LL, 1099511630632LL, "cool"},
    {19791209299987LL, "Jimmy",     "Burak",           1349532100030LL, 1099511629936LL, "great"},
    {28587302322180LL, "Bryn",      "Davies",          1349484181683LL, 1099511628659LL, "thanks"},
    {              32, "Miguel",    "Gonzalez",        1349473352485LL, 1099511628660LL, "About New France,"},
    {24189255811081LL, "Alim",      "Guliyev",         1349431344692LL, 1099511628654LL, "About Pope John XXIII,"},
};

// IC3 — friends-of-friends posting in BOTH country X and country Y.
// Mini fixture is too sparse for the SF1 anchor (Laos / Scotland yields 0
// rows on Ali's neighbourhood). Azerbaijan / Rwanda is the one country
// pair where a single FoF (Arbaaz Ali) crosses both. Date range is widened
// to [2010-01-01, 2013-01-01) so the test isn't fragile against the few
// messages Arbaaz has in those countries.
inline constexpr int64_t     IC3_ANCHOR_PERSON_ID  = IC1_ANCHOR_PERSON_ID;
inline constexpr const char* IC3_COUNTRY_X         = "Azerbaijan";
inline constexpr const char* IC3_COUNTRY_Y         = "Rwanda";
inline constexpr int64_t     IC3_DATE_START_MS     = 1262304000000LL;  // 2010-01-01
inline constexpr int64_t     IC3_DATE_END_MS       = 1356998400000LL;  // 2013-01-01
struct IcCountryFriend {
    int64_t     friend_id;
    const char* first_name;
    const char* last_name;
    int64_t     x_count;
    int64_t     y_count;
};
inline constexpr IcCountryFriend IC3_RESULTS[] = {
    {2199023255573LL, "Arbaaz", "Ali", 2, 2},
};

// IC4 — popular Tags on (anchor)'s friends' Posts created strictly in the
// valid window, with zero "older" Posts. Anchor = Ali. Window =
// [2012-01-01, 2013-01-01) chosen so Ali's 1-hop friend Posts produce a
// non-trivial top-10 (the SF1 Apr-2012 window yields 0 rows on mini).
inline constexpr int64_t IC4_ANCHOR_PERSON_ID = IC1_ANCHOR_PERSON_ID;
inline constexpr int64_t IC4_VALID_START_MS   = 1325376000000LL;  // 2012-01-01
inline constexpr int64_t IC4_VALID_END_MS     = 1356998400000LL;  // 2013-01-01
inline constexpr IcTagCount IC4_RESULTS[] = {
    {"Hannibal",            10},
    {"Nat_King_Cole",        5},
    {"Judy_Davis",           3},
    {"Saint_George",         3},
    {"Jacques_Chirac",       2},
    {"Jim_Carrey",           2},
    {"Cardinal_Richelieu",   1},
    {"Daniel_Nestor",        1},
    {"Fidel_Castro",         1},
    {"George_Lucas",         1},
};

// IC5 — Forums whose HAS_MEMBER (anchor's FoF) joined after threshold,
// with Post counts contributed by those FoF in that Forum. Anchor = Ali,
// threshold = 2012-07-24 (= SF1 anchor date — works on mini too: top
// 6 rows are real forum activity, rows 7-19 are zero-post Walls/Albums).
// Mini HAS_MEMBER edge property is `creationDate` (DATE_EPOCHMS); SF1
// uses `joinDate`. Test code branches on the property name.
inline constexpr int64_t IC5_ANCHOR_PERSON_ID = IC1_ANCHOR_PERSON_ID;
inline constexpr int64_t IC5_JOIN_THRESHOLD_MS = 1343088000000LL;
struct IcForumPostCount {
    const char* forum_title;
    int64_t     post_count;
    int64_t     forum_id;
};
inline constexpr IcForumPostCount IC5_RESULTS[] = {
    {"Group for Hannibal in Changyi",                  14, 1030792151326LL},
    {"Group for Nat_King_Cole in Cooch_Behar",          8, 1099511628156LL},
    {"Group for Saint_George in Changyi",               4,  893353197855LL},
    {"Group for Jacques_Chirac in Cooch_Behar",         3, 1099511628157LL},
    {"Group for Cardinal_Richelieu in Changyi",         2,  962072674592LL},
    {"Group for Wolfgang_Amadeus_Mozart in Cooch_Behar", 2, 1168231104894LL},
    {"Wall of Hossein Forouhar",                        0,             0},
    {"Wall of Jan Zakrzewski",                          0,            37},
    {"Wall of Miguel Gonzalez",                         0,            38},
    {"Wall of Ali Achiou",                              0,  68719476809LL},
    {"Album 5 of Ali Achiou",                           0,  68719476815LL},
    {"Album 23 of Ali Achiou",                          0,  68719476833LL},
    {"Album 8 of Ali Achiou",                           0, 137438953554LL},
    {"Album 13 of Ali Achiou",                          0, 137438953559LL},
    {"Album 16 of Ali Achiou",                          0, 137438953562LL},
    {"Album 19 of Ali Achiou",                          0, 137438953565LL},
    {"Album 24 of Ali Achiou",                          0, 137438953570LL},
    {"Album 20 of Ali Achiou",                          0, 206158430302LL},
    {"Album 28 of Ali Achiou",                          0, 206158430310LL},
    {"Album 29 of Ali Achiou",                          0, 206158430311LL},
};

// IC9 — recent messages of (anchor)'s friends-of-friends with
// creationDate < threshold. Anchor = Ali, threshold = 2013-01-01.
// Returns 20 rows ordered by creationDate DESC, message id ASC.
inline constexpr int64_t IC9_ANCHOR_PERSON_ID  = IC1_ANCHOR_PERSON_ID;
inline constexpr int64_t IC9_DATE_THRESHOLD_MS = 1356998400000LL;
struct IcFofMessage {
    int64_t     person_id;
    const char* first_name;
    const char* last_name;
    int64_t     message_id;
    const char* content;       // startswith semantics
    int64_t     message_creation_ms;
};
inline constexpr IcFofMessage IC9_RESULTS[] = {
    {19791209299968LL, "John",      "Khan",        1168231108487LL, "About Nat King Cole,",       1356994051470LL},
    {21990232555527LL, "Jun",       "Li",          1168231108577LL, "About Wolfgang Amadeus Mozart,", 1356990424630LL},
    {10995116277761LL, "Evangelos", "Alkaios",     1168231107530LL, "ok",                          1356983798219LL},
    {              32, "Miguel",    "Gonzalez",    1168231107522LL, "maybe",                       1356980204838LL},
    {26388279066641LL, "Almira",    "Patras",      1168231107529LL, "About Josip Broz Tito,",      1356978747364LL},
    {10995116277782LL, "Ken",       "Yamada",      1168231107527LL, "I see",                       1356975189220LL},
    {              32, "Miguel",    "Gonzalez",    1168231107526LL, "I see",                       1356971087128LL},
    {              32, "Miguel",    "Gonzalez",    1168231108493LL, "fine",                        1356969762149LL},
    { 2199023255573LL, "Arbaaz",    "Ali",         1168231107525LL, "About Pope Pius IX,",         1356967673506LL},
    {19791209299968LL, "John",      "Khan",        1168231107524LL, "thx",                         1356966254544LL},
    {28587302322191LL, "Ge",        "Wei",         1168231107523LL, "thx",                         1356965735991LL},
    {35184372088850LL, "Neil",      "Murray",      1168231107521LL, "About Pope Pius IX,",         1356965137335LL},
    {35184372088850LL, "Neil",      "Murray",      1168231107520LL, "About Hannibal,",             1356950518910LL},
    {              32, "Miguel",    "Gonzalez",    1168231108455LL, "About One Headlight,",        1356948775806LL},
    {26388279066658LL, "Roberto",   "Diaz",        1168231108423LL, "maybe",                       1356947952509LL},
    { 2199023255573LL, "Arbaaz",    "Ali",         1168231108573LL, "ok",                          1356945869242LL},
    {30786325577731LL, "Aleksandr", "Efimkin",     1168231107557LL, "About Saint George,",         1356939457136LL},
    {24189255811109LL, "Wei",       "Wei",         1168231108570LL, "cool",                        1356939072367LL},
    {35184372088850LL, "Neil",      "Murray",      1168231107444LL, "thx",                         1356929220284LL},
    {26388279066658LL, "Roberto",   "Diaz",        1168231108434LL, "thx",                         1356915516217LL},
};

// IC2 — recent messages of (anchor)'s friends, with creationDate <= threshold.
// Threshold = 2013-01-01T00:00:00Z. Returns 20 rows ordered by date DESC,
// id ASC. Anchor reuses IC1_ANCHOR_PERSON_ID (Ali, 17 friends → enough
// upstream messages to fill the LIMIT 20).
//
// NOTE: blocked on issue #89 (BIGINT → TIMESTAMP_MS cast); the IC2 mini
// TEST_CASE is gated SF1-only until that lands. Constants are pinned so
// re-enabling is a one-line change.
inline constexpr int64_t IC2_DATE_THRESHOLD_MS = 1356998400000LL;
struct IcRecentMessage {
    int64_t     person_id;
    const char* first_name;
    const char* last_name;
    int64_t     message_id;
    const char* content;
    int64_t     message_creation_ms;
};
inline constexpr IcRecentMessage IC2_RECENT_MESSAGES[] = {
    {10995116277761LL, "Evangelos", "Alkaios",    1168231107530LL, "ok",         1356983798219LL},
    {              32, "Miguel",    "Gonzalez",   1168231107522LL, "maybe",      1356980204838LL},
    {26388279066641LL, "Almira",    "Patras",     1168231107529LL, "About Josip Broz Tito,",  1356978747364LL},
    {              32, "Miguel",    "Gonzalez",   1168231107526LL, "I see",      1356971087128LL},
    {              32, "Miguel",    "Gonzalez",   1168231108493LL, "fine",       1356969762149LL},
    {35184372088850LL, "Neil",      "Murray",     1168231107521LL, "About Pope Pius IX,",      1356965137335LL},
    {35184372088850LL, "Neil",      "Murray",     1168231107520LL, "About Hannibal,",          1356950518910LL},
    {              32, "Miguel",    "Gonzalez",   1168231108455LL, "About One Headlight,",     1356948775806LL},
    {26388279066658LL, "Roberto",   "Diaz",       1168231108423LL, "maybe",      1356947952509LL},
    {35184372088850LL, "Neil",      "Murray",     1168231107444LL, "thx",        1356929220284LL},
    {26388279066658LL, "Roberto",   "Diaz",       1168231108434LL, "thx",        1356915516217LL},
    {35184372088856LL, "Jie",       "Yang",       1168231108526LL, "no way!",    1356895558945LL},
    {13194139533352LL, "Celso",     "Oliveira",   1168231108575LL, "duh",        1356892272437LL},
    {35184372088850LL, "Neil",      "Murray",     1168231107401LL, "great",      1356883963282LL},
    {35184372088850LL, "Neil",      "Murray",     1168231108576LL, "great",      1356879952643LL},
    {              16, "Jan",       "Zakrzewski", 1168231108569LL, "good",       1356871576404LL},
    {30786325577740LL, "Jose",      "Alonso",     1168231108568LL, "fine",       1356863089811LL},
    {30786325577740LL, "Jose",      "Alonso",     1168231108563LL, "About Wolfgang Amadeus Mozart,", 1356863067761LL},
    {24189255811081LL, "Alim",      "Guliyev",    1168231107554LL, "About Saint George,",      1356853889677LL},
    {26388279066641LL, "Almira",    "Patras",     1168231107468LL, "no way!",    1356850516262LL},
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

// ---- IS (Interactive Short) expected values (SF1, Neo4j 5.24.0 verified) ----
// Pre-migration these were hardcoded inside test_ldbc_is_correctness.cpp.
//
// IS1 used a different sample person (Changpeng Wei id=35184372099695)
// than the SAMPLE_PERSON the rest of the file shares (Mahinda Perera id=933).
// Keep the legacy SF1 anchor as-is via IS1_* constants; the mini path
// happens to reuse SAMPLE_PERSON_ID since 14 is fine for both.
inline constexpr int64_t     IS1_PERSON_ID         = 35184372099695LL;
inline constexpr const char* IS1_FIRST_NAME        = "Changpeng";
inline constexpr const char* IS1_LAST_NAME         = "Wei";
inline constexpr int64_t     IS1_BIRTHDAY_MS       = 337132800000LL;
inline constexpr const char* IS1_LOCATION_IP       = "1.1.39.242";
inline constexpr const char* IS1_BROWSER_USED      = "Internet Explorer";
inline constexpr int64_t     IS1_CITY_ID           = 367;
inline constexpr const char* IS1_GENDER            = "female";
inline constexpr int64_t     IS1_CREATION_DATE_MS  = 1347431652132LL;

struct IsRecentComment {
    int64_t     message_id;
    const char* content;
    int64_t     message_creation_ms;
    int64_t     post_id;
    int64_t     person_id;
    const char* first_name;
    const char* last_name;
};
// IS2 expected (SAMPLE_PERSON 933): 10 rows, original SF1 hardcoded values.
// Only first 7 fields per row carry, content uses substring-startswith
// for some rows; here we encode the canonical first-message-row check
// the test still does. Keeping the structure identical to mini.
inline constexpr IsRecentComment IS2_RECENT_COMMENTS[] = {
    {2199027727462LL, "good",                       1347156463979LL, 2061588773973LL, 32985348833579LL, "Otto",   "Becker"},
    {2061588773980LL, "no way!",                    1347020779693LL, 2061588773973LL, 32985348833579LL, "Otto",   "Becker"},
    {2061584946139LL, "yes",                        1343987237082LL, 2061584946128LL,            4139, "Baruch", "Dego"},
    {2061585616327LL, "About Arnold Schwarzenegger",1343919397609LL, 2061585616321LL,  6597069777240LL, "Fritz",  "Muller"},
    {2061585618894LL, "thanks",                     1343106691147LL, 2061585618887LL,  6597069777240LL, "Fritz",  "Muller"},
    {2061585619578LL, "About Josip Broz Tito",      1342380120292LL, 2061585619561LL,  6597069777240LL, "Fritz",  "Muller"},
    {1786707214487LL, "maybe",                      1335712528590LL, 1786707214481LL, 10995116284808LL, "Andrei", "Condariuc"},
    {1786707214957LL, "About Lady Gaga",            1335694024103LL, 1786707214955LL, 10995116284808LL, "Andrei", "Condariuc"},
    {1786707214469LL, "About Enrique Iglesias",     1335678484226LL, 1786707214468LL, 10995116284808LL, "Andrei", "Condariuc"},
    {1786707216224LL, "no way!",                    1335668918002LL, 1786707216218LL, 10995116284808LL, "Andrei", "Condariuc"},
};

struct IsFriend {
    int64_t     person_id;
    const char* first_name;
    const char* last_name;
    int64_t     friendship_ms;
};
inline constexpr IsFriend IS3_FRIENDS[] = {
    {32985348833579LL, "Otto",   "Becker",    1346980290195LL},
    {32985348838375LL, "Otto",   "Richter",   1342512289463LL},
    {10995116284808LL, "Andrei", "Condariuc", 1293950621955LL},
    { 6597069777240LL, "Fritz",  "Muller",    1284975763187LL},
    {            4139, "Baruch", "Dego",      1268465841718LL},
};

inline constexpr int64_t     IS4_POST_ID    = 2199029886840LL;
inline constexpr int64_t     IS4_CREATION_MS = 1347463431887LL;
inline constexpr const char* IS4_IMAGE_FILE = "photo2199029886840.jpg";

inline constexpr const char* SAMPLE_COMMENT_CREATOR_LASTNAME = "Perera";

inline constexpr int64_t     IS6_FORUM_ID    = 412317916558LL;
inline constexpr const char* IS6_FORUM_TITLE = "Wall of Fritz Muller";
inline constexpr int64_t     IS6_MOD_ID      = 6597069777240LL;
inline constexpr const char* IS6_MOD_FIRST   = "Fritz";
inline constexpr const char* IS6_MOD_LAST    = "Muller";

inline constexpr int64_t IS7_MESSAGE_ID = 824635044682LL;
struct IsReply {
    int64_t     comment_id;
    const char* content;
    int64_t     creation_ms;
    int64_t     reply_author_id;
    const char* reply_author_first;
    const char* reply_author_last;
    bool        reply_author_knows_orig;
};
inline constexpr IsReply IS7_REPLIES[] = {
    {824635044685LL, "great", 1295218398759LL,            2738, "Eden",    "Atias",  true},
    {824635044686LL, "cool",  1295203653676LL,             933, "Mahinda", "Perera", true},
};

// ---- IC (Interactive Complex) expected values (SF1, Neo4j 5.24.0 verified) ----
// IC1 anchor uses the legacy SF1 hardcoded params (Person 30786325583618,
// firstName 'Chau'). On SF1 this returns 18 rows. We pin the first 14
// distance-2 rows in the array (rows 14-17 are distance-3 and only
// distance is checked), matching what the legacy test asserted.
inline constexpr int64_t     IC1_ANCHOR_PERSON_ID  = 30786325583618LL;
inline constexpr const char* IC1_FRIEND_FIRST_NAME = "Chau";

struct IcShortestPathRow {
    int64_t     friend_id;
    const char* friend_last_name;
    int64_t     distance;
};
inline constexpr IcShortestPathRow IC1_BASIC_RESULTS[] = {
    {9101,             "Do",     2},
    {15393162793500LL, "Ha",     2},
    {10995116285282LL, "Ho",     2},
    {19791209303405LL, "Ho",     2},
    {28587302323020LL, "Ho",     2},
    {32985348842021LL, "Ho",     2},
    { 6597069771031LL, "Loan",   2},
    {13194139544258LL, "Loan",   2},
    {26388279076217LL, "Loan",   2},
    {            4848, "Nguyen", 2},
    { 2199023265573LL, "Nguyen", 2},
    { 8796093031224LL, "Nguyen", 2},
    {26388279068635LL, "Nguyen", 2},
    {28587302322743LL, "Nguyen", 2},
};

// IC3 — SF1 legacy uses anchor 17592186055119, Laos/Scotland, 41-day window.
inline constexpr int64_t     IC3_ANCHOR_PERSON_ID = 17592186055119LL;
inline constexpr const char* IC3_COUNTRY_X        = "Laos";
inline constexpr const char* IC3_COUNTRY_Y        = "Scotland";
inline constexpr int64_t     IC3_DATE_START_MS    = 1306886400000LL;
inline constexpr int64_t     IC3_DATE_END_MS      = 1310515200000LL;
struct IcCountryFriend {
    int64_t     friend_id;
    const char* first_name;
    const char* last_name;
    int64_t     x_count;
    int64_t     y_count;
};
inline constexpr IcCountryFriend IC3_RESULTS[] = {
    {8796093029689LL, "Eun-Hye", "Yoon", 1, 1},
};

// IC4 — SF1 legacy: anchor 21990232559429, window 2012-05-01 to 2012-06-07.
inline constexpr int64_t IC4_ANCHOR_PERSON_ID = 21990232559429LL;
inline constexpr int64_t IC4_VALID_START_MS   = 1335830400000LL;
inline constexpr int64_t IC4_VALID_END_MS     = 1339027200000LL;
inline constexpr IcTagCount IC4_RESULTS[] = {
    {"Hassan_II_of_Morocco",            2},
    {"Appeal_to_Reason",                1},
    {"Principality_of_Littoral_Croatia", 1},
    {"Rivers_of_Babylon",               1},
    {"Van_Morrison",                    1},
};

// IC5 — SF1 legacy: anchor 28587302325306, threshold 2012-07-24, returns 20 rows.
// Test code only spot-checks first 5 rows + ordering (postCount values),
// so we just stub the array; spot-check assertions remain inline.
inline constexpr int64_t IC5_ANCHOR_PERSON_ID = 28587302325306LL;
inline constexpr int64_t IC5_JOIN_THRESHOLD_MS = 1343088000000LL;
struct IcForumPostCount {
    const char* forum_title;
    int64_t     post_count;
    int64_t     forum_id;
};
inline constexpr IcForumPostCount IC5_RESULTS[] = {
    {"Group for She_Blinded_Me_with_Science in Antofagasta", 10, 1236950612644LL},
    // SF1 rows 1-19 are spot-checked inline (legacy values), so we
    // pad here only to mark the array non-empty for compile.
};

// IC9 — SF1 legacy: anchor 13194139542834, threshold 2011-12-17.
// Test only spot-checks rows 0/1 inline.
inline constexpr int64_t IC9_ANCHOR_PERSON_ID  = 13194139542834LL;
inline constexpr int64_t IC9_DATE_THRESHOLD_MS = 1324080000000LL;
struct IcFofMessage {
    int64_t     person_id;
    const char* first_name;
    const char* last_name;
    int64_t     message_id;
    const char* content;
    int64_t     message_creation_ms;
};
inline constexpr IcFofMessage IC9_RESULTS[] = {
    {0, "", "", 0, "", 0},  // SF1 falls back to inline spot-checks.
};

// IC2 — SF1 path keeps its inline spot-checks (legacy hardcoded values).
// IcRecentMessage is still declared so test code compiles either way; the
// SF1 path simply doesn't use IC2_RECENT_MESSAGES.
struct IcRecentMessage {
    int64_t     person_id;
    const char* first_name;
    const char* last_name;
    int64_t     message_id;
    const char* content;
    int64_t     message_creation_ms;
};

// IC6 — SF1 path keeps the legacy inline values (anchor 30786325583618,
// known tag "Angola"). The struct is declared here so the test compiles
// either way; SF1 doesn't read IC6_RESULTS.
inline constexpr int64_t     IC6_ANCHOR_PERSON_ID = 30786325583618LL;
inline constexpr const char* IC6_KNOWN_TAG_NAME   = "Angola";
struct IcTagCount {
    const char* tag_name;
    int64_t     post_count;
};
inline constexpr IcTagCount IC6_RESULTS[] = {
    {"placeholder", 0},
};

// IC7 — SF1 path keeps the legacy spot-checks (anchor 17592186053137).
// Struct/anchor declared so the test compiles either way; SF1 doesn't
// read IC7_RESULTS.
inline constexpr int64_t IC7_ANCHOR_PERSON_ID = 17592186053137LL;
struct IcRecentLiker {
    int64_t     person_id;
    const char* first_name;
    const char* last_name;
    int64_t     like_creation_ms;
    int64_t     comment_or_post_id;
    const char* content;
    int64_t     minutes_latency;
};
inline constexpr IcRecentLiker IC7_RESULTS[] = {
    {0, "", "", 0, 0, "", 0},
};

// IC8 — SF1 path keeps the legacy spot-checks (anchor 24189255818757).
inline constexpr int64_t IC8_ANCHOR_PERSON_ID = 24189255818757LL;
struct IcRecentReply {
    int64_t     person_id;
    const char* first_name;
    const char* last_name;
    int64_t     comment_creation_ms;
    int64_t     comment_id;
    const char* content;
};
inline constexpr IcRecentReply IC8_RESULTS[] = {
    {0, "", "", 0, 0, ""},
};

// IC12 — SF1 legacy uses anchor 17592186052613, base TagClass
// "BasketballPlayer", returns 6 rows.
inline constexpr int64_t     IC12_ANCHOR_PERSON_ID = 17592186052613LL;
inline constexpr const char* IC12_BASE_TAGCLASS    = "BasketballPlayer";
inline constexpr int64_t     IC12_NUM_ROWS         = 6;
inline constexpr int64_t     IC12_TOP_PERSON_ID    = 8796093029854LL;
inline constexpr const char* IC12_TOP_FIRST_NAME   = "Zaenal";
inline constexpr int64_t     IC12_TOP_REPLY_COUNT  = 5;
inline constexpr int64_t     IC12_LAST_PERSON_ID   = 6597069774392LL;
inline constexpr const char* IC12_LAST_FIRST_NAME  = "Michael";
inline constexpr int64_t     IC12_LAST_REPLY_COUNT = 1;

// IC13 / IC14 — SF1 path uses the legacy endpoint pair (17592186055119,
// 8796093025131 for IC13; 17592186055119, 10995116282665 for IC14).
inline constexpr int64_t IC13_SRC_PERSON_ID    = 17592186055119LL;
inline constexpr int64_t IC13_DST_PERSON_ID    = 8796093025131LL;
inline constexpr int64_t IC13_EXPECTED_LENGTH  = 3;
inline constexpr int64_t IC14_SRC_PERSON_ID    = 17592186055119LL;
inline constexpr int64_t IC14_DST_PERSON_ID    = 10995116282665LL;
inline constexpr int64_t IC14_NUM_SHORTEST_PATHS = 7;
inline constexpr const char* IC14_TOP_PATH_WEIGHT_STR    = "30.000000";
inline constexpr const char* IC14_SECOND_PATH_WEIGHT_STR = "28.000000";

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

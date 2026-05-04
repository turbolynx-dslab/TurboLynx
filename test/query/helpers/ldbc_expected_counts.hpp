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
#endif

// Strict lower bound for the [bug-a2] count(*) regression: count(*) on
// a no-label MATCH must exceed the largest single-label count we already
// pin (Person). Same expression on both fixtures.
inline constexpr int64_t COUNT_STAR_LOWER_BOUND = PERSON_COUNT;

}  // namespace ldbc

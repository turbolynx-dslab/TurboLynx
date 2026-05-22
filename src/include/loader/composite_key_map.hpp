#pragma once

#include "common/exception.hpp"
#include "common/typedef.hpp"
#include <cstdint>
#include <limits>
#include <utility>

namespace duckdb {

#ifndef LIDPAIR
#define LIDPAIR
typedef std::pair<idx_t, idx_t> LidPair;
struct LidPairHash {
    size_t operator()(const LidPair& p) const noexcept {
        uint64_t h = static_cast<uint64_t>(p.first) * 0x9e3779b97f4a7c15ULL
                   ^ static_cast<uint64_t>(p.second);
        h ^= h >> 30; h *= 0xbf58476d1ce4e5b9ULL;
        h ^= h >> 27; h *= 0x94d049bb133111ebULL;
        return static_cast<size_t>(h ^ (h >> 31));
    }
};
#endif

}

namespace turbolynx {

using namespace duckdb;

// Single-column reference for a composite (key1, key2) vertex follows
// Neo4j's string-concat encoding: parseUInt(str(k1) + str(k2)).
// Numerically: k1 * 10^digits(k2) + k2 (with digits("0") = 1).
// Throws InvalidInputException on uint64 overflow so the loader fails
// fast on un-representable composites instead of silently aliasing rows.
// (#24)
inline idx_t EncodeNeo4jCompositeKey(idx_t k1, idx_t k2) {
    idx_t mult = 1, t = k2;
    do { mult *= 10; t /= 10; } while (t > 0);
    constexpr idx_t kMax = std::numeric_limits<idx_t>::max();
    if (k1 != 0 && mult > kMax / k1) {
        throw InvalidInputException(
            "Composite vertex key overflows uint64 when encoded as a "
            "single column (#24): key1=%llu, key2=%llu",
            (unsigned long long)k1, (unsigned long long)k2);
    }
    idx_t hi = k1 * mult;
    if (hi > kMax - k2) {
        throw InvalidInputException(
            "Composite vertex key overflows uint64 when encoded as a "
            "single column (#24): key1=%llu, key2=%llu",
            (unsigned long long)k1, (unsigned long long)k2);
    }
    return hi + k2;
}

}

#pragma once
//
// Shared result comparator used by L2 (metamorphic), L3 (differential
// vs Neo4j), and L4 (stateful CRUD).  Operates on the existing
// `qtest::QueryResult` type so layer adapters can reuse the in-process
// `QueryRunner` without reshaping rows.
//
// NULL is unified across both sides (NULL == NULL, NULL != any
// non-null).  Int64, bool, and string compare via the std::variant
// operator==.  Row width must match.
//
// Float/double values surface through `QueryRunner` as `%f`-formatted
// strings, so they compare byte-exact today.  An epsilon-tolerant path
// is intentionally deferred until `qtest::Value` carries a real double
// variant (out of scope for P0).

#include "helpers/query_runner.hpp"
#include <string>

namespace tl_fuzz {

enum class CompareMode {
    Multiset,  // Row order does not matter; compare as a multiset.
    Ordered,   // Row order must match pairwise (use when ORDER BY drives the query).
};

struct CompareResult {
    bool equal = true;
    std::string diff;  // Human-readable diff when !equal; empty otherwise.

    explicit operator bool() const { return equal; }
};

CompareResult compare(const qtest::QueryResult& lhs,
                      const qtest::QueryResult& rhs,
                      CompareMode mode = CompareMode::Multiset);

// Lower-level helpers exposed so layer-specific comparators can build
// custom matchers without re-implementing the NULL / variant logic.
bool values_equal(const qtest::Value& a, const qtest::Value& b);
bool rows_equal(const qtest::Row& a, const qtest::Row& b);

// Stable canonical string for a row.  Used internally for multiset
// sorting; also useful for hashing in higher layers.
std::string row_canonical(const qtest::Row& r);

}  // namespace tl_fuzz

#pragma once
//
// L3 differential read-query generator.
//
// One `ReadQueryGenerator` per shape category (match-by-id,
// traverse-1hop, aggregation, …) emits N random read-only Cypher
// queries that must return identical multisets on TurboLynx and the
// Neo4j oracle.  The fuzz driver pre-seeds the same deterministic
// state on both backends before running each category.

#include <random>
#include <string>
#include <vector>

namespace tl_fuzz_oracle {

class ReadQueryGenerator {
public:
    virtual ~ReadQueryGenerator() = default;

    // Human-readable name; doubles as the Catch2 TEST_CASE tag suffix.
    virtual const char* name() const = 0;

    // Emit `n` read-only queries.  Implementations must be deterministic
    // given the same seed so failures replay exactly.
    virtual std::vector<std::string>
    emit(std::mt19937& rng, size_t n) const = 0;
};

// The shared seed state used by every L3 TEST_CASE.  Executes the same
// CREATE stream on both backends so a `Match`/`Aggregate`/`Traverse`
// generator can return queries whose expected result is the same.
std::vector<std::string> shared_l3_seed_statements();

}  // namespace tl_fuzz_oracle

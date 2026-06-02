#pragma once
//
// L2 metamorphic rewriter abstraction.
//
// A `Rewriter` knows how to emit pairs of Cypher queries
// `(Q_original, Q_rewritten)` that must produce the same multiset
// result on any TurboLynx state.  Each rewriter is one Catch2
// TEST_CASE and runs an N-shot loop against a single seeded workspace,
// comparing every pair through the shared `tl_fuzz::compare()` helper.
//
// Rewriters are intentionally narrow in v1 (template substitution, no
// random AST generation).  The framework will grow into a real random
// generator once the comparator + lifecycle have proven themselves on
// hand-crafted equivalences.

#include "result_canonicalizer.hpp"

#include <random>
#include <string>
#include <utility>
#include <vector>

namespace tl_fuzz_l2 {

struct QueryPair {
    std::string lhs;
    std::string rhs;
};

class Rewriter {
public:
    virtual ~Rewriter() = default;

    // Human-readable name; doubles as the Catch2 SECTION/tag suffix.
    virtual const char* name() const = 0;

    // Emit `n` semantically-equivalent (Q_lhs, Q_rhs) pairs using `rng`.
    // Implementations should be deterministic given the same seed so
    // failures replay exactly.
    virtual std::vector<QueryPair> emit(std::mt19937& rng, size_t n) const = 0;
};

}  // namespace tl_fuzz_l2

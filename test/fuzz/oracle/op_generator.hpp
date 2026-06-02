#pragma once
//
// L4 stateful CRUD op generator.
//
// Each `OpGenerator` is a named stream of Cypher write+read pairs that
// exercise one mutation shape (CREATE node, MATCH+SET, DETACH DELETE,
// …).  After every write the driver emits a fixed probe — at minimum
// `count(*)` for each label/rel type currently in scope — so we can
// catch silent state drift early.
//
// Generators are deterministic given an RNG seed so failures replay.

#include <random>
#include <string>
#include <vector>

namespace tl_fuzz_oracle {

// One step in an op stream: a write followed by zero or more probes.
struct Op {
    std::string write;
    std::vector<std::string> probes;
};

class OpGenerator {
public:
    virtual ~OpGenerator() = default;

    // Human-readable name; used as the Catch2 SECTION tag suffix.
    virtual const char* name() const = 0;

    // Emit `n` ops for one fuzz iteration.  Implementations should be
    // deterministic given the same seed.
    virtual std::vector<Op> emit(std::mt19937& rng, size_t n) const = 0;
};

}  // namespace tl_fuzz_oracle

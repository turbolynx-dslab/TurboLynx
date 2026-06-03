// l2.collect-count — `count(DISTINCT x)` ≡ `size(collect(DISTINCT x))`.
//
// Two equivalent ways to count distinct values must agree.  Catches
// aggregate-parity bugs where the collect-driven path and the
// dedicated count path diverge (e.g. NULL handling, dictionary
// vectors, hash-aggregate keys).

#include "../rewriter.hpp"

#include <array>

namespace tl_fuzz_l2 {

namespace {

struct AggCase {
    const char* match;
    const char* expr;
};

constexpr std::array<AggCase, 4> kCases = {{
    {"(n:Person)",                       "n.age"},
    {"(n:Person)",                       "n.id"},
    {"(m:Movie)",                        "m.year"},
    {"(p:Person)-[:ACTED_IN]->(m:Movie)", "m.title"},
}};

class CollectCountRewriter final : public Rewriter {
public:
    const char* name() const override { return "collect-count"; }

    std::vector<QueryPair> emit(std::mt19937& rng, size_t n) const override {
        std::vector<QueryPair> pairs;
        pairs.reserve(n);
        std::uniform_int_distribution<size_t> pick(0, kCases.size() - 1);
        for (size_t i = 0; i < n; ++i) {
            const AggCase& c = kCases[pick(rng)];
            std::string head = std::string("MATCH ") + c.match + " ";
            std::string lhs = head + "RETURN count(DISTINCT " + c.expr +
                              ") AS cnt";
            std::string rhs = head + "RETURN size(collect(DISTINCT " +
                              c.expr + ")) AS cnt";
            pairs.push_back({std::move(lhs), std::move(rhs)});
        }
        return pairs;
    }
};

}  // namespace

const Rewriter& collect_count_rewriter() {
    static CollectCountRewriter r;
    return r;
}

}  // namespace tl_fuzz_l2

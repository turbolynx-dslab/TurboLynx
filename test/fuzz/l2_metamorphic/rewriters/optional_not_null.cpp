// l2.optional-not-null — `OPTIONAL MATCH (a)-[r]->(b) WHERE r IS NOT NULL`
//                       ≡ plain `MATCH (a)-[r]->(b)`.
//
// An OPTIONAL MATCH that filters out the null-padded rows must collapse
// back to a regular MATCH.  Catches OPTIONAL plumbing that emits the
// nulls in the wrong shape or misses pre-filter rewriting.

#include "../rewriter.hpp"

#include <array>

namespace tl_fuzz_l2 {

namespace {

struct OptCase {
    const char* src;
    const char* rel;
    const char* dst;
    const char* ret;
};

constexpr std::array<OptCase, 4> kCases = {{
    {"a:Person", "KNOWS",    "b:Person", "a.id, b.id ORDER BY a.id, b.id"},
    {"a:Person", "ACTED_IN", "b:Movie",  "a.id, b.id ORDER BY a.id, b.id"},
    {"a:Person", "DIRECTED", "b:Movie",  "a.id, b.title ORDER BY a.id"},
    {"a:Person", "KNOWS",    "b:Person", "a.name ORDER BY a.name"},
}};

class OptionalNotNullRewriter final : public Rewriter {
public:
    const char* name() const override { return "optional-not-null"; }

    std::vector<QueryPair> emit(std::mt19937& rng, size_t n) const override {
        std::vector<QueryPair> pairs;
        pairs.reserve(n);
        std::uniform_int_distribution<size_t> pick(0, kCases.size() - 1);
        for (size_t i = 0; i < n; ++i) {
            const OptCase& c = kCases[pick(rng)];
            std::string pat = std::string("(") + c.src + ")-[r:" +
                              c.rel + "]->(" + c.dst + ")";
            std::string lhs = "MATCH " + pat + " RETURN " + c.ret;
            std::string rhs = "OPTIONAL MATCH " + pat +
                              " WHERE r IS NOT NULL RETURN " + c.ret;
            pairs.push_back({std::move(lhs), std::move(rhs)});
        }
        return pairs;
    }
};

}  // namespace

const Rewriter& optional_not_null_rewriter() {
    static OptionalNotNullRewriter r;
    return r;
}

}  // namespace tl_fuzz_l2

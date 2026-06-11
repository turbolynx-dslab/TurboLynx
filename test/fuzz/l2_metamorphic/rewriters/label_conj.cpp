// l2.label-conj — `MATCH (n:Person) WHERE …` ≡
//                  `MATCH (n) WHERE n:Person AND …`.
//
// The label predicate in the pattern position should be equivalent to
// the same predicate moved into a WHERE conjunct on a label-less
// pattern.  Exercises the binder + planner path that lowers pattern
// labels into per-PS scan dispatch vs the same predicate expressed as
// a post-scan filter conjunct.

#include "../rewriter.hpp"

#include <array>

namespace tl_fuzz_l2 {

namespace {

struct LabelCase {
    const char* label;
    const char* prop;       // property key the predicate touches
    int         threshold;  // predicate value
    const char* op;         // comparison op
};

constexpr std::array<LabelCase, 4> kCases = {{
    {"Person", "age",  20, ">"},
    {"Person", "age",  30, ">="},
    {"Movie",  "year", 2002, ">="},
    {"Movie",  "year", 2003, "<"},
}};

class LabelConjRewriter final : public Rewriter {
public:
    const char* name() const override { return "label-conj"; }

    std::vector<QueryPair> emit(std::mt19937& rng, size_t n) const override {
        std::vector<QueryPair> pairs;
        pairs.reserve(n);
        std::uniform_int_distribution<size_t> pick(0, kCases.size() - 1);
        for (size_t i = 0; i < n; ++i) {
            const LabelCase& c = kCases[pick(rng)];
            std::string lhs =
                std::string("MATCH (n:") + c.label +
                ") WHERE n." + c.prop + " " + c.op + " " +
                std::to_string(c.threshold) +
                " RETURN n.id ORDER BY n.id";
            std::string rhs =
                std::string("MATCH (n) WHERE n:") + c.label +
                " AND n." + c.prop + " " + c.op + " " +
                std::to_string(c.threshold) +
                " RETURN n.id ORDER BY n.id";
            pairs.push_back({std::move(lhs), std::move(rhs)});
        }
        return pairs;
    }
};

}  // namespace

const Rewriter& label_conj_rewriter() {
    static LabelConjRewriter r;
    return r;
}

}  // namespace tl_fuzz_l2

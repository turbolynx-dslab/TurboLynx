// l2.with-passthrough — `MATCH … WITH n RETURN …` ≡ `MATCH … RETURN …`.
//
// A `WITH n` that keeps every bound variable and adds no projection
// must be a planning no-op.  Catches WITH-level column drops and
// scope-handling regressions.

#include "../rewriter.hpp"

#include <array>

namespace tl_fuzz_l2 {

namespace {

struct WithCase {
    const char* match;
    const char* ret;
};

constexpr std::array<WithCase, 4> kCases = {{
    {"(n:Person)",                       "n.id ORDER BY n.id"},
    {"(m:Movie)",                        "m.title ORDER BY m.title"},
    {"(p:Person)-[:KNOWS]->(q:Person)", "p.id, q.id ORDER BY p.id, q.id"},
    {"(p:Person)-[:ACTED_IN]->(m:Movie)",
                                         "p.name, m.title ORDER BY p.name, m.title"},
}};

class WithPassthroughRewriter final : public Rewriter {
public:
    const char* name() const override { return "with-passthrough"; }

    std::vector<QueryPair> emit(std::mt19937& rng, size_t n) const override {
        std::vector<QueryPair> pairs;
        pairs.reserve(n);
        std::uniform_int_distribution<size_t> pick(0, kCases.size() - 1);
        for (size_t i = 0; i < n; ++i) {
            const WithCase& c = kCases[pick(rng)];
            std::string head = std::string("MATCH ") + c.match + " ";
            std::string ret  = std::string("RETURN ") + c.ret;
            std::string lhs = head + ret;
            // `WITH *` is the true passthrough — keeps every bound
            // variable in scope.  `WITH n` for a single-var match would
            // also work but generalises poorly across multi-variable
            // patterns.
            std::string rhs = head + "WITH * " + ret;
            pairs.push_back({std::move(lhs), std::move(rhs)});
        }
        return pairs;
    }
};

}  // namespace

const Rewriter& with_passthrough_rewriter() {
    static WithPassthroughRewriter r;
    return r;
}

}  // namespace tl_fuzz_l2

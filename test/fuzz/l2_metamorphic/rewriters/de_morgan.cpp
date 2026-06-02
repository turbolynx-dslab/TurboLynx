// l2.de-morgan — `NOT (p AND q)` ≡ `NOT p OR NOT q`.
//
// Classical De Morgan equivalence on the boolean operators.  Catches
// three-valued-logic regressions where TurboLynx's NULL handling
// inside NOT/OR diverges from the rewritten form.

#include "../rewriter.hpp"

#include <array>

namespace tl_fuzz_l2 {

namespace {

struct PredCase {
    const char* match;
    const char* p;
    const char* q;
};

constexpr std::array<PredCase, 4> kCases = {{
    {"(n:Person)", "n.age > 25",   "n.name = 'Bob'"},
    {"(n:Person)", "n.id < 4",     "n.age > 30"},
    {"(m:Movie)",  "m.year < 2003", "m.title = 'Beta'"},
    {"(p:Person)-[:KNOWS]->(q:Person)",
                   "p.age > 25", "q.age < 40"},
}};

class DeMorganRewriter final : public Rewriter {
public:
    const char* name() const override { return "de-morgan"; }

    std::vector<QueryPair> emit(std::mt19937& rng, size_t n) const override {
        std::vector<QueryPair> pairs;
        pairs.reserve(n);
        std::uniform_int_distribution<size_t> pick(0, kCases.size() - 1);
        for (size_t i = 0; i < n; ++i) {
            const PredCase& c = kCases[pick(rng)];
            std::string head = std::string("MATCH ") + c.match + " ";
            const char* tail = " RETURN count(*) AS cnt";
            std::string lhs = head + "WHERE NOT (" + c.p + " AND " + c.q + ")" + tail;
            std::string rhs = head + "WHERE NOT (" + c.p + ") OR NOT (" + c.q + ")" + tail;
            pairs.push_back({std::move(lhs), std::move(rhs)});
        }
        return pairs;
    }
};

}  // namespace

const Rewriter& de_morgan_rewriter() {
    static DeMorganRewriter r;
    return r;
}

}  // namespace tl_fuzz_l2

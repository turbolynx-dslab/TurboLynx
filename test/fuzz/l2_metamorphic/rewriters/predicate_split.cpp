// l2.predicate-split — `WHERE p AND q` ≡ `WHERE p WITH * WHERE q`.
//
// Splitting a conjunctive predicate across a no-op WITH must not move
// rows in or out of the result.  Catches WITH-scope handling and
// filter-pushdown regressions.

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
    {"(n:Person)", "n.age > 25",      "n.name <> 'Bob'"},
    {"(n:Person)", "n.id < 4",        "n.age >= 30"},
    {"(m:Movie)",  "m.year > 2001",   "m.title <> 'Beta'"},
    {"(p:Person)-[:KNOWS]->(q:Person)",
                   "p.age >= q.age",  "p.id <> q.id"},
}};

class PredicateSplitRewriter final : public Rewriter {
public:
    const char* name() const override { return "predicate-split"; }

    std::vector<QueryPair> emit(std::mt19937& rng, size_t n) const override {
        std::vector<QueryPair> pairs;
        pairs.reserve(n);
        std::uniform_int_distribution<size_t> pick(0, kCases.size() - 1);
        for (size_t i = 0; i < n; ++i) {
            const PredCase& c = kCases[pick(rng)];
            std::string head = std::string("MATCH ") + c.match + " ";
            const char* tail = " RETURN count(*) AS cnt";
            std::string lhs = head + "WHERE " + c.p + " AND " + c.q + tail;
            std::string rhs = head + "WHERE " + c.p +
                              " WITH * WHERE " + c.q + tail;
            pairs.push_back({std::move(lhs), std::move(rhs)});
        }
        return pairs;
    }
};

}  // namespace

const Rewriter& predicate_split_rewriter() {
    static PredicateSplitRewriter r;
    return r;
}

}  // namespace tl_fuzz_l2

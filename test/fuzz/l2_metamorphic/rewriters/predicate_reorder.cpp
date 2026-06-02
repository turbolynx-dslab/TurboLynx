// l2.predicate-reorder — `WHERE p AND q` ≡ `WHERE q AND p`.
//
// Logical AND is commutative; flipping operands must not change the
// row set.  Catches short-circuit evaluation that leaks NULL handling
// asymmetries and predicate-reorder optimisations that don't preserve
// semantics across reorders.

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
    {"(n:Person)", "n.age > 25",   "n.name <> 'Bob'"},
    {"(n:Person)", "n.age < 40",   "n.id > 1"},
    {"(m:Movie)",  "m.year > 2001", "m.title <> 'Alpha'"},
    {"(p:Person)-[:KNOWS]->(q:Person)",
                   "p.age > q.age", "p.id < q.id"},
}};

class PredicateReorderRewriter final : public Rewriter {
public:
    const char* name() const override { return "predicate-reorder"; }

    std::vector<QueryPair> emit(std::mt19937& rng, size_t n) const override {
        std::vector<QueryPair> pairs;
        pairs.reserve(n);
        std::uniform_int_distribution<size_t> pick(0, kCases.size() - 1);
        for (size_t i = 0; i < n; ++i) {
            const PredCase& c = kCases[pick(rng)];
            std::string head = std::string("MATCH ") + c.match + " ";
            // Use a stable RETURN regardless of which match we picked.
            const char* tail = " RETURN count(*) AS cnt";
            std::string lhs = head + "WHERE " + c.p + " AND " + c.q + tail;
            std::string rhs = head + "WHERE " + c.q + " AND " + c.p + tail;
            pairs.push_back({std::move(lhs), std::move(rhs)});
        }
        return pairs;
    }
};

}  // namespace

const Rewriter& predicate_reorder_rewriter() {
    static PredicateReorderRewriter r;
    return r;
}

}  // namespace tl_fuzz_l2

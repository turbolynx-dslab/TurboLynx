// l2.varlen-decomp — `[*1..2]` ≡ `[*1..1] UNION [*2..2]`.
//
// A variable-length range can be decomposed into the disjoint union
// of its fixed-length sub-ranges.  Catches VLE iterator off-by-one
// errors and UNION-of-traversals plumbing regressions.

#include "../rewriter.hpp"

#include <array>

namespace tl_fuzz_l2 {

namespace {

struct VarLenCase {
    const char* src_label;
    const char* rel;
    const char* dst_label;
};

constexpr std::array<VarLenCase, 3> kCases = {{
    {"Person", "KNOWS",    "Person"},
    {"Person", "ACTED_IN", "Movie"},
    {"Person", "DIRECTED", "Movie"},
}};

class VarLenDecompRewriter final : public Rewriter {
public:
    const char* name() const override { return "varlen-decomp"; }

    std::vector<QueryPair> emit(std::mt19937& rng, size_t n) const override {
        std::vector<QueryPair> pairs;
        pairs.reserve(n);
        std::uniform_int_distribution<size_t> pick(0, kCases.size() - 1);
        for (size_t i = 0; i < n; ++i) {
            const VarLenCase& c = kCases[pick(rng)];
            std::string a = std::string("(a:") + c.src_label + ")";
            std::string b = std::string("(b:") + c.dst_label + ")";
            std::string rel = std::string("[:") + c.rel;
            // Combined: [*1..2]
            std::string lhs =
                "MATCH " + a + "-" + rel + "*1..2]->" + b +
                " RETURN a.id, b.id ORDER BY a.id, b.id";
            // Decomposed: [*1..1] UNION ALL [*2..2]
            std::string rhs =
                "MATCH " + a + "-" + rel + "*1..1]->" + b +
                " RETURN a.id, b.id "
                "UNION ALL "
                "MATCH " + a + "-" + rel + "*2..2]->" + b +
                " RETURN a.id, b.id "
                "ORDER BY a.id, b.id";
            pairs.push_back({std::move(lhs), std::move(rhs)});
        }
        return pairs;
    }
};

}  // namespace

const Rewriter& varlen_decomp_rewriter() {
    static VarLenDecompRewriter r;
    return r;
}

}  // namespace tl_fuzz_l2

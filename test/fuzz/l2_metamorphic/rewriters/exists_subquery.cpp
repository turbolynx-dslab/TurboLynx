// l2.exists-subquery — pattern existence in WHERE ≡ semi-join via
// DISTINCT projection.
//
//   MATCH (a:Person) WHERE (a)-[:T]->()  RETURN DISTINCT a.id
// ≡ MATCH (a:Person)-[:T]->()           RETURN DISTINCT a.id
//
// The first phrases the same set as a filter using a pattern-existence
// predicate; the second projects the anchor through a regular MATCH +
// DISTINCT.  Both must enumerate the same set of anchors when the
// pattern is satisfied at least once per anchor.  Exercises the
// planner's decorrelation / semi-join handling and the binder's
// pattern-in-WHERE lowering.

#include "../rewriter.hpp"

#include <array>

namespace tl_fuzz_l2 {

namespace {

struct ExistsCase {
    const char* anchor_label;
    const char* edge_type;
    const char* direction;   // "->" or "<-"
    const char* dst_label;   // empty string = anonymous (any label)
};

constexpr std::array<ExistsCase, 4> kCases = {{
    {"Person", "KNOWS",    "->", ""},
    {"Person", "KNOWS",    "<-", ""},
    {"Person", "ACTED_IN", "->", "Movie"},
    {"Movie",  "ACTED_IN", "<-", "Person"},
}};

class ExistsSubqueryRewriter final : public Rewriter {
public:
    const char* name() const override { return "exists-subquery"; }

    std::vector<QueryPair> emit(std::mt19937& rng, size_t n) const override {
        std::vector<QueryPair> pairs;
        pairs.reserve(n);
        std::uniform_int_distribution<size_t> pick(0, kCases.size() - 1);
        auto build_pattern = [](const ExistsCase& c, const char* a_var,
                                const char* dst_alias) {
            std::string lhs_arrow, rhs_arrow;
            if (std::string(c.direction) == "->") {
                lhs_arrow = "-[:";
                lhs_arrow += c.edge_type;
                lhs_arrow += "]->";
            } else {
                lhs_arrow = "<-[:";
                lhs_arrow += c.edge_type;
                lhs_arrow += "]-";
            }
            std::string b = "(";
            if (dst_alias && *dst_alias) {
                b += dst_alias;
            }
            if (c.dst_label && *c.dst_label) {
                b += ":";
                b += c.dst_label;
            }
            b += ")";
            std::string pat = std::string("(") + a_var + ")" + lhs_arrow + b;
            return pat;
        };
        for (size_t i = 0; i < n; ++i) {
            const ExistsCase& c = kCases[pick(rng)];
            std::string anchor_pat =
                std::string("MATCH (a:") + c.anchor_label + ")";
            std::string lhs =
                anchor_pat + " WHERE " + build_pattern(c, "a", "") +
                " RETURN DISTINCT a.id ORDER BY a.id";
            std::string rhs =
                std::string("MATCH (a:") + c.anchor_label + ")" +
                build_pattern(c, "a", "").substr(3 /*drop leading "(a)"*/) +
                " RETURN DISTINCT a.id ORDER BY a.id";
            pairs.push_back({std::move(lhs), std::move(rhs)});
        }
        return pairs;
    }
};

}  // namespace

const Rewriter& exists_subquery_rewriter() {
    static ExistsSubqueryRewriter r;
    return r;
}

}  // namespace tl_fuzz_l2

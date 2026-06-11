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
};

// Anonymous endpoint is always label-less today; labelled anonymous
// endpoints (`()-[:T]->(:Movie)`) need partition filtering on the
// semi-join side and are tracked separately.
constexpr std::array<ExistsCase, 4> kCases = {{
    {"Person", "KNOWS",    "->"},
    {"Person", "KNOWS",    "<-"},
    {"Person", "ACTED_IN", "->"},
    {"Movie",  "ACTED_IN", "<-"},
}};

class ExistsSubqueryRewriter final : public Rewriter {
public:
    const char* name() const override { return "exists-subquery"; }

    std::vector<QueryPair> emit(std::mt19937& rng, size_t n) const override {
        std::vector<QueryPair> pairs;
        pairs.reserve(n);
        std::uniform_int_distribution<size_t> pick(0, kCases.size() - 1);
        for (size_t i = 0; i < n; ++i) {
            const ExistsCase& c = kCases[pick(rng)];
            std::string arrow_lhs, arrow_rhs;
            if (std::string(c.direction) == "->") {
                arrow_lhs = "-[:";
                arrow_lhs += c.edge_type;
                arrow_lhs += "]->";
            } else {
                arrow_lhs = "<-[:";
                arrow_lhs += c.edge_type;
                arrow_lhs += "]-";
            }
            std::string anchor_pat =
                std::string("MATCH (a:") + c.anchor_label + ")";
            std::string lhs =
                anchor_pat + " WHERE (a)" + arrow_lhs + "()" +
                " RETURN DISTINCT a.id ORDER BY a.id";
            std::string rhs =
                std::string("MATCH (a:") + c.anchor_label + ")" + arrow_lhs +
                "()" + " RETURN DISTINCT a.id ORDER BY a.id";
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

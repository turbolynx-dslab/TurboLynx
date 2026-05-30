// l2.edge-flip — `(a)-[r:T]->(b)` ≡ `(b)<-[r:T]-(a)`.
//
// The two patterns name the same edge from opposite ends.  TurboLynx's
// adjacency lookup must be symmetric: any traversal expressed in one
// direction must return identical rows when expressed in the other.

#include "../rewriter.hpp"

#include <array>

namespace tl_fuzz_l2 {

namespace {

struct RelCase {
    const char* type;
    const char* src_label;
    const char* dst_label;
};

constexpr std::array<RelCase, 3> kCases = {{
    {"KNOWS",    "Person", "Person"},
    {"ACTED_IN", "Person", "Movie"},
    {"DIRECTED", "Person", "Movie"},
}};

class EdgeFlipRewriter final : public Rewriter {
public:
    const char* name() const override { return "edge-flip"; }

    std::vector<QueryPair> emit(std::mt19937& rng, size_t n) const override {
        std::vector<QueryPair> pairs;
        pairs.reserve(n);
        std::uniform_int_distribution<size_t> pick(0, kCases.size() - 1);
        for (size_t i = 0; i < n; ++i) {
            const RelCase& c = kCases[pick(rng)];
            std::string lhs = std::string("MATCH (a:") + c.src_label +
                ")-[:" + c.type + "]->(b:" + c.dst_label + ") "
                "RETURN a.id, b.id ORDER BY a.id, b.id";
            std::string rhs = std::string("MATCH (b:") + c.dst_label +
                ")<-[:" + c.type + "]-(a:" + c.src_label + ") "
                "RETURN a.id, b.id ORDER BY a.id, b.id";
            pairs.push_back({std::move(lhs), std::move(rhs)});
        }
        return pairs;
    }
};

}  // namespace

const Rewriter& edge_flip_rewriter() {
    static EdgeFlipRewriter r;
    return r;
}

}  // namespace tl_fuzz_l2

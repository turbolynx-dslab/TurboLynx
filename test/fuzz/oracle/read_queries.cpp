#include "read_query_generator.hpp"

#include <array>
#include <sstream>

namespace tl_fuzz_oracle {

namespace {

// All fuzz state uses the `:Fz` namespace label and a `kind` property
// (see op_generator.cpp for why).  Person ids: 1..10; Movie ids:
// 101..105.  Edges: KNOWS (Person→Person) and ACTED_IN
// (Person→Movie); see shared_l3_seed_statements().
constexpr int64_t kPersonIdMin   = 1;
constexpr int64_t kPersonIdMax   = 10;
constexpr int64_t kMovieIdMin    = 101;
constexpr int64_t kMovieIdMax    = 105;

inline std::string to_str(int64_t v) { return std::to_string(v); }

// ---- l3.match-by-id -------------------------------------------------------
//
// Single-id seek with optional property projection.  Stresses IdSeek,
// the (mostly resolved) MPV anchor handling, and per-property reads.
class MatchByIdGen final : public ReadQueryGenerator {
public:
    const char* name() const override { return "match-by-id"; }

    std::vector<std::string>
    emit(std::mt19937& rng, size_t n) const override {
        std::vector<std::string> out;
        out.reserve(n);
        std::uniform_int_distribution<int64_t> pick_person(
            kPersonIdMin, kPersonIdMax);
        std::uniform_int_distribution<int64_t> pick_movie(
            kMovieIdMin, kMovieIdMax);
        std::uniform_int_distribution<int> coin(0, 1);
        std::uniform_int_distribution<int> proj_pick(0, 2);
        for (size_t i = 0; i < n; ++i) {
            bool is_person = coin(rng) == 0;
            int64_t id = is_person ? pick_person(rng) : pick_movie(rng);
            const char* proj =
                proj_pick(rng) == 0 ? "n.name" :
                proj_pick(rng) == 1 ? "n.kind" :
                                       "n.id";
            std::ostringstream q;
            q << "MATCH (n:Fz {id:" << id << "}) RETURN " << proj;
            out.push_back(q.str());
        }
        return out;
    }
};

// ---- l3.traverse-1hop -----------------------------------------------------
//
// Single-hop traversal with both endpoints anchored on `:Fz`.  Verifies
// adj-list direction symmetry, AdjIdxJoin output, and per-direction
// projection layout.
class Traverse1HopGen final : public ReadQueryGenerator {
public:
    const char* name() const override { return "traverse-1hop"; }

    std::vector<std::string>
    emit(std::mt19937& rng, size_t n) const override {
        std::vector<std::string> out;
        out.reserve(n);
        static constexpr std::array<const char*, 2> kRels = {
            "KNOWS", "ACTED_IN"};
        std::uniform_int_distribution<size_t> pick(0, kRels.size() - 1);
        std::uniform_int_distribution<int> coin(0, 1);
        std::uniform_int_distribution<int64_t> pick_person(
            kPersonIdMin, kPersonIdMax);
        for (size_t i = 0; i < n; ++i) {
            const char* rel = kRels[pick(rng)];
            std::ostringstream q;
            // Anchor the LHS so the test has a bounded result set; flip
            // direction half the time to exercise reverse traversal.
            int64_t aid = pick_person(rng);
            if (coin(rng) == 0) {
                q << "MATCH (a:Fz {id:" << aid << "})-[:" << rel
                  << "]->(b:Fz) RETURN b.id ORDER BY b.id";
            } else {
                q << "MATCH (b:Fz)<-[:" << rel << "]-(a:Fz {id:" << aid
                  << "}) RETURN b.id ORDER BY b.id";
            }
            out.push_back(q.str());
        }
        return out;
    }
};

// ---- l3.aggregation -------------------------------------------------------
//
// count / sum / avg / min / max / collect over a single label.  Catches
// hash-aggregate keying, NULL handling, and dictionary-vector
// regressions that the per-row probe set in L4 doesn't reach.
class AggregationGen final : public ReadQueryGenerator {
public:
    const char* name() const override { return "aggregation"; }

    std::vector<std::string>
    emit(std::mt19937& rng, size_t n) const override {
        std::vector<std::string> out;
        out.reserve(n);
        static constexpr std::array<const char*, 5> kAggs = {
            "count(n.age) AS v",
            "sum(n.age) AS v",
            "avg(n.age) AS v",
            "min(n.age) AS v",
            "max(n.age) AS v",
        };
        std::uniform_int_distribution<size_t> pick(0, kAggs.size() - 1);
        for (size_t i = 0; i < n; ++i) {
            std::ostringstream q;
            q << "MATCH (n:Fz {kind:'Person'}) RETURN " << kAggs[pick(rng)];
            out.push_back(q.str());
        }
        return out;
    }
};

// ---- l3.with-chain --------------------------------------------------------
//
// Multi-WITH projection.  Catches scope handling, column drop, and
// pass-through plumbing inside WITH.
class WithChainGen final : public ReadQueryGenerator {
public:
    const char* name() const override { return "with-chain"; }

    std::vector<std::string>
    emit(std::mt19937& rng, size_t n) const override {
        std::vector<std::string> out;
        out.reserve(n);
        std::uniform_int_distribution<int> coin(0, 1);
        for (size_t i = 0; i < n; ++i) {
            std::ostringstream q;
            // Two different chains, both equivalent to a plain MATCH+WHERE
            // when run on the seeded state.
            if (coin(rng) == 0) {
                q << "MATCH (n:Fz {kind:'Person'}) "
                  << "WITH n.age AS age "
                  << "WHERE age > 25 "
                  << "RETURN count(*) AS cnt";
            } else {
                q << "MATCH (n:Fz {kind:'Person'}) "
                  << "WITH n, n.age AS age "
                  << "WHERE age > 25 "
                  << "WITH n.id AS id "
                  << "RETURN id ORDER BY id";
            }
            out.push_back(q.str());
        }
        return out;
    }
};

// ---- l3.order-limit-skip --------------------------------------------------
//
// ORDER BY x LIMIT k SKIP m on a single label.  Catches sort
// stability, paging arithmetic, and per-column collation.
class OrderLimitSkipGen final : public ReadQueryGenerator {
public:
    const char* name() const override { return "order-limit-skip"; }

    std::vector<std::string>
    emit(std::mt19937& rng, size_t n) const override {
        std::vector<std::string> out;
        out.reserve(n);
        std::uniform_int_distribution<int> lim(1, 5);
        std::uniform_int_distribution<int> skip(0, 3);
        std::uniform_int_distribution<int> coin(0, 1);
        for (size_t i = 0; i < n; ++i) {
            int k = lim(rng);
            int m = skip(rng);
            const char* key = coin(rng) == 0 ? "n.id" : "n.age";
            std::ostringstream q;
            q << "MATCH (n:Fz {kind:'Person'}) "
              << "RETURN n.id ORDER BY " << key << ", n.id "
              << "SKIP " << m << " LIMIT " << k;
            out.push_back(q.str());
        }
        return out;
    }
};

}  // namespace

const ReadQueryGenerator& match_by_id_gen() {
    static MatchByIdGen g; return g;
}
const ReadQueryGenerator& traverse_1hop_gen() {
    static Traverse1HopGen g; return g;
}
const ReadQueryGenerator& aggregation_gen() {
    static AggregationGen g; return g;
}
const ReadQueryGenerator& with_chain_gen() {
    static WithChainGen g; return g;
}
const ReadQueryGenerator& order_limit_skip_gen() {
    static OrderLimitSkipGen g; return g;
}

// Deterministic CREATE stream used by every L3 TEST_CASE.  10 Persons,
// 5 Movies, a KNOWS ring plus a few ACTED_IN edges.  Numbers chosen so
// aggregations have non-trivial values without exploding the query
// surface.
std::vector<std::string> shared_l3_seed_statements() {
    std::vector<std::string> s;
    // Persons 1..10 with varying age.
    for (int64_t id = kPersonIdMin; id <= kPersonIdMax; ++id) {
        std::ostringstream q;
        q << "CREATE (n:Fz {id:" << id
          << ", kind:'Person'"
          << ", name:'P_" << id << "'"
          << ", age:" << (20 + (id * 3) % 50) << "})";
        s.push_back(q.str());
    }
    // Movies 101..105.
    for (int64_t id = kMovieIdMin; id <= kMovieIdMax; ++id) {
        std::ostringstream q;
        q << "CREATE (n:Fz {id:" << id
          << ", kind:'Movie'"
          << ", title:'M_" << id << "'"
          << ", year:" << (1990 + ((int)id) % 30) << "})";
        s.push_back(q.str());
    }
    // KNOWS ring 1→2→3→…→10→1.
    for (int64_t id = kPersonIdMin; id <= kPersonIdMax; ++id) {
        int64_t next =
            (id == kPersonIdMax) ? kPersonIdMin : id + 1;
        std::ostringstream q;
        q << "MATCH (a:Fz {id:" << id
          << "}), (b:Fz {id:" << next
          << "}) CREATE (a)-[:KNOWS]->(b)";
        s.push_back(q.str());
    }
    // ACTED_IN — one per Movie, anchored on the i-th Person.
    for (int64_t mid = kMovieIdMin; mid <= kMovieIdMax; ++mid) {
        int64_t pid = (mid - kMovieIdMin) + 1;
        std::ostringstream q;
        q << "MATCH (p:Fz {id:" << pid
          << "}), (m:Fz {id:" << mid
          << "}) CREATE (p)-[:ACTED_IN]->(m)";
        s.push_back(q.str());
    }
    return s;
}

}  // namespace tl_fuzz_oracle

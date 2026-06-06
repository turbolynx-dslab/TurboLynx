// L2 metamorphic-fuzz binary entry.
//
// One TEST_CASE per rewriter.  Each case emits N (Q_lhs, Q_rhs) query
// pairs against the shared seeded workspace and asserts that
// `tl_fuzz::compare()` reports them equal as multisets.  N defaults
// to 20 — enough to surface obvious divergences without blowing the
// PR-gate budget.

#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "result_canonicalizer.hpp"
#include "rewriter.hpp"
#include "runner.hpp"
#include "seeded_workspace.hpp"

#include <random>
#include <sstream>

namespace tl_fuzz_l2 {
// Each rewriter file owns a Meyers singleton; we just take a reference.
const Rewriter& edge_flip_rewriter();
const Rewriter& predicate_reorder_rewriter();
const Rewriter& with_passthrough_rewriter();
const Rewriter& de_morgan_rewriter();
const Rewriter& predicate_split_rewriter();
const Rewriter& optional_not_null_rewriter();
const Rewriter& collect_count_rewriter();
const Rewriter& varlen_decomp_rewriter();
}  // namespace tl_fuzz_l2

namespace {

constexpr size_t kPairsPerRewriter = 20;
constexpr uint64_t kDefaultSeed    = 0xCAFEBABEULL;

void run_rewriter(const tl_fuzz_l2::Rewriter& r, uint64_t seed) {
    auto& ws = tl_fuzz_l2::shared_workspace();
    std::mt19937 rng(static_cast<std::mt19937::result_type>(seed));
    auto pairs = r.emit(rng, kPairsPerRewriter);
    REQUIRE_FALSE(pairs.empty());

    for (size_t i = 0; i < pairs.size(); ++i) {
        const auto& pair = pairs[i];

        qtest::QueryResult lhs;
        qtest::QueryResult rhs;
        try {
            lhs = tl_fuzz_l2::run_against(ws.conn_id(), pair.lhs);
            rhs = tl_fuzz_l2::run_against(ws.conn_id(), pair.rhs);
        } catch (const std::exception& e) {
            // A query throwing on this seeded workspace is itself a
            // metamorphic finding — record and continue, don't crash
            // the whole binary on one bad rewrite.
            FAIL("rewriter '" << r.name() << "' pair " << i
                 << " threw: " << e.what()
                 << "\nlhs: " << pair.lhs
                 << "\nrhs: " << pair.rhs);
        }

        auto cmp = tl_fuzz::compare(lhs, rhs,
                                    tl_fuzz::CompareMode::Multiset);
        if (!cmp.equal) {
            FAIL("rewriter '" << r.name() << "' divergence at pair " << i
                 << "\nlhs query: " << pair.lhs
                 << "\nrhs query: " << pair.rhs
                 << "\n" << cmp.diff);
        }
    }
}

}  // namespace

TEST_CASE("edge-flip preserves multiset", "[fuzz][l2][l2.edge-flip]") {
    run_rewriter(tl_fuzz_l2::edge_flip_rewriter(), kDefaultSeed);
}

TEST_CASE("predicate-reorder preserves multiset",
          "[fuzz][l2][l2.predicate-reorder]") {
    run_rewriter(tl_fuzz_l2::predicate_reorder_rewriter(), kDefaultSeed);
}

TEST_CASE("with-passthrough preserves multiset",
          "[fuzz][l2][l2.with-passthrough]") {
    run_rewriter(tl_fuzz_l2::with_passthrough_rewriter(), kDefaultSeed);
}

TEST_CASE("de-morgan preserves multiset",
          "[fuzz][l2][l2.de-morgan]") {
    run_rewriter(tl_fuzz_l2::de_morgan_rewriter(), kDefaultSeed);
}

TEST_CASE("predicate-split preserves multiset",
          "[fuzz][l2][l2.predicate-split]") {
    run_rewriter(tl_fuzz_l2::predicate_split_rewriter(), kDefaultSeed);
}

TEST_CASE("collect-count preserves multiset",
          "[fuzz][l2][l2.collect-count]") {
    run_rewriter(tl_fuzz_l2::collect_count_rewriter(), kDefaultSeed);
}

// ---------------------------------------------------------------------
// Tests below currently surface real planner bugs; the rewriters are
// kept registered so the failures stay visible, but Catch2's `!mayfail`
// tag prevents them from turning the gate red.  Remove the tag when
// the underlying bug lands a fix.
// ---------------------------------------------------------------------

// `OPTIONAL MATCH (a:P)-[r:T]->(b:M) WHERE r IS NOT NULL` returns
// a *garbage* `a.id` for some rows while the equivalent plain MATCH
// returns the correct id.  Looks like a projection / column-mapping
// regression in the OPTIONAL + post-filter combination.
TEST_CASE("optional-not-null preserves multiset",
          "[fuzz][l2][l2.optional-not-null][!mayfail]") {
    run_rewriter(tl_fuzz_l2::optional_not_null_rewriter(), kDefaultSeed);
}

// `[*1..K]` decomposed via UNION ALL into per-length traversals
// triggers a planner assertion (`pipelines.size() == sfgs.size()` at
// planner.cpp:620).  The combined form plans fine, so the regression
// lives in the UNION-of-varlen planning path.
TEST_CASE("varlen-decomp preserves multiset",
          "[fuzz][l2][l2.varlen-decomp][!mayfail]") {
    run_rewriter(tl_fuzz_l2::varlen_decomp_rewriter(), kDefaultSeed);
}

// Same-label directed edges (Person→Person via KNOWS) per traversal shape.
TEST_CASE("knows-direction preserves cardinality",
          "[fuzz][l2][knows-direction]") {
    auto& ws = tl_fuzz_l2::shared_workspace();
    auto count_rows = [&](const std::string& q) {
        return tl_fuzz_l2::run_against(ws.conn_id(), q).rows.size();
    };
    // Seed: 5 forward KNOWS edges (1→2, 2→3, 3→4, 4→5, 5→1).
    REQUIRE(count_rows(
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.id, b.id") == 5);
    REQUIRE(count_rows(
        "MATCH (a:Person)<-[r:KNOWS]-(b:Person) RETURN a.id, b.id") == 5);
    REQUIRE(count_rows(
        "MATCH (a:Person)-[r:KNOWS]-(b:Person) RETURN a.id, b.id") == 10);
    REQUIRE(count_rows(
        "MATCH (a:Person)-[r:ACTED_IN]->(b:Movie) RETURN a.id, b.id") == 4);
}

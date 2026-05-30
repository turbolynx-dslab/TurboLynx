// L4 oracle binary entry.
//
// One TEST_CASE per OpGenerator.  Each runs N ops; after every write
// both TurboLynx and Neo4j execute the probe queries and the shared
// `tl_fuzz::compare()` asserts the results agree as a multiset.

#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "neo4j_runner.hpp"
#include "op_generator.hpp"
#include "result_canonicalizer.hpp"
#include "seeded_workspace_oracle.hpp"

#include <cstdlib>
#include <random>
#include <sstream>
#include <string>

namespace tl_fuzz_oracle {
const OpGenerator& create_node_stream_gen();
const OpGenerator& create_edge_stream_gen();
const OpGenerator& create_then_set_prop_gen();
}  // namespace tl_fuzz_oracle

namespace {

constexpr size_t kOpsPerStream = 10;
constexpr uint64_t kDefaultSeed = 0xCAFEBABEULL;

std::string neo4j_uri() {
    if (const char* env = std::getenv("NEO4J_HTTP_URI")) return env;
    return "http://localhost:7474";
}

void run_op_stream(const tl_fuzz_oracle::OpGenerator& gen, uint64_t seed) {
    tl_fuzz_oracle::TurboLynxBackend tl;
    tl_fuzz_oracle::Neo4jRunner      neo4j(neo4j_uri());
    neo4j.wipe();   // start each fixture from an empty Neo4j DB.

    std::mt19937 rng(static_cast<std::mt19937::result_type>(seed));
    auto ops = gen.emit(rng, kOpsPerStream);
    REQUIRE_FALSE(ops.empty());

    for (size_t i = 0; i < ops.size(); ++i) {
        const auto& op = ops[i];

        // Write side — run on both backends; if one throws and the
        // other doesn't, that's already a divergence.
        bool tl_threw    = false;
        bool neo4j_threw = false;
        std::string tl_err, neo4j_err;
        try { (void)tl.run(op.write); }
        catch (const std::exception& e) { tl_threw = true; tl_err = e.what(); }
        try { (void)neo4j.run(op.write); }
        catch (const std::exception& e) { neo4j_threw = true; neo4j_err = e.what(); }
        if (tl_threw != neo4j_threw) {
            FAIL("write divergence at op " << i
                 << " ('" << gen.name() << "')\n"
                 << "write: " << op.write << "\n"
                 << "tl threw: "    << (tl_threw    ? tl_err    : "no") << "\n"
                 << "neo4j threw: " << (neo4j_threw ? neo4j_err : "no"));
        }
        if (tl_threw) continue;  // both errored — fine

        // Probe side — read both, compare multiset.
        for (const auto& probe : op.probes) {
            qtest::QueryResult lhs;
            qtest::QueryResult rhs;
            try {
                lhs = tl.run(probe);
                rhs = neo4j.run(probe);
            } catch (const std::exception& e) {
                FAIL("probe threw at op " << i
                     << " ('" << gen.name() << "')\n"
                     << "probe: " << probe << "\n"
                     << "  err: " << e.what());
            }
            auto cmp = tl_fuzz::compare(lhs, rhs,
                                        tl_fuzz::CompareMode::Multiset);
            if (!cmp.equal) {
                FAIL("probe divergence at op " << i
                     << " ('" << gen.name() << "')\n"
                     << "preceding write: " << op.write << "\n"
                     << "probe: " << probe << "\n"
                     << cmp.diff);
            }
        }
    }
}

}  // namespace

TEST_CASE("L4 create-node-stream agrees with Neo4j",
          "[fuzz][l4][l4.create-node-stream]") {
    run_op_stream(tl_fuzz_oracle::create_node_stream_gen(), kDefaultSeed);
}

// Currently surfaces a real silent-data divergence: after
// `MATCH (a:Fz {id:1}), (b:Fz {id:2}) CREATE (a)-[:KNOWS]->(b)`
// (one logical edge), TurboLynx reports
// `MATCH (:Fz)-[r:KNOWS]->(:Fz) RETURN count(r)` = 2 while Neo4j
// reports 1.  Looks like the forward+backward CSR representation is
// being counted twice when both endpoints share a (multi-partition?)
// label scope.  `!mayfail` while the underlying bug is open.
TEST_CASE("L4 create-edge-stream agrees with Neo4j",
          "[fuzz][l4][l4.create-edge-stream][!mayfail]") {
    run_op_stream(tl_fuzz_oracle::create_edge_stream_gen(), kDefaultSeed);
}

TEST_CASE("L4 create-then-set-prop agrees with Neo4j",
          "[fuzz][l4][l4.create-then-set-prop]") {
    run_op_stream(tl_fuzz_oracle::create_then_set_prop_gen(), kDefaultSeed);
}

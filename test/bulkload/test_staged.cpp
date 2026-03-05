// Staged bulkload tests: start tiny, grow progressively to isolate crashes.
// Each stage adds more data/complexity so the exact failing point is obvious.
//
//  Stage 1 : REGION+NATION vertices only            (30 rows, no edges)
//  Stage 2 : REGION+NATION + nation_isLocatedIn fwd (25 edges, fwd only)
//  Stage 3 : REGION+NATION + nation_isLocatedIn fwd+bwd
//  Stage 4 : All TPC-H vertices, no edges           (~8M rows)
//  Stage 5 : All TPC-H vertices + small edges only  (fwd+bwd)
//  Stage 6 : Full TPC-H SF1                         (all edges fwd+bwd)

#include "catch.hpp"
#include <iostream>
#include <filesystem>

#include "bulkload_test_settings.hpp"
#include "helpers/dataset_registry.hpp"
#include "helpers/dataset_locator.hpp"
#include "helpers/bulkload_runner.hpp"
#include "helpers/db_verifier.hpp"

namespace fs = std::filesystem;

extern BulkloadTestSettings g_settings;
extern bool g_skip_requested;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static fs::path tpch_dir(const BulkloadTestSettings& s) {
    bulktest::DatasetConfig probe;
    probe.local_path = "tpch/sf1";
    auto d = bulktest::DatasetLocator::find(probe, s.data_dir, false);
    return d ? *d : fs::path{};
}

static void run_and_check(const bulktest::DatasetConfig& cfg,
                           const fs::path& data_dir) {
    if (data_dir.empty() || !fs::exists(data_dir)) {
        WARN("tpch/sf1 data not found — skipping staged test");
        g_skip_requested = true;
        return;
    }
    INFO("Running bulkload for '" << cfg.name << "' from: " << data_dir.string());
    auto result = bulktest::BulkloadRunner::run(cfg, data_dir);
    bulktest::WorkspaceGuard ws_guard(result.workspace);
    REQUIRE(result.exit_code == 0);
    // Lightweight structural checks only (no count assertions in staged tests)
    bulktest::DbVerifier v(result.workspace);
    v.check_labels(cfg);
    v.check_edge_types(cfg);
}

// ---------------------------------------------------------------------------
// Stage 1 — Vertices only: REGION (5 rows) + NATION (25 rows)
// ---------------------------------------------------------------------------
TEST_CASE("Staged bulkload: stage1 vertices only (REGION+NATION)",
          "[bulkload][staged][stage1]")
{
    auto dir = tpch_dir(g_settings);

    bulktest::DatasetConfig cfg;
    cfg.name         = "staged-stage1";
    cfg.local_path   = "tpch/sf1";
    cfg.skip_histogram = true;

    cfg.vertices.push_back({"REGION", "REGION", {"region.tbl"}, 5});
    cfg.vertices.push_back({"NATION", "NATION", {"nation.tbl"}, 25});
    // no edges

    run_and_check(cfg, dir);
}

// ---------------------------------------------------------------------------
// Stage 2 — REGION+NATION + IS_LOCATED_IN fwd only (25 edges)
// ---------------------------------------------------------------------------
TEST_CASE("Staged bulkload: stage2 fwd edge (NATION->REGION)",
          "[bulkload][staged][stage2]")
{
    auto dir = tpch_dir(g_settings);

    bulktest::DatasetConfig cfg;
    cfg.name         = "staged-stage2";
    cfg.local_path   = "tpch/sf1";
    cfg.skip_histogram = true;

    cfg.vertices.push_back({"REGION", "REGION", {"region.tbl"}, 5});
    cfg.vertices.push_back({"NATION", "NATION", {"nation.tbl"}, 25});

    bulktest::EdgeConfig e;
    e.type = "IS_LOCATED_IN";
    e.fwd_files = {"nation_isLocatedIn_region.tbl"};
    // no bwd_files
    cfg.edges.push_back(std::move(e));

    run_and_check(cfg, dir);
}

// ---------------------------------------------------------------------------
// Stage 3 — REGION+NATION + IS_LOCATED_IN fwd+bwd (25 edges each way)
// ---------------------------------------------------------------------------
TEST_CASE("Staged bulkload: stage3 fwd+bwd edge (NATION<->REGION)",
          "[bulkload][staged][stage3]")
{
    auto dir = tpch_dir(g_settings);

    bulktest::DatasetConfig cfg;
    cfg.name         = "staged-stage3";
    cfg.local_path   = "tpch/sf1";
    cfg.skip_histogram = true;

    cfg.vertices.push_back({"REGION", "REGION", {"region.tbl"}, 5});
    cfg.vertices.push_back({"NATION", "NATION", {"nation.tbl"}, 25});

    bulktest::EdgeConfig e;
    e.type = "IS_LOCATED_IN";
    e.fwd_files = {"nation_isLocatedIn_region.tbl"};
    e.bwd_files = {"nation_isLocatedIn_region.tbl.backward"};
    cfg.edges.push_back(std::move(e));

    run_and_check(cfg, dir);
}

// ---------------------------------------------------------------------------
// Stage 4 — All TPC-H vertices, no edges (~8M rows total)
// ---------------------------------------------------------------------------
TEST_CASE("Staged bulkload: stage4 all vertices no edges",
          "[bulkload][staged][stage4]")
{
    auto dir = tpch_dir(g_settings);

    bulktest::DatasetConfig cfg;
    cfg.name         = "staged-stage4";
    cfg.local_path   = "tpch/sf1";
    cfg.skip_histogram = true;

    cfg.vertices.push_back({"CUSTOMER", "CUSTOMER", {"customer.tbl"}, 150000});
    cfg.vertices.push_back({"LINEITEM", "LINEITEM", {"lineitem.tbl"}, 6001215});
    cfg.vertices.push_back({"NATION",   "NATION",   {"nation.tbl"},   25});
    cfg.vertices.push_back({"ORDERS",   "ORDERS",   {"orders.tbl"},   1500000});
    cfg.vertices.push_back({"PART",     "PART",     {"part.tbl"},     200000});
    cfg.vertices.push_back({"REGION",   "REGION",   {"region.tbl"},   5});
    cfg.vertices.push_back({"SUPPLIER", "SUPPLIER", {"supplier.tbl"}, 10000});
    // no edges

    run_and_check(cfg, dir);
}

// ---------------------------------------------------------------------------
// Stage 5 — All TPC-H vertices + small edges only (fwd+bwd)
//   Small edges: IS_LOCATED_IN (25), CUST_BELONG_TO (150K), SUPP_BELONG_TO (10K)
// ---------------------------------------------------------------------------
TEST_CASE("Staged bulkload: stage5 all vertices + small edges",
          "[bulkload][staged][stage5]")
{
    auto dir = tpch_dir(g_settings);

    bulktest::DatasetConfig cfg;
    cfg.name         = "staged-stage5";
    cfg.local_path   = "tpch/sf1";
    cfg.skip_histogram = true;

    cfg.vertices.push_back({"CUSTOMER", "CUSTOMER", {"customer.tbl"}, 150000});
    cfg.vertices.push_back({"LINEITEM", "LINEITEM", {"lineitem.tbl"}, 6001215});
    cfg.vertices.push_back({"NATION",   "NATION",   {"nation.tbl"},   25});
    cfg.vertices.push_back({"ORDERS",   "ORDERS",   {"orders.tbl"},   1500000});
    cfg.vertices.push_back({"PART",     "PART",     {"part.tbl"},     200000});
    cfg.vertices.push_back({"REGION",   "REGION",   {"region.tbl"},   5});
    cfg.vertices.push_back({"SUPPLIER", "SUPPLIER", {"supplier.tbl"}, 10000});

    {
        bulktest::EdgeConfig e;
        e.type = "IS_LOCATED_IN";
        e.fwd_files = {"nation_isLocatedIn_region.tbl"};
        e.bwd_files = {"nation_isLocatedIn_region.tbl.backward"};
        cfg.edges.push_back(std::move(e));
    }
    {
        bulktest::EdgeConfig e;
        e.type = "CUST_BELONG_TO";
        e.fwd_files = {"customer_belongTo_nation.tbl"};
        e.bwd_files = {"customer_belongTo_nation.tbl.backward"};
        cfg.edges.push_back(std::move(e));
    }
    {
        bulktest::EdgeConfig e;
        e.type = "SUPP_BELONG_TO";
        e.fwd_files = {"supplier_belongTo_nation.tbl"};
        e.bwd_files = {"supplier_belongTo_nation.tbl.backward"};
        cfg.edges.push_back(std::move(e));
    }

    run_and_check(cfg, dir);
}

// ---------------------------------------------------------------------------
// Stage 6 — Full TPC-H SF1 (all vertices + all 8 edge types, fwd+bwd)
// ---------------------------------------------------------------------------
TEST_CASE("Staged bulkload: stage6 full TPC-H SF1",
          "[bulkload][staged][stage6]")
{
    auto dir = tpch_dir(g_settings);

    bulktest::DatasetConfig cfg;
    cfg.name         = "staged-stage6";
    cfg.local_path   = "tpch/sf1";
    cfg.skip_histogram = true;

    cfg.vertices.push_back({"CUSTOMER", "CUSTOMER", {"customer.tbl"}, 150000});
    cfg.vertices.push_back({"LINEITEM", "LINEITEM", {"lineitem.tbl"}, 6001215});
    cfg.vertices.push_back({"NATION",   "NATION",   {"nation.tbl"},   25});
    cfg.vertices.push_back({"ORDERS",   "ORDERS",   {"orders.tbl"},   1500000});
    cfg.vertices.push_back({"PART",     "PART",     {"part.tbl"},     200000});
    cfg.vertices.push_back({"REGION",   "REGION",   {"region.tbl"},   5});
    cfg.vertices.push_back({"SUPPLIER", "SUPPLIER", {"supplier.tbl"}, 10000});

    auto add_edge = [&](const char* type, const char* fwd, const char* bwd) {
        bulktest::EdgeConfig e;
        e.type = type;
        e.fwd_files = {fwd};
        e.bwd_files = {bwd};
        cfg.edges.push_back(std::move(e));
    };

    add_edge("CUST_BELONG_TO", "customer_belongTo_nation.tbl",    "customer_belongTo_nation.tbl.backward");
    add_edge("COMPOSED_BY",    "lineitem_composedBy_part.tbl",     "lineitem_composedBy_part.tbl.backward");
    add_edge("IS_PART_OF",     "lineitem_isPartOf_orders.tbl",     "lineitem_isPartOf_orders.tbl.backward");
    add_edge("SUPPLIED_BY",    "lineitem_suppliedBy_supplier.tbl", "lineitem_suppliedBy_supplier.tbl.backward");
    add_edge("IS_LOCATED_IN",  "nation_isLocatedIn_region.tbl",    "nation_isLocatedIn_region.tbl.backward");
    add_edge("MADE_BY",        "orders_madeBy_customer.tbl",       "orders_madeBy_customer.tbl.backward");
    add_edge("SUPP_BELONG_TO", "supplier_belongTo_nation.tbl",     "supplier_belongTo_nation.tbl.backward");
    add_edge("PARTSUPP",       "partsupp.tbl",                     "partsupp.tbl.backward");

    run_and_check(cfg, dir);
}

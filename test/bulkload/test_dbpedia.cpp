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

// ---- DBpedia ----------------------------------------------------------------
// DBpedia is a large homogeneous graph: single vertex type NODE (~77M nodes),
// 32 selected edge types (full RDF URIs) out of 1945 available.
// Uses --skip-histogram (set in datasets.json) to reduce bulkload time.
//
// Tests are split into stages to isolate memory issues:
//   [dbpedia][vertex]          - vertex loading only (77M nodes)
//   [dbpedia][edge-small]      - small edge types (<100K edges each, ~300K total)
//   [dbpedia][edge-medium]     - medium edge types (100K–5M edges each, ~7M total)
//   [dbpedia][edge-large]      - large edge types (5M+ edges: type/sameAs/subject/redirects)
//   [bulkload][dbpedia]        - full dataset (original test, all 32 edge types)

// Helper: run a sub-dataset and verify counts. Returns false if data not found.
static bool run_dbpedia_subset(const std::string& dataset_name) {
    const auto& cfg = bulktest::DatasetRegistry::get(dataset_name);

    auto data_dir = bulktest::DatasetLocator::find(cfg, g_settings.data_dir,
                                                    g_settings.do_download);
    if (!data_dir) {
        WARN(dataset_name + " data not found at: "
             + (g_settings.data_dir / cfg.local_path).string()
             + "  -- set --data-dir to the directory containing dbpedia/");
        return false;
    }

    INFO("Running bulkload for " << dataset_name << " from: " << data_dir->string());
    auto result = bulktest::BulkloadRunner::run(cfg, *data_dir);

    bulktest::WorkspaceGuard ws_guard(result.workspace);
    REQUIRE(result.exit_code == 0);

    bulktest::DbVerifier v(result.workspace);
    v.check_labels(cfg);
    if (!cfg.edges.empty()) v.check_edge_types(cfg);

    if (!g_settings.do_generate) {
        v.check_vertex_counts(cfg);
        if (!cfg.edges.empty()) v.check_edge_counts(cfg);
    }
    return true;
}

TEST_CASE("Bulkload DBpedia vertex only", "[dbpedia][vertex]") {
    if (!run_dbpedia_subset("dbpedia-vertex"))
        g_skip_requested = true;
}

TEST_CASE("Bulkload DBpedia edges small", "[dbpedia][edge-small]") {
    // Small edge types: each < 100K edges. Total ~300K edges.
    // Memory budget: lid_to_pid(77M nodes ~3.5GB) + small raw_edges.
    if (!run_dbpedia_subset("dbpedia-edge-small"))
        g_skip_requested = true;
}

TEST_CASE("Bulkload DBpedia edges medium", "[dbpedia][edge-medium]") {
    // Medium edge types: 100K–5M edges each. Total ~7M edges.
    if (!run_dbpedia_subset("dbpedia-edge-medium"))
        g_skip_requested = true;
}

TEST_CASE("Bulkload DBpedia edges large", "[dbpedia][edge-large]") {
    // Large edge types: wikiPageRedirects(6M), subject(19M), sameAs(33M), type(98M).
    // This is the likely OOM trigger. Run after smaller tests pass.
    if (!run_dbpedia_subset("dbpedia-edge-large"))
        g_skip_requested = true;
}

TEST_CASE("Bulkload DBpedia", "[bulkload][dbpedia]") {
    const auto& cfg = bulktest::DatasetRegistry::get("dbpedia");

    auto data_dir = bulktest::DatasetLocator::find(cfg, g_settings.data_dir,
                                                    g_settings.do_download);
    if (!data_dir) {
        WARN("dbpedia data not found at: " + (g_settings.data_dir / cfg.local_path).string()
             + "  -- set --data-dir to the directory containing dbpedia/");
        g_skip_requested = true;
        return;
    }

    INFO("Running bulkload for dbpedia from: " << data_dir->string());
    auto result = bulktest::BulkloadRunner::run(cfg, *data_dir);

    // RAII cleanup of workspace on scope exit
    bulktest::WorkspaceGuard ws_guard(result.workspace);

    REQUIRE(result.exit_code == 0);

    bulktest::DbVerifier v(result.workspace);

    // Run all checks sequentially — SECTIONs are intentionally avoided because
    // Catch2 re-executes the entire TEST_CASE body for each SECTION, which would
    // re-run the bulkload N times.
    v.check_labels(cfg);
    v.check_edge_types(cfg);

    if (g_settings.do_generate) {
        auto vcounts = v.measure_vertex_counts(cfg);
        bulktest::DatasetRegistry::write_counts(cfg.name, vcounts, TEST_DATASETS_JSON);
        std::cout << "[generate] Updated expected_count for " << cfg.name << ":\n";
        for (size_t i = 0; i < cfg.vertices.size(); ++i)
            std::cout << "  " << cfg.vertices[i].label << ": " << vcounts[i] << "\n";

        auto ecounts = v.measure_edge_counts(cfg);
        bulktest::DatasetRegistry::write_edge_counts(cfg.name, ecounts, TEST_DATASETS_JSON);
        std::cout << "[generate] Updated expected_fwd_count for " << cfg.name << " edges:\n";
        for (size_t i = 0; i < cfg.edges.size(); ++i)
            std::cout << "  " << cfg.edges[i].type << ": " << ecounts[i] << "\n";
    } else {
        v.check_vertex_counts(cfg);
        v.check_edge_counts(cfg);
    }
}

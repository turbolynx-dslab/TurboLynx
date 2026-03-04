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

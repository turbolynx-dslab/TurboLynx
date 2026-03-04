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

// ---- LDBC SF1 ---------------------------------------------------------------

TEST_CASE("Bulkload LDBC SF1", "[bulkload][ldbc][sf1]") {
    const auto& cfg = bulktest::DatasetRegistry::get("ldbc-sf1");

    auto data_dir = bulktest::DatasetLocator::find(cfg, g_settings.data_dir,
                                                    g_settings.do_download);
    if (!data_dir) {
        WARN("ldbc-sf1 data not found at: " + (g_settings.data_dir / cfg.local_path).string()
             + "  -- re-run with --download to fetch, or set --data-dir");
        g_skip_requested = true;
        return;
    }

    INFO("Running bulkload for ldbc-sf1 from: " << data_dir->string());
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
        auto counts = v.measure_vertex_counts(cfg);
        bulktest::DatasetRegistry::write_counts(cfg.name, counts, TEST_DATASETS_JSON);
        std::cout << "[generate] Updated expected_count for " << cfg.name << ":\n";
        for (size_t i = 0; i < cfg.vertices.size(); ++i)
            std::cout << "  " << cfg.vertices[i].label << ": " << counts[i] << "\n";
    } else {
        v.check_vertex_counts(cfg);
    }
}

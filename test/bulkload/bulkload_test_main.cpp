#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include <string>
#include <vector>
#include <iostream>
#include <filesystem>

#include "helpers/dataset_registry.hpp"
#include "bulkload_test_settings.hpp"

// Global test settings
BulkloadTestSettings g_settings;
bool g_skip_requested = false;

static void parse_args(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--data-dir" && i + 1 < argc)
            g_settings.data_dir = argv[++i];
        else if (a == "--download")
            g_settings.do_download = true;
        else if (a == "--generate")
            g_settings.do_generate = true;
    }
}

static std::vector<char*> strip_custom_args(int argc, char* argv[], int& out_argc) {
    std::vector<char*> out;
    out.push_back(argv[0]);
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--data-dir")        { ++i; continue; }
        if (a == "--download")        { continue; }
        if (a == "--generate")        { continue; }
        out.push_back(argv[i]);
    }
    out_argc = (int)out.size();
    return out;
}

int main(int argc, char* argv[]) {
    parse_args(argc, argv);

    // Load datasets.json
    try {
        bulktest::DatasetRegistry::load(TEST_DATASETS_JSON);
    } catch (const std::exception& e) {
        std::cerr << "[ERROR] Failed to load datasets.json: " << e.what() << "\n";
        return 1;
    }

    int catch_argc;
    auto catch_argv = strip_custom_args(argc, argv, catch_argc);

    int result = Catch::Session().run(catch_argc, catch_argv.data());

    // If all tests were skipped (data missing), return SKIP code 77
    if (g_skip_requested && result == 0) return 77;
    return result;
}

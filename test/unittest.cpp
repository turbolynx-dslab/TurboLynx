#define CATCH_CONFIG_RUNNER

#include "catch.hpp"
#include "test_config.hpp"

#include <string>
#include <iostream>
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

int main(int argc, char* argv[]) {
    // Parse our custom args first
    ParseArguments(argc, argv);

    // Ensure workspace directory exists
    fs::path workspace_path(g_test_settings.test_workspace);
    if (!fs::exists(workspace_path)) {
        try {
            fs::create_directories(workspace_path);
            std::cout << "[INFO] Created test workspace at: " << workspace_path << std::endl;
        } catch (const std::exception& ex) {
            std::cerr << "[ERROR] Failed to create test workspace: " << ex.what() << std::endl;
            return 1;
        }
    }

    // Strip our custom args before passing to Catch2
    // Catch2 will error on unknown flags like --test-workspace
    std::vector<char*> catch_argv;
    catch_argv.push_back(argv[0]);
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--test-workspace") {
            ++i;  // skip the value too
        } else {
            catch_argv.push_back(argv[i]);
        }
    }
    int catch_argc = static_cast<int>(catch_argv.size());

    return Catch::Session().run(catch_argc, catch_argv.data());
}

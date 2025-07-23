#include "test_config.hpp"

TestSettings g_test_settings;

void ParseArguments(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--test-workspace" && i + 1 < argc) {
            g_test_settings.test_workspace = argv[++i];
        }
    }
}

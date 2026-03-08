#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include <string>
#include <vector>
#include <iostream>

std::string g_db_path;
bool g_skip_requested = false;

static void parse_args(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--db-path" && i + 1 < argc)
            g_db_path = argv[++i];
    }
}

static std::vector<char*> strip_custom_args(int argc, char* argv[], int& out_argc) {
    std::vector<char*> out;
    out.push_back(argv[0]);
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--db-path") { ++i; continue; }
        out.push_back(argv[i]);
    }
    out_argc = (int)out.size();
    return out;
}

int main(int argc, char* argv[]) {
    parse_args(argc, argv);

    if (g_db_path.empty()) {
        std::cerr << "[WARN] No --db-path given; all query tests will be skipped.\n";
    }

    int catch_argc;
    auto catch_argv = strip_custom_args(argc, argv, catch_argc);
    int result = Catch::Session().run(catch_argc, catch_argv.data());

    if (g_skip_requested && result == 0) return 77;
    return result;
}

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "helpers/query_runner.hpp"

#include <string>
#include <vector>
#include <iostream>

std::string g_db_path;
bool g_skip_requested = false;
bool g_has_tpch = false;

// Shared QueryRunner — one connection for all test files.
qtest::QueryRunner* get_runner() {
    static qtest::QueryRunner* runner = nullptr;
    if (!runner) {
        if (g_db_path.empty()) return nullptr;
        runner = new qtest::QueryRunner(g_db_path);
    }
    return runner;
}

static void probe_schema() {
    auto* qr = get_runner();
    if (!qr) return;
    try {
        qr->run("MATCH (n:LINEITEM) RETURN n LIMIT 1");
        g_has_tpch = true;
    } catch (...) {
        g_has_tpch = false;
    }
}

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
    } else {
        probe_schema();
        std::cerr << "[INFO] DB schema: "
                  << (g_has_tpch ? "TPC-H" : "LDBC")
                  << " (" << g_db_path << ")\n";
    }

    int catch_argc;
    auto catch_argv = strip_custom_args(argc, argv, catch_argc);
    int result = Catch::Session().run(catch_argc, catch_argv.data());

    if (g_skip_requested && result == 0) return 77;
    return result;
}

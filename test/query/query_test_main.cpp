#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "helpers/query_runner.hpp"

#include <string>
#include <vector>
#include <iostream>
#include <cstdlib>

std::string g_db_path;
bool g_skip_requested = false;
bool g_has_ldbc = false;
bool g_has_tpch = false;

// Shared QueryRunner — one connection for all test files.
qtest::QueryRunner* get_runner() {
    static qtest::QueryRunner* runner = nullptr;
    if (!runner) {
        if (g_db_path.empty()) return nullptr;
        runner = new qtest::QueryRunner(g_db_path);
        runner->clearDelta();
    }
    return runner;
}

// Expected LDBC SF1 vertex counts (verified against Neo4j 5.24.0)
struct ExpectedCount { const char* label; const char* query; int64_t expected; };
static const ExpectedCount LDBC_CHECKS[] = {
    {"Person",       "MATCH (n:Person) RETURN count(n)",       9892},
    {"Comment",      "MATCH (n:Comment) RETURN count(n)",      2052169},
    {"Post",         "MATCH (n:Post) RETURN count(n)",         1003605},
    {"Forum",        "MATCH (n:Forum) RETURN count(n)",        90492},
    {"Tag",          "MATCH (n:Tag) RETURN count(n)",          16080},
    {"Organisation", "MATCH (n:Organisation) RETURN count(n)", 7955},
    {"Place",        "MATCH (n:Place) RETURN count(n)",        1460},
};

// Expected TPC-H SF1 vertex counts
static const ExpectedCount TPCH_CHECKS[] = {
    {"LINEITEM", "MATCH (n:LINEITEM) RETURN count(n)", 6001215},
    {"ORDERS",   "MATCH (n:ORDERS) RETURN count(n)",   1500000},
    {"CUSTOMER", "MATCH (n:CUSTOMER) RETURN count(n)", 150000},
    {"SUPPLIER", "MATCH (n:SUPPLIER) RETURN count(n)", 10000},
    {"PART",     "MATCH (n:PART) RETURN count(n)",     200000},
    {"NATION",   "MATCH (n:NATION) RETURN count(n)",   25},
};

enum class DbStatus { OK, CONTAMINATED, MISSING };

static DbStatus verify_counts(qtest::QueryRunner* qr, const ExpectedCount* checks,
                               size_t n, const char* db_name) {
    bool any_found = false;
    bool any_mismatch = false;

    for (size_t i = 0; i < n; i++) {
        try {
            int64_t actual = qr->count(checks[i].query);
            any_found = true;
            if (actual != checks[i].expected) {
                std::cerr << "[INTEGRITY] " << db_name << " " << checks[i].label
                          << ": expected " << checks[i].expected
                          << ", got " << actual << "\n";
                any_mismatch = true;
            }
        } catch (...) {
            // Label doesn't exist — not this DB type
        }
    }

    if (!any_found) return DbStatus::MISSING;
    if (any_mismatch) return DbStatus::CONTAMINATED;
    return DbStatus::OK;
}

static void probe_and_verify() {
    auto* qr = get_runner();
    if (!qr) return;

    // Probe LDBC
    auto ldbc_status = verify_counts(qr, LDBC_CHECKS,
                                     sizeof(LDBC_CHECKS) / sizeof(LDBC_CHECKS[0]), "LDBC");
    if (ldbc_status != DbStatus::MISSING) {
        g_has_ldbc = true;
        if (ldbc_status == DbStatus::CONTAMINATED) {
            std::cerr << "[ERROR] LDBC database is contaminated! "
                      << "Reload with: bash /turbograph-v3/scripts/load-ldbc.sh\n";
            std::cerr << "[ERROR] Or run: ./test/bulkload/bulkload_test \"[bulkload][ldbc][sf1]\" "
                      << "--data-dir /source-data\n";
        } else {
            std::cerr << "[OK] LDBC SF1 integrity verified\n";
        }
    }

    // Probe TPC-H
    auto tpch_status = verify_counts(qr, TPCH_CHECKS,
                                     sizeof(TPCH_CHECKS) / sizeof(TPCH_CHECKS[0]), "TPC-H");
    if (tpch_status != DbStatus::MISSING) {
        g_has_tpch = true;
        if (tpch_status == DbStatus::CONTAMINATED) {
            std::cerr << "[ERROR] TPC-H database is contaminated! "
                      << "Reload with: bash /turbograph-v3/scripts/load-tpch.sh\n";
        } else {
            std::cerr << "[OK] TPC-H SF1 integrity verified\n";
        }
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
        probe_and_verify();

        std::string schema;
        if (g_has_ldbc) schema += "LDBC";
        if (g_has_tpch) { if (!schema.empty()) schema += "+"; schema += "TPC-H"; }
        if (schema.empty()) schema = "UNKNOWN";
        std::cerr << "[INFO] DB schema: " << schema << " (" << g_db_path << ")\n";
    }

    int catch_argc;
    auto catch_argv = strip_custom_args(argc, argv, catch_argc);
    int result = Catch::Session().run(catch_argc, catch_argv.data());

    if (g_skip_requested && result == 0) return 77;
    return result;
}

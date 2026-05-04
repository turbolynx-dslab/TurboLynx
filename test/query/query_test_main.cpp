#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include "helpers/ldbc_expected_counts.hpp"
#include "helpers/tpch_expected_counts.hpp"

#include <string>
#include <vector>
#include <iostream>
#include <cstdlib>

std::string g_ldbc_path;
std::string g_tpch_path;
std::string g_oss_path;
bool g_skip_requested = false;
bool g_has_ldbc = false;
bool g_has_tpch = false;
bool g_has_oss = false;

// Only one DB connection can be active at a time (global singletons).
// The runners lazily connect/disconnect as needed.
static qtest::QueryRunner* g_active_runner = nullptr;
static std::string g_active_db;

// Isolated CRUD workspace (copy of LDBC), created lazily on first CRUD test.
// Each CRUD test calls get_crud_runner() which resets the workspace to pristine
// state and reconnects, guaranteeing no catalog/store bleed between tests.
static qtest::IsolatedWorkspace* g_crud_ws = nullptr;

static void disconnect_active_runner_impl() {
    if (g_active_runner) {
        delete g_active_runner;
        g_active_runner = nullptr;
        g_active_db.clear();
    }
}

static qtest::QueryRunner* activate_runner(const std::string& db_path) {
    if (db_path.empty()) return nullptr;
    if (g_active_runner && g_active_db == db_path) return g_active_runner;

    disconnect_active_runner_impl();

    g_active_runner = new qtest::QueryRunner(db_path);
    g_active_db = db_path;
    return g_active_runner;
}

qtest::QueryRunner* get_ldbc_runner() {
    return activate_runner(g_ldbc_path);
}

qtest::QueryRunner* get_tpch_runner() {
    return activate_runner(g_tpch_path);
}

qtest::QueryRunner* get_oss_runner() {
    return activate_runner(g_oss_path);
}

// Returns a fresh runner on a pristine isolated CRUD workspace.
// Every call disconnects the active runner, resets the workspace files,
// and reconnects — so CRUD tests never pollute each other or LDBC.
qtest::QueryRunner* get_crud_runner() {
    if (g_ldbc_path.empty() || !g_has_ldbc) return nullptr;

    disconnect_active_runner_impl();

    if (!g_crud_ws) {
        g_crud_ws = new qtest::IsolatedWorkspace(g_ldbc_path, "/tmp/tl_crud_XXXXXX");
    } else {
        g_crud_ws->reset();
    }

    g_active_runner = new qtest::QueryRunner(g_crud_ws->path());
    g_active_db = g_crud_ws->path();
    return g_active_runner;
}

const std::string& get_crud_workspace_path() {
    static const std::string empty;
    return g_crud_ws ? g_crud_ws->path() : empty;
}

void disconnect_active_runner() {
    disconnect_active_runner_impl();
}

// Expected vertex counts for the LDBC and TPC-H integrity probes.
// Both sets pull from cmake-selected helper headers — the same
// `*_FIXTURE_MINI` define that gates the test files below also picks
// the right values here, so the integrity message stays accurate
// regardless of which fixture (full SF1/SF1, mini SF0.003/SF0.01) the
// caller loads.
struct ExpectedCount { const char* label; const char* query; int64_t expected; };
static const ExpectedCount LDBC_CHECKS[] = {
    {"Person",       "MATCH (n:Person) RETURN count(n)",       ldbc::PERSON_COUNT},
    {"Comment",      "MATCH (n:Comment) RETURN count(n)",      ldbc::COMMENT_COUNT},
    {"Post",         "MATCH (n:Post) RETURN count(n)",         ldbc::POST_COUNT},
    {"Forum",        "MATCH (n:Forum) RETURN count(n)",        ldbc::FORUM_COUNT},
    {"Tag",          "MATCH (n:Tag) RETURN count(n)",          ldbc::TAG_COUNT},
    {"TagClass",     "MATCH (n:TagClass) RETURN count(n)",     ldbc::TAGCLASS_COUNT},
    {"Organisation", "MATCH (n:Organisation) RETURN count(n)", ldbc::ORGANISATION_COUNT},
    {"Place",        "MATCH (n:Place) RETURN count(n)",        ldbc::PLACE_COUNT},
};

static const ExpectedCount TPCH_CHECKS[] = {
    {"LINEITEM", "MATCH (n:LINEITEM) RETURN count(n)", tpch::LINEITEM_COUNT},
    {"ORDERS",   "MATCH (n:ORDERS) RETURN count(n)",   tpch::ORDERS_COUNT},
    {"CUSTOMER", "MATCH (n:CUSTOMER) RETURN count(n)", tpch::CUSTOMER_COUNT},
    {"SUPPLIER", "MATCH (n:SUPPLIER) RETURN count(n)", tpch::SUPPLIER_COUNT},
    {"PART",     "MATCH (n:PART) RETURN count(n)",     tpch::PART_COUNT},
    {"NATION",   "MATCH (n:NATION) RETURN count(n)",   tpch::NATION_COUNT},
    {"REGION",   "MATCH (n:REGION) RETURN count(n)",   tpch::REGION_COUNT},
};

// Expected OSS supply-chain fixture vertex counts
// (applications/oss-supply-chain/tests/fixtures)
static const ExpectedCount OSS_CHECKS[] = {
    {"Package",    "MATCH (n:Package) RETURN count(n)",    6},
    {"Version",    "MATCH (n:Version) RETURN count(n)",    8},
    {"Maintainer", "MATCH (n:Maintainer) RETURN count(n)", 3},
    {"Repository", "MATCH (n:Repository) RETURN count(n)", 3},
    {"License",    "MATCH (n:License) RETURN count(n)",    3},
    {"CVE",        "MATCH (n:CVE) RETURN count(n)",        2},
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
    // Verify LDBC
    if (auto* qr = get_ldbc_runner()) {
        auto status = verify_counts(qr, LDBC_CHECKS,
                                    sizeof(LDBC_CHECKS) / sizeof(LDBC_CHECKS[0]), "LDBC");
        if (status != DbStatus::MISSING) {
            g_has_ldbc = true;
            if (status == DbStatus::CONTAMINATED) {
                std::cerr << "[ERROR] LDBC database is contaminated or wrong scale!\n"
                          << "        Counts must match the SF0.003 mini fixture.\n"
                          << "        Reload with: bash scripts/load-ldbc-mini.sh <build-dir> <ws>\n";
            } else {
                std::cerr << "[OK] LDBC SF0.003 mini fixture integrity verified\n";
            }
        } else {
            std::cerr << "[WARN] --ldbc-path given but DB has no LDBC schema\n";
        }
    }

    // Verify TPC-H (disconnects LDBC first)
    if (auto* qr = get_tpch_runner()) {
        auto status = verify_counts(qr, TPCH_CHECKS,
                                    sizeof(TPCH_CHECKS) / sizeof(TPCH_CHECKS[0]), "TPC-H");
        if (status != DbStatus::MISSING) {
            g_has_tpch = true;
            if (status == DbStatus::CONTAMINATED) {
                std::cerr << "[ERROR] TPC-H database is contaminated or wrong scale!\n"
                          << "        Counts must match the SF0.01 mini fixture.\n"
                          << "        Reload with: bash scripts/load-tpch-mini.sh <build-dir> <ws>\n";
            } else {
                std::cerr << "[OK] TPC-H SF0.01 mini fixture integrity verified\n";
            }
        } else {
            std::cerr << "[WARN] --tpch-path given but DB has no TPC-H schema\n";
        }
    }

    // Verify OSS supply-chain (disconnects TPC-H first)
    if (auto* qr = get_oss_runner()) {
        auto status = verify_counts(qr, OSS_CHECKS,
                                    sizeof(OSS_CHECKS) / sizeof(OSS_CHECKS[0]), "OSS");
        if (status != DbStatus::MISSING) {
            g_has_oss = true;
            if (status == DbStatus::CONTAMINATED) {
                std::cerr << "[ERROR] OSS database is contaminated! "
                          << "Reload with: bash /turbograph-v3/scripts/load-oss.sh\n";
            } else {
                std::cerr << "[OK] OSS supply-chain fixture integrity verified\n";
            }
        } else {
            std::cerr << "[WARN] --oss-path given but DB has no OSS schema\n";
        }
    }
}

static void parse_args(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--ldbc-path" && i + 1 < argc)
            g_ldbc_path = argv[++i];
        else if (a == "--tpch-path" && i + 1 < argc)
            g_tpch_path = argv[++i];
        else if (a == "--oss-path" && i + 1 < argc)
            g_oss_path = argv[++i];
        // Legacy: --db-path sets both (auto-detect which schema it has)
        else if (a == "--db-path" && i + 1 < argc) {
            std::string path = argv[++i];
            if (g_ldbc_path.empty()) g_ldbc_path = path;
            if (g_tpch_path.empty()) g_tpch_path = path;
        }
    }
}

static std::vector<char*> strip_custom_args(int argc, char* argv[], int& out_argc) {
    std::vector<char*> out;
    out.push_back(argv[0]);
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--ldbc-path" || a == "--tpch-path" ||
            a == "--oss-path" || a == "--db-path") { ++i; continue; }
        out.push_back(argv[i]);
    }
    out_argc = (int)out.size();
    return out;
}

int main(int argc, char* argv[]) {
    parse_args(argc, argv);

    if (g_ldbc_path.empty() && g_tpch_path.empty() && g_oss_path.empty()) {
        std::cerr << "[WARN] No --ldbc-path, --tpch-path, or --oss-path given; all query tests will be skipped.\n";
        std::cerr << "[HINT] Usage: query_test --ldbc-path /data/ldbc/sf1 --tpch-path /data/tpch/sf1 --oss-path /data/oss/small\n";
    } else {
        probe_and_verify();

        std::string schema;
        if (g_has_ldbc) schema += "LDBC(" + g_ldbc_path + ")";
        if (g_has_tpch) { if (!schema.empty()) schema += " + "; schema += "TPC-H(" + g_tpch_path + ")"; }
        if (g_has_oss)  { if (!schema.empty()) schema += " + "; schema += "OSS(" + g_oss_path + ")"; }
        if (schema.empty()) schema = "UNKNOWN";
        std::cerr << "[INFO] DB schema: " << schema << "\n";
    }

    int catch_argc;
    auto catch_argv = strip_custom_args(argc, argv, catch_argc);
    int result = Catch::Session().run(catch_argc, catch_argv.data());

    // Clean up active runner
    delete g_active_runner;
    g_active_runner = nullptr;

    // Tear down CRUD workspace after runner (dtor rm -rf's the temp dir)
    delete g_crud_ws;
    g_crud_ws = nullptr;

    if (g_skip_requested && result == 0) return 77;
    return result;
}

// Stage 10 — TPC-H parallel benchmark
// Measures query execution time to evaluate parallel scan performance.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <chrono>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <numeric>

extern std::string g_db_path;
extern bool g_skip_requested;
extern qtest::QueryRunner* get_runner();

#define SKIP_IF_NO_DB() \
    if (g_db_path.empty()) { WARN("--db-path not set, skipping"); g_skip_requested = true; return; } \
    auto* qr = get_runner(); \
    if (!qr) { FAIL("QueryRunner not initialized"); return; }

static const std::string QUERY_DIR = "/turbograph-v3/benchmark/tpch/sf1/";

static std::string readFile(const std::string &path) {
    std::ifstream f(path);
    if (!f.is_open()) return "";
    std::stringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

// Run a query N times and report min/avg/max latency in ms.
static void benchQuery(qtest::QueryRunner *qr, const char *label,
                       const std::string &query, int warmup, int runs) {
    // Warmup
    for (int i = 0; i < warmup; i++) {
        qr->clearDelta();
        qr->run(query.c_str(), {});
    }

    std::vector<double> times;
    times.reserve(runs);
    for (int i = 0; i < runs; i++) {
        qr->clearDelta();
        auto t0 = std::chrono::high_resolution_clock::now();
        auto r = qr->run(query.c_str(), {});
        auto t1 = std::chrono::high_resolution_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        times.push_back(ms);
    }

    double mn = *std::min_element(times.begin(), times.end());
    double mx = *std::max_element(times.begin(), times.end());
    double avg = std::accumulate(times.begin(), times.end(), 0.0) / times.size();

    fprintf(stdout, "[BENCH] %-12s  min=%7.2f ms  avg=%7.2f ms  max=%7.2f ms  (runs=%d)\n",
            label, mn, avg, mx, runs);
    fflush(stdout);
}

// Queries that hit the parallel path (NodeScan → Filter/Proj → HashAgg):
// Q1, Q5, Q6, Q12, Q14, Q15, Q16, Q19

TEST_CASE("Q10-bench TPC-H parallel scan benchmark", "[q10][bench][tpch]") {
    SKIP_IF_NO_DB();

    const int WARMUP = 1;
    const int RUNS = 5;

    fprintf(stdout, "\n=== TPC-H Parallel Scan Benchmark (warmup=%d, runs=%d) ===\n", WARMUP, RUNS);

    // Queries where parallel path activates (simple scan pipelines)
    std::vector<std::pair<std::string, int>> bench_queries = {
        {"Q1",  1}, {"Q5",  5}, {"Q6",  6}, {"Q12", 12},
        {"Q14", 14}, {"Q15", 15}, {"Q16", 16}, {"Q19", 19},
    };

    for (auto &[label, qnum] : bench_queries) {
        std::string query = readFile(QUERY_DIR + "q" + std::to_string(qnum) + ".cql");
        if (query.empty()) {
            WARN("Skipping " << label << ": query file not found");
            continue;
        }
        benchQuery(qr, label.c_str(), query, WARMUP, RUNS);
    }

    fprintf(stdout, "=== End Benchmark ===\n\n");
    fflush(stdout);
}

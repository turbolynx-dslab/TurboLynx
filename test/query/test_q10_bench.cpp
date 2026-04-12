// Stage 10 — TPC-H parallel benchmark
// Measures query execution time to evaluate parallel scan performance.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include "main/capi/turbolynx.h"
#include <chrono>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <numeric>

extern std::string g_db_path;
extern bool g_skip_requested;
extern bool g_has_tpch;
extern qtest::QueryRunner* get_runner();

#define SKIP_IF_NO_DB() \
    if (g_db_path.empty()) { WARN("--db-path not set, skipping"); g_skip_requested = true; return; } \
    if (!g_has_tpch) { WARN("DB has no TPC-H schema, skipping"); return; } \
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

// Drain a resultset and free it.
static void drainResult(turbolynx_resultset_wrapper *rw) {
    if (!rw) return;
    while (turbolynx_fetch_next(rw) != TURBOLYNX_END_OF_RESULT) {}
    turbolynx_close_resultset(rw);
}

// Bench the **execute-only** time of a query (compile is done once outside
// the timing loop and excluded). Returns min execute latency in ms.
static double benchQueryExecOnly(qtest::QueryRunner *qr, const char *label,
                                 const std::string &query, int warmup, int runs) {
    int64_t conn = qr->conn_id();

    // Warmup
    for (int i = 0; i < warmup; i++) {
        auto *prep = turbolynx_prepare(conn, const_cast<char *>(query.c_str()));
        if (!prep) return -1.0;
        turbolynx_resultset_wrapper *rw = nullptr;
        turbolynx_execute(conn, prep, &rw);
        drainResult(rw);
        turbolynx_close_prepared_statement(prep);
    }

    std::vector<double> times;
    times.reserve(runs);
    for (int i = 0; i < runs; i++) {
        auto *prep = turbolynx_prepare(conn, const_cast<char *>(query.c_str()));
        if (!prep) return -1.0;
        auto t0 = std::chrono::high_resolution_clock::now();
        turbolynx_resultset_wrapper *rw = nullptr;
        turbolynx_execute(conn, prep, &rw);
        drainResult(rw);
        auto t1 = std::chrono::high_resolution_clock::now();
        turbolynx_close_prepared_statement(prep);
        times.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
    }

    double mn = *std::min_element(times.begin(), times.end());
    double mx = *std::max_element(times.begin(), times.end());
    double avg = std::accumulate(times.begin(), times.end(), 0.0) / times.size();
    fprintf(stdout, "[BENCH] %-6s  min=%8.2f ms  avg=%8.2f ms  max=%8.2f ms  (runs=%d)\n",
            label, mn, avg, mx, runs);
    fflush(stdout);
    return mn;
}

// Queries that hit the parallel path (NodeScan → Filter/Proj → HashAgg):
// Q1, Q5, Q6, Q12, Q14, Q15, Q16, Q19

static void stressLoop(qtest::QueryRunner *qr, const std::string &query,
                       int threads, int iters, const char *label) {
    turbolynx_set_max_threads(qr->conn_id(), (size_t)threads);
    fprintf(stdout, "[STRESS] %s @ %dt: starting %d iters\n", label, threads, iters);
    fflush(stdout);
    for (int i = 0; i < iters; i++) {
        auto *prep = turbolynx_prepare(qr->conn_id(), const_cast<char *>(query.c_str()));
        REQUIRE(prep != nullptr);
        turbolynx_resultset_wrapper *rw = nullptr;
        turbolynx_execute(qr->conn_id(), prep, &rw);
        if (rw) {
            while (turbolynx_fetch_next(rw) != TURBOLYNX_END_OF_RESULT) {}
            turbolynx_close_resultset(rw);
        }
        turbolynx_close_prepared_statement(prep);
        if ((i + 1) % 5 == 0) {
            fprintf(stdout, "[STRESS] %s iter %d ok\n", label, i + 1);
            fflush(stdout);
        }
    }
}

TEST_CASE("Q10-stress Q21 16-thread loop", "[q10stress][tpch]") {
    SKIP_IF_NO_DB();
    std::string query = readFile(QUERY_DIR + "q21.cql");
    REQUIRE(!query.empty());
    stressLoop(qr, query, 16, 30, "Q21");
}

TEST_CASE("Q10-stress Q12 64-thread loop", "[q10stress][tpch]") {
    SKIP_IF_NO_DB();
    std::string query = readFile(QUERY_DIR + "q12.cql");
    REQUIRE(!query.empty());
    stressLoop(qr, query, 64, 30, "Q12");
}

TEST_CASE("Q10-bench TPC-H parallel scan benchmark", "[q10][bench][tpch]") {
    SKIP_IF_NO_DB();

    const int WARMUP = 1;
    const int RUNS = 3;

    fprintf(stdout, "\n=== TPC-H Parallel Scan Benchmark (warmup=%d, runs=%d) ===\n", WARMUP, RUNS);

    // All TPC-H queries (Q18 skipped — intermittent timeout in debug build).
    std::vector<std::pair<std::string, int>> bench_queries;
    for (int i = 1; i <= 22; i++) {
        if (i == 18) continue;
        bench_queries.push_back({"Q" + std::to_string(i), i});
    }

    // Read queries once
    std::vector<std::pair<std::string, std::string>> qs;
    for (auto &[label, qnum] : bench_queries) {
        std::string query = readFile(QUERY_DIR + "q" + std::to_string(qnum) + ".cql");
        if (query.empty()) {
            WARN("Skipping " << label << ": query file not found");
            continue;
        }
        qs.push_back({label, std::move(query)});
    }

    // Sweep multiple thread counts. Compile time is excluded — only execute
    // time is measured.
    std::vector<int> thread_counts = {1, 4, 16, 64};
    std::vector<std::vector<double>> times_by_threads(thread_counts.size());

    for (size_t ti = 0; ti < thread_counts.size(); ti++) {
        int n = thread_counts[ti];
        fprintf(stdout, "\n--- max_threads=%d (execute-only) ---\n", n);
        turbolynx_set_max_threads(qr->conn_id(), (size_t)n);
        for (auto &[label, q] : qs) {
            times_by_threads[ti].push_back(
                benchQueryExecOnly(qr, label.c_str(), q, WARMUP, RUNS));
        }
    }
    auto &seq_times = times_by_threads[0];      // 1-thread baseline
    auto &par_times = times_by_threads.back();  // 64-thread (compat with old report)

    // Speedup table — show ms for each thread count plus speedup vs 1-thread.
    fprintf(stdout, "\n--- Speedup vs 1 thread (execute-only) ---\n");
    fprintf(stdout, "%-6s", "Query");
    for (int n : thread_counts) fprintf(stdout, "  %8dt(ms)", n);
    for (size_t i = 1; i < thread_counts.size(); i++)
        fprintf(stdout, "  %6dx-sp", thread_counts[i]);
    fprintf(stdout, "\n");

    std::vector<double> totals(thread_counts.size(), 0.0);
    int valid = 0;
    for (size_t i = 0; i < qs.size(); i++) {
        bool ok = true;
        for (size_t ti = 0; ti < thread_counts.size(); ti++) {
            if (times_by_threads[ti][i] < 0) { ok = false; break; }
        }
        if (!ok) {
            fprintf(stdout, "%-6s  ERR\n", qs[i].first.c_str());
            continue;
        }
        fprintf(stdout, "%-6s", qs[i].first.c_str());
        for (size_t ti = 0; ti < thread_counts.size(); ti++) {
            fprintf(stdout, "  %10.2f", times_by_threads[ti][i]);
            totals[ti] += times_by_threads[ti][i];
        }
        double base = times_by_threads[0][i];
        for (size_t ti = 1; ti < thread_counts.size(); ti++) {
            double t = times_by_threads[ti][i];
            fprintf(stdout, "  %7.2fx", t > 0 ? (base / t) : 0.0);
        }
        fprintf(stdout, "\n");
        valid++;
    }
    if (valid > 0) {
        fprintf(stdout, "%-6s", "TOTAL");
        for (size_t ti = 0; ti < thread_counts.size(); ti++)
            fprintf(stdout, "  %10.2f", totals[ti]);
        for (size_t ti = 1; ti < thread_counts.size(); ti++) {
            double t = totals[ti];
            fprintf(stdout, "  %7.2fx", t > 0 ? (totals[0] / t) : 0.0);
        }
        fprintf(stdout, "\n");
    }

    fprintf(stdout, "=== End Benchmark ===\n\n");
    fflush(stdout);
}

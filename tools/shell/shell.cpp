// Signal recovery for crash-proof operation
#include <signal.h>
#include <setjmp.h>
static sigjmp_buf g_crash_jmpbuf;
static volatile sig_atomic_t g_in_query = 0;

static void crash_signal_handler(int sig) {
    if (g_in_query) {
        g_in_query = 0;
        siglongjmp(g_crash_jmpbuf, sig);
    }
    // Not in query context — fall back to default handler
    signal(sig, SIG_DFL);
    raise(sig);
}

// antlr4 headers must come before ORCA (c.h postgres macros)
#include "CypherLexer.h"
#include "CypherParser.h"
#include "parser/cypher_transformer.hpp"
#include "binder/binder.hpp"
#include "common/logger.hpp"
#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "main/client_config.hpp"
#include <regex>
#include "storage/cache/chunk_cache_manager.h"
#include "planner/planner.hpp"
#include "catalog/catalog_wrapper.hpp"
#include "optimizer/orca/gpopt/tbgppdbwrappers.hpp"
#include "replxx.hxx"

#include "include/shell.hpp"
#include "include/commands.hpp"
#include "include/renderer.hpp"
#include "include/completion.hpp"

#include "nl2cypher/nl2cypher_engine.hpp"
#include "nl2cypher/cypher_executor.hpp"
#include "nl2cypher/profile_collector.hpp"

#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <getopt.h>
#include <unistd.h>
#include <chrono>
#include <iostream>
#include <numeric>
#include <optional>
#include <string>

using namespace antlr4;
using namespace gpopt;
using namespace duckdb;

// ------------------------------------------------------------------ ANTLR error listener

// Replaces ANTLR's default stderr printer with an exception throw.
// This prevents the "extraneous input ..." message and stops execution
// before transform() walks a malformed parse tree.
class ThrowingErrorListener : public BaseErrorListener {
public:
    void syntaxError(Recognizer*, Token*, size_t line, size_t col,
                     const std::string& msg, std::exception_ptr) override {
        throw std::runtime_error(
            "Syntax error at " + std::to_string(line) + ":" +
            std::to_string(col) + " — " + msg);
    }
};

// ------------------------------------------------------------------ ANSI helpers

static void PrintError(const std::string& msg) {
    if (isatty(STDERR_FILENO))
        std::cerr << "\033[1;31mError:\033[0m " << msg << '\n';
    else
        std::cerr << "Error: " << msg << '\n';
}

// ------------------------------------------------------------------ options

struct ShellCliOptions {
    std::string workspace;
    std::string query;
    bool        standalone     = false;
    bool        compile_only   = false;
    bool        enable_profile = false;
    int         iterations     = 1;
    bool        warmup         = false;
    turbolynx::PlannerConfig planner_config;
};

static void ParseShellOptions(int argc, char** argv, ShellCliOptions& opts) {
    optind = 1;
    static struct option long_options[] = {
        {"help",                no_argument,       0, 'h'},
        {"workspace",           required_argument, 0, 'w'},
        {"log-level",           required_argument, 0, 'L'},
        {"standalone",          no_argument,       0, 'S'},
        {"compile-only",        no_argument,       0, 'c'},
        {"orca-compile-only",   no_argument,       0, 'O'},
        {"index-join-only",     no_argument,       0, 1001},
        {"hash-join-only",      no_argument,       0, 1002},
        {"merge-join-only",     no_argument,       0, 1003},
        {"disable-merge-join",  no_argument,       0, 1004},
        {"disable-index-join",  no_argument,       0, 1005},
        {"join-order-optimizer",required_argument, 0, 'j'},
        {"debug-orca",          no_argument,       0, 1006},
        {"query",               required_argument, 0, 'q'},
        {"iterations",          required_argument, 0, 'i'},
        {"warmup",              no_argument,       0, 1007},
        {"profile",             no_argument,       0, 1008},
        {"explain",             no_argument,       0, 1009},
        {"mode",                required_argument, 0, 'm'},
        {0, 0, 0, 0}
    };

    int opt, option_index = 0;
    while ((opt = getopt_long(argc, argv, "hw:L:ScOj:q:i:m:", long_options, &option_index)) != -1) {
        switch (opt) {
        case 'h':
            std::cout << "Usage: turbolynx shell [options]\n"
                      << "  --workspace <path>             Workspace directory\n"
                      << "  --query <string>               Execute a single query\n"
                      << "  --mode <table|box|csv|json|...> Output format\n"
                      << "  --iterations <n>               Repeat query N times\n"
                      << "  --warmup                       First iteration is warmup\n"
                      << "  --standalone                   Standalone mode\n"
                      << "  --compile-only                 Compile without execution\n"
                      << "  --profile                      Enable profiling\n"
                      << "  --log-level <level>            Log level\n";
            exit(0);
        case 'w': opts.workspace = optarg; break;
        case 'L': setLogLevel(getLogLevel(optarg)); break;
        case 'S': opts.standalone = true; break;
        case 'c': opts.compile_only = true; break;
        case 'O': opts.compile_only = true; opts.planner_config.ORCA_COMPILE_ONLY = true; break;
        case 1001: opts.planner_config.INDEX_JOIN_ONLY = true; break;
        case 1002: opts.planner_config.HASH_JOIN_ONLY = true; break;
        case 1003: opts.planner_config.MERGE_JOIN_ONLY = true; break;
        case 1004: opts.planner_config.DISABLE_MERGE_JOIN = true; break;
        case 1005: opts.planner_config.DISABLE_INDEX_JOIN = true; break;
        case 'j': {
            std::string o = optarg;
            using JOT = turbolynx::PlannerConfig::JoinOrderType;
            if      (o == "query")       opts.planner_config.JOIN_ORDER_TYPE = JOT::JOIN_ORDER_IN_QUERY;
            else if (o == "greedy")      opts.planner_config.JOIN_ORDER_TYPE = JOT::JOIN_ORDER_GREEDY_SEARCH;
            else if (o == "exhaustive")  opts.planner_config.JOIN_ORDER_TYPE = JOT::JOIN_ORDER_EXHAUSTIVE_SEARCH;
            else if (o == "exhaustive2") opts.planner_config.JOIN_ORDER_TYPE = JOT::JOIN_ORDER_EXHAUSTIVE2_SEARCH;
            else if (o == "gem")         opts.planner_config.JOIN_ORDER_TYPE = JOT::JOIN_ORDER_GEM;
            break;
        }
        case 1006: opts.planner_config.ORCA_DEBUG_PRINT = true; break;
        case 'q': opts.query = optarg; break;
        case 'i': opts.iterations = std::stoi(optarg); break;
        case 1007: opts.warmup = true; break;
        case 1008: opts.enable_profile = true; break;
        case 1009: opts.planner_config.DEBUG_PRINT = true; break;
        case 'm': opts.planner_config.DEBUG_PRINT = false; break; // .mode via CLI (handled separately)
        default: break;
        }
    }
}

// ------------------------------------------------------------------ query execution

struct ExecContext {
    std::shared_ptr<duckdb::ClientContext> client;
    ShellCliOptions&                       cli;
    turbolynx::ShellState&                 state;
    turbolynx::Planner&                    planner;
    // NL2Cypher engine — lazily constructed on the first NL prompt so
    // shells that never use `.nl on` don't pay the LLM-pool warmup cost.
    std::unique_ptr<turbolynx::nl2cypher::NL2CypherEngine> nl_engine;
};

// Query plan cache: query string → parsed AST (skips lexing, parsing, transform)
static std::unordered_map<std::string, std::shared_ptr<RegularQuery>> g_query_cache;

static std::shared_ptr<RegularQuery> ParseAndTransform(
    const std::string& query, ScopedTimer& timer) {
    // Check cache
    auto it = g_query_cache.find(query);
    if (it != g_query_cache.end()) {
        spdlog::debug("[QueryCache] hit");
        return it->second;
    }

    ThrowingErrorListener error_listener;

    timer.start("Lexing");
    auto inputStream = ANTLRInputStream(query);
    CypherLexer lexer(&inputStream);
    lexer.removeErrorListeners();
    lexer.addErrorListener(&error_listener);
    CommonTokenStream tokens(&lexer);
    tokens.fill();
    timer.stop("Lexing");

    timer.start("Parse");
    CypherParser parser(&tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(&error_listener);
    timer.stop("Parse");

    timer.start("ANTLR Parse");
    auto* cypher_ctx = parser.oC_Cypher();
    if (!cypher_ctx)
        throw std::runtime_error("Parser returned no tree — check Cypher syntax");
    timer.stop("ANTLR Parse");

    timer.start("Transform");
    CypherTransformer transformer(*cypher_ctx);
    auto stmt = transformer.transform();
    if (!stmt)
        throw std::runtime_error("Transformer returned null — check Cypher syntax");
    timer.stop("Transform");

    auto shared_stmt = std::shared_ptr<RegularQuery>(std::move(stmt));
    g_query_cache[query] = shared_stmt;
    return shared_stmt;
}

static void CompileQuery(const std::string& query, ExecContext& ctx, double& compile_ms) {
    SCOPED_TIMER(CompileQuery, spdlog::level::info, spdlog::level::debug, compile_ms);

    auto stmt = ParseAndTransform(query, CompileQuery_timer);

    SUBTIMER_START(CompileQuery, "Bind");
    Binder binder(ctx.client.get());
    auto bound = binder.Bind(*stmt);
    SUBTIMER_STOP(CompileQuery, "Bind");

    SUBTIMER_START(CompileQuery, "Orca Compile");
    ctx.planner.execute(bound.get());
    SUBTIMER_STOP(CompileQuery, "Orca Compile");
}

// Build RenderOptions from the current ShellState, consuming output_once if set.
static turbolynx::RenderOptions BuildRenderOptions(turbolynx::ShellState& state) {
    turbolynx::RenderOptions opts;
    opts.show_headers  = state.show_headers;
    opts.null_value    = state.null_value;
    opts.col_sep       = state.col_sep;
    opts.max_rows      = state.max_rows;
    opts.min_col_width = state.min_col_width;
    opts.insert_label  = state.insert_label;
    if (!state.output_once.empty()) {
        opts.output_file   = state.output_once;
        state.output_once.clear();
    } else {
        opts.output_file = state.output_file;
    }
    return opts;
}

static void LogQuery(const std::string& query, double compile_ms, double exec_ms,
                     const std::string& log_file) {
    if (log_file.empty()) return;
    std::ofstream f(log_file, std::ios::app);
    if (!f) return;
    f << "-- " << query << '\n';
    f << "-- compile: " << compile_ms << " ms, execute: " << exec_ms << " ms\n";
}

// Session config statement: PRAGMA threads = N
// Returns parsed N if matched, -1 otherwise.
static int64_t ParseSetThreadsStmt(const std::string& q) {
    std::regex re(R"(^\s*PRAGMA\s+threads\s*=\s*(\d+)\s*;?\s*$)",
                  std::regex::icase);
    std::smatch m;
    if (std::regex_match(q, m, re)) {
        try { return std::stoll(m[1].str()); } catch (...) { return -1; }
    }
    return -1;
}

static void RunOneIteration(const std::string& query, ExecContext& ctx,
                            double& compile_ms, double& exec_ms) {
    // Session config: PRAGMA threads = N  /  SET parallel_threads = N
    {
        int64_t n = ParseSetThreadsStmt(query);
        if (n >= 0) {
            duckdb::ClientConfig::GetConfig(*ctx.client).maximum_threads = (idx_t)n;
            std::cout << "parallel_threads = " << n
                      << (n == 0 ? " (auto)" : "") << "\n";
            compile_ms = 0;
            exec_ms = 0;
            return;
        }
    }

    CompileQuery(query, ctx, compile_ms);
    if (ctx.cli.compile_only) return;

    auto executors = ctx.planner.genPipelineExecutors();
    if (executors.empty()) { spdlog::error("Plan empty"); return; }
    if (!executors.back() || !executors.back()->pipeline ||
        !executors.back()->pipeline->GetSink())
        throw std::runtime_error("Pipeline executor is incomplete");

    bool do_profile = ctx.state.profile || ctx.cli.enable_profile;
    auto& profiler = QueryProfiler::Get(*ctx.client);
    profiler.StartQuery(query, do_profile);
    profiler.Initialize(executors.back()->pipeline->GetSink());

    {
        SCOPED_TIMER(Execute, spdlog::level::info, spdlog::level::info, exec_ms);
        for (size_t i = 0; i < executors.size(); i++) {
            SUBTIMER_START(Execute, "Pipeline " + std::to_string(i));
            executors[i]->ExecutePipeline();
            SUBTIMER_STOP(Execute, "Pipeline " + std::to_string(i));
        }
    }
    profiler.EndQuery();

    auto& results   = executors.back()->context->query_results;
    auto& schema    = executors.back()->pipeline->GetSink()->schema;
    auto  col_names = ctx.planner.getQueryOutputColNames();

    auto render_opts = BuildRenderOptions(ctx.state);
    turbolynx::RenderResults(ctx.state.output_mode, col_names, *results, schema, render_opts);

    // If log_file is set, also render to log file
    if (!ctx.state.log_file.empty()) {
        turbolynx::RenderOptions log_opts = render_opts;
        log_opts.output_file = ctx.state.log_file;
        turbolynx::RenderResults(ctx.state.output_mode, col_names, *results, schema, log_opts);
    }

    // cleanup
    for (auto& chunk : *results) chunk.reset();
    results->clear();
    for (auto* e : executors) delete e;
}

// ------------------------------------------------------------------ NL2Cypher executor
//
// Concrete CypherExecutor that drives the shell's existing Planner
// pipeline and captures the result rows into duckdb::Value vectors
// instead of rendering them. Used by ProfileCollector and (later)
// the S3 multi-candidate validator.
//
// Caveats:
//   * Re-uses ExecContext::planner. Profiling queries share the same
//     planner state as the user queries — harmless because each
//     execute() resets it.
//   * Catches all exceptions and returns {ok=false, error=...} so a
//     bad property doesn't kill a profiling sweep.
//   * Skips rendering, profiling, and the QueryProfiler hooks; this
//     is intentional — profiling queries should be invisible to the
//     normal user-facing profiler.

class ShellCypherExecutor : public turbolynx::nl2cypher::CypherExecutor {
public:
    explicit ShellCypherExecutor(ExecContext& ctx) : ctx_(ctx) {}

    turbolynx::nl2cypher::CypherResult Execute(const std::string& cypher) override {
        using turbolynx::nl2cypher::CypherResult;
        CypherResult out;

        std::string q = cypher;
        if (q.empty() || q.back() != ';') q.push_back(';');

        try {
            double compile_ms = 0;
            CompileQuery(q, ctx_, compile_ms);

            auto executors = ctx_.planner.genPipelineExecutors();
            if (executors.empty()) {
                out.ok = false; out.error = "empty plan";
                return out;
            }
            if (!executors.back() || !executors.back()->pipeline ||
                !executors.back()->pipeline->GetSink()) {
                for (auto* e : executors) delete e;
                out.ok = false; out.error = "incomplete pipeline";
                return out;
            }

            for (size_t i = 0; i < executors.size(); ++i) {
                executors[i]->ExecutePipeline();
            }

            auto& results = executors.back()->context->query_results;
            out.col_names = ctx_.planner.getQueryOutputColNames();

            // Materialise every (chunk, row) into a flat row vector.
            for (auto& chunk : *results) {
                if (!chunk) continue;
                idx_t n_rows = chunk->size();
                idx_t n_cols = chunk->ColumnCount();
                for (idx_t r = 0; r < n_rows; ++r) {
                    std::vector<duckdb::Value> row;
                    row.reserve(n_cols);
                    for (idx_t c = 0; c < n_cols; ++c) {
                        row.push_back(chunk->GetValue(c, r));
                    }
                    out.rows.push_back(std::move(row));
                }
            }

            // cleanup (mirrors RunOneIteration)
            for (auto& chunk : *results) chunk.reset();
            results->clear();
            for (auto* e : executors) delete e;

            out.ok = true;
        } catch (const std::exception& e) {
            out.ok = false; out.error = e.what();
        } catch (...) {
            out.ok = false; out.error = "unknown exception";
        }
        return out;
    }

private:
    ExecContext& ctx_;
};

// Execute `.nl profile` — sweeps the catalog and dumps a JSON profile
// next to the LLM cache. Heavy: every (label/edge × property) gets
// 4 Cypher queries plus an endpoint-inference query per edge type.
static void RunNLProfile(ExecContext& ctx) {
    using namespace turbolynx::nl2cypher;

    GraphSchema schema;
    try {
        schema = IntrospectGraphSchema(*ctx.client);
    } catch (const std::exception& e) {
        PrintError(std::string("schema introspection failed: ") + e.what());
        return;
    }

    std::cout << "[nl-profile] sweeping " << schema.labels.size()
              << " label(s), " << schema.edges.size() << " edge type(s)\n";

    ShellCypherExecutor executor(ctx);
    ProfileCollector::Config cfg;
    cfg.on_property_start = [](const std::string& tag) {
        std::cout << "  " << tag << "\n" << std::flush;
    };
    ProfileCollector collector(executor, cfg);

    GraphProfile profile;
    try {
        profile = collector.CollectAll(schema);
    } catch (const std::exception& e) {
        PrintError(std::string("profile collect threw: ") + e.what());
        return;
    }

    std::cout << "[nl-profile] done in " << profile.collect_seconds
              << "s, " << profile.n_queries << " queries ("
              << profile.n_failed << " failed)\n";

    // Persist to <workspace>/.nl2cypher/metadata.json if a workspace
    // is set; otherwise just print to stdout.
    if (!ctx.state.workspace.empty()) {
        std::string dir  = ctx.state.workspace + "/.nl2cypher";
        std::string path = dir + "/metadata.json";
        std::string mkdir_cmd = "mkdir -p '" + dir + "'";
        if (std::system(mkdir_cmd.c_str()) != 0) {
            PrintError("failed to create " + dir);
            return;
        }
        std::ofstream f(path);
        if (!f) {
            PrintError("failed to open " + path);
            return;
        }
        // `replace` substitutes U+FFFD for any invalid UTF-8 byte —
        // LDBC has binary-ish strings (locationIP, browserUsed) that
        // are not valid UTF-8 and would otherwise abort the dump.
        f << profile.ToJson().dump(
                 2, ' ', false, nlohmann::json::error_handler_t::replace)
          << '\n';
        std::cout << "[nl-profile] wrote " << path << "\n";
    } else {
        std::cout << profile.ToJson().dump(
                         2, ' ', false, nlohmann::json::error_handler_t::replace)
                  << "\n";
    }
}

static void RunQuery(const std::string& query, ExecContext& ctx) {
    if (ctx.state.echo) std::cout << query << '\n';

    int iters = ctx.cli.iterations + (ctx.cli.warmup ? 1 : 0);
    std::vector<double> compile_times, exec_times;

    for (int i = 0; i < iters; i++) {
        double c = 0, e = 0;
        RunOneIteration(query, ctx, c, e);
        compile_times.push_back(c);
        exec_times.push_back(e);
    }

    if (ctx.cli.warmup) {
        if (!compile_times.empty()) compile_times.erase(compile_times.begin());
        if (!exec_times.empty())    exec_times.erase(exec_times.begin());
    }

    if (ctx.state.timer_enabled && !exec_times.empty()) {
        double avg_c = std::accumulate(compile_times.begin(), compile_times.end(), 0.0) / compile_times.size();
        double avg_e = std::accumulate(exec_times.begin(),    exec_times.end(),    0.0) / exec_times.size();
        std::cout << "Time: compile " << avg_c << " ms, execute " << avg_e
                  << " ms, total " << (avg_c + avg_e) << " ms\n";
        LogQuery(query, avg_c, avg_e, ctx.state.log_file);
    }
}

// ------------------------------------------------------------------ AIO init

static void InitializeDiskAIO(const std::string& workspace) {
    DiskAioParameters::NUM_THREADS          = 32;
    DiskAioParameters::NUM_TOTAL_CPU_CORES  = 32;
    DiskAioParameters::NUM_CPU_SOCKETS      = 2;
    DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
    DiskAioParameters::WORKSPACE            = workspace;

    int res;
    new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
    core_id::set_core_ids(DiskAioParameters::NUM_THREADS);
}

// ------------------------------------------------------------------ rc file

static void LoadRcFile(const std::string& path, ExecContext& ctx) {
    std::ifstream f(path);
    if (!f) return;

    auto executor = [&ctx](const std::string& q) {
        try { RunQuery(q, ctx); }
        catch (const std::exception& ex) { PrintError(ex.what()); }
    };

    std::string line;
    while (std::getline(f, line)) {
        // Strip comments
        size_t hash = line.find('#');
        if (hash != std::string::npos) line = line.substr(0, hash);

        size_t start = line.find_first_not_of(" \t");
        if (start == std::string::npos) continue;
        line = line.substr(start);
        if (line.empty()) continue;

        if (line[0] == '.' || line[0] == ':') {
            turbolynx::HandleDotCommand(line, ctx.state, ctx.client, executor);
        } else if (!line.empty() && line.back() == ';') {
            line.pop_back();
            try { RunQuery(line, ctx); }
            catch (const std::exception& ex) { PrintError(ex.what()); }
        }
    }
}

// ------------------------------------------------------------------ replxx input

// Read one logical "unit" from the user.
// Handles:
//   - single-line input (interactive typing)
//   - multi-line paste: replxx accumulates the full paste (including embedded
//     \n) and returns it in a single input() call when paste mode ends
//   - continuation prompting when no ';' found yet
//
// In NL mode (`nl_mode == true`) the function:
//   - shows a "(NL)" suffix in the prompt
//   - submits on ENTER (no semicolon required, no continuation)
//   - still recognises dot/colon commands so the user can `.nl off`
static std::optional<std::string> GetQueryString(replxx::Replxx& rx,
                                                  const std::string& prompt,
                                                  bool nl_mode) {
    std::string full_input;
    std::string mode_tag    = nl_mode ? " (NL)" : "";
    std::string shell_prompt = prompt + mode_tag + " >> ";
    std::string cont_prompt  = prompt + mode_tag + " -> ";

    while (true) {
        errno = 0;
        const char* raw = rx.input(full_input.empty() ? shell_prompt : cont_prompt);
        if (!raw) {
            if (errno == EAGAIN) {
                // Ctrl+C — abort current input and restart
                std::cout << "Interrupted "
                          "(Note that Cypher queries must end with a semicolon. "
                          "Type :exit to exit the shell.)\n";
                full_input.clear();
                continue;
            }
            return std::nullopt;   // Ctrl+D / EOF
        }

        // replxx may return embedded \n when pasting multi-line content.
        // Flatten newlines to spaces for the accumulated query string
        // (Cypher treats whitespace uniformly).
        std::string s;
        s.reserve(strlen(raw));
        for (const char* p = raw; *p; ++p)
            s += (*p == '\n' || *p == '\r') ? ' ' : *p;

        // Strip trailing whitespace
        while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) s.pop_back();
        if (s.empty()) continue;

        // Dot/colon commands — submit immediately without semicolon.
        // Recognised in both Cypher and NL modes so the user can always
        // type `.nl off`, `.exit`, etc.
        if (full_input.empty()) {
            size_t tstart = s.find_first_not_of(" \t");
            if (tstart != std::string::npos &&
                (s[tstart] == '.' || s[tstart] == ':')) {
                return s;
            }
        }

        // NL mode: ENTER submits the line as-is. Natural language has no
        // ';' terminator, and we don't accumulate multi-line input.
        if (nl_mode) {
            return s;
        }

        // Cypher mode: accumulate until ';'
        size_t semi = s.find(';');
        if (semi != std::string::npos) {
            full_input += s.substr(0, semi + 1);
            break;
        }
        full_input += s + ' ';
    }
    return full_input;
}

// NL prompt handler. Lazily constructs the NL2CypherEngine on the
// first call (so the LLM pool is only spawned when the user actually
// turns NL mode on), translates the question to Cypher, prints the
// generated query for transparency, and then runs it through the
// existing Cypher executor path (RunQuery).
static void RunNL(const std::string& nl_text, ExecContext& ctx) {
    using namespace turbolynx::nl2cypher;
    if (!ctx.nl_engine) {
        NL2CypherEngine::Config cfg;
        cfg.workspace = ctx.state.workspace;
        // Pool size 1 — interactive use, one prompt at a time.
        cfg.llm.pool_size = 1;
        try {
            ctx.nl_engine = std::make_unique<NL2CypherEngine>(*ctx.client, cfg);
        } catch (const std::exception& e) {
            PrintError(std::string("NL2Cypher init failed: ") + e.what());
            return;
        }
    }

    TranslationResult tr;
    try {
        tr = ctx.nl_engine->Translate(nl_text);
    } catch (const std::exception& e) {
        PrintError(std::string("NL2Cypher translate threw: ") + e.what());
        return;
    }

    if (!tr.ok) {
        PrintError("NL2Cypher: " + tr.error);
        return;
    }

    // Show the generated Cypher so the user can verify / copy / edit.
    std::cout << "-- generated Cypher"
              << (tr.used_cache ? " (cached)" : "")
              << " --\n"
              << tr.cypher << "\n"
              << "-------------------------\n";

    // Hand off to the existing executor path. We append a semicolon if
    // missing — RunQuery itself trims trailing semicolons.
    std::string to_run = tr.cypher;
    if (to_run.empty() || to_run.back() != ';') to_run.push_back(';');
    try {
        RunQuery(to_run, ctx);
    } catch (const std::exception& e) {
        PrintError(std::string("execution failed: ") + e.what());
    }
}

// ------------------------------------------------------------------ REPL

static void RunInteractive(replxx::Replxx& rx, ExecContext& ctx) {
    std::string prev;
    std::cout << "TurboLynx shell — type '.help' for commands, ':exit' to quit\n";

    while (true) {
        auto opt_input = GetQueryString(rx, ctx.state.prompt, ctx.state.nl_mode);
        if (!opt_input) break;   // Ctrl+D / EOF
        std::string input = std::move(*opt_input);
        if (input.empty()) continue;

        std::string trimmed = input;
        size_t start = trimmed.find_first_not_of(" \t\n\r");
        if (start != std::string::npos) trimmed = trimmed.substr(start);

        // Exit commands
        if (trimmed == ":exit" || trimmed == ".exit" || trimmed == ".quit") break;

        if (!trimmed.empty() && (trimmed[0] == '.' || trimmed[0] == ':')) {
            if (!trimmed.empty() && trimmed.back() == ';') trimmed.pop_back();
            // `.nl profile` needs ExecContext (planner) — handle it here
            // before HandleDotCommand, which only sees state+client.
            if (trimmed == ".nl profile" || trimmed == ":nl profile") {
                if (trimmed != prev) { rx.history_add(trimmed); prev = trimmed; }
                try { RunNLProfile(ctx); }
                catch (const std::exception& e) { PrintError(e.what()); }
                continue;
            }
            auto executor = [&ctx](const std::string& q) {
                try { RunQuery(q, ctx); }
                catch (const std::exception& ex) { PrintError(ex.what()); }
            };
            if (turbolynx::HandleDotCommand(trimmed, ctx.state, ctx.client, executor)) {
                if (trimmed != prev) {
                    rx.history_add(trimmed);
                    prev = trimmed;
                }
                continue;
            }
            break;
        }

        // NL mode: bypass the Cypher pipeline entirely. Each ENTER is one
        // NL prompt, sent (eventually) through NL2Cypher. For now we hand
        // it to a stub so the mode is testable without the LLM path.
        if (ctx.state.nl_mode) {
            // Skip empty / whitespace-only prompts
            if (trimmed.empty()) continue;
            if (trimmed != prev) {
                rx.history_add(trimmed);
                prev = trimmed;
            }
            try {
                RunNL(trimmed, ctx);
            } catch (const std::exception& e) {
                PrintError(e.what());
                if (ctx.state.bail) break;
            }
            continue;
        }

        // Cypher query — strip trailing semicolon and whitespace
        while (!input.empty() && (input.back() == ';' || input.back() == ' '))
            input.pop_back();

        // Skip empty / whitespace-only queries
        if (input.find_first_not_of(" \t\n\r") == std::string::npos) continue;

        if (input != prev) {
            rx.history_add(input);
            prev = input;
        }

        // Signal-safe query execution: catch SIGSEGV/SIGABRT via longjmp
        int crash_sig = sigsetjmp(g_crash_jmpbuf, 1);
        if (crash_sig != 0) {
            // Recovered from a signal — report as error, not crash
            const char *sig_name = crash_sig == SIGSEGV ? "SIGSEGV" :
                                   crash_sig == SIGABRT ? "SIGABRT" :
                                   crash_sig == SIGFPE  ? "SIGFPE" : "SIGNAL";
            PrintError(std::string("Internal error (") + sig_name +
                       ") — query could not be executed safely");
            // Re-register signal handlers (they reset after longjmp)
            signal(SIGSEGV, crash_signal_handler);
            signal(SIGABRT, crash_signal_handler);
            signal(SIGFPE, crash_signal_handler);
            if (ctx.state.bail) break;
            continue;
        }

        g_in_query = 1;
        try {
            RunQuery(input, ctx);
        } catch (const duckdb::Exception& e) {
            PrintError(e.what());
            if (ctx.state.bail) break;
        } catch (const std::exception& e) {
            PrintError(e.what());
            if (ctx.state.bail) break;
        }
        g_in_query = 0;
    }
}

// ------------------------------------------------------------------ entry point

int RunShell(int argc, char** argv) {
    SetupLogger();

    // Register signal handlers for crash recovery in interactive mode
    signal(SIGSEGV, crash_signal_handler);
    signal(SIGABRT, crash_signal_handler);
    signal(SIGFPE, crash_signal_handler);

    ShellCliOptions cli;
    ParseShellOptions(argc, argv, cli);

    replxx::Replxx rx;
    rx.history_load(cli.workspace + "/.history");
    turbolynx::SetupCompletion(rx);

    InitializeDiskAIO(cli.workspace);

    ChunkCacheManager::ccm = new ChunkCacheManager(cli.workspace.c_str(), cli.standalone);
    auto database = std::make_unique<DuckDB>(cli.workspace.c_str());
    auto client   = std::make_shared<duckdb::ClientContext>(database->instance->shared_from_this());
    duckdb::SetClientWrapper(client, std::make_shared<CatalogWrapper>(client->db->GetCatalogWrapper()));
    if (cli.enable_profile) client->EnableProfiling(); else client->DisableProfiling();

    turbolynx::Planner planner(cli.planner_config, turbolynx::MDProviderType::TBGPP, client.get());

    turbolynx::ShellState state;
    state.workspace = cli.workspace;

    ExecContext ctx{client, cli, state, planner, nullptr};

    // Populate autocomplete with vertex labels + edge types from catalog
    turbolynx::PopulateCompletions(rx, *client);

    // Load ~/.turbolynxrc if it exists
    const char* home = std::getenv("HOME");
    if (home) {
        LoadRcFile(std::string(home) + "/.turbolynxrc", ctx);
    }

    if (!cli.query.empty()) {
        try {
            RunQuery(cli.query, ctx);
        } catch (const std::exception& e) {
            PrintError(e.what());
        }
    } else {
        RunInteractive(rx, ctx);
    }

    rx.history_save(cli.workspace + "/.history");
    delete ChunkCacheManager::ccm;
    return 0;
}

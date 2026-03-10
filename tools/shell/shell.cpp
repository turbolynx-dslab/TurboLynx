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
#include "storage/cache/chunk_cache_manager.h"
#include "planner/planner.hpp"
#include "catalog/catalog_wrapper.hpp"
#include "optimizer/orca/gpopt/tbgppdbwrappers.hpp"
#include "linenoise.h"

#include "include/shell.hpp"
#include "include/commands.hpp"
#include "include/renderer.hpp"
#include "include/completion.hpp"

#include <cstdlib>
#include <fstream>
#include <getopt.h>
#include <unistd.h>
#include <chrono>
#include <iostream>
#include <numeric>
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
};

static void CompileQuery(const std::string& query, ExecContext& ctx, double& compile_ms) {
    SCOPED_TIMER(CompileQuery, spdlog::level::info, spdlog::level::debug, compile_ms);
    auto inputStream = ANTLRInputStream(query);

    ThrowingErrorListener error_listener;

    SUBTIMER_START(CompileQuery, "Lexing");
    CypherLexer lexer(&inputStream);
    lexer.removeErrorListeners();
    lexer.addErrorListener(&error_listener);
    CommonTokenStream tokens(&lexer);
    tokens.fill();
    SUBTIMER_STOP(CompileQuery, "Lexing");

    SUBTIMER_START(CompileQuery, "Parse");
    CypherParser parser(&tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(&error_listener);
    SUBTIMER_STOP(CompileQuery, "Parse");

    SUBTIMER_START(CompileQuery, "Transform");
    auto* cypher_ctx = parser.oC_Cypher();
    if (!cypher_ctx)
        throw std::runtime_error("Parser returned no tree — check Cypher syntax");
    CypherTransformer transformer(*cypher_ctx);
    auto stmt = transformer.transform();
    if (!stmt)
        throw std::runtime_error("Transformer returned null — check Cypher syntax");
    SUBTIMER_STOP(CompileQuery, "Transform");

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

static void RunOneIteration(const std::string& query, ExecContext& ctx,
                            double& compile_ms, double& exec_ms) {
    CompileQuery(query, ctx, compile_ms);
    if (ctx.cli.compile_only) return;

    auto executors = ctx.planner.genPipelineExecutors();
    if (executors.empty()) { spdlog::error("Plan empty"); return; }
    if (!executors.back() || !executors.back()->pipeline ||
        !executors.back()->pipeline->GetSink())
        throw std::runtime_error("Pipeline executor is incomplete");

    auto& profiler = QueryProfiler::Get(*ctx.client);
    profiler.StartQuery(query, ctx.cli.enable_profile);
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

// ------------------------------------------------------------------ linenoise input

static std::string GetQueryString(const std::string& prompt) {
    std::string full_input;
    std::string shell_prompt = prompt + " >> ";
    std::string cont_prompt  = prompt + " -> ";

    while (true) {
        char* line = linenoise(full_input.empty() ? shell_prompt.c_str() : cont_prompt.c_str());
        if (!line) break;   // EOF (Ctrl+D)
        std::string s(line);
        free(line);

        // Strip trailing whitespace
        while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) s.pop_back();
        if (s.empty()) continue;

        // Dot/colon commands submit immediately — no semicolon required
        if (full_input.empty()) {
            size_t tstart = s.find_first_not_of(" \t");
            if (tstart != std::string::npos &&
                (s[tstart] == '.' || s[tstart] == ':')) {
                return s;
            }
        }

        // Cypher: accumulate until semicolon
        size_t semi = s.find(';');
        if (semi != std::string::npos) {
            full_input += s.substr(0, semi + 1);
            break;
        }
        full_input += s + ' ';
    }
    return full_input;
}

// ------------------------------------------------------------------ REPL

static void RunInteractive(ExecContext& ctx) {
    std::string prev;
    std::cout << "TurboLynx shell — type '.help' for commands, ':exit' to quit\n";

    while (true) {
        std::string input = GetQueryString(ctx.state.prompt);
        if (input.empty()) continue;

        std::string trimmed = input;
        size_t start = trimmed.find_first_not_of(" \t\n\r");
        if (start != std::string::npos) trimmed = trimmed.substr(start);

        // Exit commands
        if (trimmed == ":exit" || trimmed == ".exit" || trimmed == ".quit") break;

        if (!trimmed.empty() && (trimmed[0] == '.' || trimmed[0] == ':')) {
            if (!trimmed.empty() && trimmed.back() == ';') trimmed.pop_back();
            auto executor = [&ctx](const std::string& q) {
                try { RunQuery(q, ctx); }
                catch (const std::exception& ex) { PrintError(ex.what()); }
            };
            if (turbolynx::HandleDotCommand(trimmed, ctx.state, ctx.client, executor)) {
                if (trimmed != prev) {
                    linenoiseHistoryAdd(trimmed.c_str());
                    prev = trimmed;
                }
                continue;
            }
            break;
        }

        // Cypher query — strip trailing semicolon and whitespace
        while (!input.empty() && (input.back() == ';' || input.back() == ' '))
            input.pop_back();

        // Skip empty / whitespace-only queries
        if (input.find_first_not_of(" \t\n\r") == std::string::npos) continue;

        if (input != prev) {
            linenoiseHistoryAdd(input.c_str());
            prev = input;
        }

        try {
            RunQuery(input, ctx);
        } catch (const duckdb::Exception& e) {
            PrintError(e.what());
            if (ctx.state.bail) break;
        } catch (const std::exception& e) {
            PrintError(e.what());
            if (ctx.state.bail) break;
        }
    }
}

// ------------------------------------------------------------------ entry point

int RunShell(int argc, char** argv) {
    SetupLogger();

    ShellCliOptions cli;
    ParseShellOptions(argc, argv, cli);

    linenoiseHistoryLoad((cli.workspace + "/.history").c_str());
    linenoiseHistorySetMaxLen(1000);
    turbolynx::SetupCompletion();

    InitializeDiskAIO(cli.workspace);

    ChunkCacheManager::ccm = new ChunkCacheManager(cli.workspace.c_str(), cli.standalone);
    auto database = std::make_unique<DuckDB>(cli.workspace.c_str());
    auto client   = std::make_shared<duckdb::ClientContext>(database->instance->shared_from_this());
    duckdb::SetClientWrapper(client, std::make_shared<CatalogWrapper>(client->db->GetCatalogWrapper()));
    if (cli.enable_profile) client->EnableProfiling(); else client->DisableProfiling();

    turbolynx::Planner planner(cli.planner_config, turbolynx::MDProviderType::TBGPP, client.get());

    turbolynx::ShellState state;
    state.workspace = cli.workspace;

    ExecContext ctx{client, cli, state, planner};

    // Populate autocomplete with vertex labels + edge types from catalog
    turbolynx::PopulateCompletions(*client);

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
        RunInteractive(ctx);
    }

    linenoiseHistorySave((cli.workspace + "/.history").c_str());
    delete ChunkCacheManager::ccm;
    return 0;
}

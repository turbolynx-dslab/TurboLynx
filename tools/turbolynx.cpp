// ============================================================
// turbolynx — single entry point
//   turbolynx [shell] [options]   — interactive query shell
//   turbolynx import [options]    — bulk CSV loader
// ============================================================

// antlr4 headers must come before ORCA (c.h postgres macros)
#include "CypherLexer.h"
#include "CypherParser.h"
#include "parser/cypher_transformer.hpp"
#include "binder/binder.hpp"
#include "nlohmann/json.hpp"
#include "common/logger.hpp"
#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "storage/statistics/histogram_generator.hpp"
#include "storage/cache/chunk_cache_manager.h"
#include "planner/planner.hpp"
#include "catalog/catalog_wrapper.hpp"
#include "optimizer/orca/gpopt/tbgppdbwrappers.hpp"
#include "linenoise.h"
#include "loader/bulkload_options.hpp"
#include "loader/bulkload_pipeline.hpp"

#include <getopt.h>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <optional>
#include <csignal>
#include <string>

using namespace antlr4;
using namespace gpopt;
using namespace duckdb;
using json = nlohmann::json;

// ============================================================
// shell
// ============================================================

struct ClientOptions {
    string workspace;
    string query;
    string output_file;
    bool slient         = false;
    bool standalone     = false;
    bool dump_output    = false;
    bool compile_only   = false;
    bool warmup         = false;
    bool enable_profile = false;
    int  iterations     = 1;
    turbolynx::PlannerConfig planner_config;
};

enum class ProcessQueryMode { SINGLE_QUERY, INTERACTIVE };

static ProcessQueryMode getQueryMode(const ClientOptions& options) {
    return options.query.empty() ? ProcessQueryMode::INTERACTIVE : ProcessQueryMode::SINGLE_QUERY;
}

static void ParseShellOptions(int argc, char** argv, ClientOptions& options) {
    optind = 1;
    static struct option long_options[] = {
        {"help",                no_argument,       0, 'h'},
        {"workspace",           required_argument, 0, 'w'},
        {"log-level",           required_argument, 0, 'L'},
        {"slient",              no_argument,       0, 's'},
        {"output-file",         required_argument, 0, 'o'},
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
        {0, 0, 0, 0}
    };

    int opt, option_index = 0;
    while ((opt = getopt_long(argc, argv, "hw:L:so:ScOj:q:i:", long_options, &option_index)) != -1) {
        switch (opt) {
        case 'h':
            std::cout << "Usage: turbolynx shell [options]\n"
                      << "  --workspace <path>    Workspace directory\n"
                      << "  --log-level <level>   Set logging level\n"
                      << "  --query <string>      Execute a single query\n"
                      << "  --iterations <n>      Number of iterations\n"
                      << "  --standalone          Standalone mode\n"
                      << "  --compile-only        Compile without execution\n"
                      << "  --warmup              Perform warmup iteration\n"
                      << "  --profile             Enable profiling\n"
                      << "  --explain             Print query plan\n";
            exit(0);
        case 'w': options.workspace = optarg; spdlog::trace("\nWorkspace: {}\n", options.workspace); break;
        case 'L': setLogLevel(getLogLevel(optarg)); break;
        case 's': options.slient = true; break;
        case 'o': options.output_file = optarg; break;
        case 'S': options.standalone = true; break;
        case 'c': options.compile_only = true; break;
        case 'O': options.compile_only = true; options.planner_config.ORCA_COMPILE_ONLY = true; break;
        case 1001: options.planner_config.INDEX_JOIN_ONLY = true; break;
        case 1002: options.planner_config.HASH_JOIN_ONLY = true; break;
        case 1003: options.planner_config.MERGE_JOIN_ONLY = true; break;
        case 1004: options.planner_config.DISABLE_MERGE_JOIN = true; break;
        case 1005: options.planner_config.DISABLE_INDEX_JOIN = true; break;
        case 'j': {
            std::string opt_str = optarg;
            if      (opt_str == "query")       options.planner_config.JOIN_ORDER_TYPE = turbolynx::PlannerConfig::JoinOrderType::JOIN_ORDER_IN_QUERY;
            else if (opt_str == "greedy")      options.planner_config.JOIN_ORDER_TYPE = turbolynx::PlannerConfig::JoinOrderType::JOIN_ORDER_GREEDY_SEARCH;
            else if (opt_str == "exhaustive")  options.planner_config.JOIN_ORDER_TYPE = turbolynx::PlannerConfig::JoinOrderType::JOIN_ORDER_EXHAUSTIVE_SEARCH;
            else if (opt_str == "exhaustive2") options.planner_config.JOIN_ORDER_TYPE = turbolynx::PlannerConfig::JoinOrderType::JOIN_ORDER_EXHAUSTIVE2_SEARCH;
            else if (opt_str == "gem")         options.planner_config.JOIN_ORDER_TYPE = turbolynx::PlannerConfig::JoinOrderType::JOIN_ORDER_GEM;
            else throw std::invalid_argument("Invalid --join-order-optimizer parameter");
            break;
        }
        case 1006: options.planner_config.ORCA_DEBUG_PRINT = true; break;
        case 'q': options.query = optarg; break;
        case 'i': options.iterations = std::stoi(optarg); break;
        case 1007: options.warmup = true; break;
        case 1008: options.enable_profile = true; break;
        case 1009: options.planner_config.DEBUG_PRINT = true; break;
        default: break;
        }
    }
}

static std::string GetQueryString(std::string& prompt) {
    std::string full_input, current_input;
    std::string shell_prompt = prompt + " >> ";
    std::string prev_prompt  = prompt + " -> ";

    while (true) {
        char* line = linenoise(full_input.empty() ? shell_prompt.c_str() : prev_prompt.c_str());
        if (!line) break;
        current_input = line;
        free(line);
        if (current_input.empty()) continue;
        size_t semi_pos = current_input.find(';');
        if (semi_pos != std::string::npos) {
            full_input += current_input.substr(0, semi_pos + 1);
            break;
        }
        full_input += current_input + " ";
    }
    return full_input;
}

static void CompileQuery(const string& query, std::shared_ptr<ClientContext> client,
                         turbolynx::Planner& planner, double& compile_elapsed_time) {
    SCOPED_TIMER(CompileQuery, spdlog::level::info, spdlog::level::debug, compile_elapsed_time);
    auto inputStream = ANTLRInputStream(query);

    SUBTIMER_START(CompileQuery, "Lexing");
    auto cypherLexer = CypherLexer(&inputStream);
    auto tokens = CommonTokenStream(&cypherLexer);
    tokens.fill();
    SUBTIMER_STOP(CompileQuery, "Lexing");

    SUBTIMER_START(CompileQuery, "Parse");
    auto cypherParser = CypherParser(&tokens);
    SUBTIMER_STOP(CompileQuery, "Parse");

    SUBTIMER_START(CompileQuery, "Transform");
    duckdb::CypherTransformer transformer(*cypherParser.oC_Cypher());
    auto statement = transformer.transform();
    SUBTIMER_STOP(CompileQuery, "Transform");

    SUBTIMER_START(CompileQuery, "Bind");
    duckdb::Binder binder(client.get());
    auto boundQuery = binder.Bind(*statement);
    SUBTIMER_STOP(CompileQuery, "Bind");

    SUBTIMER_START(CompileQuery, "Orca Compile");
    planner.execute(boundQuery.get());
    SUBTIMER_STOP(CompileQuery, "Orca Compile");
}

static void ExecuteQuery(const string& query, std::shared_ptr<ClientContext> client,
                         ClientOptions& options, vector<duckdb::CypherPipelineExecutor*>& executors,
                         double& exec_elapsed_time) {
    if (executors.empty()) { spdlog::error("[ExecuteQuery] Plan Empty"); return; }

    auto& profiler = QueryProfiler::Get(*client.get());
    profiler.StartQuery(query, options.enable_profile);
    profiler.Initialize(executors.back()->pipeline->GetSink());

    {
        SCOPED_TIMER(ExecuteQuery, spdlog::level::info, spdlog::level::info, exec_elapsed_time);
        for (int i = 0; i < (int)executors.size(); i++) {
            auto exec = executors[i];
            spdlog::info("[ExecuteQuery] Pipeline {:02d} Plan\n{}", i, exec->pipeline->toString());
            spdlog::debug("[ExecuteQuery] Pipeline ID {:02d} Started", i);
            SUBTIMER_START(ExecuteQuery, "Pipeline " + std::to_string(i));
            exec->ExecutePipeline();
            SUBTIMER_STOP(ExecuteQuery, "Pipeline " + std::to_string(i));
            spdlog::debug("[ExecuteQuery] Pipeline ID {:02d} Finished", i);
        }
    }
    profiler.EndQuery();
}

static void PrintOutputToFile(PropertyKeys col_names,
                              std::vector<std::shared_ptr<duckdb::DataChunk>>* query_results_ptr,
                              duckdb::Schema& schema, std::string& file_path) {
    if (!query_results_ptr) { spdlog::error("[PrintOutputToFile] Query Results Empty"); return; }
    if (col_names.empty()) col_names = schema.getStoredColumnNames();

    auto& query_results = *query_results_ptr;
    std::ofstream dump_file(file_path, std::ios::trunc);
    if (!dump_file) { spdlog::error("[PrintOutputToFile] Failed to open dump file: {}", file_path); return; }

    spdlog::info("[PrintOutputToFile] Dumping Output File. Path: {}", file_path);
    for (size_t i = 0; i < col_names.size(); i++)
        dump_file << col_names[i] << (i != col_names.size() - 1 ? "|" : "\n");

    for (const auto& chunk : query_results) {
        size_t num_cols = chunk->ColumnCount();
        for (size_t idx = 0; idx < chunk->size(); idx++) {
            for (size_t i = 0; i < num_cols; i++) {
                dump_file << chunk->GetValue(i, idx).ToString();
                if (i != num_cols - 1) dump_file << "|";
            }
            dump_file << "\n";
        }
    }
    spdlog::info("[PrintOutputToFile] Dump Done");
}

static void PrintOutputConsole(const PropertyKeys& col_names,
                               std::vector<std::shared_ptr<duckdb::DataChunk>>* query_results_ptr,
                               duckdb::Schema& schema, bool top_10_only) {
    if (!query_results_ptr) { spdlog::error("[PrintOutputConsole] Query Results Empty"); return; }
    auto final_col_names = col_names.empty() ? schema.getStoredColumnNames() : col_names;
    auto& query_results = *query_results_ptr;
    OutputUtil::PrintQueryOutput(final_col_names, query_results, top_10_only);
}

static size_t GetResultSizeInBytes(std::vector<std::shared_ptr<duckdb::DataChunk>>& query_results) {
    size_t result_size = 0;
    for (const auto& chunk : query_results)
        for (size_t i = 0; i < chunk->ColumnCount(); i++)
            result_size += GetTypeIdSize(chunk->data[i].GetType().InternalType()) * chunk->size();
    return result_size;
}

static void DeallocateIterationMemory(std::vector<duckdb::CypherPipelineExecutor*>& executors) {
    auto& query_results = *executors.back()->context->query_results;
    GetResultSizeInBytes(query_results);
    for (auto& chunk : query_results) chunk.reset();
    query_results.clear();
    for (auto& exec : executors) delete exec;
    executors.clear();
}

static void CompileAndExecuteIteration(const std::string& query_str,
                                       std::shared_ptr<ClientContext> client,
                                       ClientOptions& options, turbolynx::Planner& planner,
                                       std::vector<double>& compile_times,
                                       std::vector<double>& execution_times) {
    double compile_time = 0.0, exec_time = 0.0;
    CompileQuery(query_str, client, planner, compile_time);

    if (!options.compile_only) {
        auto executors = planner.genPipelineExecutors();
        ExecuteQuery(query_str, client, options, executors, exec_time);

        auto& query_results = executors.back()->context->query_results;
        auto& schema        = executors.back()->pipeline->GetSink()->schema;
        auto  col_names     = planner.getQueryOutputColNames();

        PrintOutputConsole(col_names, query_results, schema, options.slient);
        if (!options.output_file.empty())
            PrintOutputToFile(col_names, query_results, schema, options.output_file);

        DeallocateIterationMemory(executors);
    }
    compile_times.push_back(compile_time);
    execution_times.push_back(exec_time);
}

static double CalculateAverageTime(const std::vector<double>& times) {
    return times.empty() ? 0.0 : std::accumulate(times.begin(), times.end(), 0.0) / times.size();
}

static void CompileExecuteQuery(const std::string& query_str, std::shared_ptr<ClientContext> client,
                                ClientOptions& options, turbolynx::Planner& planner) {
    std::vector<double> compile_times, execution_times;
    if (options.warmup) options.iterations += 1;
    for (int i = 0; i < options.iterations; ++i) {
        spdlog::info("[CompileExecuteQuery] Iteration {} Started", i + 1);
        CompileAndExecuteIteration(query_str, client, options, planner, compile_times, execution_times);
        spdlog::info("[CompileExecuteQuery] Iteration {} Finished", i + 1);
    }
    if (options.warmup && !execution_times.empty()) execution_times.erase(execution_times.begin());
    if (options.warmup && !compile_times.empty())   compile_times.erase(compile_times.begin());

    double avg_exec    = CalculateAverageTime(execution_times);
    double avg_compile = CalculateAverageTime(compile_times);
    spdlog::info("Average Query Execution Time: {} ms", avg_exec);
    spdlog::info("Average Compile Time: {} ms", avg_compile);
    spdlog::info("Average End to End Time: {} ms", avg_compile + avg_exec);
}

static void InitializeDiskAIO(string& workspace) {
    DiskAioParameters::NUM_THREADS          = 32;
    DiskAioParameters::NUM_TOTAL_CPU_CORES  = 32;
    DiskAioParameters::NUM_CPU_SOCKETS      = 2;
    DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
    DiskAioParameters::WORKSPACE            = workspace;

    int res;
    new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
    core_id::set_core_ids(DiskAioParameters::NUM_THREADS);
}

static void InitializeClient(std::shared_ptr<ClientContext>& client, ClientOptions& options) {
    duckdb::SetClientWrapper(client, std::make_shared<CatalogWrapper>(client->db->GetCatalogWrapper()));
    options.enable_profile ? client->EnableProfiling() : client->DisableProfiling();
}

static void ProcessQuery(const std::string& query, std::shared_ptr<ClientContext>& client,
                         ClientOptions& options, turbolynx::Planner& planner) {
    if (query == "analyze") {
        HistogramGenerator hist_gen;
        hist_gen.CreateHistogram(client);
    } else if (query == "flush_file_meta") {
        ChunkCacheManager::ccm->FlushMetaInfo(options.workspace.c_str());
    } else {
        CompileExecuteQuery(query, client, options, planner);
    }
}

static void ProcessQueryInteractive(std::shared_ptr<ClientContext>& client,
                                    ClientOptions& options, turbolynx::Planner& planner) {
    std::string shell_prompt = "TurboLynx";
    std::string prev_query_str, query_str;

    while (true) {
        query_str = GetQueryString(shell_prompt);
        if (query_str == ":exit") break;

        if (query_str != prev_query_str) {
            linenoiseHistoryAdd(query_str.c_str());
            linenoiseHistorySave((options.workspace + "/.history").c_str());
            prev_query_str = query_str;
        }

        try {
            query_str.pop_back();
            ProcessQuery(query_str, client, options, planner);
        } catch (const duckdb::Exception& e) {
            spdlog::error("DuckDB Exception: {}", e.what());
        } catch (const std::exception& e) {
            spdlog::error("Unexpected Exception: {} (Type: {})", e.what(), typeid(e).name());
        }
    }
}

static int RunShell(int argc, char** argv) {
    SetupLogger();

    ClientOptions options;
    options.planner_config = turbolynx::PlannerConfig();
    ParseShellOptions(argc, argv, options);

    linenoiseHistoryLoad((options.workspace + "/.history").c_str());
    InitializeDiskAIO(options.workspace);

    ChunkCacheManager::ccm = new ChunkCacheManager(options.workspace.c_str(), options.standalone);
    auto database = std::make_unique<DuckDB>(options.workspace.c_str());
    auto client   = std::make_shared<ClientContext>(database->instance->shared_from_this());
    InitializeClient(client, options);

    turbolynx::Planner planner(options.planner_config, turbolynx::MDProviderType::TBGPP, client.get());

    switch (getQueryMode(options)) {
        case ProcessQueryMode::SINGLE_QUERY:
            ProcessQuery(options.query, client, options, planner);
            break;
        case ProcessQueryMode::INTERACTIVE:
            ProcessQueryInteractive(client, options, planner);
            break;
    }

    delete ChunkCacheManager::ccm;
    return 0;
}

// ============================================================
// import
// ============================================================

static void cntl_c_signal_handler(int) {
    spdlog::info("Capture Ctrl+C");
    if (ChunkCacheManager::ccm) delete ChunkCacheManager::ccm;
    exit(0);
}

static void RegisterSignalHandler() {
    if (signal(SIGINT, cntl_c_signal_handler) == SIG_ERR) {
        spdlog::error("[Main] Cannot register signal handler");
        exit(-1);
    }
}

static void ParseImportOptions(int argc, char** argv, BulkloadOptions& options) {
    optind = 1;
    static struct option long_options[] = {
        {"help",           no_argument,       0, 'h'},
        {"nodes",          required_argument, 0, 'n'},
        {"relationships",  required_argument, 0, 'r'},
        {"workspace",      required_argument, 0, 'w'},
        {"output_dir",     required_argument, 0, 'd'},  // legacy alias
        {"incremental",    required_argument, 0, 2001},
        {"skip-histogram", no_argument,       0, 2002},
        {"standalone",     no_argument,       0, 2003},
        {"log-level",      required_argument, 0, 'L'},
        {0, 0, 0, 0}
    };

    std::vector<std::string> nodes_args, rel_args;

    int opt, option_index = 0;
    while ((opt = getopt_long(argc, argv, "hn:r:w:d:L:", long_options, &option_index)) != -1) {
        switch (opt) {
        case 'h':
            std::cout << "Usage: turbolynx import [options]\n"
                      << "  --workspace <path>               Workspace directory\n"
                      << "  --nodes <label> <file> [size]    Node CSV files\n"
                      << "  --relationships <label> <file>   Edge CSV files\n"
                      << "  --incremental <true|false>       Incremental load\n"
                      << "  --skip-histogram                 Skip histogram generation\n"
                      << "  --standalone                     Standalone mode\n"
                      << "  --log-level <level>              Log level\n";
            exit(0);
        case 'n': nodes_args.push_back(optarg); break;
        case 'r': rel_args.push_back(optarg); break;
        case 'w': options.output_dir = optarg; break;
        case 'd': options.output_dir = optarg; break;  // legacy alias
        case 2001:
            options.incremental = (std::string(optarg) == "true");
            if (options.incremental && !options.vertex_files.empty())
                throw InvalidInputException("Incremental load only supports edge label");
            break;
        case 2002: options.skip_histogram = true; break;
        case 2003: options.standalone = true; break;
        case 'L': setLogLevel(getLogLevel(optarg)); break;
        default: break;
        }
    }

    {
        size_t i = 0;
        while (i + 1 < nodes_args.size()) {
            std::string label = nodes_args[i++];
            std::string file  = nodes_args[i++];
            std::optional<size_t> file_size;
            if (i < nodes_args.size() && std::all_of(nodes_args[i].begin(), nodes_args[i].end(), ::isdigit))
                file_size = std::stoull(nodes_args[i++]);
            options.vertex_files.emplace_back(label, file, file_size);
        }
    }
    {
        size_t i = 0;
        while (i + 1 < rel_args.size()) {
            std::string label = rel_args[i++];
            std::string file  = rel_args[i++];
            std::optional<FileSize> file_size;
            if (i < rel_args.size() && std::all_of(rel_args[i].begin(), rel_args[i].end(), ::isdigit))
                file_size = std::stoull(rel_args[i++]);
            options.edge_files.emplace_back(label, file, file_size);
        }
        if (!rel_args.empty()) options.load_edge = true;
    }

    spdlog::info("[ParseImportOptions] Workspace: {}", options.output_dir);
    spdlog::info("[ParseImportOptions] Incremental Load: {}", options.incremental);
    spdlog::info("[ParseImportOptions] Load Edge: {}", options.load_edge);
    spdlog::info("[ParseImportOptions] Standalone: {}", options.standalone);
    spdlog::info("[ParseImportOptions] Nodes:");
    for (const auto& f : options.vertex_files) spdlog::info("\t{} : {}", std::get<0>(f), std::get<1>(f));
    spdlog::info("[ParseImportOptions] Relationships:");
    for (const auto& f : options.edge_files)   spdlog::info("\t{} : {}", std::get<0>(f), std::get<1>(f));
}

static int RunImport(int argc, char** argv) {
    SetupLogger();
    RegisterSignalHandler();

    BulkloadOptions opts;
    ParseImportOptions(argc, argv, opts);
    BulkloadPipeline(std::move(opts)).Run();
    return 0;
}

// ============================================================
// main
// ============================================================

static void PrintUsage() {
    std::cout << "Usage: turbolynx [subcommand] [options]\n"
              << "\n"
              << "Subcommands:\n"
              << "  shell    Launch interactive query shell (default)\n"
              << "  import   Bulk-load graph data from CSV files\n"
              << "\n"
              << "Examples:\n"
              << "  turbolynx --workspace /path/to/db\n"
              << "  turbolynx shell --workspace /path/to/db --query \"MATCH (n:Person) RETURN count(*)\"\n"
              << "  turbolynx import --workspace /path/to/db \\\n"
              << "      --nodes Person dynamic/Person.csv \\\n"
              << "      --relationships KNOWS dynamic/Person_knows_Person.csv\n"
              << "\n"
              << "Run 'turbolynx <subcommand> --help' for subcommand-specific options.\n";
}

int main(int argc, char** argv) {
    std::string subcmd = "shell";
    if (argc > 1 && argv[1][0] != '-') {
        subcmd = argv[1];
        argc--;
        argv++;
    }

    if (subcmd == "shell")       return RunShell(argc, argv);
    if (subcmd == "import")      return RunImport(argc, argv);

    std::cerr << "turbolynx: unknown subcommand '" << subcmd << "'\n\n";
    PrintUsage();
    return 1;
}

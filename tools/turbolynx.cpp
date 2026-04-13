// ============================================================
// turbolynx — entry point
//   turbolynx [shell] [options]   — interactive query shell (default)
//   turbolynx import   [options]  — bulk CSV loader
// ============================================================

#include "shell/include/shell.hpp"

#include "loader/bulkload_options.hpp"
#include "loader/bulkload_pipeline.hpp"
#include "common/logger.hpp"
#include "storage/cache/chunk_cache_manager.h"

#include <getopt.h>
#include <csignal>
#include <iostream>
#include <optional>
#include <string>

using namespace duckdb;

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
            if (i < nodes_args.size() &&
                std::all_of(nodes_args[i].begin(), nodes_args[i].end(), ::isdigit))
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
            if (i < rel_args.size() &&
                std::all_of(rel_args[i].begin(), rel_args[i].end(), ::isdigit))
                file_size = std::stoull(rel_args[i++]);
            options.edge_files.emplace_back(label, file, file_size);
        }
        // edge_files non-empty signals edge loading
    }

    spdlog::info("[import] Workspace: {}", options.output_dir);
    spdlog::info("[import] Nodes: {}", options.vertex_files.size());
    spdlog::info("[import] Relationships: {}", options.edge_files.size());
}

static int RunImport(int argc, char** argv) {
    SetupLogger();
    RegisterSignalHandler();

    BulkloadOptions opts;
    ParseImportOptions(argc, argv, opts);
    if (opts.output_dir.empty()) {
        std::cerr << "Error: --workspace <path> is required.\n"
                  << "Run 'turbolynx import --help' for usage.\n";
        return 1;
    }
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
              << "Run 'turbolynx <subcommand> --help' for subcommand options.\n";
}

int main(int argc, char** argv) {
    // No arguments at all → print usage and exit cleanly.
    if (argc <= 1) {
        PrintUsage();
        return 0;
    }

    // First positional (non-flag) arg selects the subcommand; default shell.
    std::string subcmd = "shell";
    if (argc > 1 && argv[1][0] != '-') {
        subcmd = argv[1];
        argc--;
        argv++;
    }

    if (subcmd == "shell")  return RunShell(argc, argv);
    if (subcmd == "import") return RunImport(argc, argv);

    std::cerr << "turbolynx: unknown subcommand '" << subcmd << "'\n\n";
    PrintUsage();
    return 1;
}

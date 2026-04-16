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

static bool IsOptionToken(const char* arg) {
    return arg && arg[0] == '-' && arg[1] != '\0';
}

static const char* RequireImportArg(int argc, char** argv, int& index, const char* option_name) {
    if (index + 1 >= argc) {
        throw InvalidInputException(std::string("Missing value for ") + option_name);
    }
    return argv[++index];
}

static void ConsumeTrailingImportArgs(int argc, char** argv, int& index,
                                      std::vector<std::string>& out, size_t max_extra_args) {
    size_t consumed = 0;
    while (index + 1 < argc && consumed < max_extra_args) {
        const char* next = argv[index + 1];
        if (IsOptionToken(next)) break;
        out.emplace_back(next);
        index++;
        consumed++;
    }
}

static void ParseImportOptions(int argc, char** argv, BulkloadOptions& options) {
    std::vector<std::string> nodes_args, rel_args;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];

        if (arg == "-h" || arg == "--help") {
            std::cout << "Usage: turbolynx import [options]\n"
                      << "  --workspace <path>               Workspace directory\n"
                      << "  --nodes <label> <file> [size]    Node CSV files\n"
                      << "  --relationships <label> <file>   Edge CSV files\n"
                      << "  --incremental <true|false>       Incremental load\n"
                      << "  --skip-histogram                 Skip histogram generation\n"
                      << "  --log-level <level>              Log level\n";
            exit(0);
        } else if (arg == "-n" || arg == "--nodes") {
            nodes_args.push_back(RequireImportArg(argc, argv, i, arg.c_str()));
            ConsumeTrailingImportArgs(argc, argv, i, nodes_args, 2);
        } else if (arg == "-r" || arg == "--relationships") {
            rel_args.push_back(RequireImportArg(argc, argv, i, arg.c_str()));
            ConsumeTrailingImportArgs(argc, argv, i, rel_args, 2);
        } else if (arg == "-w" || arg == "--workspace" ||
                   arg == "-d" || arg == "--output_dir") {
            options.output_dir = RequireImportArg(argc, argv, i, arg.c_str());
        } else if (arg == "--incremental") {
            options.incremental = (std::string(RequireImportArg(argc, argv, i, arg.c_str())) == "true");
            if (options.incremental && !options.vertex_files.empty())
                throw InvalidInputException("Incremental load only supports edge label");
        } else if (arg == "--skip-histogram") {
            options.skip_histogram = true;
        } else if (arg == "-L" || arg == "--log-level") {
            setLogLevel(getLogLevel(RequireImportArg(argc, argv, i, arg.c_str())));
        } else {
            throw InvalidInputException("Unknown import option: " + arg);
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

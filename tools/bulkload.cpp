#include <iostream>
#include <getopt.h>
#include <optional>
#include "loader/bulkload_options.hpp"
#include "loader/bulkload_pipeline.hpp"
#include "common/logger.hpp"
#include "storage/cache/chunk_cache_manager.h"

#include <csignal>

using namespace duckdb;

void cntl_c_signal_handler(int sig_number) {
    spdlog::info("Capture Ctrl+C");
    if (ChunkCacheManager::ccm) delete ChunkCacheManager::ccm;
    exit(0);
}

void RegisterSignalHandler() {
    if (signal(SIGINT, cntl_c_signal_handler) == SIG_ERR) {
        spdlog::error("[Main] Cannot register signal handler");
        exit(-1);
    }
}

void ParseConfig(int argc, char** argv, BulkloadOptions& options) {
    static struct option long_options[] = {
        {"help",                    no_argument,       0, 'h'},
        {"nodes",                   required_argument, 0, 'n'},
        {"relationships",           required_argument, 0, 'r'},
        {"output_dir",              required_argument, 0, 'd'},
        {"incremental",             required_argument, 0, 2001},
        {"skip-histogram",          no_argument,       0, 2002},
        {"standalone",              no_argument,       0, 2003},
        {"log-level",               required_argument, 0, 'L'},
        {0, 0, 0, 0}
    };

    std::vector<std::string> nodes_args, rel_args;

    int opt, option_index = 0;
    while ((opt = getopt_long(argc, argv, "hn:r:d:L:", long_options, &option_index)) != -1) {
        switch (opt) {
        case 'h':
            std::cout << "Usage: bulkload [options]\n"
                      << "  --nodes <label> <file> [size]    Node files\n"
                      << "  --relationships <label> <file>    Edge files\n"
                      << "  --output_dir <path>              Output directory\n"
                      << "  --incremental <true|false>       Incremental load\n"
                      << "  --skip-histogram                 Skip histogram generation\n"
                      << "  --standalone                     Standalone mode\n"
                      << "  --log-level <level>              Log level\n";
            exit(0);
        case 'n': nodes_args.push_back(optarg); break;
        case 'r': rel_args.push_back(optarg); break;
        case 'd': options.output_dir = optarg; break;
        case 2001:
            options.incremental = (std::string(optarg) == "true");
            if (options.incremental && options.vertex_files.size() > 0)
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
        while (i < nodes_args.size()) {
            if (i + 1 >= nodes_args.size()) break;
            std::string label = nodes_args[i++];
            std::string file  = nodes_args[i++];
            std::optional<size_t> file_size;
            if (i < nodes_args.size() && std::all_of(nodes_args[i].begin(), nodes_args[i].end(), ::isdigit)) {
                file_size = std::stoull(nodes_args[i++]);
            }
            options.vertex_files.emplace_back(label, file, file_size);
        }
    }
    {
        size_t i = 0;
        while (i < rel_args.size()) {
            if (i + 1 >= rel_args.size()) break;
            std::string label = rel_args[i++];
            std::string file  = rel_args[i++];
            std::optional<FileSize> file_size;
            if (i < rel_args.size() && std::all_of(rel_args[i].begin(), rel_args[i].end(), ::isdigit)) {
                file_size = std::stoull(rel_args[i++]);
            }
            options.edge_files.emplace_back(label, file, file_size);
        }
        if (!rel_args.empty()) options.load_edge = true;
    }

    spdlog::info("[ParseConfig] Output Directory: {}", options.output_dir);
    spdlog::info("[ParseConfig] Incremental Load: {}", options.incremental);
    spdlog::info("[ParseConfig] Load Edge: {}", options.load_edge);
    spdlog::info("[ParseConfig] Storage Standalone: {}", options.standalone);

    spdlog::info("[ParseConfig] Load Following Nodes");
    for (const auto& file : options.vertex_files) {
        spdlog::info("\t{} : {}", std::get<0>(file), std::get<1>(file));
    }
    spdlog::info("[ParseConfig] Load Following Relationships");
    for (const auto& file : options.edge_files) {
        spdlog::info("\t{} : {}", std::get<0>(file), std::get<1>(file));
    }
}

int main(int argc, char** argv) {
    SetupLogger();
    RegisterSignalHandler();

    BulkloadOptions opts;
    ParseConfig(argc, argv, opts);
    BulkloadPipeline(std::move(opts)).Run();
    return 0;
}

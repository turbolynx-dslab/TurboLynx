#pragma once
#include <filesystem>
#include <string>
#include <vector>
#include <stdexcept>
#include <cstdio>
#include <unistd.h>
#include <sys/wait.h>
#include <chrono>
#include <thread>
#include "dataset_registry.hpp"

namespace fs = std::filesystem;

namespace bulktest {

struct WorkspaceGuard {
    fs::path path;
    explicit WorkspaceGuard(fs::path p) : path(std::move(p)) {}
    ~WorkspaceGuard() {
        if (!path.empty() && fs::exists(path))
            fs::remove_all(path);
    }
    WorkspaceGuard(const WorkspaceGuard&) = delete;
    WorkspaceGuard& operator=(const WorkspaceGuard&) = delete;
};

struct BulkloadResult {
    int      exit_code = -1;
    fs::path workspace;
};

class BulkloadRunner {
public:
    static BulkloadResult run(const DatasetConfig& cfg, const fs::path& data_dir) {
        // Create temp workspace
        char tmpl[] = "/tmp/turbolynx_bltest_XXXXXX";
        char* tmp = mkdtemp(tmpl);
        if (!tmp) throw std::runtime_error("mkdtemp failed");
        fs::path ws(tmp);

        // Build argv
        std::vector<std::string> args;
        args.push_back(TEST_BULKLOAD_BIN);
        args.push_back("import");
        if (cfg.skip_histogram)
            args.push_back("--skip-histogram");
        args.push_back("--workspace");
        args.push_back(ws.string());

        // The bulkload tool uses getopt_long with required_argument, so each
        // --nodes / --relationships invocation accepts exactly ONE token as
        // optarg.  The tool then pairs adjacent entries in nodes_args[] as
        // (label, file).  Therefore we must emit two separate --nodes flags
        // per vertex file: one for the label and one for the file path.
        for (const auto& v : cfg.vertices) {
            for (const auto& f : v.files) {
                args.push_back("--nodes");
                args.push_back(v.label_arg);
                args.push_back("--nodes");
                args.push_back((data_dir / f).string());
            }
        }
        for (const auto& e : cfg.edges) {
            for (const auto& f : e.fwd_files) {
                args.push_back("--relationships");
                args.push_back(e.type);
                args.push_back("--relationships");
                args.push_back((data_dir / f).string());
            }
        }

        // Convert to char* array
        std::vector<char*> argv_c;
        for (auto& s : args) argv_c.push_back(const_cast<char*>(s.c_str()));
        argv_c.push_back(nullptr);

        // Fork + exec
        pid_t pid = fork();
        if (pid < 0) throw std::runtime_error("fork failed");
        if (pid == 0) {
            execv(argv_c[0], argv_c.data());
            _exit(127);
        }

        int status = 0;
        waitpid(pid, &status, 0);

        BulkloadResult res;
        res.exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
        res.workspace = ws;
        return res;
    }
};

} // namespace bulktest

#pragma once
#include <filesystem>
#include <optional>
#include <cstdlib>
#include <string>
#include "dataset_registry.hpp"

namespace fs = std::filesystem;

namespace bulktest {

class DatasetLocator {
public:
    static std::optional<fs::path> find(const DatasetConfig& cfg,
                                         const fs::path& data_root,
                                         bool do_download = false) {
        fs::path p = data_root / cfg.local_path;
        if (fs::exists(p) && fs::is_directory(p)) return p;

        if (do_download) {
            fs::create_directories(p.parent_path());
            std::string cmd = std::string(TEST_DOWNLOAD_SCRIPT)
                + " \"" + cfg.hf_repo + "\" \"" + p.string() + "\"";
            int rc = std::system(cmd.c_str());
            if (rc == 0 && fs::exists(p)) return p;
        }
        return std::nullopt;
    }
};

} // namespace bulktest

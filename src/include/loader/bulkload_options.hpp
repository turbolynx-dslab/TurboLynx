#pragma once

#include <optional>
#include <string>
#include <tuple>
#include <vector>
#include "common/typedef.hpp"

namespace turbolynx {
using namespace duckdb;

using FilePath         = std::string;
using FileSize         = size_t;
using OptionalFileSize = std::optional<FileSize>;
using LabeledFile      = std::tuple<Labels, FilePath, OptionalFileSize>;

struct BulkloadOptions {
    std::vector<LabeledFile> vertex_files;
    std::vector<LabeledFile> edge_files;
    std::string output_dir;
    bool incremental        = false;
    bool skip_histogram     = false;
};

} // namespace turbolynx

namespace duckdb {
using FilePath = turbolynx::FilePath;
using FileSize = turbolynx::FileSize;
using OptionalFileSize = turbolynx::OptionalFileSize;
using LabeledFile = turbolynx::LabeledFile;
using BulkloadOptions = turbolynx::BulkloadOptions;
} // namespace duckdb

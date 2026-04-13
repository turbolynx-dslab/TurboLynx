#pragma once

#include <optional>
#include <string>
#include <tuple>
#include <vector>
#include "common/typedef.hpp"

namespace duckdb {

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

} // namespace duckdb

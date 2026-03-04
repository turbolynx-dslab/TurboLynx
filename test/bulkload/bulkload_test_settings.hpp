#pragma once
#include <filesystem>

namespace fs = std::filesystem;

struct BulkloadTestSettings {
    fs::path data_dir   = TEST_DEFAULT_DATA_DIR;
    bool do_download    = false;
    bool do_generate    = false;
};

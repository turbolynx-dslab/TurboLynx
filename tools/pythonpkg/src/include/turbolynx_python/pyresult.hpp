#pragma once

// Project headers first (defines idx_t etc.)
#include "common/constants.hpp"
#include "common/common.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/value.hpp"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <vector>
#include <memory>
#include <string>

namespace py = pybind11;

namespace turbolynx {

class TurboLynxPyResult {
public:
    TurboLynxPyResult(std::vector<std::shared_ptr<duckdb::DataChunk>> chunks,
                      std::vector<std::string> col_names,
                      std::vector<duckdb::LogicalType> col_types,
                      int64_t total_rows);

    ~TurboLynxPyResult() = default;

    py::object Fetchone();
    py::list Fetchmany(int64_t size);
    py::list Fetchall();
    py::object FetchDF();
    py::object FetchDFChunk(int64_t vectors_per_chunk);
    py::object FetchNumpy();
    void Close();
    py::list GetColumnNames();
    py::list GetColumnTypes();
    py::object GetDescription();
    int64_t RowCount() const { return total_rows_; }
    bool IsClosed() const { return closed_; }

    static void Initialize(py::module_ &m);

private:
    py::object ConvertValue(duckdb::DataChunk &chunk, uint64_t col, uint64_t row);
    bool AdvanceCursor(uint64_t &chunk_idx, uint64_t &row_idx);

    std::vector<std::shared_ptr<duckdb::DataChunk>> chunks_;
    std::vector<std::string> col_names_;
    std::vector<duckdb::LogicalType> col_types_;
    int64_t total_rows_;

    uint64_t current_chunk_ = 0;
    uint64_t current_row_ = 0;
    bool closed_ = false;
};

} // namespace turbolynx

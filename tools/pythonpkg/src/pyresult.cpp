#include "turbolynx_python/pyresult.hpp"
#include "common/types/vector.hpp"
#include "common/types/string_type.hpp"
#include "common/types/date.hpp"
#include "common/types/time.hpp"
#include "common/types/timestamp.hpp"
#include "common/types/hugeint.hpp"

namespace turbolynx {

TurboLynxPyResult::TurboLynxPyResult(
    std::vector<std::shared_ptr<duckdb::DataChunk>> chunks,
    std::vector<std::string> col_names,
    std::vector<duckdb::LogicalType> col_types,
    int64_t total_rows)
    : chunks_(std::move(chunks)),
      col_names_(std::move(col_names)),
      col_types_(std::move(col_types)),
      total_rows_(total_rows) {}

bool TurboLynxPyResult::AdvanceCursor(uint64_t &chunk_idx, uint64_t &row_idx) {
    while (current_chunk_ < chunks_.size()) {
        if (current_row_ < chunks_[current_chunk_]->size()) {
            chunk_idx = current_chunk_;
            row_idx = current_row_;
            current_row_++;
            return true;
        }
        current_chunk_++;
        current_row_ = 0;
    }
    return false;
}

py::object TurboLynxPyResult::ConvertValue(duckdb::DataChunk &chunk, uint64_t col, uint64_t row) {
    auto &vec = chunk.data[col];
    auto data = vec.GetData();
    auto &type = col_types_[col];

    // Check for NULL via validity mask
    duckdb::VectorData vdata;
    vec.Orrify(chunk.size(), vdata);
    if (!vdata.validity.RowIsValid(row)) {
        return py::none();
    }

    switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
        return py::cast(((bool *)data)[row]);
    case duckdb::LogicalTypeId::TINYINT:
        return py::cast(((int8_t *)data)[row]);
    case duckdb::LogicalTypeId::SMALLINT:
        return py::cast(((int16_t *)data)[row]);
    case duckdb::LogicalTypeId::INTEGER:
        return py::cast(((int32_t *)data)[row]);
    case duckdb::LogicalTypeId::BIGINT:
        return py::cast(((int64_t *)data)[row]);
    case duckdb::LogicalTypeId::UTINYINT:
        return py::cast(((uint8_t *)data)[row]);
    case duckdb::LogicalTypeId::USMALLINT:
        return py::cast(((uint16_t *)data)[row]);
    case duckdb::LogicalTypeId::UINTEGER:
        return py::cast(((uint32_t *)data)[row]);
    case duckdb::LogicalTypeId::UBIGINT:
        return py::cast(((uint64_t *)data)[row]);
    case duckdb::LogicalTypeId::FLOAT:
        return py::cast(((float *)data)[row]);
    case duckdb::LogicalTypeId::DOUBLE:
        return py::cast(((double *)data)[row]);
    case duckdb::LogicalTypeId::VARCHAR: {
        auto str = ((duckdb::string_t *)data)[row];
        return py::cast(std::string(str.GetDataUnsafe(), str.GetSize()));
    }
    case duckdb::LogicalTypeId::DATE: {
        auto days = ((duckdb::date_t *)data)[row];
        int32_t year, month, day;
        duckdb::Date::Convert(days, year, month, day);
        py::object date_mod = py::module_::import("datetime").attr("date");
        return date_mod(year, month, day);
    }
    case duckdb::LogicalTypeId::TIMESTAMP: {
        auto micros = ((duckdb::timestamp_t *)data)[row];
        duckdb::date_t d;
        duckdb::dtime_t t;
        duckdb::Timestamp::Convert(micros, d, t);
        int32_t year, month, day;
        duckdb::Date::Convert(d, year, month, day);
        int32_t hour, min, sec, usec;
        duckdb::Time::Convert(t, hour, min, sec, usec);
        py::object datetime_mod = py::module_::import("datetime").attr("datetime");
        return datetime_mod(year, month, day, hour, min, sec, usec);
    }
    case duckdb::LogicalTypeId::ID:
        return py::cast(((uint64_t *)data)[row]);
    default:
        // Fallback: convert via Value::ToString
        auto val = chunk.GetValue(col, row);
        return py::cast(val.ToString());
    }
}

py::object TurboLynxPyResult::Fetchone() {
    if (closed_) {
        throw std::runtime_error("Result is closed");
    }
    uint64_t chunk_idx, row_idx;
    if (!AdvanceCursor(chunk_idx, row_idx)) {
        return py::none();
    }
    auto &chunk = *chunks_[chunk_idx];
    py::tuple row(col_names_.size());
    for (uint64_t c = 0; c < col_names_.size(); c++) {
        row[c] = ConvertValue(chunk, c, row_idx);
    }
    return std::move(row);
}

py::list TurboLynxPyResult::Fetchmany(int64_t size) {
    if (closed_) {
        throw std::runtime_error("Result is closed");
    }
    py::list result;
    for (int64_t i = 0; i < size; i++) {
        auto row = Fetchone();
        if (row.is_none()) break;
        result.append(row);
    }
    return result;
}

py::list TurboLynxPyResult::Fetchall() {
    if (closed_) {
        throw std::runtime_error("Result is closed");
    }
    py::list result;
    while (true) {
        auto row = Fetchone();
        if (row.is_none()) break;
        result.append(row);
    }
    return result;
}

py::object TurboLynxPyResult::FetchDF() {
    if (closed_) {
        throw std::runtime_error("Result is closed");
    }
    py::object pd = py::module_::import("pandas");
    py::dict columns;

    // Build column data as Python lists
    std::vector<py::list> col_data(col_names_.size());

    for (auto &chunk : chunks_) {
        for (uint64_t row = 0; row < chunk->size(); row++) {
            for (uint64_t col = 0; col < col_names_.size(); col++) {
                col_data[col].append(ConvertValue(*chunk, col, row));
            }
        }
    }

    for (uint64_t col = 0; col < col_names_.size(); col++) {
        columns[py::cast(col_names_[col])] = col_data[col];
    }

    return pd.attr("DataFrame")(columns);
}

py::object TurboLynxPyResult::FetchNumpy() {
    if (closed_) {
        throw std::runtime_error("Result is closed");
    }
    py::object np = py::module_::import("numpy");

    // First fetch as DataFrame, then convert
    auto df = FetchDF();
    py::dict result;
    for (uint64_t col = 0; col < col_names_.size(); col++) {
        result[py::cast(col_names_[col])] = df.attr("__getitem__")(col_names_[col]).attr("values");
    }
    return result;
}

void TurboLynxPyResult::Close() {
    closed_ = true;
    chunks_.clear();
}

py::list TurboLynxPyResult::GetColumnNames() {
    py::list names;
    for (auto &name : col_names_) {
        names.append(name);
    }
    return names;
}

py::list TurboLynxPyResult::GetColumnTypes() {
    py::list types;
    for (auto &type : col_types_) {
        types.append(type.ToString());
    }
    return types;
}

py::object TurboLynxPyResult::GetDescription() {
    if (col_names_.empty()) return py::none();
    py::list desc;
    for (uint64_t i = 0; i < col_names_.size(); i++) {
        // DB-API 2.0: (name, type_code, display_size, internal_size, precision, scale, null_ok)
        py::tuple col_desc(7);
        col_desc[0] = py::cast(col_names_[i]);
        col_desc[1] = py::cast(col_types_[i].ToString());
        col_desc[2] = py::none();
        col_desc[3] = py::none();
        col_desc[4] = py::none();
        col_desc[5] = py::none();
        col_desc[6] = py::none();
        desc.append(col_desc);
    }
    return desc;
}

void TurboLynxPyResult::Initialize(py::module_ &m) {
    py::class_<TurboLynxPyResult, std::shared_ptr<TurboLynxPyResult>>(m, "TurboLynxPyResult")
        .def("fetchone", &TurboLynxPyResult::Fetchone,
             "Fetch a single row as a tuple, or None if exhausted")
        .def("fetchmany", &TurboLynxPyResult::Fetchmany,
             "Fetch many rows as a list of tuples",
             py::arg("size") = 1)
        .def("fetchall", &TurboLynxPyResult::Fetchall,
             "Fetch all remaining rows as a list of tuples")
        .def("fetchdf", &TurboLynxPyResult::FetchDF,
             "Fetch all rows as a pandas DataFrame")
        .def("df", &TurboLynxPyResult::FetchDF,
             "Alias for fetchdf()")
        .def("fetchnumpy", &TurboLynxPyResult::FetchNumpy,
             "Fetch all rows as a dict of numpy arrays")
        .def("close", &TurboLynxPyResult::Close,
             "Close the result")
        .def("column_names", &TurboLynxPyResult::GetColumnNames,
             "Get column names")
        .def("column_types", &TurboLynxPyResult::GetColumnTypes,
             "Get column types")
        .def_property_readonly("description", &TurboLynxPyResult::GetDescription,
             "DB-API 2.0 description")
        .def_property_readonly("rowcount", &TurboLynxPyResult::RowCount,
             "Total number of rows")
        .def("__iter__", [](std::shared_ptr<TurboLynxPyResult> self) {
            return self;
        })
        .def("__next__", [](TurboLynxPyResult &self) {
            auto row = self.Fetchone();
            if (row.is_none()) throw py::stop_iteration();
            return row;
        });
}

} // namespace turbolynx

#pragma once
#include <string>
#include <vector>
#include <variant>
#include <stdexcept>
#include <cstring>
#include "main/capi/s62.h"

namespace qtest {

// Null sentinel type
struct Null {};

// A single cell value: int64, string, or null
using Value = std::variant<int64_t, std::string, Null>;

inline bool is_null(const Value& v) { return std::holds_alternative<Null>(v); }
inline int64_t as_int64(const Value& v) { return std::get<int64_t>(v); }
inline const std::string& as_string(const Value& v) { return std::get<std::string>(v); }

// A single result row
struct Row {
    std::vector<Value> cols;
    size_t size() const { return cols.size(); }
    int64_t     int64_at(size_t i) const { return as_int64(cols[i]); }
    std::string str_at(size_t i) const { return as_string(cols[i]); }
    bool        is_null_at(size_t i) const { return is_null(cols[i]); }
};

struct QueryResult {
    std::vector<Row> rows;
    size_t size() const { return rows.size(); }
    bool empty() const { return rows.empty(); }
    const Row& operator[](size_t i) const { return rows[i]; }
    int64_t single_int64() const {
        if (rows.empty()) throw std::runtime_error("No rows returned");
        return rows[0].int64_at(0);
    }
    std::string single_string() const {
        if (rows.empty()) throw std::runtime_error("No rows returned");
        return rows[0].str_at(0);
    }
};

// Column type hint for fetching
enum class ColType { AUTO, INT64, UINT64, STRING };

class QueryRunner {
public:
    explicit QueryRunner(const std::string& db_path) {
        conn_id_ = s62_connect(db_path.c_str());
        if (conn_id_ < 0)
            throw std::runtime_error("s62_connect failed for: " + db_path);
    }
    ~QueryRunner() {
        if (conn_id_ >= 0) s62_disconnect(conn_id_);
    }

    int64_t conn_id() const { return conn_id_; }

    // Execute query and fetch all rows.
    // col_types: optional per-column type hints (defaults to AUTO).
    QueryResult run(const char* query,
                    const std::vector<ColType>& col_types = {}) const {
        auto* prep = s62_prepare(conn_id_, const_cast<char*>(query));
        if (!prep)
            throw std::runtime_error(std::string("s62_prepare failed: ") + query);

        s62_resultset_wrapper* rw = nullptr;
        s62_num_rows total = s62_execute(conn_id_, prep, &rw);

        QueryResult result;
        if (rw) {
            // Determine num columns from first resultset
            size_t ncols = 0;
            if (rw->result_set) ncols = rw->result_set->num_properties;

            while (s62_fetch_next(rw) != S62_END_OF_RESULT) {
                Row row;
                for (size_t c = 0; c < ncols; ++c) {
                    ColType ct = (c < col_types.size()) ? col_types[c] : ColType::AUTO;
                    // Determine actual type from resultset column metadata
                    s62_type dtype = S62_TYPE_INVALID;
                    if (rw->result_set && rw->result_set->result) {
                        auto* res = rw->result_set->result;
                        for (size_t ci = 0; ci < c; ++ci) {
                            if (res->next) res = res->next;
                        }
                        dtype = res->data_type;
                    }

                    Value v;
                    if (ct == ColType::STRING || dtype == S62_TYPE_VARCHAR) {
                        s62_string sv = s62_get_varchar(rw, (idx_t)c);
                        if (sv.data == nullptr)
                            v = Null{};
                        else
                            v = std::string(sv.data, sv.size);
                    } else if (ct == ColType::UINT64 || dtype == S62_TYPE_UBIGINT) {
                        v = (int64_t)s62_get_uint64(rw, (idx_t)c);
                    } else if (dtype == S62_TYPE_ID) {
                        v = (int64_t)s62_get_id(rw, (idx_t)c);
                    } else {
                        // Default: try int64
                        v = s62_get_int64(rw, (idx_t)c);
                    }
                    row.cols.push_back(std::move(v));
                }
                result.rows.push_back(std::move(row));
            }
            s62_close_resultset(rw);
        }
        s62_close_prepared_statement(prep);
        return result;
    }

    // Convenience: run and return single int64 value (first row, first col)
    int64_t count(const char* query) const {
        auto r = run(query);
        if (r.empty()) return 0;
        return r[0].int64_at(0);
    }

private:
    int64_t conn_id_ = -1;
};

} // namespace qtest

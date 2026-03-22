#pragma once
#include <string>
#include <vector>
#include <variant>
#include <stdexcept>
#include <cstring>
#include "main/capi/turbolynx.h"

namespace qtest {

// Null sentinel type
struct Null {};

// A single cell value: int64, bool, string, or null
using Value = std::variant<int64_t, bool, std::string, Null>;

inline bool is_null(const Value& v) { return std::holds_alternative<Null>(v); }
inline int64_t as_int64(const Value& v) { return std::get<int64_t>(v); }
inline bool as_bool(const Value& v) { return std::get<bool>(v); }
inline const std::string& as_string(const Value& v) { return std::get<std::string>(v); }

// A single result row
struct Row {
    std::vector<Value> cols;
    size_t size() const { return cols.size(); }
    int64_t     int64_at(size_t i) const { return as_int64(cols[i]); }
    bool        bool_at(size_t i) const { return as_bool(cols[i]); }
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
enum class ColType { AUTO, INT64, UINT64, STRING, BOOL };

class QueryRunner {
public:
    explicit QueryRunner(const std::string& db_path) {
        conn_id_ = turbolynx_connect(db_path.c_str());
        if (conn_id_ < 0)
            throw std::runtime_error("turbolynx_connect failed for: " + db_path);
    }
    ~QueryRunner() {
        if (conn_id_ >= 0) turbolynx_disconnect(conn_id_);
    }

    int64_t conn_id() const { return conn_id_; }

    // Execute query and fetch all rows.
    // col_types: optional per-column type hints (defaults to AUTO).
    QueryResult run(const char* query,
                    const std::vector<ColType>& col_types = {}) const {
        auto* prep = turbolynx_prepare(conn_id_, const_cast<char*>(query));
        if (!prep)
            throw std::runtime_error(std::string("turbolynx_prepare failed: ") + query);

        turbolynx_resultset_wrapper* rw = nullptr;
        turbolynx_num_rows total = turbolynx_execute(conn_id_, prep, &rw);
        if (total == TURBOLYNX_ERROR)
            throw std::runtime_error(std::string("turbolynx_execute failed: ") + query);

        QueryResult result;
        if (rw) {
            // Determine num columns from first resultset
            size_t ncols = 0;
            if (rw->result_set) ncols = rw->result_set->num_properties;

            while (turbolynx_fetch_next(rw) != TURBOLYNX_END_OF_RESULT) {
                Row row;
                for (size_t c = 0; c < ncols; ++c) {
                    ColType ct = (c < col_types.size()) ? col_types[c] : ColType::AUTO;
                    // Determine actual type from resultset column metadata
                    turbolynx_type dtype = TURBOLYNX_TYPE_INVALID;
                    if (rw->result_set && rw->result_set->result) {
                        auto* res = rw->result_set->result;
                        for (size_t ci = 0; ci < c; ++ci) {
                            if (res->next) res = res->next;
                        }
                        dtype = res->data_type;
                    }

                    Value v;
                    if (ct == ColType::BOOL || dtype == TURBOLYNX_TYPE_BOOLEAN) {
                        v = (bool)turbolynx_get_bool(rw, (idx_t)c);
                    } else if (ct == ColType::STRING || dtype == TURBOLYNX_TYPE_VARCHAR) {
                        turbolynx_string sv = turbolynx_get_varchar(rw, (idx_t)c);
                        if (sv.data == nullptr)
                            v = Null{};
                        else
                            v = std::string(sv.data, sv.size);
                    } else if (ct == ColType::UINT64 || dtype == TURBOLYNX_TYPE_UBIGINT) {
                        v = (int64_t)turbolynx_get_uint64(rw, (idx_t)c);
                    } else if (dtype == TURBOLYNX_TYPE_HUGEINT) {
                        auto hi = turbolynx_get_hugeint(rw, (idx_t)c);
                        v = (int64_t)hi.lower;
                    } else if (dtype == TURBOLYNX_TYPE_ID) {
                        v = (int64_t)turbolynx_get_id(rw, (idx_t)c);
                    } else {
                        // Default: try int64
                        v = turbolynx_get_int64(rw, (idx_t)c);
                    }
                    row.cols.push_back(std::move(v));
                }
                result.rows.push_back(std::move(row));
            }
            turbolynx_close_resultset(rw);
        }
        turbolynx_close_prepared_statement(prep);
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

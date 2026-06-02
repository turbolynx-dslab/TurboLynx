#include "seeded_workspace_oracle.hpp"

#include "main/capi/turbolynx.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <stdexcept>

namespace tl_fuzz_oracle {

namespace {

void unlink_recursive(const std::filesystem::path& p) noexcept {
    std::error_code ec;
    std::filesystem::remove_all(p, ec);
}

qtest::Value fetch_one(turbolynx_resultset_wrapper* rw, size_t c,
                       turbolynx_type dtype) {
    if (turbolynx_value_is_null(rw, (idx_t)c)) {
        return qtest::Null{};
    }
    switch (dtype) {
        case TURBOLYNX_TYPE_BOOLEAN:
            return qtest::Value{(bool)turbolynx_get_bool(rw, (idx_t)c)};
        case TURBOLYNX_TYPE_VARCHAR: {
            turbolynx_string sv = turbolynx_get_varchar(rw, (idx_t)c);
            return qtest::Value{std::string(sv.data, sv.size)};
        }
        case TURBOLYNX_TYPE_UBIGINT:
            return qtest::Value{(int64_t)turbolynx_get_uint64(rw, (idx_t)c)};
        case TURBOLYNX_TYPE_HUGEINT: {
            auto hi = turbolynx_get_hugeint(rw, (idx_t)c);
            return qtest::Value{(int64_t)hi.lower};
        }
        case TURBOLYNX_TYPE_ID:
            return qtest::Value{(int64_t)turbolynx_get_id(rw, (idx_t)c)};
        case TURBOLYNX_TYPE_DOUBLE: {
            char buf[64];
            std::snprintf(buf, sizeof(buf), "%f",
                          turbolynx_get_double(rw, (idx_t)c));
            return qtest::Value{std::string(buf)};
        }
        case TURBOLYNX_TYPE_FLOAT: {
            char buf[64];
            std::snprintf(buf, sizeof(buf), "%f",
                          (double)turbolynx_get_float(rw, (idx_t)c));
            return qtest::Value{std::string(buf)};
        }
        default:
            return qtest::Value{turbolynx_get_int64(rw, (idx_t)c)};
    }
}

}  // namespace

TurboLynxBackend::TurboLynxBackend() {
    char tmpl[] = "/tmp/tl_fuzz_oracle_XXXXXX";
    char* d = mkdtemp(tmpl);
    if (!d) {
        throw std::runtime_error(std::string("mkdtemp failed: ") +
                                 std::strerror(errno));
    }
    path_    = d;
    conn_id_ = turbolynx_connect(path_.c_str());
    if (conn_id_ < 0) {
        unlink_recursive(path_);
        throw std::runtime_error("turbolynx_connect failed for " + path_);
    }
}

TurboLynxBackend::~TurboLynxBackend() {
    if (conn_id_ >= 0) {
        turbolynx_disconnect(conn_id_);
    }
    unlink_recursive(path_);
}

qtest::QueryResult TurboLynxBackend::run(const std::string& cypher) {
    auto* prep = turbolynx_prepare(conn_id_,
                                    const_cast<char*>(cypher.c_str()));
    if (!prep) {
        char* errmsg = nullptr;
        turbolynx_get_last_error(&errmsg);
        std::string msg = errmsg ? errmsg : cypher;
        throw std::runtime_error("TurboLynx prepare failed: " + msg);
    }

    turbolynx_resultset_wrapper* rw = nullptr;
    turbolynx_num_rows total = turbolynx_execute(conn_id_, prep, &rw);
    if (total == TURBOLYNX_ERROR) {
        char* errmsg = nullptr;
        turbolynx_get_last_error(&errmsg);
        std::string msg = errmsg ? errmsg : cypher;
        turbolynx_close_prepared_statement(prep);
        throw std::runtime_error("TurboLynx execute failed: " + msg);
    }

    qtest::QueryResult result;
    if (rw) {
        size_t ncols = (rw->result_set ? rw->result_set->num_properties : 0);
        while (turbolynx_fetch_next(rw) != TURBOLYNX_END_OF_RESULT) {
            qtest::Row row;
            for (size_t c = 0; c < ncols; ++c) {
                turbolynx_type dtype = TURBOLYNX_TYPE_INVALID;
                if (rw->result_set && rw->result_set->result) {
                    auto* res = rw->result_set->result;
                    for (size_t ci = 0; ci < c; ++ci) {
                        if (res->next) res = res->next;
                    }
                    dtype = res->data_type;
                }
                row.cols.push_back(fetch_one(rw, c, dtype));
            }
            result.rows.push_back(std::move(row));
        }
        turbolynx_close_resultset(rw);
    }
    turbolynx_close_prepared_statement(prep);
    return result;
}

}  // namespace tl_fuzz_oracle

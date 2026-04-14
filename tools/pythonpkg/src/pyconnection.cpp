#include "turbolynx_python/pyconnection.hpp"
#include "main/capi/turbolynx.h"
#include "main/capi/cypher_prepared_statement.hpp"
#include "common/common.hpp"
#include "common/typedef.hpp"
#include "common/types/data_chunk.hpp"

namespace turbolynx {

TurboLynxPyConnection::~TurboLynxPyConnection() {
    try {
        Close();
    } catch (...) {
    }
}

std::shared_ptr<TurboLynxPyConnection> TurboLynxPyConnection::Connect(
    const std::string &database, bool read_only) {
    auto conn = std::make_shared<TurboLynxPyConnection>();
    int64_t id;
    {
        py::gil_scoped_release release;
        if (read_only) {
            id = turbolynx_connect_readonly(database.c_str());
        } else {
            id = turbolynx_connect(database.c_str());
        }
    }
    if (id < 0) {
        char *errmsg = nullptr;
        turbolynx_get_last_error(&errmsg);
        std::string msg = errmsg ? errmsg : "Failed to connect to database";
        throw std::runtime_error(msg);
    }
    conn->conn_id_ = id;
    conn->connected_ = true;
    return conn;
}

std::shared_ptr<TurboLynxPyResult> TurboLynxPyConnection::Execute(
    const std::string &query, py::object params) {
    AssertConnected();
    std::lock_guard<std::mutex> guard(lock_);

    // Substitute parameters into query before prepare
    std::string bound_query = query;
    if (!params.is_none() && py::isinstance<py::dict>(params)) {
        auto dict = params.cast<py::dict>();
        for (auto &item : dict) {
            std::string key = "$" + item.first.cast<std::string>();
            std::string replacement;
            auto val = item.second;
            if (val.is_none()) {
                replacement = "NULL";
            } else if (py::isinstance<py::bool_>(val)) {
                replacement = val.cast<bool>() ? "true" : "false";
            } else if (py::isinstance<py::int_>(val)) {
                replacement = std::to_string(val.cast<int64_t>());
            } else if (py::isinstance<py::float_>(val)) {
                replacement = std::to_string(val.cast<double>());
            } else if (py::isinstance<py::str>(val)) {
                replacement = "'" + val.cast<std::string>() + "'";
            } else {
                replacement = py::str(val).cast<std::string>();
            }
            // Replace all occurrences of $key in query
            size_t pos = 0;
            while ((pos = bound_query.find(key, pos)) != std::string::npos) {
                // Ensure we match the full parameter name (not a prefix)
                size_t end = pos + key.size();
                if (end < bound_query.size() && (std::isalnum(bound_query[end]) || bound_query[end] == '_')) {
                    pos = end;
                    continue;
                }
                bound_query.replace(pos, key.size(), replacement);
                pos += replacement.size();
            }
        }
    }

    // Prepare
    turbolynx_prepared_statement *prep;
    {
        py::gil_scoped_release release;
        prep = turbolynx_prepare(conn_id_, const_cast<char *>(bound_query.c_str()));
    }
    if (!prep) {
        char *errmsg = nullptr;
        turbolynx_get_last_error(&errmsg);
        std::string msg = errmsg ? errmsg : "Failed to prepare query";
        throw std::runtime_error(msg);
    }

    // Execute
    std::vector<std::shared_ptr<duckdb::DataChunk>> chunks;
    duckdb::Schema schema;
    std::vector<std::string> col_names;
    bool is_mutation = false;
    int64_t num_rows;

    {
        py::gil_scoped_release release;
        num_rows = turbolynx_execute_raw(conn_id_, prep, chunks, schema, col_names, is_mutation);
    }

    turbolynx_close_prepared_statement(prep);

    if (num_rows < 0) {
        char *errmsg = nullptr;
        turbolynx_get_last_error(&errmsg);
        std::string msg = errmsg ? errmsg : "Query execution failed";
        throw std::runtime_error(msg);
    }

    if (is_mutation) {
        // Mutation queries return empty result
        return std::make_shared<TurboLynxPyResult>(
            std::vector<std::shared_ptr<duckdb::DataChunk>>(),
            std::vector<std::string>(),
            std::vector<duckdb::LogicalType>(),
            0);
    }

    // Extract column types from chunks
    std::vector<duckdb::LogicalType> col_types;
    if (!chunks.empty() && chunks[0]->ColumnCount() > 0) {
        for (idx_t i = 0; i < chunks[0]->ColumnCount(); i++) {
            col_types.push_back(chunks[0]->data[i].GetType());
        }
    } else {
        // No data — fill with VARCHAR placeholders
        for (size_t i = 0; i < col_names.size(); i++) {
            col_types.push_back(duckdb::LogicalType::VARCHAR);
        }
    }

    return std::make_shared<TurboLynxPyResult>(
        std::move(chunks), std::move(col_names), std::move(col_types), num_rows);
}

void TurboLynxPyConnection::Close() {
    if (connected_) {
        py::gil_scoped_release release;
        turbolynx_disconnect(conn_id_);
        connected_ = false;
        conn_id_ = -1;
    }
}

bool TurboLynxPyConnection::IsConnected() const {
    return connected_;
}

void TurboLynxPyConnection::Checkpoint() {
    AssertConnected();
    py::gil_scoped_release release;
    turbolynx_checkpoint(conn_id_);
}

void TurboLynxPyConnection::ClearDelta() {
    AssertConnected();
    py::gil_scoped_release release;
    turbolynx_clear_delta(conn_id_);
}

void TurboLynxPyConnection::SetMaxThreads(int64_t max_threads) {
    AssertConnected();
    py::gil_scoped_release release;
    turbolynx_set_max_threads(conn_id_, (size_t)max_threads);
}

std::string TurboLynxPyConnection::GetVersion() {
    return std::string(turbolynx_get_version());
}

std::shared_ptr<TurboLynxPyConnection> TurboLynxPyConnection::Enter() {
    return shared_from_this();
}

void TurboLynxPyConnection::Exit(const py::object &exc_type,
                                  const py::object &exc_val,
                                  const py::object &exc_tb) {
    Close();
}

void TurboLynxPyConnection::AssertConnected() const {
    if (!connected_) {
        throw std::runtime_error("Not connected to database");
    }
}

void TurboLynxPyConnection::Initialize(py::module_ &m) {
    auto conn_class = py::class_<TurboLynxPyConnection, std::shared_ptr<TurboLynxPyConnection>>(
        m, "TurboLynxPyConnection");

    conn_class
        .def("execute", &TurboLynxPyConnection::Execute,
             "Execute a Cypher query",
             py::arg("query"), py::arg("params") = py::none())
        .def("close", &TurboLynxPyConnection::Close,
             "Close the connection")
        .def("is_connected", &TurboLynxPyConnection::IsConnected,
             "Check if connected")
        .def("checkpoint", &TurboLynxPyConnection::Checkpoint,
             "Flush DeltaStore to disk")
        .def("clear_delta", &TurboLynxPyConnection::ClearDelta,
             "Clear in-memory mutations")
        .def("set_max_threads", &TurboLynxPyConnection::SetMaxThreads,
             "Set max threads for parallel execution",
             py::arg("max_threads"))
        .def("__enter__", &TurboLynxPyConnection::Enter)
        .def("__exit__", &TurboLynxPyConnection::Exit)
        .def("__del__", &TurboLynxPyConnection::Close);
}

} // namespace turbolynx

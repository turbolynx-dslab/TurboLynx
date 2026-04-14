#include "turbolynx_python/pyconnection.hpp"
#include "main/capi/turbolynx.h"
#include "main/capi/cypher_prepared_statement.hpp"
#include "common/common.hpp"
#include "common/typedef.hpp"

#include <set>
#include "common/types/data_chunk.hpp"

namespace turbolynx {

// ============================================================
// Exception types (DB-API 2.0 hierarchy)
// ============================================================
static py::exception<std::runtime_error> *ex_error = nullptr;
static py::exception<std::runtime_error> *ex_database_error = nullptr;
static py::exception<std::runtime_error> *ex_operational_error = nullptr;
static py::exception<std::runtime_error> *ex_programming_error = nullptr;
static py::exception<std::runtime_error> *ex_data_error = nullptr;
static py::exception<std::runtime_error> *ex_integrity_error = nullptr;
static py::exception<std::runtime_error> *ex_internal_error = nullptr;
static py::exception<std::runtime_error> *ex_not_supported_error = nullptr;

void TurboLynxPyConnection::RegisterExceptions(py::module_ &m) {
    // DB-API 2.0 exception hierarchy
    ex_error = new py::exception<std::runtime_error>(m, "Error");
    ex_database_error = new py::exception<std::runtime_error>(m, "DatabaseError", *ex_error);
    ex_operational_error = new py::exception<std::runtime_error>(m, "OperationalError", *ex_database_error);
    ex_programming_error = new py::exception<std::runtime_error>(m, "ProgrammingError", *ex_database_error);
    ex_data_error = new py::exception<std::runtime_error>(m, "DataError", *ex_database_error);
    ex_integrity_error = new py::exception<std::runtime_error>(m, "IntegrityError", *ex_database_error);
    ex_internal_error = new py::exception<std::runtime_error>(m, "InternalError", *ex_database_error);
    ex_not_supported_error = new py::exception<std::runtime_error>(m, "NotSupportedError", *ex_database_error);
}

// ============================================================
// Connection lifecycle
// ============================================================

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
    conn->read_only_ = read_only;
    conn->db_path_ = database;
    return conn;
}

std::shared_ptr<TurboLynxPyConnection> TurboLynxPyConnection::Cursor() {
    AssertConnected();
    // Return self — TurboLynx connections share database state,
    // so opening/closing a second connection can corrupt shared resources.
    return shared_from_this();
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

// ============================================================
// Query execution
// ============================================================

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
            size_t pos = 0;
            while ((pos = bound_query.find(key, pos)) != std::string::npos) {
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
        for (size_t i = 0; i < col_names.size(); i++) {
            col_types.push_back(duckdb::LogicalType::VARCHAR);
        }
    }

    return std::make_shared<TurboLynxPyResult>(
        std::move(chunks), std::move(col_names), std::move(col_types), num_rows);
}

void TurboLynxPyConnection::ExecuteMany(const std::string &query, py::list params_list) {
    for (auto &params : params_list) {
        Execute(query, py::reinterpret_borrow<py::object>(params));
    }
}

// ============================================================
// Database operations
// ============================================================

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

void TurboLynxPyConnection::Interrupt() {
    AssertConnected();
    turbolynx_interrupt(conn_id_);
}

int64_t TurboLynxPyConnection::QueryProgress() {
    AssertConnected();
    return turbolynx_query_progress(conn_id_);
}

// Transaction control — TurboLynx uses implicit transactions,
// so these are no-ops for API compatibility.
void TurboLynxPyConnection::Begin() {
    AssertConnected();
}

void TurboLynxPyConnection::Commit() {
    AssertConnected();
}

void TurboLynxPyConnection::Rollback() {
    AssertConnected();
    // Best-effort: clear in-memory mutations
    ClearDelta();
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

std::shared_ptr<TurboLynxPyPreparedStatement> TurboLynxPyConnection::Prepare(const std::string &query) {
    AssertConnected();
    return std::make_shared<TurboLynxPyPreparedStatement>(conn_id_, query);
}

void TurboLynxPyConnection::AssertConnected() const {
    if (!connected_) {
        throw std::runtime_error("Not connected to database");
    }
}

// ============================================================
// Schema / Metadata
// ============================================================

py::list TurboLynxPyConnection::Labels() {
    AssertConnected();
    py::list result;
    turbolynx_metadata *metadata = nullptr;
    turbolynx_num_metadata count;
    {
        py::gil_scoped_release release;
        count = turbolynx_get_metadata_from_catalog(conn_id_, nullptr, false, false, &metadata);
    }
    turbolynx_metadata *curr = metadata;
    while (curr) {
        if (curr->type == TURBOLYNX_NODE && curr->label_name) {
            result.append(py::str(curr->label_name));
        }
        curr = curr->next;
    }
    if (metadata) {
        turbolynx_close_metadata(metadata);
    }
    return result;
}

py::list TurboLynxPyConnection::RelationshipTypes() {
    AssertConnected();
    py::list result;
    std::set<std::string> seen;
    turbolynx_metadata *metadata = nullptr;
    turbolynx_num_metadata count;
    {
        py::gil_scoped_release release;
        count = turbolynx_get_metadata_from_catalog(conn_id_, nullptr, false, false, &metadata);
    }
    turbolynx_metadata *curr = metadata;
    while (curr) {
        if (curr->type == TURBOLYNX_EDGE && curr->label_name) {
            // Edge labels are stored as "TYPE@SrcLabel@DstLabel" — extract just the type
            std::string full_name = curr->label_name;
            auto at_pos = full_name.find('@');
            std::string type_name = (at_pos != std::string::npos) ? full_name.substr(0, at_pos) : full_name;
            if (seen.insert(type_name).second) {
                result.append(py::str(type_name));
            }
        }
        curr = curr->next;
    }
    if (metadata) {
        turbolynx_close_metadata(metadata);
    }
    return result;
}

py::dict TurboLynxPyConnection::Schema(const std::string &label, bool is_edge) {
    AssertConnected();
    py::dict result;
    turbolynx_metadata_type mtype = is_edge ? TURBOLYNX_EDGE : TURBOLYNX_NODE;

    // For edges, the catalog uses "TYPE@Src@Dst" format.
    // If the user passes just "KNOWS", find the first matching full name.
    std::string lookup_label = label;
    if (is_edge && label.find('@') == std::string::npos) {
        turbolynx_metadata *metadata = nullptr;
        {
            py::gil_scoped_release release;
            turbolynx_get_metadata_from_catalog(conn_id_, nullptr, false, false, &metadata);
        }
        turbolynx_metadata *curr = metadata;
        while (curr) {
            if (curr->type == TURBOLYNX_EDGE && curr->label_name) {
                std::string full_name = curr->label_name;
                auto at_pos = full_name.find('@');
                std::string type_name = (at_pos != std::string::npos) ? full_name.substr(0, at_pos) : full_name;
                if (type_name == label) {
                    lookup_label = full_name;
                    break;
                }
            }
            curr = curr->next;
        }
        if (metadata) {
            turbolynx_close_metadata(metadata);
        }
    }

    turbolynx_property *props = nullptr;
    turbolynx_num_properties count;
    {
        py::gil_scoped_release release;
        count = turbolynx_get_property_from_catalog(
            conn_id_, const_cast<char *>(lookup_label.c_str()), mtype, &props);
    }
    turbolynx_property *curr = props;
    while (curr) {
        if (curr->property_name && curr->property_sql_type) {
            result[py::str(curr->property_name)] = py::str(curr->property_sql_type);
        }
        curr = curr->next;
    }
    if (props) {
        turbolynx_close_property(props);
    }
    return result;
}

// ============================================================
// PreparedStatement
// ============================================================

TurboLynxPyPreparedStatement::TurboLynxPyPreparedStatement(int64_t conn_id, const std::string &query)
    : conn_id_(conn_id), query_(query) {
    py::gil_scoped_release release;
    prep_ = turbolynx_prepare(conn_id_, const_cast<char *>(query_.c_str()));
    if (!prep_) {
        char *errmsg = nullptr;
        turbolynx_get_last_error(&errmsg);
        std::string msg = errmsg ? errmsg : "Failed to prepare statement";
        throw std::runtime_error(msg);
    }
    // Get number of parameters from the CypherPreparedStatement
    if (prep_->__internal_prepared_statement &&
        prep_->__internal_prepared_statement != (void*)0x1 &&
        prep_->__internal_prepared_statement != (void*)0x2 &&
        prep_->__internal_prepared_statement != (void*)0x3) {
        auto *cypher_stmt = reinterpret_cast<duckdb::CypherPreparedStatement *>(
            prep_->__internal_prepared_statement);
        num_params_ = cypher_stmt->getNumParams();
    }
}

TurboLynxPyPreparedStatement::~TurboLynxPyPreparedStatement() {
    try {
        Close();
    } catch (...) {
    }
}

void TurboLynxPyPreparedStatement::Close() {
    if (prep_) {
        turbolynx_close_prepared_statement(prep_);
        prep_ = nullptr;
    }
}

std::shared_ptr<TurboLynxPyResult> TurboLynxPyPreparedStatement::Execute(py::object params) {
    std::lock_guard<std::mutex> guard(lock_);
    if (!prep_) {
        throw std::runtime_error("PreparedStatement is closed");
    }

    // Bind parameters
    if (num_params_ > 0) {
        auto *cypher_stmt = reinterpret_cast<duckdb::CypherPreparedStatement *>(
            prep_->__internal_prepared_statement);

        if (params.is_none()) {
            throw std::runtime_error("Statement has " + std::to_string(num_params_) +
                                     " parameter(s) but no values provided");
        }

        if (py::isinstance<py::dict>(params)) {
            auto dict = params.cast<py::dict>();
            for (auto &item : dict) {
                std::string key = "$" + item.first.cast<std::string>();
                auto val = item.second;
                // Find parameter index
                int idx = -1;
                for (int i = 0; i < (int)cypher_stmt->paramOrder.size(); i++) {
                    if (cypher_stmt->paramOrder[i] == key) {
                        idx = i;
                        break;
                    }
                }
                if (idx < 0) {
                    throw std::runtime_error("Unknown parameter: " + key);
                }
                // Convert Python value to string representation
                if (val.is_none()) {
                    cypher_stmt->params[key] = "NULL";
                } else if (py::isinstance<py::bool_>(val)) {
                    cypher_stmt->params[key] = val.cast<bool>() ? "true" : "false";
                } else if (py::isinstance<py::int_>(val)) {
                    cypher_stmt->params[key] = std::to_string(val.cast<int64_t>());
                } else if (py::isinstance<py::float_>(val)) {
                    cypher_stmt->params[key] = std::to_string(val.cast<double>());
                } else if (py::isinstance<py::str>(val)) {
                    cypher_stmt->params[key] = "'" + val.cast<std::string>() + "'";
                } else {
                    cypher_stmt->params[key] = py::str(val).cast<std::string>();
                }
            }
        } else if (py::isinstance<py::list>(params) || py::isinstance<py::tuple>(params)) {
            auto seq = params.cast<py::sequence>();
            if ((int64_t)py::len(seq) != num_params_) {
                throw std::runtime_error("Expected " + std::to_string(num_params_) +
                                         " parameter(s) but got " + std::to_string(py::len(seq)));
            }
            for (int i = 0; i < (int)num_params_; i++) {
                auto val = seq[i];
                std::string &target = cypher_stmt->params[cypher_stmt->paramOrder[i]];
                if (val.is_none()) {
                    target = "NULL";
                } else if (py::isinstance<py::bool_>(val)) {
                    target = val.cast<bool>() ? "true" : "false";
                } else if (py::isinstance<py::int_>(val)) {
                    target = std::to_string(val.cast<int64_t>());
                } else if (py::isinstance<py::float_>(val)) {
                    target = std::to_string(val.cast<double>());
                } else if (py::isinstance<py::str>(val)) {
                    target = "'" + val.cast<std::string>() + "'";
                } else {
                    target = py::str(val).cast<std::string>();
                }
            }
        } else {
            throw std::runtime_error("Parameters must be a dict, list, or tuple");
        }
    }

    // Execute
    std::vector<std::shared_ptr<duckdb::DataChunk>> chunks;
    duckdb::Schema schema;
    std::vector<std::string> col_names;
    bool is_mutation = false;
    int64_t num_rows;

    {
        py::gil_scoped_release release;
        num_rows = turbolynx_execute_raw(conn_id_, prep_, chunks, schema, col_names, is_mutation);
    }

    if (num_rows < 0) {
        char *errmsg = nullptr;
        turbolynx_get_last_error(&errmsg);
        std::string msg = errmsg ? errmsg : "Query execution failed";
        throw std::runtime_error(msg);
    }

    if (is_mutation) {
        return std::make_shared<TurboLynxPyResult>(
            std::vector<std::shared_ptr<duckdb::DataChunk>>(),
            std::vector<std::string>(),
            std::vector<duckdb::LogicalType>(),
            0);
    }

    std::vector<duckdb::LogicalType> col_types;
    if (!chunks.empty() && chunks[0]->ColumnCount() > 0) {
        for (uint64_t i = 0; i < chunks[0]->ColumnCount(); i++) {
            col_types.push_back(chunks[0]->data[i].GetType());
        }
    } else {
        for (size_t i = 0; i < col_names.size(); i++) {
            col_types.push_back(duckdb::LogicalType::VARCHAR);
        }
    }

    return std::make_shared<TurboLynxPyResult>(
        std::move(chunks), std::move(col_names), std::move(col_types), num_rows);
}

void TurboLynxPyPreparedStatement::Initialize(py::module_ &m) {
    py::class_<TurboLynxPyPreparedStatement, std::shared_ptr<TurboLynxPyPreparedStatement>>(
        m, "PreparedStatement")
        .def("execute", &TurboLynxPyPreparedStatement::Execute,
             "Execute the prepared statement with parameters",
             py::arg("params") = py::none())
        .def("close", &TurboLynxPyPreparedStatement::Close,
             "Close the prepared statement")
        .def_property_readonly("num_params", &TurboLynxPyPreparedStatement::NumParams,
             "Number of parameters")
        .def_property_readonly("query", &TurboLynxPyPreparedStatement::GetQuery,
             "The query string")
        .def("__del__", &TurboLynxPyPreparedStatement::Close);
}

// ============================================================
// pybind11 bindings
// ============================================================

void TurboLynxPyConnection::Initialize(py::module_ &m) {
    auto conn_class = py::class_<TurboLynxPyConnection, std::shared_ptr<TurboLynxPyConnection>>(
        m, "TurboLynxPyConnection");

    conn_class
        // Query execution
        .def("execute", &TurboLynxPyConnection::Execute,
             "Execute a Cypher query",
             py::arg("query"), py::arg("params") = py::none())
        .def("executemany", &TurboLynxPyConnection::ExecuteMany,
             "Execute a query with multiple parameter sets",
             py::arg("query"), py::arg("params"))
        .def("prepare", &TurboLynxPyConnection::Prepare,
             "Prepare a statement for execution with parameters",
             py::arg("query"))
        // Cursor
        .def("cursor", &TurboLynxPyConnection::Cursor,
             "Create a duplicate connection to the same database")
        .def("duplicate", &TurboLynxPyConnection::Cursor,
             "Create a duplicate connection (alias for cursor)")
        // Connection management
        .def("close", &TurboLynxPyConnection::Close,
             "Close the connection")
        .def("is_connected", &TurboLynxPyConnection::IsConnected,
             "Check if connected")
        // Database operations
        .def("checkpoint", &TurboLynxPyConnection::Checkpoint,
             "Flush DeltaStore to disk")
        .def("clear_delta", &TurboLynxPyConnection::ClearDelta,
             "Clear in-memory mutations")
        .def("set_max_threads", &TurboLynxPyConnection::SetMaxThreads,
             "Set max threads for parallel execution",
             py::arg("max_threads"))
        // Query control
        .def("interrupt", &TurboLynxPyConnection::Interrupt,
             "Interrupt a currently executing query")
        .def("query_progress", &TurboLynxPyConnection::QueryProgress,
             "Get rows processed by the running query (-1 if idle)")
        // Transaction control
        .def("begin", &TurboLynxPyConnection::Begin,
             "Begin a transaction")
        .def("commit", &TurboLynxPyConnection::Commit,
             "Commit the current transaction")
        .def("rollback", &TurboLynxPyConnection::Rollback,
             "Rollback the current transaction (clears in-memory mutations)")
        // Schema / Metadata
        .def("labels", &TurboLynxPyConnection::Labels,
             "Get all node labels from catalog")
        .def("relationship_types", &TurboLynxPyConnection::RelationshipTypes,
             "Get all relationship types from catalog")
        .def("schema", &TurboLynxPyConnection::Schema,
             "Get property schema for a label",
             py::arg("label"), py::arg("edge") = false)
        // Context manager
        .def("__enter__", &TurboLynxPyConnection::Enter)
        .def("__exit__", &TurboLynxPyConnection::Exit)
        .def("__del__", &TurboLynxPyConnection::Close);
}

} // namespace turbolynx

#pragma once

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <string>
#include <memory>
#include <mutex>

#include "turbolynx_python/pyresult.hpp"

namespace py = pybind11;

// Forward declaration (matches turbolynx.h)
struct _turbolynx_prepared_statement;
typedef struct _turbolynx_prepared_statement turbolynx_prepared_statement;

namespace turbolynx {

class TurboLynxPyConnection;

//! Wraps a turbolynx_prepared_statement for parameter binding + execution
class TurboLynxPyPreparedStatement {
public:
    TurboLynxPyPreparedStatement(int64_t conn_id, const std::string &query);
    ~TurboLynxPyPreparedStatement();

    //! Bind parameters and execute
    std::shared_ptr<TurboLynxPyResult> Execute(py::object params = py::none());

    //! Get number of parameters
    int64_t NumParams() const { return num_params_; }

    //! Get the query string
    const std::string &GetQuery() const { return query_; }

    //! Close / release the prepared statement
    void Close();

    //! Register pybind11 bindings
    static void Initialize(py::module_ &m);

private:
    int64_t conn_id_;
    std::string query_;
    int64_t num_params_ = 0;
    turbolynx_prepared_statement *prep_ = nullptr;
    std::mutex lock_;
};

class TurboLynxPyConnection : public std::enable_shared_from_this<TurboLynxPyConnection> {
public:
    TurboLynxPyConnection() = default;
    ~TurboLynxPyConnection();

    //! Connect to a database
    static std::shared_ptr<TurboLynxPyConnection> Connect(const std::string &database,
                                                           bool read_only = false);

    //! Execute a Cypher query with optional parameters
    std::shared_ptr<TurboLynxPyResult> Execute(const std::string &query,
                                                py::object params = py::none());

    //! Execute a query with multiple parameter sets
    void ExecuteMany(const std::string &query, py::list params_list);

    //! Prepare a statement for later execution with parameters
    std::shared_ptr<TurboLynxPyPreparedStatement> Prepare(const std::string &query);

    //! Create a duplicate connection to the same database
    std::shared_ptr<TurboLynxPyConnection> Cursor();

    //! Close the connection
    void Close();

    //! Check if connected
    bool IsConnected() const;

    //! Flush DeltaStore to disk
    void Checkpoint();

    //! Clear in-memory mutations
    void ClearDelta();

    //! Set max threads
    void SetMaxThreads(int64_t max_threads);

    //! Interrupt a running query
    void Interrupt();

    //! Get rows processed by the currently executing query (-1 if idle)
    int64_t QueryProgress();

    //! Transaction control
    void Begin();
    void Commit();
    void Rollback();

    //! Get version
    static std::string GetVersion();

    //! Context manager support
    std::shared_ptr<TurboLynxPyConnection> Enter();
    void Exit(const py::object &exc_type, const py::object &exc_val, const py::object &exc_tb);

    //! Register pybind11 bindings
    static void Initialize(py::module_ &m);

    //! Register exception types
    static void RegisterExceptions(py::module_ &m);

    //! Get the database path for this connection
    const std::string &GetDatabasePath() const { return db_path_; }

    //! Schema / Metadata
    py::list Labels();
    py::list RelationshipTypes();
    py::dict Schema(const std::string &label, bool is_edge = false);

private:
    void AssertConnected() const;

    int64_t conn_id_ = -1;
    bool connected_ = false;
    bool read_only_ = false;
    std::string db_path_;
    std::mutex lock_;
};

} // namespace turbolynx

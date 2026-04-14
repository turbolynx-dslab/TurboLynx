#pragma once

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <string>
#include <memory>
#include <mutex>

#include "turbolynx_python/pyresult.hpp"

namespace py = pybind11;

namespace turbolynx {

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

    //! Get version
    static std::string GetVersion();

    //! Context manager support
    std::shared_ptr<TurboLynxPyConnection> Enter();
    void Exit(const py::object &exc_type, const py::object &exc_val, const py::object &exc_tb);

    //! Register pybind11 bindings
    static void Initialize(py::module_ &m);

private:
    void AssertConnected() const;

    int64_t conn_id_ = -1;
    bool connected_ = false;
    std::mutex lock_;
};

} // namespace turbolynx

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "turbolynx_python/pyconnection.hpp"
#include "turbolynx_python/pyresult.hpp"
#include "main/capi/turbolynx.h"

namespace py = pybind11;

PYBIND11_MODULE(turbolynx_core, m) {
    m.doc() = "TurboLynx — High-performance graph database Python API";

    // Register exception types (must come before classes that throw them)
    turbolynx::TurboLynxPyConnection::RegisterExceptions(m);

    // Register classes
    turbolynx::TurboLynxPyResult::Initialize(m);
    turbolynx::TurboLynxPyPreparedStatement::Initialize(m);
    turbolynx::TurboLynxPyConnection::Initialize(m);

    // Module-level functions (DuckDB-compatible pattern)
    m.def("connect", &turbolynx::TurboLynxPyConnection::Connect,
          "Connect to a TurboLynx database",
          py::arg("database"),
          py::arg("read_only") = false);

    // Module attributes
    m.attr("__version__") = turbolynx_get_version();
    m.attr("apilevel") = "2.0";
    m.attr("threadsafety") = 1;
    m.attr("paramstyle") = "named";
}

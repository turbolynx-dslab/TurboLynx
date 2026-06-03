#pragma once
//
// Oracle-side TurboLynx workspace: an empty-bootstrap temp directory
// plus a single live connection.  Each Catch2 TEST_CASE constructs
// one of these (and one Neo4jRunner) so the per-test state is
// isolated.

#include "helpers/query_runner.hpp"

#include <string>

namespace tl_fuzz_oracle {

class TurboLynxBackend {
public:
    TurboLynxBackend();
    ~TurboLynxBackend();

    TurboLynxBackend(const TurboLynxBackend&)            = delete;
    TurboLynxBackend& operator=(const TurboLynxBackend&) = delete;

    qtest::QueryResult run(const std::string& cypher);

    int64_t conn_id() const { return conn_id_; }

private:
    std::string path_;
    int64_t     conn_id_ = -1;
};

}  // namespace tl_fuzz_oracle

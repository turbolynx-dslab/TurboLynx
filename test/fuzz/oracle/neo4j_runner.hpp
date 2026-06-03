#pragma once
//
// Thin Neo4j adapter for the L3 / L4 oracle.
//
// Uses Neo4j's transactional HTTP endpoint (`/db/{db}/tx/commit`) over
// localhost so we don't need a persistent bolt driver dependency.
// libcurl is already a system package on the dev/CI hosts; the result
// JSON is parsed via the third_party/json header that ships with the
// TurboLynx tree.
//
// One Neo4jRunner serves the whole fuzz session: connection reuse is
// handled by libcurl's keep-alive.

#include "helpers/query_runner.hpp"

#include <string>

namespace tl_fuzz_oracle {

class Neo4jRunner {
public:
    // `uri` is the Neo4j HTTP base, e.g. "http://localhost:7474".
    // `database` is the target Neo4j database name (default "neo4j").
    explicit Neo4jRunner(const std::string& uri = "http://localhost:7474",
                         const std::string& database = "neo4j");
    ~Neo4jRunner();

    Neo4jRunner(const Neo4jRunner&)            = delete;
    Neo4jRunner& operator=(const Neo4jRunner&) = delete;

    // Run a Cypher query and return rows in the same shape the
    // TurboLynx side uses, so `tl_fuzz::compare()` consumes both.
    // Throws std::runtime_error on HTTP / Cypher error.
    qtest::QueryResult run(const std::string& cypher);

    // Wipe every node and edge — call between Catch2 test cases for
    // a clean state.
    void wipe();

private:
    std::string                          uri_;
    std::string                          database_;
    void*                                curl_handle_ = nullptr;  // CURL*
};

}  // namespace tl_fuzz_oracle

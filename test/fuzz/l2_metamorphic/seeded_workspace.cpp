#include "seeded_workspace.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

namespace tl_fuzz_l2 {

namespace {

void unlink_recursive(const std::filesystem::path& p) noexcept {
    std::error_code ec;
    std::filesystem::remove_all(p, ec);
}

void drain_resultset(turbolynx_resultset_wrapper* rw) {
    if (!rw) return;
    while (turbolynx_fetch_next(rw) != TURBOLYNX_END_OF_RESULT) {
        // no-op; we just want to exercise materialisation
    }
}

}  // namespace

SeededWorkspace::SeededWorkspace() {
    char tmpl[] = "/tmp/tl_fuzz_l2_XXXXXX";
    char* d = mkdtemp(tmpl);
    if (!d) {
        throw std::runtime_error(std::string("mkdtemp failed: ") +
                                 std::strerror(errno));
    }
    path_ = d;
    conn_id_ = turbolynx_connect(path_.c_str());
    if (conn_id_ < 0) {
        unlink_recursive(path_);
        throw std::runtime_error("turbolynx_connect failed for " + path_);
    }

    // Seed graph.  Order matters: nodes before edges.
    static const char* kSeed[] = {
        // Persons
        "CREATE (n:Person {id: 1, name: 'Alice',   age: 30})",
        "CREATE (n:Person {id: 2, name: 'Bob',     age: 25})",
        "CREATE (n:Person {id: 3, name: 'Carol',   age: 40})",
        "CREATE (n:Person {id: 4, name: 'Dave',    age: 35})",
        "CREATE (n:Person {id: 5, name: 'Erin',    age: 22})",
        // Movies
        "CREATE (m:Movie  {id: 101, title: 'Alpha',   year: 2001})",
        "CREATE (m:Movie  {id: 102, title: 'Beta',    year: 2002})",
        "CREATE (m:Movie  {id: 103, title: 'Gamma',   year: 2003})",
        // KNOWS ring 1→2→3→4→5→1
        "MATCH (a:Person {id:1}), (b:Person {id:2}) CREATE (a)-[:KNOWS]->(b)",
        "MATCH (a:Person {id:2}), (b:Person {id:3}) CREATE (a)-[:KNOWS]->(b)",
        "MATCH (a:Person {id:3}), (b:Person {id:4}) CREATE (a)-[:KNOWS]->(b)",
        "MATCH (a:Person {id:4}), (b:Person {id:5}) CREATE (a)-[:KNOWS]->(b)",
        "MATCH (a:Person {id:5}), (b:Person {id:1}) CREATE (a)-[:KNOWS]->(b)",
        // ACTED_IN
        "MATCH (p:Person {id:1}), (m:Movie {id:101}) CREATE (p)-[:ACTED_IN]->(m)",
        "MATCH (p:Person {id:2}), (m:Movie {id:101}) CREATE (p)-[:ACTED_IN]->(m)",
        "MATCH (p:Person {id:3}), (m:Movie {id:102}) CREATE (p)-[:ACTED_IN]->(m)",
        "MATCH (p:Person {id:4}), (m:Movie {id:103}) CREATE (p)-[:ACTED_IN]->(m)",
        // DIRECTED
        "MATCH (p:Person {id:5}), (m:Movie {id:101}) CREATE (p)-[:DIRECTED]->(m)",
    };

    try {
        for (const char* q : kSeed) {
            exec_or_throw(q);
        }
    } catch (...) {
        turbolynx_disconnect(conn_id_);
        unlink_recursive(path_);
        throw;
    }
}

SeededWorkspace::~SeededWorkspace() {
    if (conn_id_ >= 0) {
        turbolynx_disconnect(conn_id_);
    }
    unlink_recursive(path_);
}

void SeededWorkspace::exec_or_throw(const char* query) {
    auto* prep = turbolynx_prepare(conn_id_,
                                    const_cast<char*>(query));
    if (!prep) {
        char* errmsg = nullptr;
        turbolynx_get_last_error(&errmsg);
        std::string msg = errmsg ? errmsg : query;
        throw std::runtime_error("seed prepare failed: " + msg);
    }

    turbolynx_resultset_wrapper* rw = nullptr;
    turbolynx_num_rows total = turbolynx_execute(conn_id_, prep, &rw);
    if (total == TURBOLYNX_ERROR) {
        char* errmsg = nullptr;
        turbolynx_get_last_error(&errmsg);
        std::string msg = errmsg ? errmsg : query;
        turbolynx_close_prepared_statement(prep);
        throw std::runtime_error("seed execute failed: " + msg);
    }
    drain_resultset(rw);
    if (rw) turbolynx_close_resultset(rw);
    turbolynx_close_prepared_statement(prep);
}

SeededWorkspace& shared_workspace() {
    // Static-local construction is thread-safe (Magic Statics).  We
    // need exactly one workspace per binary invocation; the dtor
    // running at process exit cleans up the temp dir.
    static SeededWorkspace ws;
    return ws;
}

}  // namespace tl_fuzz_l2

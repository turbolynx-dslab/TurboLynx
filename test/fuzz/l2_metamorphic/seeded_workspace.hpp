#pragma once
//
// Empty-bootstrap workspace seeded with a small deterministic graph,
// shared across all L2 rewriter TEST_CASEs in a single binary
// invocation.  Created lazily on first use; torn down at process exit.
//
// Schema (kept tiny so divergences are easy to inspect):
//   (:Person {id, name, age})         5 nodes  id ∈ [1, 5]
//   (:Movie  {id, title, year})       3 nodes  id ∈ [101, 103]
//   (:Person)-[:KNOWS]->(:Person)     KNOWS ring 1→2→3→4→5→1
//   (:Person)-[:ACTED_IN]->(:Movie)   1→101, 2→101, 3→102, 4→103
//   (:Person)-[:DIRECTED]->(:Movie)   5→101

#include "main/capi/turbolynx.h"

#include <filesystem>
#include <string>

namespace tl_fuzz_l2 {

class SeededWorkspace {
public:
    SeededWorkspace();
    ~SeededWorkspace();

    SeededWorkspace(const SeededWorkspace&) = delete;
    SeededWorkspace& operator=(const SeededWorkspace&) = delete;

    int64_t conn_id() const { return conn_id_; }
    const std::string& path() const { return path_; }

private:
    void exec_or_throw(const char* query);

    std::string path_;
    int64_t     conn_id_ = -1;
};

// Process-wide singleton — Catch2 fixtures share it.
SeededWorkspace& shared_workspace();

}  // namespace tl_fuzz_l2

# TurboLynx — Execution Plan

## Current Status

Core build is stable. Catalog, Storage, Execution layers tested.
Build runs inside `turbograph-s62` Docker container.

LDBC SF1 + TPC-H SF1 + DBpedia bulkload E2E tests all passing.
Next: Milestone 10 — extract `BulkloadPipeline` from `tools/bulkload.cpp`.

## Completed Milestones

| # | Milestone | Status |
|---|-----------|--------|
| 1 | Remove Velox dependency | ✅ Done |
| 2 | Remove Boost dependency | ✅ Done |
| 3 | Remove Python3 dependency | ✅ Done |
| 4 | Bundle TBB / libnuma / hwloc | ✅ Done |
| 5 | Single-file block store (`store.db`) | ✅ Done |
| 6 | Test suite: catalog, storage, execution | ✅ Done |
| 7 | Remove libaio-dev system dependency (direct syscalls) | ✅ Done |
| 8 | Rename library: `libs62gdb.so` → `libturbolynx.so` | ✅ Done |
| 9 | E2E bulkload test suite (LDBC SF1, TPC-H SF1, DBpedia) | ✅ Done |

---

## Milestone 10 — Extract `BulkloadPipeline` from `tools/bulkload.cpp`

### Goal

`tools/bulkload.cpp` is currently a ~1500-line monolith where all loading logic lives
directly inside free functions at file scope. The goal is to move every piece of
business logic into a proper `BulkloadPipeline` class in `src/loader/`, leaving
`tools/bulkload.cpp` as a ~150-line CLI shim.

```
Before                              After
──────────────────────────────────  ────────────────────────────────────
tools/bulkload.cpp  ~1 500 lines    tools/bulkload.cpp  ~150 lines
                                    src/include/loader/
                                        bulkload_options.hpp   ← InputOptions
                                        bulkload_pipeline.hpp  ← BulkloadPipeline
                                    src/loader/
                                        bulkload_pipeline.cpp  ← all loader logic
```

No behavioral change — existing bulkload E2E tests pass unchanged.

---

### Target public API

```cpp
// src/include/loader/bulkload_options.hpp
namespace duckdb {

using Labels           = std::string;
using FilePath         = std::string;
using FileSize         = size_t;
using OptionalFileSize = std::optional<FileSize>;
using LabeledFile      = std::tuple<Labels, FilePath, OptionalFileSize>;

struct BulkloadOptions {
    std::vector<LabeledFile> vertex_files;
    std::vector<LabeledFile> edge_files;
    std::vector<LabeledFile> edge_files_backward;
    std::string output_dir;
    bool incremental        = false;
    bool standalone         = false;
    bool skip_histogram     = false;
    bool load_edge          = false;
    bool load_backward_edge = false;
};

} // namespace duckdb

// src/include/loader/bulkload_pipeline.hpp
namespace duckdb {

class BulkloadPipeline {
public:
    explicit BulkloadPipeline(BulkloadOptions opts);
    void Run();  // vertices → fwd edges → bwd edges → histogram → persist

private:
    void InitializeWorkspace();
    void LoadVertices();
    void LoadForwardEdges();
    void LoadBackwardEdges();
    void RunPostProcessing();

    BulkloadOptions opts_;
    // BulkloadContext and DuckDB instance owned here (not exposed)
};

} // namespace duckdb
```

After the refactor, `tools/bulkload.cpp` becomes:

```cpp
int main(int argc, char** argv) {
    SetupLogger();
    RegisterSignalHandler();
    duckdb::BulkloadOptions opts;
    ParseConfig(argc, argv, opts);
    duckdb::BulkloadPipeline(std::move(opts)).Run();
    return 0;
}
```

---

### 구현 순서

| 단계 | 내용 | 선행 | 상태 |
|------|------|------|------|
| **10a** | `src/include/loader/bulkload_options.hpp` 생성 — `InputOptions` → `BulkloadOptions`; `LabeledFile` typedef 이동; `src/loader/` 디렉토리 생성 | — | ⬜ |
| **10b** | `src/include/loader/bulkload_pipeline.hpp` + `src/loader/bulkload_pipeline.cpp` 생성 — `bulkload.cpp` free functions (~줄 90–1313) 을 `BulkloadPipeline` private 메서드로 이동; `BulkloadContext` 는 `.cpp` 내부 detail로 유지 | 10a | ⬜ |
| **10c** | `src/CMakeLists.txt` 에 `src/loader/bulkload_pipeline.cpp` 추가 → `libturbolynx.so` 포함; 빌드 확인 | 10b | ⬜ |
| **10d** | `tools/bulkload.cpp` 를 CLI shim으로 교체 — `ParseConfig()`, `RegisterSignalHandler()`, `main()` 만 남김; 빌드 + `ctest -L bulkload` 통과 확인 | 10c | ⬜ |
| **10e** | *(선택)* `InitializeDiskAio` → `src/common/` 로 이동해 `client.cpp` 와 중복 제거 | 10d | ⬜ |

### 주의사항

- `BulkloadContext` 는 `BulkloadPipeline` 의 내부 구현 세부사항. 헤더로 노출하지 않음.
- Signal handler 는 CLI 관심사 → `tools/bulkload.cpp` 에 유지.
- `bulkload.cpp` 상단의 `#include "nlohmann/json.hpp"` + `using json = ...` 는 미사용 — 이동 중 제거.
- 행동 변화 없음. 기존 `ctest -L bulkload` 가 그대로 통과해야 함.

---

## Notes

- Keep this file updated at milestone completion.
- One milestone at a time. Validate with `ctest` before closing a milestone.
- Build always in `turbograph-s62` container: `cd /turbograph-v3/build-lwtest && ninja`

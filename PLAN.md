# TurboLynx — Execution Plan

## Current Status

Core build is stable. All unit tests (catalog 51, storage 68, common 10) pass.
LDBC SF1 + TPC-H SF1 + DBpedia bulkload E2E tests all passing.

**M22 완료 ✅. 다음 Milestone TBD.**

---

## Completed Milestones

| # | Milestone | 핵심 변경 | Status |
|---|-----------|-----------|--------|
| 1 | Remove Velox dependency | Velox 빌드 제거, 대체 구현 인라인 | ✅ |
| 2 | Remove Boost dependency | Boost 헤더/라이브러리 전면 제거 | ✅ |
| 3 | Remove Python3 dependency | Python 바인딩 제거, 빌드 단순화 | ✅ |
| 4 | Bundle TBB / libnuma / hwloc | 시스템 의존성 → 소스 번들로 교체 | ✅ |
| 5 | Single-file block store (`store.db`) | 컬럼별 개별 파일 → `store.db` 단일 파일 + `base_offset_` region 분할 | ✅ |
| 6 | Test suite: catalog, storage, execution | `[catalog]` `[storage]` `[execution]` 단위 테스트 | ✅ |
| 7 | Remove libaio-dev system dependency | `libaio` → 직접 `io_submit` syscall | ✅ |
| 8 | Rename library | `libs62gdb.so` → `libturbolynx.so`, CAPI 정리 | ✅ |
| 9 | E2E bulkload test suite | LDBC SF1 / TPC-H SF1 / DBpedia 전체 검증 자동화 | ✅ |
| 10 | Extract `BulkloadPipeline` | `tools/bulkload.cpp` 모놀리식 → `BulkloadPipeline` 클래스 분리 | ✅ |
| 11 | Multi-client support (prototype) | `g_connections` 맵으로 세션 분리, `s62_connect` CAPI | ✅ |
| 12 | Bulkload performance optimization | 백그라운드 flush 스레드, ThrottleIfNeeded, fwd/bwd interleave, DBpedia OOM 수정 | ✅ |
| 13 | LDBC Query Test Suite | Q1-01~21 ✅, Q2-01~09 ✅ — is_reserved 버그 수정, DATE→ms 변환, 공유 Runner | ✅ |
| 14 | LightningClient Dead Code 제거 | MultiPut/Get/Update, ObjectLog, UndoLogDisk, MPK, 생성자 파라미터 제거 | ✅ |
| 15 | Catalog ChunkDefinitionID 복원 수정 | Serialize에 cdf_id 직접 기록 → Deserialize 파싱 오류(`stoull("0_0")→0`) 수정 | ✅ |
| 16 | Multi-Process Read/Write 지원 | `fcntl` F_WRLCK/F_RDLCK, `read_only_` CCM 플래그, `s62_connect_readonly()`, `s62_reopen()` CAPI | ✅ |
| 17 | Multi-Process 테스트 | `[storage][multiproc]` — fcntl lock 시맨틱 5개 + CCM 락 충돌 3개, 총 8 테스트 | ✅ |
| 18 | LightningClient → BufferPool 교체 | shm/300GB mmap 제거, malloc + Second-Chance Clock eviction, UnPinSegment 실제 활성화 | ✅ |
| 19 | Storage Dead Code 제거 | 항상 false인 validator, 미호출 함수 5개, exit(-1) → throw IOException 교체 | ✅ |
| 20 | Kuzu 제거 — TurboLynx Parser/Binder/Converter | Kuzu Parser·Binder 전면 교체, TurboLynx-native 4단계 파이프라인 구현, `optimizer/kuzu/` 삭제 | ✅ |
| 21 | `s62` legacy naming → `turbolynx` 전면 교체 | C API 함수·타입·파일명, 소켓 서버 클래스, enum 상수 전부 rename | ✅ |
| 22 | 단일 바이너리 통합 (`turbolynx import` + `turbolynx shell`) | `bulkload` 바이너리 제거, `client` → `turbolynx` 서브커맨드 구조로 통합 | ✅ |
| 23 | Shell subdirectory 리팩터링 | `tools/shell/` 분리, renderer(4종), dot commands(9개), executor callback 패턴 | ✅ |
| 24 | Shell UX — dot commands + output modes | 누락 dot commands 완성, 출력 모드 확장, 설정 파일 | 🔲 |
| 25 | Shell UX — 자동완성 + 문법 하이라이팅 | linenoise 탭 자동완성, ANSI 컬러 키워드 하이라이팅 | 🔲 |

---

## M24 — Shell UX: Dot Commands + Output Modes

**목표:** DuckDB CLI 수준의 일상 사용성 확보. 빠진 dot command와 출력 모드를 일괄 추가.

### M24-A: 누락 Dot Commands

| 커맨드 | 설명 | 구현 위치 |
|--------|------|-----------|
| `.once <file>` | 다음 결과 하나만 파일로 redirect (이후 stdout 복구) | `ShellState::output_once` 플래그 추가 |
| `.headers on\|off` | 결과 헤더 행 토글 (기본 on) | `ShellState::show_headers` → `RenderResults` 참조 |
| `.nullvalue <str>` | NULL 표시 문자열 설정 (기본 `""`) | `ShellState::null_value` → `CollectRows` 참조 |
| `.separator <col> [row]` | CSV/list 모드 컬럼·행 구분자 설정 | `ShellState::col_sep`, `row_sep` |
| `.maxrows <n>` | 출력 최대 행 수 제한 (0 = 무제한) | `ShellState::max_rows` → `CollectRows` 잘라내기 |
| `.width <n>` | 테이블 모드 최소 컬럼 폭 | `ShellState::min_col_width` |
| `.show` | 현재 모든 설정 일괄 출력 | ShellState 필드 전체 dump |
| `.prompt <str>` | REPL 프롬프트 문자열 변경 | `ShellState::prompt` |
| `.echo on\|off` | 실행 전 쿼리 echo 출력 | `ShellState::echo` |
| `.bail on\|off` | 에러 발생 시 즉시 중단 (비대화형 .read 시 유용) | `ShellState::bail` |
| `.shell <cmd>` / `.system <cmd>` | OS 명령 실행 (`system()`) | commands.cpp |
| `.print <text>` | 텍스트 출력 (스크립트용) | commands.cpp |
| `.log [file]` | 모든 출력을 파일에도 병렬 기록; 인수 없으면 중단 | `ShellState::log_file` |
| `.indexes` | 그래프 인덱스 정보 (향후 인덱스 구현 후 활성화, 현재 stub) | commands.cpp |

### M24-B: 출력 모드 확장 (renderer.cpp)

| 모드 | 설명 | 우선순위 |
|------|------|----------|
| `jsonlines` | 줄당 1 JSON 객체 (NDJSON) — 스트리밍 파이프라인 친화적 | 높음 |
| `box` | 유니코드 박스 문자 테두리 (`┌─┬─┐` 스타일) | 높음 |
| `line` | 컬럼명 = 값 한 줄씩 출력, 행 사이 빈 줄 | 중간 |
| `column` | 테두리 없는 공백 정렬 컬럼 | 중간 |
| `list` | 파이프(`\|`) 구분 (구분자 변경 가능) | 중간 |
| `tabs` | 탭 구분 (TSV) | 중간 |
| `html` | `<table>` HTML 출력 | 낮음 |
| `latex` | LaTeX `tabular` 환경 출력 | 낮음 |
| `insert` | `INSERT INTO label VALUES (...)` SQL 출력 | 낮음 |
| `trash` | 결과 버림 (벤치마크용) | 낮음 |

### M24-C: 시작 설정 파일 (`~/.turbolynxrc`)

- `RunShell()` 진입 시 `~/.turbolynxrc` 파일 존재하면 `.read`와 동일하게 실행
- 사용 예: `.mode box`, `.timer on`, `.nullvalue NULL` 등 초기 설정
- 파일 없으면 조용히 무시

### 파일 변경 범위

| 파일 | 변경 내용 |
|------|-----------|
| `tools/shell/include/commands.hpp` | `ShellState`에 `show_headers`, `null_value`, `col_sep`, `row_sep`, `max_rows`, `min_col_width`, `prompt`, `echo`, `bail`, `log_file`, `output_once` 필드 추가 |
| `tools/shell/commands.cpp` | 위 14개 dot command 구현 |
| `tools/shell/include/renderer.hpp` | `RenderResults` 시그니처에 `ShellState` 참조 추가 (또는 `RenderOptions` 구조체로 묶기) |
| `tools/shell/renderer.cpp` | 6개 신규 모드 + headers/nullvalue/separator/maxrows 반영 |
| `tools/shell/shell.cpp` | `.once` 처리 (렌더링 후 `output_file` 초기화), `echo` 처리, `bail` 처리, `~/.turbolynxrc` 로드 |

---

## M25 — Shell UX: 자동완성 + 문법 하이라이팅

**목표:** 탭 자동완성과 ANSI 컬러 하이라이팅으로 개발자 경험 완성.

### M25-A: 탭 자동완성 (linenoise completion API)

linenoise는 `linenoiseSetCompletionCallback()` + `linenoiseAddCompletion()` API 제공.

| 완성 대상 | 트리거 | 데이터 소스 |
|-----------|--------|-------------|
| 도트 커맨드 | `.` 입력 후 Tab | 하드코딩 커맨드 목록 |
| Cypher 키워드 | 단어 입력 중 Tab | 하드코딩 키워드 목록 (`MATCH`, `WHERE`, `RETURN`, ...) |
| 버텍스 레이블 | `(n:` 입력 후 Tab | 카탈로그 조회 (`GetVertexLabels()`) |
| 엣지 타입 | `[r:` 입력 후 Tab | 카탈로그 조회 (`GetEdgeTypes()`) |
| 프로퍼티 키 | `.` 이후 Tab | 카탈로그 조회 (`GetUniversalPropertyKeyNames()`) |

구현: `shell.cpp`에 `CompletionCallback(const char* buf, linenoiseCompletions* lc)` 함수 추가. 카탈로그는 `ShellState`에 캐싱 (최초 완성 요청 시 1회 로드).

### M25-B: ANSI 문법 하이라이팅

linenoise는 `linenoiseSetHintsCallback()` API로 hint 문자열(dim 색상) 표시 가능. 입력 라인 자체 컬러링은 linenoise 자체에는 없으므로, 출력 결과 하이라이팅(결과 테이블 헤더 컬러)부터 시작.

단계:
1. **결과 헤더 컬러** — 테이블/box 모드에서 헤더 행을 bold/cyan으로 출력 (`\033[1;36m`)
2. **에러 메시지 컬러** — `std::cerr` 출력을 red로 (`\033[1;31m`)
3. **입력 hint** — linenoise hints callback으로 키워드 다음 예상 토큰 hint 표시 (선택)
4. **전체 입력 라인 하이라이팅** — linenoise fork 또는 readline 전환 필요시 검토

### 구현 파일

| 파일 | 변경 내용 |
|------|-----------|
| `tools/shell/shell.cpp` | completion callback 등록, hint callback |
| `tools/shell/include/shell.hpp` | 필요 시 `ShellCompletionState` 구조체 |
| `tools/shell/renderer.cpp` | ANSI 코드 조건부 출력 (isatty 체크) |

---

## Known Technical Debt (미래 Milestone 후보)

### Persistence Tier (미구현 — 의도적)
- `storage_manager.cpp`: WAL, Checkpoint, Transaction Manager 전체 주석 처리 상태
- in-memory only 모드. 디스크 persistence 구현 시 일괄 해제

### 버그 (수정 필요)
| 파일 | 위치 | 내용 |
|------|------|------|
| `extent_iterator.cpp` | line 205 | `target_idx[j++] - target_idxs_offset` 인덱스 계산 오류 (`// TODO bug..`) |
| `adjlist_iterator.cpp` | line 99 | adjacency direction hard-coded `true` (forward/backward 미구분) |
| `adjlist_iterator.cpp` | line 342 | 양방향 BFS meeting point 레벨 체크 오류 |
| `histogram_generator.cpp` | line 215 | boundary 값 strictly ascending 보장 미흡 |

### 성능 최적화 (기능 정상, 느림)
- `histogram_generator.cpp`: 전체 컬럼/전체 행 스캔 — 샘플링 미구현
- `graph_storage_wrapper.cpp`: 쿼리 루프마다 catalog lookup 반복 (컴파일 타임으로 이동 필요)
- `graph_storage_wrapper.cpp`: 단일 레이블 제한 (`D_ASSERT(labels.size() == 1)`)
- `buffer_manager.cpp:314`: eviction queue housekeeping 없음 (장시간 실행 시 누적)

---

## Notes

- Build: `cd /turbograph-v3/build-lwtest && ninja`
- Unit tests: `./test/unittest "[catalog]"` / `"[storage]"` / `"[common]"` (각각 별도 실행)
- E2E bulkload: `ctest --output-on-failure -R "bulkload_ldbc_sf1|bulkload_tpch_sf1"`
- DBpedia (수 시간): `ctest --output-on-failure -R "bulkload_dbpedia"`
- **Never modify a test just because it fails. Fix the implementation.**

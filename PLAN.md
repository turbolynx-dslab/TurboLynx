# TPC-H SF1 Performance Recovery Plan

## Context / Root cause

The TPC-H SF1 join queries regressed 2–20× vs the paper-submission baseline
(Nov 2025). The regression is **not a single bad commit** — it accumulated
across the CRUD / multi-partition (M27/M28/M30) development period. Two patterns:

1. **Leftover debug instrumentation** left in operator hot paths (per-chunk /
   per-row `GetValue()`+`ToString()` behind `spdlog::debug`, whose args are
   always evaluated). Biggest single contributor.
2. **Hand-tuned optimizations silently reverted** to "correct but slower"
   forms when delta/CRUD/multi-partition code landed:
   - `eid_to_bufptr_idx_map`: vector (direct index) → `unordered_map` (per-edge hash)
   - seek vid read: type-dispatched direct access → per-row `Value` boxing
   - `FillLHSOutput`: zero-copy `Slice` → per-chunk `Normalify` (full copy)
   - `getAdjListFromVid`: added per-edge delta hash lookups

Pure-scan queries (q1, q6) are unaffected (q1 even faster) → regression is
100% in the join operators (`AdjIdxJoin` / `IdSeek`).

There is **no CI perf-regression gate**, which is why this went unnoticed.

## Correctness gate (run after EVERY item)

`bash /tmp/correctness.sh` — mirrors CI `build-test.yml` mini-fixture lanes:
ctest + `[tpch]` + `[ldbc][count|traversal|is|ic|robustness|filter|func|crud]`
+ `[types]` + `[bulkload][dbpedia-mini]`. Workspaces preloaded at
`/tmp/{tpch,ldbc,types}-mini-ws`. Build configured with
`-DTURBOLYNX_{LDBC,TPCH}_FIXTURE_MINI=ON`.
(Fuzz oracle L3/L4 needs a Neo4j backend + sanitizer build — separate lane.)

## Performance tracking (execute-time ms, SF1, single-thread, --warmup -i5)

| Query | Nov (target) | Session start | Current | ratio→target |
|-------|-------------:|--------------:|--------:|-------------:|
| q5  |  86.71 | 3567 |  575 | 6.6× |
| q9  | 334.34 | 1856 | 1348 | 4.0× |
| q15 |  98.19 | 5891 |  603 | 6.1× |
| q18 | 424.25 | 3703 | 1175 | 2.8× |
| q21 | 662.15 | 3040 | 2289 | 3.5× |

(Geomean target = 172 ms. q1/q6 already at/under target. Run-to-run ±10%.)

## Profiling method (no perf/valgrind in env; gprof+sprof available)

`sprof` profiles the shared lib without rebuild:
```
rm -f /tmp/libturbolynx.so.profile
LD_PROFILE_OUTPUT=/tmp LD_PROFILE=libturbolynx.so ./build-release/tools/turbolynx -w /data/tpch/sf1 -f benchmark/tpch/sf1/q5.cql --warmup -i 5
sprof build-release/src/libturbolynx.so /tmp/libturbolynx.so.profile -p | ...
```
Caveats: self-time is biased toward call-count-heavy code (ORCA optimize), so
trust **call counts**, not %time. Subtract a `-i 1` (compile-only-ish) run from a
`-i 5 --warmup` run to isolate per-execute counts.

**Finding:** ~293K `Value::GetValue<uint64>` + ~150K `<uint32>` boxing calls
**per execute** of q5 — the `all_valid` direct-access fast paths are bypassed
because chained-join vectors carry NULLs (IdSeek marks unmatched rows invalid),
so mixed-validity branches still box per row.

## Items

### ✅ Done (all verified GREEN by /tmp/correctness.sh)
- [x] **1. Debug-probe guards** — wrap `[AIJ-OUT]`/IdSeek/AdjIdxJoin probe blocks in `spdlog::should_log(debug)`. *Biggest win (q5 3567→813).*
- [x] **2. Per-edge catalog lookup** absorbed into `AdjacencyListIterator::Initialize` (per-extent, single lookup reused by extent loaders).
- [x] **3. `eid_to_bufptr_idx_map` → vector** (restore direct seqno index; no per-edge hash). Multi-partition correctness verified (each iterator single-partition).
- [x] **4. `InitializeVertexIndexSeek`** per-row `GetValue` → type-dispatched direct access.
- [x] **5. `BuildSeekInput`** per-row `GetValue` → direct access + delta fast-path.
- [x] **6. `FillLHSOutput` Normalify removed** (restore zero-copy slice). *Big on wide queries (q15, q18).*
- [x] **7. `getAdjListFromVid` delta fast-path** (`NodeDeltasEmpty()` skips per-edge delta hash lookups on bulk-load-only graphs).

### ⏳ Pending
- [ ] **8. Mixed-validity Value boxing (sprof-confirmed, ~293K+150K/exec)** — the `all_valid` fast paths in `InitializeVertexIndexSeek` / `BuildSeekInput` / AdjIdxJoin `IterateSourceVids` are bypassed when vectors carry NULLs (common in join chains). Make the **mixed-validity** branch also use type-dispatched direct access (read value directly; check nullity via child+sel for DICTIONARY). Add a shared `bool TryReadVid(Vector&, idx_t, uint64_t&)` helper.
- [ ] **9. IdSeek `doSeekColumnar` read path** — Nov-diff for per-row copy/materialize (sprof: doSeekColumnar ~99K calls/run).
- [ ] **10. Per-chunk setup overhead** — q2/q11/q20 are 13–18× on *small* data ⇒ fixed per-chunk/per-operator cost dominates. Profile `InitializeInfosForProcessing`, `initializeSeek` scratch rebuild, `GetOperatorState`.
- [ ] **11. ~~Chunk granularity~~** — checked: `FillLHSOutput`/slice is only ~1% of AdjIdxJoin (negligible). Cost is the per-edge `IterateSourceVids` loop.
- [ ] **12. Sweep remaining hot functions** Nov-vs-now for reverted efficient patterns (sprof-guided by call count).

### Notes from bisection (ruled OUT as the dominant cost)
- `getAdjListFromVid` body (delta/bounds/in-memory wrapper) — Nov-minimal bisect showed no change.
- `getAdjListPtr` map→vector — restored (correct, your optimization) but small TPC-H delta.
- `FillLHSOutput` slice — ~1% of AdjIdxJoin.
- Remaining AdjIdxJoin cost is the inherent per-edge `IterateSourceVids` loop + the mixed-validity boxing (item 8).

### 🔧 Follow-ups
- [ ] Add CI bench-regression gate (sf0.01 geomean threshold).
- [ ] Remove temporary `[OP-PROFILE]` accumulator in `query_profiler.cpp` before final commit (currently env/`--profile`-gated).

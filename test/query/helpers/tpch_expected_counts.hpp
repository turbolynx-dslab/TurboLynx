// TPC-H expected per-query row counts and vertex cardinalities.
//
// Two fixture sizes are supported by the same test sources:
//
//   * SF1     — the legacy benchmark fixture, loaded externally via
//               `scripts/load-tpch.sh`. Default selection. Counts are
//               the originals previously hard-coded in
//               `test_tpch_correctness.cpp` ("DuckDB reference CSVs").
//   * SF0.01  — the committed mini fixture under `test/data/tpch-mini/`,
//               loaded via `scripts/load-tpch-mini.sh` and used by CI.
//               Selected by `cmake -DTURBOLYNX_TPCH_FIXTURE_MINI=ON`.
//               Counts measured on the committed fixture against the
//               engine that introduced this fixture (consistent for
//               regression detection — when individual queries are
//               cross-validated with DuckDB, swap the literal here).

#pragma once
#include <cstdint>

namespace tpch {

#ifdef TURBOLYNX_TPCH_FIXTURE_MINI
// SF0.01 mini fixture (test/data/tpch-mini/).
// Q1, Q3, Q5, Q6, Q7, Q9, Q10, Q11, Q14, Q15, Q19 currently SIGSEGV
// inside the engine on this fixture (issue #69) and are wrapped with
// `TPCH_TEST_BROKEN_MINI` in the test file — no expected count needed.
inline constexpr int64_t Q2  = 5;
inline constexpr int64_t Q4  = 5;
inline constexpr int64_t Q8  = 2;
inline constexpr int64_t Q12 = 2;
inline constexpr int64_t Q13 = 32;
inline constexpr int64_t Q16 = 315;
inline constexpr int64_t Q17 = 1;
inline constexpr int64_t Q18 = 0;  // predicate `sum(l_quantity) > 300` filters all rows at SF0.01
inline constexpr int64_t Q20 = 2;
inline constexpr int64_t Q21 = 1;
inline constexpr int64_t Q22 = 7;

// Vertex counts for the SF0.01 integrity probe.
inline constexpr int64_t LINEITEM_COUNT = 60175;
inline constexpr int64_t ORDERS_COUNT   = 15000;
inline constexpr int64_t CUSTOMER_COUNT = 1500;
inline constexpr int64_t SUPPLIER_COUNT = 100;
inline constexpr int64_t PART_COUNT     = 2000;
inline constexpr int64_t NATION_COUNT   = 25;
inline constexpr int64_t REGION_COUNT   = 5;
#else
// SF1 (full benchmark) — DuckDB-reference verified.
inline constexpr int64_t Q1  = 4;
inline constexpr int64_t Q2  = 100;
inline constexpr int64_t Q3  = 10;
inline constexpr int64_t Q4  = 5;
inline constexpr int64_t Q5  = 5;
inline constexpr int64_t Q6  = 1;
inline constexpr int64_t Q7  = 4;
inline constexpr int64_t Q8  = 2;
inline constexpr int64_t Q9  = 175;
inline constexpr int64_t Q10 = 20;
inline constexpr int64_t Q11 = 925;
inline constexpr int64_t Q12 = 2;
inline constexpr int64_t Q13 = 42;
inline constexpr int64_t Q14 = 1;
inline constexpr int64_t Q15 = 1;
inline constexpr int64_t Q16 = 18354;
inline constexpr int64_t Q17 = 1;
inline constexpr int64_t Q18 = 9;
inline constexpr int64_t Q19 = 1;
inline constexpr int64_t Q20 = 165;
inline constexpr int64_t Q21 = 100;
inline constexpr int64_t Q22 = 7;

// Vertex counts for the SF1 integrity probe.
inline constexpr int64_t LINEITEM_COUNT = 6001215;
inline constexpr int64_t ORDERS_COUNT   = 1500000;
inline constexpr int64_t CUSTOMER_COUNT = 150000;
inline constexpr int64_t SUPPLIER_COUNT = 10000;
inline constexpr int64_t PART_COUNT     = 200000;
inline constexpr int64_t NATION_COUNT   = 25;
inline constexpr int64_t REGION_COUNT   = 5;
#endif

}  // namespace tpch

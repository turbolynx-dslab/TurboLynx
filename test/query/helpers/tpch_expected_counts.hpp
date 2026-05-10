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

// ---- Per-query expected result rows (DuckDB-verified on SF0.01) ----
//
// Generated via DuckDB v1.5.2 against the same scale-factor data:
//   duckdb -c "INSTALL tpch; LOAD tpch; CALL dbgen(sf=0.01); <SQL>"
// dbgen at SF=0.01 is deterministic and produces byte-for-byte identical
// rows to test/data/tpch-mini/*.tbl (verified by spot-checking lineitem
// rows 1, 30000, 60000), so DuckDB's output is a valid oracle.
//
// DECIMAL columns are encoded as fixed-point strings ("82845.34"); the
// QueryRunner formats DECIMAL results via "%.<scale>f" so the comparison
// is exact.

struct Q22Row {
    const char* cntrycode;     // varchar (substring of phone)
    int64_t     numcust;       // bigint (count(*))
    const char* totacctbal;    // decimal(38,2) — formatted "%.2f"
};
inline constexpr Q22Row Q22_ROWS[] = {
    {"11", 11, "82845.34"},
    {"12",  9, "64585.54"},
    {"14",  7, "51640.71"},
    {"16",  4, "24532.04"},
    {"27",  9, "70735.49"},
    {"29", 12, "93510.05"},
    {"32", 10, "66977.84"},
};

// Q17 — single-row scalar: avg yearly extended price for `Brand#15` /
// `LG CASE` parts, 0.2-of-avg threshold. The expression `sum(...)/7.0`
// promotes the result to DOUBLE, which the runner formats as `"%f"`
// (six fractional digits). DuckDB rounds the same value to 4229.144286.
inline constexpr const char* Q17_AVG_YEARLY_STR = "4229.144286";

// Q21 — single-row supplier ranking: SAUDI ARABIA suppliers with the
// most "wait" orders (l_receiptdate > l_commitdate) where they were
// the only late party for the order.
struct Q21Row {
    const char* s_name;
    int64_t     numwait;
};
inline constexpr Q21Row Q21_ROWS[] = {
    {"Supplier#000000074", 9},
};

// Q12 — late-shipped order priority breakdown for ship modes
// `REG AIR` and `FOB` in calendar 1997.
struct Q12Row {
    const char* l_shipmode;
    int64_t     high_line_count;
    int64_t     low_line_count;
};
inline constexpr Q12Row Q12_ROWS[] = {
    {"FOB",     49,  93},
    {"REG AIR", 72, 106},
};

// Q2 — top suppliers offering minimum-cost AMERICA parts of size 43
// ending in "COPPER". 5-row top-K. s_comment (varchar) uses startswith
// semantics for the long comment field; s_address checked exactly
// (TPC-H load preserves trailing whitespace where present in .tbl).
struct Q2Row {
    const char* s_acctbal;     // decimal(15,2) → "%.2f"
    const char* s_name;
    const char* n_name;
    int64_t     p_partkey;
    const char* p_mfgr;
    const char* s_address;
    const char* s_phone;
    const char* s_comment_prefix;  // startswith
};
inline constexpr Q2Row Q2_ROWS[] = {
    {"9107.22", "Supplier#000000013", "CANADA",        1374, "Manufacturer#1",
     "kgTZjbt4CAa4c3SlirlBLqIL41YbCj",        "13-727-620-7813",
     "against the quickly ironic packages."},
    {"9107.22", "Supplier#000000013", "CANADA",        1495, "Manufacturer#5",
     "kgTZjbt4CAa4c3SlirlBLqIL41YbCj",        "13-727-620-7813",
     "against the quickly ironic packages."},
    {"7162.15", "Supplier#000000055", "UNITED STATES", 1437, "Manufacturer#4",
     "dAN28JcaMkXMkIUYU7H",                   "34-876-912-6007",
     "al requests after the blithely"},
    {"4746.66", "Supplier#000000087", "UNITED STATES", 1114, "Manufacturer#4",
     "5ovT6anHSsD1TyApXOBEU",                 "34-860-229-1674",
     " beans are silently idle requests."},
    {"3580.35", "Supplier#000000046", "UNITED STATES",  487, "Manufacturer#5",
     "N,6964Lnc2fNgMZV1VJV9ye9PtkE7z1nYOHRB", "34-748-308-3215",
     "d somas use around the furious"},
};

// Q4 — order priority counts for orders made in Q1 1994 with at least
// one late-shipped lineitem.
struct Q4Row {
    const char* o_orderpriority;
    int64_t     order_count;
};
inline constexpr Q4Row Q4_ROWS[] = {
    {"1-URGENT",        115},
    {"2-HIGH",          109},
    {"3-MEDIUM",        105},
    {"4-NOT SPECIFIED", 112},
    {"5-LOW",            94},
};

// Q8 — strengthening deferred (issue #97: mkt_share returns int64(0)
// instead of the DuckDB-verified double 0.041475 / 0.0). The CASE
// expression promotes integer literal 0 over DOUBLE volume, truncating
// every contribution to 0. Re-enable Q8_ROWS comparison once #97 ships.

// Q20 — BRAZIL suppliers shipping "smoke%" parts in 1997 in
// quantities exceeding 0.5× the lineitem total. 2 rows.
struct Q20Row {
    const char* s_name;
    const char* s_address;
};
inline constexpr Q20Row Q20_ROWS[] = {
    {"Supplier#000000021", "TZoQwNFFO i,baXpbpin02,hvuhE,GRVIKm "},
    {"Supplier#000000092", "EWS4tXaiXFFFS7Y,T G"},
};
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

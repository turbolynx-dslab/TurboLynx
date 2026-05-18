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
// Q1/Q3/Q5/Q6/Q7/Q9/Q10/Q11/Q14/Q15/Q19 originally SIGSEGV'd (#69)
// from BindDecimalSum / BindDecimalRoundPrecision crashes when the
// converter passed ANY-typed expressions into DuckDB's DECIMAL bind
// functions; resolved by deriving the LogicalType from the ORCA
// scalar / schema colref instead of the binder's stale data_type.
inline constexpr int64_t Q1  = 4;
inline constexpr int64_t Q2  = 5;
inline constexpr int64_t Q3  = 10;
inline constexpr int64_t Q4  = 5;
inline constexpr int64_t Q5  = 5;
inline constexpr int64_t Q6  = 1;
inline constexpr int64_t Q7  = 4;
inline constexpr int64_t Q8  = 2;
inline constexpr int64_t Q9  = 175;
inline constexpr int64_t Q10 = 20;
inline constexpr int64_t Q11 = 357;
inline constexpr int64_t Q12 = 2;
inline constexpr int64_t Q13 = 32;
inline constexpr int64_t Q14 = 1;
// DuckDB SF0.01 oracle: 17.34314126450495.
inline constexpr const char* Q14_PROMO_REVENUE_STR = "17.343141";
inline constexpr int64_t Q15 = 1;
inline constexpr int64_t Q16 = 315;
inline constexpr int64_t Q17 = 1;
inline constexpr int64_t Q18 = 12;  // sf0.01/q18.cql uses `> 270` (mini max sum is 305; SF1 uses `> 315`)
inline constexpr int64_t Q19 = 1;   // single row, NULL revenue (no parts match the brand+container+size filter on mini)
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

// Q13 — customer order-count distribution, filtering out orders whose
// O_COMMENT matches `.*express.*deposits.*`. 32-bucket histogram
// ordered by custdist DESC, c_count DESC. The c_count=0 / custdist=500
// row is the LEFT OUTER MATCH wing (customers with no orders).
struct Q13Row {
    int64_t c_count;
    int64_t custdist;
};
inline constexpr Q13Row Q13_ROWS[] = {
    { 0, 500}, {11,  74}, { 8,  68}, {10,  66}, {12,  58}, { 9,  58},
    {14,  57}, {20,  55}, {13,  49}, {18,  47}, {16,  46}, {15,  45},
    {21,  44}, { 7,  41}, {17,  40}, {24,  34}, {22,  34}, { 6,  33},
    {19,  29}, {23,  24}, {25,  20}, {26,  16}, { 5,  16}, {27,  15},
    {28,   7}, { 4,   6}, {32,   4}, {30,   4}, {29,   4}, {31,   2},
    { 3,   2}, { 2,   2},
};

// Q16 — supplier-cardinality breakdown by (brand, type, size) tuple
// excluding `Brand#51`, `PROMO PLATED%`, complaint-flagged suppliers.
// 315 rows: top 4 distinct rows have supplier_cnt=8, the remaining
// 311 all have supplier_cnt=4 (= partsupp wires each part to exactly
// 4 distinct suppliers in TPC-H, modulo the rare 8-supplier overlaps).
// Encoded as top-K + first/last cnt=4 row + the
// `supplier_cnt + ordering` invariant rather than all 315 rows.
inline constexpr int64_t Q16_NUM_ROWS = 315;
struct Q16Row {
    const char* p_brand;
    const char* p_type;
    int64_t     p_size;
    int64_t     supplier_cnt;
};
inline constexpr Q16Row Q16_TOP4_ROWS[] = {
    {"Brand#12", "STANDARD BRUSHED STEEL",   40, 8},
    {"Brand#21", "ECONOMY POLISHED STEEL",   44, 8},
    {"Brand#35", "SMALL POLISHED COPPER",    14, 8},
    {"Brand#55", "STANDARD ANODIZED STEEL",  42, 8},
};
// First row of the long supplier_cnt=4 tail (= row index 4 in the
// full result). Brand#11 sorts first among non-Brand#51 brands.
inline constexpr Q16Row Q16_FIRST_TAIL_ROW =
    {"Brand#11", "MEDIUM ANODIZED BRASS", 45, 4};
inline constexpr Q16Row Q16_LAST_ROW =
    {"Brand#55", "STANDARD PLATED TIN",   44, 4};

// Q18 — large-order customer detail. Mini fixture caps sum_lquantity
// at 305 so the SF1 `> 315` threshold returns 0 rows; sf0.01/q18.cql
// uses `> 270` instead and yields 12 rows. O_ORDERDATE comes back as
// int64 epoch ms (TurboLynx C API multiplies date.days by 86_400_000);
// O_TOTALPRICE and SUM(L_QUANTITY) are DECIMAL → "%.2f" string.
// (L_QUANTITY's .tbl header was previously declared `:INT` against
// the TPC-H spec's DECIMAL(15,2) — fixed in the same change as this
// row encoding so SUM(L_QUANTITY) returns DECIMAL like spec mandates.)
struct Q18Row {
    const char* c_name;
    int64_t     c_custkey;
    int64_t     o_orderkey;
    int64_t     o_orderdate_ms;
    const char* o_totalprice;
    const char* sum_l_quantity;
};
inline constexpr Q18Row Q18_ROWS[] = {
    {"Customer#000000676",  676, 52965, 843350400000LL, "466001.28", "271.00"},
    {"Customer#000000667",  667, 29158, 814233600000LL, "439687.23", "305.00"},
    {"Customer#000001013", 1013, 44707, 871516800000LL, "431771.98", "279.00"},
    {"Customer#000000953",  953, 59106, 846115200000LL, "430619.75", "276.00"},
    {"Customer#000000178",  178,  6882, 860544000000LL, "422359.65", "303.00"},
    {"Customer#000001279", 1279, 39620, 781315200000LL, "406938.36", "272.00"},
    {"Customer#000000107",  107,  8516, 828921600000LL, "377636.63", "271.00"},
    {"Customer#000001360", 1360, 23943, 803952000000LL, "372934.56", "271.00"},
    {"Customer#000000538",  538, 55234, 743904000000LL, "367176.04", "280.00"},
    {"Customer#000001226", 1226, 36673, 747964800000LL, "364437.75", "279.00"},
    {"Customer#000000331",  331, 38405, 742780800000LL, "359455.08", "271.00"},
    {"Customer#000000136",  136, 19968, 881452800000LL, "359373.75", "273.00"},
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

// Q8 — ETHIOPIA market-share within AFRICA region for the
// "ECONOMY BRUSHED BRASS" part type, by ship year. 2 rows.
struct Q8Row {
    int64_t     o_year;
    const char* mkt_share;   // double, formatted "%f"
};
inline constexpr Q8Row Q8_ROWS[] = {
    {1995, "0.041475"},
    {1996, "0.000000"},
};

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

// Q1 — full lineitem aggregation by (returnflag, linestatus). Mix of
// DECIMAL sums (exact "%.Nf" strings) and DOUBLE averages (formatted
// "%f" → 6 fractional digits). DuckDB SF0.01 reference values.
struct Q1Row {
    const char* ret_flag;
    const char* line_stat;
    const char* sum_qty;           // DECIMAL(38,2)
    const char* sum_base_price;    // DECIMAL(38,2)
    const char* sum_disc_price;    // DECIMAL(38,4)
    const char* sum_charge;        // DECIMAL(38,6)
    const char* avg_qty;           // DOUBLE
    const char* avg_price;         // DOUBLE
    const char* avg_disc;          // DOUBLE
    int64_t     count_order;
};
inline constexpr Q1Row Q1_ROWS[] = {
    {"A","F", "380456.00",  "532348211.65",  "505822441.4861",  "526165934.000839",  "25.575155", "35785.709307", "0.050081", 14876},
    {"N","F",   "8971.00",   "12384801.37",   "11798257.2080",   "12282485.056933",  "25.778736", "35588.509684", "0.047759",   348},
    {"N","O", "738774.00", "1035875694.71",  "984393404.8664", "1023859818.407892",  "25.451270", "35686.626062", "0.049922", 29027},
    {"R","F", "381449.00",  "534594445.35",  "507996454.4067",  "528524219.358903",  "25.597168", "35874.006533", "0.049828", 14902},
};

// Q3 — top-10 in-flight orders for FURNITURE customers (revenue DESC,
// O_ORDERDATE ASC). REVENUE is DECIMAL(38,4); O_ORDERDATE is rendered
// as the C-API DATE-epoch-ms int64.
struct Q3Row {
    int64_t     o_orderkey;
    const char* revenue;        // DECIMAL(38,4) → "%.4f"
    int64_t     o_orderdate_ms;
    int64_t     o_shippriority;
};
inline constexpr Q3Row Q3_ROWS[] = {
    {47525, "243081.6966", 794275200000LL, 0},  // 1995-03-04
    {12868, "221241.4305", 794102400000LL, 0},  // 1995-03-02
    {22849, "215389.6280", 792806400000LL, 0},  // 1995-02-15
    { 9221, "207109.5308", 789177600000LL, 0},  // 1995-01-04
    {37734, "204279.4455", 788486400000LL, 0},  // 1994-12-27
    {14503, "190618.5163", 792028800000LL, 0},  // 1995-02-06
    {21219, "190266.7609", 790300800000LL, 0},  // 1995-01-17
    {28773, "183596.8430", 792460800000LL, 0},  // 1995-02-11
    { 9537, "180368.6940", 792720000000LL, 0},  // 1995-02-14
    {49312, "175723.1224", 793411200000LL, 0},  // 1995-02-22
};

// Q5 — EUROPE-region 1994 revenue by nation. 5 rows DESC.
struct Q5Row {
    const char* n_name;
    const char* revenue;        // DECIMAL(38,4)
};
inline constexpr Q5Row Q5_ROWS[] = {
    {"ROMANIA",        "738458.3390"},
    {"RUSSIA",         "642220.3110"},
    {"GERMANY",        "540272.0412"},
    {"UNITED KINGDOM", "512853.7625"},
    {"FRANCE",         "132804.8366"},
};

// Q6 — single-row revenue scalar from a calendar-1994 LINEITEM
// scan with discount/quantity bounds.
inline constexpr const char* Q6_REVENUE_STR = "996212.3469";

// Q7 — shipping volume between IRAN and ETHIOPIA, by (supp_nation,
// cust_nation, year). 4 rows.
struct Q7Row {
    const char* supp_nation;
    const char* cust_nation;
    int64_t     l_year;
    const char* revenue;        // DECIMAL(38,4)
};
inline constexpr Q7Row Q7_ROWS[] = {
    {"ETHIOPIA", "IRAN",     1995, "347608.1349"},
    {"ETHIOPIA", "IRAN",     1996, "703045.9414"},
    {"IRAN",     "ETHIOPIA", 1995, "270996.1957"},
    {"IRAN",     "ETHIOPIA", 1996, "322515.0610"},
};

// Q9 — profit by supplier-nation × order-year for parts whose name
// contains "salmon". 175 rows. Pin the first 7 (ALGERIA) and the
// total count rather than the full set.
inline constexpr int64_t Q9_NUM_ROWS = 175;
struct Q9Row {
    const char* nation;
    int64_t     year;
    const char* amount;         // DECIMAL(38,4)
};
inline constexpr Q9Row Q9_FIRST_7_ROWS[] = {
    {"ALGERIA", 1998, "231358.5658"},
    {"ALGERIA", 1997, "203944.6086"},
    {"ALGERIA", 1996, "138857.8791"},
    {"ALGERIA", 1995,  "13736.3900"},
    {"ALGERIA", 1994,  "96759.1130"},
    {"ALGERIA", 1993,  "86062.2353"},
    {"ALGERIA", 1992, "253781.0598"},
};

// Q10 — top-20 customers by returned-order revenue in 1993 Q3.
struct Q10Row {
    int64_t     c_custkey;
    const char* c_name;
    const char* revenue;       // DECIMAL(38,4)
    const char* c_acctbal;     // DECIMAL(12,2)
    const char* n_name;
    const char* c_address;
    const char* c_phone;
    const char* c_comment_prefix;   // startswith match (long varchar)
};
inline constexpr Q10Row Q10_ROWS[] = {
    {1355, "Customer#000001355", "456831.6010", "2351.10", "SAUDI ARABIA",   "PZ UkSSIusgaGw66YgBJ lvJ2xBnCZTC7Ckq", "30-918-883-1662", "arefully even deposits"},
    { 109, "Customer#000000109", "392384.5988", "-716.10", "MOZAMBIQUE",     "dccG2qKoesLea36FGCOCgPVU M N4BZdmYO",  "26-992-422-8153", "osits; blithely special braids"},
    { 481, "Customer#000000481", "325194.7804", "7157.21", "VIETNAM",        "DWuQTiY,dAkeSm,uGj0dhMZKbt,WcAjD",     "31-363-392-6461", "accounts above the theodolites"},
    {1249, "Customer#000001249", "324966.3197",  "448.49", "GERMANY",        "vRxG1EdI6eWBpb3umXMBRKMv5bpb1svk2nU ej","17-866-269-1165", " accounts. special platelets"},
    { 967, "Customer#000000967", "320041.7205", "5710.41", "UNITED KINGDOM", "vgNqFVj84wrEByskKaj3YOeEu2nJLtAeAu",   "33-687-917-3598", "quickly even asymptotes"},
};

// Q11 — partkeys whose ROMANIA supplier-cost share exceeds 0.01% of
// the total ROMANIA cost. 357 rows DESC by value. Pin the top 5 and
// the total count.
inline constexpr int64_t Q11_NUM_ROWS = 357;
struct Q11Row {
    int64_t     partkey;
    const char* value;          // DECIMAL(38,2)
};
inline constexpr Q11Row Q11_TOP5_ROWS[] = {
    { 917, "12344895.22"},
    { 685, "11950940.60"},
    {1081, "10580686.59"},
    {1011,  "9608892.80"},
    { 623,  "9343081.80"},
};

// Q15 — supplier with the maximum 4-decimal-rounded revenue in a
// 3-month window. Single row.
struct Q15Row {
    int64_t     s_suppkey;
    const char* s_name;
    const char* s_address;
    const char* s_phone;
    const char* total_revenue;  // DECIMAL(38,4)
};
inline constexpr Q15Row Q15_ROW =
    {82, "Supplier#000000082", "5t7gqU1BlZWyZQoeF7X", "28-177-572-9691", "1488143.1723"};

// Q19 — revenue across three brand×container×size×shipmode buckets.
// All three are empty on the mini fixture (no PARTs match the brand /
// container / size predicates), so the SUM is NULL.
inline constexpr bool Q19_REVENUE_IS_NULL = true;

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

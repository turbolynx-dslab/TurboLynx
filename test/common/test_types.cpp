// =============================================================================
// [common] DataChunk & Value unit tests
// =============================================================================
// Tag: [common][types]
//
// Tests core data types (Value, DataChunk) that underpin all query execution.
// No database instance needed — pure type-system tests.
// =============================================================================

#include "catch.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/vector.hpp"
#include "common/types/value.hpp"
#include "common/typedef.hpp"

using namespace duckdb;

// ---------------------------------------------------------------------------
TEST_CASE("Value: basic scalar construction and equality", "[common][types]") {
    SECTION("INTEGER") {
        auto v = Value::INTEGER(42);
        REQUIRE(v == Value::INTEGER(42));
        REQUIRE_FALSE(v == Value::INTEGER(0));
        REQUIRE(v.type() == LogicalType::INTEGER);
    }
    SECTION("BIGINT") {
        auto v = Value::BIGINT(1234567890123LL);
        REQUIRE(v == Value::BIGINT(1234567890123LL));
        REQUIRE(v.type() == LogicalType::BIGINT);
    }
    SECTION("BOOLEAN") {
        REQUIRE(Value::BOOLEAN(true)  == Value::BOOLEAN(true));
        REQUIRE(Value::BOOLEAN(false) == Value::BOOLEAN(false));
        REQUIRE_FALSE(Value::BOOLEAN(true) == Value::BOOLEAN(false));
    }
    SECTION("NULL value") {
        Value null_val;
        REQUIRE(null_val.IsNull());
    }
}

TEST_CASE("Value: GetValue<T> round-trip", "[common][types]") {
    REQUIRE(Value::INTEGER(99).GetValue<int32_t>()  == 99);
    REQUIRE(Value::BIGINT(-1LL).GetValue<int64_t>() == -1LL);
    REQUIRE(Value::BOOLEAN(true).GetValue<bool>()   == true);
    REQUIRE(Value::FLOAT(3.14f).GetValue<float>()   == Approx(3.14f));
    REQUIRE(Value::DOUBLE(2.718).GetValue<double>() == Approx(2.718));
}

TEST_CASE("DataChunk: initialize and set/get values", "[common][types]") {
    DataChunk chunk;
    vector<LogicalType> types = {LogicalType::INTEGER, LogicalType::BIGINT};
    chunk.Initialize(types);
    chunk.SetCardinality(3);

    // Fill column 0 (INTEGER)
    chunk.SetValue(0, 0, Value::INTEGER(10));
    chunk.SetValue(0, 1, Value::INTEGER(20));
    chunk.SetValue(0, 2, Value::INTEGER(30));

    // Fill column 1 (BIGINT)
    chunk.SetValue(1, 0, Value::BIGINT(100));
    chunk.SetValue(1, 1, Value::BIGINT(200));
    chunk.SetValue(1, 2, Value::BIGINT(300));

    REQUIRE(chunk.size() == 3);
    REQUIRE(chunk.ColumnCount() == 2);

    REQUIRE(chunk.GetValue(0, 0) == Value::INTEGER(10));
    REQUIRE(chunk.GetValue(0, 1) == Value::INTEGER(20));
    REQUIRE(chunk.GetValue(0, 2) == Value::INTEGER(30));

    REQUIRE(chunk.GetValue(1, 0) == Value::BIGINT(100));
    REQUIRE(chunk.GetValue(1, 2) == Value::BIGINT(300));
}

TEST_CASE("DataChunk: zero-column initialize tracks cardinality only", "[common][types]") {
    // Regression: count(*) with no projected columns feeds through operators
    // whose output schema is empty. DataChunk must accept zero-column Initialize
    // and still track row counts via SetCardinality.
    DataChunk chunk;
    vector<LogicalType> empty_types;
    chunk.Initialize(empty_types);
    REQUIRE(chunk.ColumnCount() == 0);
    REQUIRE(chunk.size() == 0);

    chunk.SetCardinality(7);
    REQUIRE(chunk.size() == 7);

    chunk.Reset();
    REQUIRE(chunk.size() == 0);

    DataChunk chunk2;
    chunk2.InitializeEmpty(empty_types);
    REQUIRE(chunk2.ColumnCount() == 0);
}

TEST_CASE("DataChunk: reset clears cardinality", "[common][types]") {
    DataChunk chunk;
    chunk.Initialize({LogicalType::INTEGER});
    chunk.SetCardinality(5);
    REQUIRE(chunk.size() == 5);

    chunk.Reset();
    REQUIRE(chunk.size() == 0);
}

TEST_CASE("CpuTimer: elapsed time is non-negative and monotonic", "[common][timer]") {
    CpuTimer timer;
    // Burn a tiny bit of CPU
    volatile int64_t sum = 0;
    for (int i = 0; i < 10000; ++i) sum += i;
    (void)sum;

    auto e1 = timer.elapsed();
    REQUIRE(e1.wall >= 0);

    // Second reading should be >= first
    auto e2 = timer.elapsed();
    REQUIRE(e2.wall >= e1.wall);
}

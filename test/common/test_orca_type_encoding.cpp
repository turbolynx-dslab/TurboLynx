// =============================================================================
// [common][orca_type_encoding] round-trip unit tests
// =============================================================================
// Tag: [common][orca_type_encoding]
//
// Encode/decode LogicalType <-> (oid, type_mod) goes through three call sites
// (converter::GetTypeMod, the static OidToLogicalType in cypher2orca_scalar,
// Planner::pConvertTypeOidToLogicalType). The shared helper in
// common/orca_type_encoding.hpp is the single source of truth; these tests
// pin the round-trip behavior so any future encoder reform stays compatible.
//
// Issue #249 tracks the longer-term reform (boundary widening + sentinel
// bit); these tests document what the *current* encoding can represent.
// =============================================================================

#include "catch.hpp"

#include "catalog/catalog.hpp"
#include "common/orca_type_encoding.hpp"
#include "common/types.hpp"

using namespace duckdb;
using turbolynx::ComplexTypeRegistry;
using turbolynx::DecodeTypeMod;
using turbolynx::EncodeTypeMod;

namespace {

// Convenience: encode + decode through a freshly-allocated registry.
LogicalType RoundTrip(const LogicalType &input) {
    ComplexTypeRegistry registry;
    int32_t next_id = turbolynx::COMPLEX_TYPE_REGISTRY_MIN;
    int32_t mod = EncodeTypeMod(input, &registry, &next_id);
    uint32_t oid = (uint32_t)LOGICAL_TYPE_BASE_ID + (uint32_t)input.id();
    return DecodeTypeMod(oid, mod, &registry);
}

}  // namespace

TEST_CASE("orca_type_encoding: scalar round-trip", "[common][orca_type_encoding]") {
    for (auto id : {LogicalTypeId::BIGINT, LogicalTypeId::UBIGINT,
                    LogicalTypeId::INTEGER, LogicalTypeId::VARCHAR,
                    LogicalTypeId::BOOLEAN, LogicalTypeId::DOUBLE,
                    LogicalTypeId::ID}) {
        LogicalType t(id);
        auto rt = RoundTrip(t);
        CHECK(rt.id() == t.id());
    }
}

TEST_CASE("orca_type_encoding: DECIMAL preserves width and scale",
          "[common][orca_type_encoding]") {
    auto t = LogicalType::DECIMAL(18, 4);
    auto rt = RoundTrip(t);
    REQUIRE(rt.id() == LogicalTypeId::DECIMAL);
    CHECK(DecimalType::GetWidth(rt) == 18);
    CHECK(DecimalType::GetScale(rt) == 4);
}

TEST_CASE("orca_type_encoding: LIST of small-id scalar round-trips",
          "[common][orca_type_encoding]") {
    // Both BIGINT (id=14) and UBIGINT (id=31) are safely below the
    // historical 256-wraparound and the 10000 registry boundary.
    auto t1 = LogicalType::LIST(LogicalType::BIGINT);
    auto rt1 = RoundTrip(t1);
    REQUIRE(rt1.id() == LogicalTypeId::LIST);
    CHECK(ListType::GetChildType(rt1).id() == LogicalTypeId::BIGINT);

    auto t2 = LogicalType::LIST(LogicalType::UBIGINT);
    auto rt2 = RoundTrip(t2);
    REQUIRE(rt2.id() == LogicalTypeId::LIST);
    CHECK(ListType::GetChildType(rt2).id() == LogicalTypeId::UBIGINT);
}

TEST_CASE("orca_type_encoding: LIST(LIST(UBIGINT)) round-trips",
          "[common][orca_type_encoding]") {
    // PR-3c's working case: nodes(p) / relationships(p) emit this shape.
    // Encoded mod = LIST.id | (UBIGINT.id << 8) = 101 | 7936 = 8037, which
    // stays below the registry boundary.
    auto t = LogicalType::LIST(LogicalType::LIST(LogicalType::UBIGINT));
    auto rt = RoundTrip(t);
    REQUIRE(rt.id() == LogicalTypeId::LIST);
    auto &child = ListType::GetChildType(rt);
    REQUIRE(child.id() == LogicalTypeId::LIST);
    CHECK(ListType::GetChildType(child).id() == LogicalTypeId::UBIGINT);
}

TEST_CASE("orca_type_encoding: LIST(LIST(ID)) round-trips after boundary widening",
          "[common][orca_type_encoding]") {
    // Plain encoding now lives in [0, 0x40000000); the previous 10000
    // boundary collided with LIST(LIST(ID)) (encoded mod = 27749) and
    // sent it through the registry branch, which had no entry and
    // returned ANY. With the wider boundary the same modifier decodes
    // back to the original type.
    auto t = LogicalType::LIST(LogicalType::LIST(LogicalType::ID));
    auto rt = RoundTrip(t);
    REQUIRE(rt.id() == LogicalTypeId::LIST);
    auto &child = ListType::GetChildType(rt);
    REQUIRE(child.id() == LogicalTypeId::LIST);
    CHECK(ListType::GetChildType(child).id() == LogicalTypeId::ID);
}

TEST_CASE("orca_type_encoding: STRUCT round-trip via registry",
          "[common][orca_type_encoding]") {
    child_list_t<LogicalType> fields;
    fields.emplace_back("a", LogicalType::BIGINT);
    fields.emplace_back("b", LogicalType::VARCHAR);
    auto t = LogicalType::STRUCT(std::move(fields));
    auto rt = RoundTrip(t);
    REQUIRE(rt.id() == LogicalTypeId::STRUCT);
    CHECK(StructType::GetChildCount(rt) == 2);
}

TEST_CASE("orca_type_encoding: decode of unknown registry handle throws",
          "[common][orca_type_encoding]") {
    ComplexTypeRegistry empty;
    CHECK_THROWS(DecodeTypeMod(
        (uint32_t)LOGICAL_TYPE_BASE_ID + (uint32_t)LogicalTypeId::LIST,
        turbolynx::COMPLEX_TYPE_REGISTRY_MIN + 1, &empty));
}

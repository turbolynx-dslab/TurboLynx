#pragma once

#include "catalog/catalog.hpp"  // LOGICAL_TYPE_BASE_ID, NUM_MAX_LOGICAL_TYPES
#include "common/exception.hpp"
#include "common/types.hpp"

#include <cstdint>
#include <type_traits>
#include <unordered_map>

namespace turbolynx {

// Single source of truth for packing a duckdb::LogicalType into ORCA's
// (oid, type_mod) pair. ORCA's type metadata is a single INT modifier
// alongside the type OID, so anything that can't fit in that INT — STRUCT
// fields, deeply-nested LIST element types — lives in a side registry the
// converter writes and the planner reads.
//
// The three sites that previously open-coded this logic
// (Cypher2OrcaConverter::GetTypeMod, the static OidToLogicalType in
// cypher2orca_scalar.cpp, and Planner::pConvertTypeOidToLogicalType) now go
// through these helpers so encode and decode stay in lock-step.

using ComplexTypeRegistry = std::unordered_map<int32_t, duckdb::LogicalType>;

// type_mod is a signed 32-bit ORCA INT. We carve it into three regions:
//   * mod < 0                          — historical sentinel (-1 = default
//                                        LIST(UBIGINT)); left intact for
//                                        backward compatibility.
//   * 0 ≤ mod < COMPLEX_TYPE_REGISTRY_MIN
//                                      — bit-packed plain encoding.
//   * mod ≥ COMPLEX_TYPE_REGISTRY_MIN  — registry handle (bit 30 set).
//
// Plain encoding uses an 8-bit-per-level layout: for a LIST type, the low
// byte holds the child LogicalTypeId and the next byte starts the
// grand-child's recursive encoding. That gives us 30 usable bits / 8 bits
// per level → up to ~LIST(LIST(LIST(scalar))) before plain encoding
// overflows. Beyond that the encoder falls back to the registry. The
// previous boundary (10000) collided with the plain encoding of
// LIST(LIST(child)) whenever the inner child had a LogicalTypeId ≥ ~39,
// which silently aliased the type back to ANY (issue #249).
constexpr int32_t COMPLEX_TYPE_REGISTRY_MIN = 0x40000000;  // bit 30

inline duckdb::LogicalTypeId DecodeTypeId(uint32_t oid)
{
    using TidUnder = std::underlying_type_t<duckdb::LogicalTypeId>;
    return static_cast<duckdb::LogicalTypeId>(
        static_cast<TidUnder>((oid - LOGICAL_TYPE_BASE_ID) % NUM_MAX_LOGICAL_TYPES));
}

// Encode a LogicalType into ORCA's (oid, INT type_mod). The OID is
// LOGICAL_TYPE_BASE_ID + LogicalTypeId.id() at the call site; this helper
// only produces the INT modifier.
//
// `registry` and `next_complex_id` are required for any type that can't
// fit in the bit-packed layout (STRUCT fields, LIST(STRUCT), deeply-nested
// LIST whose packed encoding would overflow into the registry-handle
// region). Pass nullptr only when the caller already knows the type is
// bit-packable.
inline int32_t EncodeTypeMod(const duckdb::LogicalType &type,
                              ComplexTypeRegistry *registry,
                              int32_t *next_complex_id)
{
    using duckdb::LogicalTypeId;
    auto stash_registry = [&]() -> int32_t {
        if (!registry || !next_complex_id) {
            throw duckdb::InternalException(
                "EncodeTypeMod: complex/deeply-nested type requires registry");
        }
        int32_t id = (*next_complex_id)++;
        (*registry)[id] = type;
        return id;
    };

    if (type.id() == LogicalTypeId::DECIMAL) {
        uint16_t w = (uint16_t)duckdb::DecimalType::GetWidth(type);
        uint16_t s = (uint16_t)duckdb::DecimalType::GetScale(type);
        return ((int32_t)w << 8) | s;
    }
    if (type.id() == LogicalTypeId::LIST) {
        auto &child = duckdb::ListType::GetChildType(type);
        if (child.id() == LogicalTypeId::LIST) {
            int32_t cmod = EncodeTypeMod(child, registry, next_complex_id);
            // Child itself ended up as a registry handle — promote the
            // whole type. Same idea if the packed result would overflow
            // into the registry-handle region.
            if (cmod >= COMPLEX_TYPE_REGISTRY_MIN) return stash_registry();
            int32_t packed = (int32_t)LogicalTypeId::LIST | (cmod << 8);
            if (packed >= COMPLEX_TYPE_REGISTRY_MIN) return stash_registry();
            return packed;
        }
        if (child.id() == LogicalTypeId::STRUCT) return stash_registry();
        return (int32_t)child.id();
    }
    if (type.id() == LogicalTypeId::STRUCT) return stash_registry();
    return 0;
}

// Decode (oid, INT type_mod) back into a LogicalType. `registry` is the
// converter-populated map. Two-mode behavior:
//   * With a registry: handles in the [COMPLEX_TYPE_REGISTRY_MIN, …) region
//     are looked up and an entry miss throws — silent ANY-on-miss is the
//     class of footgun this whole refactor is meant to eliminate.
//   * Without a registry: callers (notably the file-scope helper in
//     cypher2orca_scalar) historically plain-decoded the modifier
//     unconditionally; preserve that so they don't suddenly start seeing
//     ANY/throw for registry-encoded LIST(STRUCT).
inline duckdb::LogicalType DecodeTypeMod(uint32_t oid, int32_t type_mod,
                                          const ComplexTypeRegistry *registry)
{
    using duckdb::LogicalTypeId;
    auto tid = DecodeTypeId(oid);
    if (tid == LogicalTypeId::DECIMAL) {
        if (type_mod == 0 || type_mod == -1) {
            return duckdb::LogicalType::DECIMAL(12, 2);
        }
        uint8_t w = (uint8_t)(type_mod >> 8);
        uint8_t s = (uint8_t)(type_mod & 0xFF);
        return duckdb::LogicalType::DECIMAL(w, s);
    }
    if (tid == LogicalTypeId::LIST) {
        if (type_mod == -1) {
            return duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT);
        }
        if (registry && type_mod >= COMPLEX_TYPE_REGISTRY_MIN) {
            auto it = registry->find(type_mod);
            if (it != registry->end()) return it->second;
            throw duckdb::InternalException(
                "DecodeTypeMod: LIST registry handle " +
                std::to_string(type_mod) + " has no entry");
        }
        uint32_t cooid = (uint32_t)(type_mod & 0xFF) + LOGICAL_TYPE_BASE_ID;
        int32_t cmod = (type_mod >> 8);
        return duckdb::LogicalType::LIST(DecodeTypeMod(cooid, cmod, registry));
    }
    if (tid == LogicalTypeId::PATH) {
        return duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT);
    }
    if (tid == LogicalTypeId::STRUCT && registry &&
        type_mod >= COMPLEX_TYPE_REGISTRY_MIN) {
        auto it = registry->find(type_mod);
        if (it != registry->end()) return it->second;
        throw duckdb::InternalException(
            "DecodeTypeMod: STRUCT registry handle " +
            std::to_string(type_mod) + " has no entry");
    }
    return duckdb::LogicalType(tid);
}

}  // namespace turbolynx

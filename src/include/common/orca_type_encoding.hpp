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

// Modifiers in [COMPLEX_TYPE_REGISTRY_MIN, ...) are treated as registry
// handles rather than bit-packed metadata. PR-α preserves the historical
// boundary; PR-β will widen it together with the LIST child slot.
constexpr int32_t COMPLEX_TYPE_REGISTRY_MIN = 10000;

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
// `registry` and `next_complex_id` are used to stash types that don't fit
// the bit-packed layout (STRUCT fields, LIST(STRUCT)). They are required
// for those cases — pass nullptr only when the caller already knows the
// type is bit-packable.
inline int32_t EncodeTypeMod(const duckdb::LogicalType &type,
                              ComplexTypeRegistry *registry,
                              int32_t *next_complex_id)
{
    using duckdb::LogicalTypeId;
    if (type.id() == LogicalTypeId::DECIMAL) {
        uint16_t w = (uint16_t)duckdb::DecimalType::GetWidth(type);
        uint16_t s = (uint16_t)duckdb::DecimalType::GetScale(type);
        return ((int32_t)w << 8) | s;
    }
    if (type.id() == LogicalTypeId::LIST) {
        auto &child = duckdb::ListType::GetChildType(type);
        if (child.id() == LogicalTypeId::LIST) {
            int32_t cmod = EncodeTypeMod(child, registry, next_complex_id);
            return (int32_t)LogicalTypeId::LIST | (cmod << 8);
        }
        if (child.id() == LogicalTypeId::STRUCT) {
            if (!registry || !next_complex_id) {
                throw duckdb::InternalException(
                    "EncodeTypeMod: LIST(STRUCT) requires registry");
            }
            int32_t id = (*next_complex_id)++;
            (*registry)[id] = type;  // store full LIST(STRUCT(...))
            return id;
        }
        return (int32_t)child.id();
    }
    if (type.id() == LogicalTypeId::STRUCT) {
        if (!registry || !next_complex_id) {
            throw duckdb::InternalException(
                "EncodeTypeMod: STRUCT requires registry");
        }
        int32_t id = (*next_complex_id)++;
        (*registry)[id] = type;
        return id;
    }
    return 0;
}

// Decode (oid, INT type_mod) back into a LogicalType. `registry` is the
// converter-populated map; when null, registry handles fall through to the
// legacy ANY-on-miss behavior (preserved so call sites that historically
// didn't pass a registry still compile).
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
        if (type_mod >= COMPLEX_TYPE_REGISTRY_MIN) {
            if (registry) {
                auto it = registry->find(type_mod);
                if (it != registry->end()) return it->second;
            }
            return duckdb::LogicalType::ANY;
        }
        uint32_t cooid = (uint32_t)(type_mod & 0xFF) + LOGICAL_TYPE_BASE_ID;
        int32_t cmod = (type_mod >> 8);
        return duckdb::LogicalType::LIST(DecodeTypeMod(cooid, cmod, registry));
    }
    if (tid == LogicalTypeId::PATH) {
        return duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT);
    }
    if (tid == LogicalTypeId::STRUCT && type_mod >= COMPLEX_TYPE_REGISTRY_MIN) {
        if (registry) {
            auto it = registry->find(type_mod);
            if (it != registry->end()) return it->second;
        }
        return duckdb::LogicalType::ANY;
    }
    return duckdb::LogicalType(tid);
}

}  // namespace turbolynx

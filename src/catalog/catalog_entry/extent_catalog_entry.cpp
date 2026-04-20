#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_serializer.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "common/exception.hpp"
#include "common/enums/graph_component_type.hpp"

#include <memory>
#include <algorithm>

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
    using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

ExtentCatalogEntry::ExtentCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, duckdb::CreateExtentInfo *info)
    : StandardEntry(CatalogType::EXTENT_ENTRY, schema, catalog, info->extent) {
	this->temporary = info->temporary;
	this->extent_type = info->extent_type;
	this->eid = info->eid;
	this->local_cdf_id_version = 0;
	this->local_adjlist_cdf_id_version = std::numeric_limits<LocalChunkDefinitionID>::max();
	this->pid = info->pid;
	this->ps_oid = info->ps_oid;
	this->num_tuples_in_extent = info->num_tuples_in_extent;
}

unique_ptr<CatalogEntry> ExtentCatalogEntry::Copy(duckdb::ClientContext &context) {
	D_ASSERT(false);
	//auto create_info = make_unique<duckdb::CreateExtentInfo>(schema->name, name, extent_type, eid);
	//return make_unique<ExtentCatalogEntry>(catalog, schema, create_info.get());
}

void ExtentCatalogEntry::SetExtentType(ExtentType extent_type_) {
	extent_type = extent_type_;
}

LocalChunkDefinitionID ExtentCatalogEntry::GetNextChunkDefinitionID() {
	return local_cdf_id_version++;
}

void ExtentCatalogEntry::AddChunkDefinitionID(ChunkDefinitionID cdf_id) {
	chunks.push_back(cdf_id);
}

LocalChunkDefinitionID ExtentCatalogEntry::GetNextAdjListChunkDefinitionID() {
	return local_adjlist_cdf_id_version--;
}

void ExtentCatalogEntry::AddAdjListChunkDefinitionID(ChunkDefinitionID cdf_id) {
	adjlist_chunks.push_back(cdf_id);
}

// ---------------------------------------------------------------------------
// Serialization — ChunkDef entries are embedded inline (not as separate catalog entries)
// ---------------------------------------------------------------------------

void ExtentCatalogEntry::Serialize(duckdb::CatalogSerializer &ser, duckdb::ClientContext &ctx) const {
    ser.Write(static_cast<uint32_t>(eid));
    ser.Write(static_cast<uint8_t>(extent_type));
    ser.Write(static_cast<uint32_t>(pid));           // PartitionID (uint16_t) → uint32_t
    ser.Write(static_cast<uint64_t>(ps_oid));
    ser.Write(static_cast<uint64_t>(num_tuples_in_extent));
    ser.Write(static_cast<uint32_t>(local_cdf_id_version.load()));
    ser.Write(static_cast<uint32_t>(local_adjlist_cdf_id_version.load()));

    // Format version marker:
    //   0xCAFEBABE = v1: cdf_id + name + type + num_entries (no min-max)
    //   0xCAFEBABF = v2: same + per-chunk min-max array
    // Old format has no marker here; the next uint32 is n_chunks directly.
    static constexpr uint32_t kFormatMagicV1 = 0xCAFEBABEu;
    static constexpr uint32_t kFormatMagicV2 = 0xCAFEBABFu;
    ser.Write(kFormatMagicV2);

    auto resolve_cdf_list = [&](const std::vector<ChunkDefinitionID> &cdf_ids) {
        std::vector<std::pair<ChunkDefinitionID, ChunkDefinitionCatalogEntry *>> resolved;
        resolved.reserve(cdf_ids.size());

        for (auto cdf_id : cdf_ids) {
            std::string cdf_name = DEFAULT_CHUNKDEFINITION_PREFIX + std::to_string(cdf_id);
            auto *cdf = (ChunkDefinitionCatalogEntry *)catalog->GetEntry(
                ctx, CatalogType::CHUNKDEFINITION_ENTRY, schema->name, cdf_name, /*if_exists=*/true);
            if (!cdf) {
                cdf = (ChunkDefinitionCatalogEntry *)catalog->GetEntry(ctx, schema->name, cdf_id, /*if_exists=*/true);
            }
            if (!cdf) {
                throw InternalException(
                    "Extent '%s' (eid=%u) references missing ChunkDefinition oid %llu during serialization",
                    name.c_str(), static_cast<uint32_t>(eid),
                    static_cast<unsigned long long>(cdf_id));
            }
            resolved.emplace_back(cdf_id, cdf);
        }

        return resolved;
    };

    auto write_cdf_list = [&](const std::vector<ChunkDefinitionID> &cdf_ids) {
        auto resolved = resolve_cdf_list(cdf_ids);
        ser.Write(static_cast<uint32_t>(resolved.size()));
        for (auto &entry : resolved) {
            auto cdf_id = entry.first;
            auto *cdf = entry.second;
            ser.Write(static_cast<uint64_t>(cdf_id));
            ser.WriteString(cdf->name);
            ser.Write(static_cast<uint8_t>(cdf->data_type_id));
            ser.Write(static_cast<uint64_t>(cdf->num_entries_in_column));
            // v2: write min-max array
            if (cdf->is_min_max_array_exist) {
                ser.Write(static_cast<uint8_t>(1));
                ser.Write(static_cast<uint32_t>(cdf->min_max_array.size()));
                for (auto &mm : cdf->min_max_array) {
                    ser.Write(static_cast<uint64_t>(mm.min));
                    ser.Write(static_cast<uint64_t>(mm.max));
                }
            } else {
                ser.Write(static_cast<uint8_t>(0));
            }
        }
    };

    // Property ChunkDefs
    write_cdf_list(chunks);

    // AdjList ChunkDefs
    write_cdf_list(adjlist_chunks);
}

// Recover cdf_id from a CDF entry name for old-format catalog.bin files.
// Bulkload names: "cdf_{numeric_id}"   → parse directly.
// Test names:     "cdf_{eid}_{col}"    → reconstruct as (eid<<32)|col.
static ChunkDefinitionID cdf_id_from_name(const std::string &cdf_name) {
    constexpr size_t prefix_len = sizeof(DEFAULT_CHUNKDEFINITION_PREFIX) - 1;
    auto suffix = cdf_name.substr(prefix_len);
    size_t underscore = suffix.find('_');
    if (underscore != std::string::npos) {
        uint64_t eid_part = std::stoull(suffix.substr(0, underscore));
        uint64_t col_part = std::stoull(suffix.substr(underscore + 1));
        return (eid_part << 32) | col_part;
    }
    return std::stoull(suffix);
}

void ExtentCatalogEntry::Deserialize(duckdb::CatalogDeserializer &des, duckdb::ClientContext &ctx) {
    eid                = des.ReadU32();
    extent_type        = static_cast<ExtentType>(des.ReadU8());
    pid                = static_cast<PartitionID>(des.ReadU32());
    ps_oid             = des.ReadU64();
    num_tuples_in_extent = des.ReadU64();
    local_cdf_id_version.store(des.ReadU32());
    local_adjlist_cdf_id_version.store(des.ReadU32());

    // Format detection:
    //   v0 (old): no magic — first uint32 is n_chunks directly
    //   v1: magic 0xCAFEBABE — cdf_id + name + type + num_entries
    //   v2: magic 0xCAFEBABF — same as v1 + per-chunk min-max array
    static constexpr uint32_t kFormatMagicV1 = 0xCAFEBABEu;
    static constexpr uint32_t kFormatMagicV2 = 0xCAFEBABFu;
    uint32_t maybe_magic = des.ReadU32();
    int format_version = 0;
    if (maybe_magic == kFormatMagicV2) format_version = 2;
    else if (maybe_magic == kFormatMagicV1) format_version = 1;
    uint32_t n = (format_version > 0) ? des.ReadU32() : maybe_magic;

    auto read_cdf_list = [&](std::vector<ChunkDefinitionID> &out) {
        for (uint32_t i = 0; i < n; i++) {
            ChunkDefinitionID cdf_id;
            if (format_version > 0) {
                cdf_id = static_cast<ChunkDefinitionID>(des.ReadU64());
            }
            std::string cdf_name  = des.ReadString();
            LogicalTypeId type_id = static_cast<LogicalTypeId>(des.ReadU8());
            uint64_t num_entries  = des.ReadU64();
            if (format_version == 0) {
                cdf_id = cdf_id_from_name(cdf_name);
            }
            CreateChunkDefinitionInfo ci(schema->name, cdf_name, LogicalType(type_id));
            auto *cdf = (ChunkDefinitionCatalogEntry *)catalog->CreateChunkDefinition(ctx, schema, &ci);
            cdf->SetNumEntriesInColumn(static_cast<size_t>(num_entries));
            // v2: read min-max array
            if (format_version >= 2) {
                uint8_t has_minmax = des.ReadU8();
                if (has_minmax) {
                    uint32_t mm_count = des.ReadU32();
                    cdf->min_max_array.resize(mm_count);
                    for (uint32_t j = 0; j < mm_count; j++) {
                        cdf->min_max_array[j].min = static_cast<int64_t>(des.ReadU64());
                        cdf->min_max_array[j].max = static_cast<int64_t>(des.ReadU64());
                    }
                    cdf->is_min_max_array_exist = true;
                }
            }
            out.push_back(cdf_id);
        }
    };

    read_cdf_list(chunks);
    n = des.ReadU32();
    read_cdf_list(adjlist_chunks);
}

} // namespace turbolynx

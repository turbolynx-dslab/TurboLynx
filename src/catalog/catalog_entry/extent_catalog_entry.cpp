#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_serializer.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "common/enums/graph_component_type.hpp"

#include <memory>
#include <algorithm>

namespace duckdb {

ExtentCatalogEntry::ExtentCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateExtentInfo *info)
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

unique_ptr<CatalogEntry> ExtentCatalogEntry::Copy(ClientContext &context) {
	D_ASSERT(false);
	//auto create_info = make_unique<CreateExtentInfo>(schema->name, name, extent_type, eid);
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

void ExtentCatalogEntry::Serialize(CatalogSerializer &ser, ClientContext &ctx) const {
    ser.Write(static_cast<uint32_t>(eid));
    ser.Write(static_cast<uint8_t>(extent_type));
    ser.Write(static_cast<uint32_t>(pid));           // PartitionID (uint16_t) → uint32_t
    ser.Write(static_cast<uint64_t>(ps_oid));
    ser.Write(static_cast<uint64_t>(num_tuples_in_extent));
    ser.Write(static_cast<uint32_t>(local_cdf_id_version.load()));
    ser.Write(static_cast<uint32_t>(local_adjlist_cdf_id_version.load()));

    // Format version marker — distinguishes new format (with explicit cdf_id per entry)
    // from old format (cdf_id absent; name-parsing was used). Old format has no marker
    // here, so the next uint32 would be n_chunks directly.
    static constexpr uint32_t kFormatMagic = 0xCAFEBABEu;
    ser.Write(kFormatMagic);

    // Property ChunkDefs: look up from catalog by name and serialize inline.
    // chunks[] stores ChunkDefinitionIDs which are either:
    //   - catalog OIDs (unit-test path: CreateChunkDefinition → cdf->oid pushed)
    //   - custom encoded IDs (bulkload path: (eid<<32)|local_idx pushed)
    ser.Write(static_cast<uint32_t>(chunks.size()));
    for (auto cdf_id : chunks) {
        std::string cdf_name = DEFAULT_CHUNKDEFINITION_PREFIX + std::to_string(cdf_id);
        auto *cdf = (ChunkDefinitionCatalogEntry *)catalog->GetEntry(
            ctx, CatalogType::CHUNKDEFINITION_ENTRY, schema->name, cdf_name, /*if_exists=*/true);
        if (!cdf) {
            // Fall back: look up by OID (unit-test path stores catalog OID in chunks)
            cdf = (ChunkDefinitionCatalogEntry *)catalog->GetEntry(ctx, schema->name, cdf_id, /*if_exists=*/true);
        }
        if (!cdf) continue;
        ser.Write(static_cast<uint64_t>(cdf_id));   // preserve original ID (OID or eid-encoded)
        ser.WriteString(cdf->name);
        ser.Write(static_cast<uint8_t>(cdf->data_type_id));
        ser.Write(static_cast<uint64_t>(cdf->num_entries_in_column));
    }

    // AdjList ChunkDefs: same approach
    ser.Write(static_cast<uint32_t>(adjlist_chunks.size()));
    for (auto cdf_id : adjlist_chunks) {
        std::string cdf_name = DEFAULT_CHUNKDEFINITION_PREFIX + std::to_string(cdf_id);
        auto *cdf = (ChunkDefinitionCatalogEntry *)catalog->GetEntry(
            ctx, CatalogType::CHUNKDEFINITION_ENTRY, schema->name, cdf_name, /*if_exists=*/true);
        if (!cdf) {
            cdf = (ChunkDefinitionCatalogEntry *)catalog->GetEntry(ctx, schema->name, cdf_id, /*if_exists=*/true);
        }
        if (!cdf) continue;
        ser.Write(static_cast<uint64_t>(cdf_id));   // preserve original ID
        ser.WriteString(cdf->name);
        ser.Write(static_cast<uint8_t>(cdf->data_type_id));
        ser.Write(static_cast<uint64_t>(cdf->num_entries_in_column));
    }
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

void ExtentCatalogEntry::Deserialize(CatalogDeserializer &des, ClientContext &ctx) {
    eid                = des.ReadU32();
    extent_type        = static_cast<ExtentType>(des.ReadU8());
    pid                = static_cast<PartitionID>(des.ReadU32());
    ps_oid             = des.ReadU64();
    num_tuples_in_extent = des.ReadU64();
    local_cdf_id_version.store(des.ReadU32());
    local_adjlist_cdf_id_version.store(des.ReadU32());

    // Format detection: new format writes 0xCAFEBABE before n_chunks.
    // Old format writes n_chunks directly (no magic marker).
    static constexpr uint32_t kFormatMagic = 0xCAFEBABEu;
    uint32_t maybe_magic = des.ReadU32();
    bool new_format = (maybe_magic == kFormatMagic);
    uint32_t n = new_format ? des.ReadU32() : maybe_magic;

    auto read_cdf_list = [&](std::vector<ChunkDefinitionID> &out) {
        for (uint32_t i = 0; i < n; i++) {
            ChunkDefinitionID cdf_id;
            if (new_format) {
                cdf_id = static_cast<ChunkDefinitionID>(des.ReadU64());
            }
            std::string cdf_name  = des.ReadString();
            LogicalTypeId type_id = static_cast<LogicalTypeId>(des.ReadU8());
            uint64_t num_entries  = des.ReadU64();
            if (!new_format) {
                cdf_id = cdf_id_from_name(cdf_name);
            }
            CreateChunkDefinitionInfo ci(schema->name, cdf_name, LogicalType(type_id));
            auto *cdf = (ChunkDefinitionCatalogEntry *)catalog->CreateChunkDefinition(ctx, schema, &ci);
            cdf->SetNumEntriesInColumn(static_cast<size_t>(num_entries));
            out.push_back(cdf_id);
        }
    };

    read_cdf_list(chunks);
    n = des.ReadU32();
    read_cdf_list(adjlist_chunks);
}

} // namespace duckdb

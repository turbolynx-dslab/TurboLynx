#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog_serializer.hpp"
#include "common/enums/graph_component_type.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/parsed_data/create_partition_info.hpp"

#include <algorithm>
#include <memory>

namespace duckdb {

PartitionCatalogEntry::PartitionCatalogEntry(Catalog *catalog,
                                             SchemaCatalogEntry *schema,
                                             CreatePartitionInfo *info)
    : StandardEntry(CatalogType::PARTITION_ENTRY, schema, catalog, info->partition)
{
    this->temporary = info->temporary;
    this->pid = info->pid;
    this->num_columns = 0;
    this->local_temporal_id_version = 0;
    this->local_extent_id_version = 0;
    this->physical_id_index = INVALID_OID;
}

void PartitionCatalogEntry::AddPropertySchema(
    ClientContext &context, idx_t ps_oid,
    vector<PropertyKeyID> &property_schemas)
{
    property_schema_array.push_back(ps_oid);
    for (size_t i = 0; i < property_schemas.size(); i++) {
        auto target_partitions = property_schema_index.find(property_schemas[i]);
        if (target_partitions != property_schema_index.end()) {
            property_schema_index.at(property_schemas[i]).push_back({ps_oid, i});
        } else {
            idx_t_pair_vector tmp_vec;
            tmp_vec.push_back({ps_oid, i});
            property_schema_index.insert({property_schemas[i], tmp_vec});
        }
    }
}

void PartitionCatalogEntry::SetUnivPropertySchema(idx_t psid) {
    univ_ps_oid = psid;
}

void PartitionCatalogEntry::SetIdKeyColumnIdxs(vector<idx_t> &key_column_idxs) {
    for (auto &it : key_column_idxs) {
        id_key_column_idxs.push_back(it);
    }
}

unique_ptr<CatalogEntry> PartitionCatalogEntry::Copy(ClientContext &context) {
    D_ASSERT(false);
}

void PartitionCatalogEntry::SetPartitionID(PartitionID pid) {
    this->pid = pid;
}

PartitionID PartitionCatalogEntry::GetPartitionID() {
    return pid;
}

ExtentID PartitionCatalogEntry::GetLocalExtentID() {
    return local_extent_id_version;
}

ExtentID PartitionCatalogEntry::GetCurrentExtentID() {
    ExtentID eid = pid;
    eid = eid << 16;
    return eid + local_extent_id_version;
}

ExtentID PartitionCatalogEntry::GetNewExtentID() {
    ExtentID new_eid = pid;
    new_eid = new_eid << 16;
    return new_eid + local_extent_id_version++;
}

void PartitionCatalogEntry::GetPropertySchemaIDs(vector<idx_t> &psids) {
    for (auto &psid : property_schema_array) {
        psids.push_back(psid);
    }
}

void PartitionCatalogEntry::SetPhysicalIDIndex(idx_t index_oid) {
    physical_id_index = index_oid;
}

void PartitionCatalogEntry::SetSrcDstPartOid(idx_t src_part_oid, idx_t dst_part_oid) {
    this->src_part_oid = src_part_oid;
    this->dst_part_oid = dst_part_oid;
}

void PartitionCatalogEntry::AddAdjIndex(idx_t index_oid) {
    adjlist_indexes.push_back(index_oid);
}

void PartitionCatalogEntry::AddPropertyIndex(idx_t index_oid) {
    property_indexes.push_back(index_oid);
}

idx_t PartitionCatalogEntry::GetPhysicalIDIndexOid() {
    D_ASSERT(physical_id_index != INVALID_OID);
    return physical_id_index;
}

void PartitionCatalogEntry::SetSchema(ClientContext &context,
                                      vector<string> &key_names,
                                      vector<LogicalType> &types,
                                      vector<PropertyKeyID> &univ_prop_key_ids)
{
    D_ASSERT(global_property_typesid.empty());
    D_ASSERT(global_property_key_names.empty());

    for (auto &it : types) {
        if (it != LogicalType::FORWARD_ADJLIST && it != LogicalType::BACKWARD_ADJLIST)
            num_columns++;
        global_property_typesid.push_back(it.id());
        if (it.id() == LogicalTypeId::DECIMAL) {
            uint16_t width_scale = DecimalType::GetWidth(it);
            width_scale = width_scale << 8 | DecimalType::GetScale(it);
            extra_typeinfo_vec.push_back(width_scale);
        } else {
            extra_typeinfo_vec.push_back(0);
        }
    }

    for (auto &it : key_names) {
        global_property_key_names.push_back(it);
    }

    for (size_t i = 0; i < univ_prop_key_ids.size(); i++) {
        global_property_key_to_location.insert({univ_prop_key_ids[i], i});
		global_property_key_ids.push_back(univ_prop_key_ids[i]);
    }

    min_max_array.resize(types.size());
    welford_array.resize(types.size());
}

void PartitionCatalogEntry::SetTypes(vector<LogicalType> &types) {
    D_ASSERT(global_property_typesid.empty());
    for (auto &it : types) {
        if (it != LogicalType::FORWARD_ADJLIST && it != LogicalType::BACKWARD_ADJLIST)
            num_columns++;
        global_property_typesid.push_back(it.id());
        if (it.id() == LogicalTypeId::DECIMAL) {
            uint16_t width_scale = DecimalType::GetWidth(it);
            width_scale = width_scale << 8 | DecimalType::GetScale(it);
            extra_typeinfo_vec.push_back(width_scale);
        } else {
            extra_typeinfo_vec.push_back(0);
        }
    }
}

vector<LogicalType> PartitionCatalogEntry::GetTypes() {
    vector<LogicalType> universal_schema;
    for (size_t i = 0; i < global_property_typesid.size(); i++) {
        if (extra_typeinfo_vec[i] == 0) {
            universal_schema.push_back(LogicalType(global_property_typesid[i]));
        } else {
            uint8_t width = (uint8_t)(extra_typeinfo_vec[i] >> 8);
            uint8_t scale = (uint8_t)(extra_typeinfo_vec[i] & 0xFF);
            universal_schema.push_back(LogicalType::DECIMAL(width, scale));
        }
    }
    return universal_schema;
}

uint64_t PartitionCatalogEntry::GetNumberOfColumns() const {
    return num_columns;
}

void PartitionCatalogEntry::UpdateMinMaxArray(PropertyKeyID key_id, int64_t min, int64_t max) {
    auto location = global_property_key_to_location.find(key_id);
    if (location != global_property_key_to_location.end()) {
        auto idx = location->second;
        auto minmax = min_max_array[idx];
        if (minmax.min > min) minmax.min = min;
        if (minmax.max < max) minmax.max = max;
        min_max_array[idx] = minmax;
    }
}

void PartitionCatalogEntry::UpdateWelfordStdDevArray(PropertyKeyID key_id, Vector &data, size_t size) {
    D_ASSERT(data.GetType().IsNumeric());

    auto location = global_property_key_to_location.find(key_id);
    if (location != global_property_key_to_location.end()) {
        for (size_t i = 0; i < size; i++) {
            auto original_value = data.GetValue(i);
            auto value = original_value.GetValue<idx_t>();
            auto &welford_v = welford_array[location->second];
            welford_v.n++;
            auto delta = value - welford_v.mean;
            welford_v.mean += delta / welford_v.n;
            auto delta2 = value - welford_v.mean;
            welford_v.M2 += delta * delta2;
        }
    }
}

StdDev PartitionCatalogEntry::GetStdDev(PropertyKeyID key_id) {
    StdDev std_dev = 0.0;
    auto location = global_property_key_to_location.find(key_id);
    if (location != global_property_key_to_location.end()) {
        auto welford_v = welford_array[location->second];
        if (welford_v.n > 1) {
            std_dev = sqrt(welford_v.M2 / (welford_v.n));
        }
    }
    return std_dev;
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

void PartitionCatalogEntry::Serialize(CatalogSerializer &ser, ClientContext &ctx) const {
    // PartitionID (uint16_t) stored as uint32_t
    ser.Write(static_cast<uint32_t>(pid));

    // src/dst partition OIDs (0 for vertex partitions)
    ser.Write(static_cast<uint64_t>(src_part_oid));
    ser.Write(static_cast<uint64_t>(dst_part_oid));
    ser.Write(static_cast<uint64_t>(univ_ps_oid));
    ser.Write(static_cast<uint64_t>(physical_id_index));

    // property_schema_array: OIDs of PS entries
    ser.WriteVector<uint64_t>(property_schema_array);

    // property_schema_index: PropertyKeyID → vector<pair<OID, col_idx>>
    ser.Write(static_cast<uint32_t>(property_schema_index.size()));
    for (auto &kv : property_schema_index) {
        ser.Write(static_cast<uint64_t>(kv.first));
        ser.Write(static_cast<uint32_t>(kv.second.size()));
        for (auto &p : kv.second) {
            ser.Write(static_cast<uint64_t>(p.first));
            ser.Write(static_cast<uint64_t>(p.second));
        }
    }

    // adjlist / property index OIDs
    ser.WriteVector<uint64_t>(adjlist_indexes);
    ser.WriteVector<uint64_t>(property_indexes);

    // Universal schema: type IDs (serialize each as uint8_t)
    ser.Write(static_cast<uint32_t>(global_property_typesid.size()));
    for (auto v : global_property_typesid) {
        ser.Write(static_cast<uint8_t>(v));
    }
    ser.WriteVector<uint16_t>(extra_typeinfo_vec);
    ser.WriteStringVector(global_property_key_names);
    ser.WriteVector<uint64_t>(global_property_key_ids);
    ser.WriteVector<uint64_t>(id_key_column_idxs);

    // global_property_key_to_location: PropertyKeyID → idx_t
    ser.Write(static_cast<uint32_t>(global_property_key_to_location.size()));
    for (auto &kv : global_property_key_to_location) {
        ser.Write(static_cast<uint64_t>(kv.first));
        ser.Write(static_cast<uint64_t>(kv.second));
    }

    ser.Write(static_cast<uint64_t>(num_columns));
    ser.Write(static_cast<uint32_t>(local_extent_id_version.load()));

    // Histogram / stats vectors
    ser.WriteVector<uint64_t>(offset_infos);
    ser.WriteVector<uint64_t>(boundary_values);
    ser.WriteVector<uint64_t>(num_groups_for_each_column);
    ser.WriteVector<uint64_t>(multipliers_for_each_column);
    ser.WriteVector<uint64_t>(group_info_for_each_table);

    // min/max and Welford stats arrays (POD structs)
    ser.WriteVector<minmax_t>(min_max_array);
    ser.WriteVector<welford_t>(welford_array);

    // Format version >= 2: sub_partition_oids for virtual unified partitions
    ser.WriteVector<uint64_t>(sub_partition_oids);
}

void PartitionCatalogEntry::Deserialize(CatalogDeserializer &des, ClientContext &ctx) {
    pid = static_cast<PartitionID>(des.ReadU32());
    src_part_oid     = des.ReadU64();
    dst_part_oid     = des.ReadU64();
    univ_ps_oid      = des.ReadU64();
    physical_id_index = des.ReadU64();

    property_schema_array = des.ReadVector<uint64_t>();

    uint32_t n = des.ReadU32();
    for (uint32_t i = 0; i < n; i++) {
        uint64_t key  = des.ReadU64();
        uint32_t psz  = des.ReadU32();
        idx_t_pair_vector pairs;
        pairs.reserve(psz);
        for (uint32_t j = 0; j < psz; j++) {
            uint64_t a = des.ReadU64();
            uint64_t b = des.ReadU64();
            pairs.emplace_back(a, b);
        }
        property_schema_index[key] = std::move(pairs);
    }

    adjlist_indexes  = des.ReadVector<uint64_t>();
    property_indexes = des.ReadVector<uint64_t>();

    uint32_t tc = des.ReadU32();
    global_property_typesid.resize(tc);
    for (uint32_t i = 0; i < tc; i++) {
        global_property_typesid[i] = static_cast<LogicalTypeId>(des.ReadU8());
    }
    extra_typeinfo_vec         = des.ReadVector<uint16_t>();
    global_property_key_names  = des.ReadStringVector();
    global_property_key_ids    = des.ReadVector<uint64_t>();
    id_key_column_idxs         = des.ReadVector<uint64_t>();

    n = des.ReadU32();
    for (uint32_t i = 0; i < n; i++) {
        uint64_t k = des.ReadU64();
        uint64_t v = des.ReadU64();
        global_property_key_to_location[k] = v;
    }

    num_columns              = des.ReadU64();
    local_extent_id_version.store(des.ReadU32());

    offset_infos                 = des.ReadVector<uint64_t>();
    boundary_values              = des.ReadVector<uint64_t>();
    num_groups_for_each_column   = des.ReadVector<uint64_t>();
    multipliers_for_each_column  = des.ReadVector<uint64_t>();
    group_info_for_each_table    = des.ReadVector<uint64_t>();

    min_max_array  = des.ReadVector<minmax_t>();
    welford_array  = des.ReadVector<welford_t>();

    // Format version >= 2: sub_partition_oids for virtual unified partitions
    auto &catalog = Catalog::GetCatalog(ctx);
    if (catalog.catalog_format_version_ >= 2) {
        sub_partition_oids = des.ReadVector<uint64_t>();
    }
}

}  // namespace duckdb

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog_serializer.hpp"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "common/enums/graph_component_type.hpp"
#include "parser/parsed_data/create_graph_info.hpp"

#include "icecream.hpp"

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

GraphCatalogEntry::GraphCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, duckdb::CreateGraphInfo *info)
    : StandardEntry(CatalogType::GRAPH_ENTRY, schema, catalog, info->graph)
{
	this->temporary = info->temporary;
	vertex_label_id_version = 0;
	edge_type_id_version = 0;
	property_key_id_version = 1; // reserve 0 for '_id'
	partition_id_version = 0;
}

unique_ptr<CatalogEntry> GraphCatalogEntry::Copy(duckdb::ClientContext &context) {
	D_ASSERT(false);
}

void GraphCatalogEntry::AddEdgePartition(duckdb::ClientContext &context, PartitionID pid, idx_t oid, EdgeTypeID edge_type_id) {
	type_to_partition_index[edge_type_id].push_back(oid);
	edge_partitions.push_back(oid);
}

void GraphCatalogEntry::AddEdgePartition(duckdb::ClientContext &context, PartitionID pid, idx_t oid, string type) {
	EdgeTypeID edge_type_id;
	auto type_id = edgetype_map.find(type);
	if (type_id != edgetype_map.end()) {
		edge_type_id = type_id->second;
	} else {
		edge_type_id = GetEdgeTypeID();
		edgetype_map.insert({type, edge_type_id});
	}
	type_to_partition_index[edge_type_id].push_back(oid);
	edge_partitions.push_back(oid);
}

void GraphCatalogEntry::AddVertexPartition(duckdb::ClientContext &context, PartitionID pid, idx_t oid, vector<VertexLabelID>& label_ids) {
	for (size_t i = 0; i < label_ids.size(); i++) {
		auto target_ids = label_to_partition_index.find(label_ids[i]);
		if (target_ids != label_to_partition_index.end()) {
			target_ids->second.push_back(oid);
		} else {
			std::vector<idx_t> tmp_vec;
			tmp_vec.push_back(oid);
			label_to_partition_index.insert({label_ids[i], tmp_vec});
		}
	}
	vertex_partitions.push_back(oid);
}

void GraphCatalogEntry::AddVertexPartition(duckdb::ClientContext &context, PartitionID pid, idx_t oid, vector<string> &labels) {
	for (size_t i = 0; i < labels.size(); i++) {
		VertexLabelID vertex_label_id;
		auto label_id = vertexlabel_map.find(labels[i]);
		if (label_id != vertexlabel_map.end()) {
			vertex_label_id = label_id->second;
		} else {
			vertex_label_id = GetVertexLabelID();
			vertexlabel_map.insert({labels[i], vertex_label_id});
		}
		auto target_ids = label_to_partition_index.find(vertex_label_id);
		if (target_ids != label_to_partition_index.end()) {
			target_ids->second.push_back(oid);
		} else {
			std::vector<idx_t> tmp_vec;
			tmp_vec.push_back(oid);
			label_to_partition_index.insert({vertex_label_id, tmp_vec});
		}
	}
	vertex_partitions.push_back(oid);
}

void GraphCatalogEntry::AddEdgeConnectionInfo(duckdb::ClientContext &context, idx_t src_part_oid, idx_t edge_part_oid) {
	auto it = src_part_to_connected_edge_part_index.find(src_part_oid);
	if (it != src_part_to_connected_edge_part_index.end()) {
		it->second.push_back(edge_part_oid);
	} else {
		std::vector<idx_t> tmp_vec;
		tmp_vec.push_back(edge_part_oid);
		src_part_to_connected_edge_part_index.insert({src_part_oid, tmp_vec});
	}
}

vector<idx_t> GraphCatalogEntry::Intersection(duckdb::ClientContext &context, vector<VertexLabelID>& label_ids) {
	vector<idx_t> curr_intersection;
	vector<idx_t> last_intersection;
	{
		auto it0 = label_to_partition_index.find(label_ids[0]);
		if (it0 == label_to_partition_index.end()) return last_intersection;
		last_intersection = it0->second;
	}

    for (std::size_t i = 1; i < label_ids.size(); ++i) {
        auto it = label_to_partition_index.find(label_ids[i]);
        if (it == label_to_partition_index.end()) return {};
        std::set_intersection(last_intersection.begin(), last_intersection.end(),
            it->second.begin(), it->second.end(),
            std::back_inserter(curr_intersection));
        std::swap(last_intersection, curr_intersection);
        curr_intersection.clear();
    }
    return last_intersection;
}

vector<idx_t> GraphCatalogEntry::LookupPartition(duckdb::ClientContext &context, vector<string> keys, GraphComponentType graph_component_type) {
	vector<idx_t> return_pids;

	if (graph_component_type == GraphComponentType::EDGE) {
		D_ASSERT(keys.size() <= 1);
		if (keys.size() == 0) {
			for (size_t i = 0; i < edge_partitions.size(); i++)
				return_pids.push_back(edge_partitions[i]);
			return return_pids;
		}
		auto target_id = edgetype_map.find(keys[0]);
		if (target_id != edgetype_map.end()) {
			auto& oids = type_to_partition_index[target_id->second];
			for (auto oid : oids) return_pids.push_back(oid);
		} else {
			std::string available;
			for (auto& kv : edgetype_map) available += " " + kv.first;
			throw InvalidInputException("There is no edge with the given type '%s'. Available:%s", keys[0], available);
		}
	} else if (graph_component_type == GraphComponentType::VERTEX) {
		size_t key_len = keys.size();
		if (key_len == 0) {
			for (size_t i = 0; i < vertex_partitions.size(); i++)
				return_pids.push_back(vertex_partitions[i]);
			return return_pids;
		}
		vector<VertexLabelID> label_ids;
		for (size_t i = 0; i < key_len; i++) {
			auto target_id = vertexlabel_map.find(keys[i]);
			if (target_id != vertexlabel_map.end()) {
				label_ids.push_back(target_id->second);
			} else {
                std::string error_msg =
                    "There is no vertex with the given label " + keys[i] +
                    "\nPossible vertex labels: \n";
                for (auto &vlabel : vertexlabel_map) {
                    error_msg += "\t" + vlabel.first + "\n";
                }
                throw InvalidInputException(error_msg);
            }
		}
		return_pids = Intersection(context, label_ids);
	}
	return return_pids;
}

void GraphCatalogEntry::GetPropertyKeyIDs(duckdb::ClientContext &context, vector<string> &property_names, vector<LogicalType> &property_types, vector<PropertyKeyID> &property_key_ids) {
	D_ASSERT(property_names.size() == property_types.size());

	if (property_key_id_to_name_vec.size() == 0) {
		// reserve 0 for '_id'
		PropertyKeyID new_pkid = 0;
		propertykey_map.insert(std::make_pair(std::string("_id"), new_pkid));
		propertykey_to_typeid_map.insert(std::make_pair(new_pkid, (uint8_t)LogicalTypeId::ID));
		property_key_id_to_name_vec.push_back(std::string("_id"));
	}

	for (size_t i = 0; i < property_names.size(); i++) {
		auto property_key_id = propertykey_map.find(property_names[i]);
		if (property_key_id != propertykey_map.end()) {
			property_key_ids.push_back(property_key_id->second);
		} else {
			PropertyKeyID new_pkid = GetPropertyKeyID();
			uint8_t type_id = (uint8_t) property_types[i].id();
			propertykey_map.insert(std::make_pair(property_names[i], new_pkid));
			propertykey_to_typeid_map.insert(std::make_pair(new_pkid, type_id));
			property_key_ids.push_back(new_pkid);
			property_key_id_to_name_vec.push_back(property_names[i]);
		}
	}
}

void GraphCatalogEntry::GetPropertyNames(duckdb::ClientContext &context, vector<PropertyKeyID> &property_key_ids,
	vector<string> &property_names)
{
	property_names.reserve(property_key_ids.size());
	for (size_t i = 0; i < property_key_ids.size(); i++) {
		property_names.push_back(property_key_id_to_name_vec[property_key_ids[i]]);
	}
}

string GraphCatalogEntry::GetPropertyName(duckdb::ClientContext &context, PropertyKeyID property_key_id) {
	return property_key_id_to_name_vec[property_key_id];
}

void GraphCatalogEntry::GetVertexLabels(vector<string> &label_names) {
	for(auto it = vertexlabel_map.begin(); it != vertexlabel_map.end(); ++it) {
		label_names.push_back(it->first);
	}
}

void GraphCatalogEntry::GetEdgeTypes(vector<string> &type_names) {
	for(auto it = edgetype_map.begin(); it != edgetype_map.end(); ++it) {
		type_names.push_back(it->first);
	}
}

void GraphCatalogEntry::GetVertexPartitionIndexesInLabel(duckdb::ClientContext &context, string label, vector<idx_t> &partition_indexes) {
	auto vertex_label_it = vertexlabel_map.find(label);
	if (vertex_label_it != vertexlabel_map.end()) {
		auto &cat_partition_indexes = label_to_partition_index.find(vertex_label_it->second)->second;
		partition_indexes.reserve(cat_partition_indexes.size());
		for (auto& partition_index : cat_partition_indexes) {
			partition_indexes.push_back(partition_index);
		}
	}
}

string GraphCatalogEntry::GetLabelFromVertexPartitionIndex(duckdb::ClientContext &context, idx_t index) {
    bool found = false;
    string label_found;

    for (auto& vertex_label_pair : vertexlabel_map) {
        auto& current_label = vertex_label_pair.first;
        auto& partition_indices = label_to_partition_index.find(vertex_label_pair.second)->second;

        if (std::find(partition_indices.begin(), partition_indices.end(), index) != partition_indices.end()) {
            label_found = current_label;
            found = true;
            break;
        }
    }

    return found ? label_found : string();
}

string GraphCatalogEntry::GetTypeFromEdgePartitionIndex(duckdb::ClientContext &context, idx_t index) {
    for (auto& edge_type_pair : edgetype_map) {
        auto& current_type = edge_type_pair.first;
        auto& partition_oids = type_to_partition_index.find(edge_type_pair.second)->second;

        if (std::find(partition_oids.begin(), partition_oids.end(), index) != partition_oids.end()) {
            return current_type;
        }
    }
    return string();
}

void GraphCatalogEntry::GetEdgePartitionIndexesInType(duckdb::ClientContext &context, string type, vector<idx_t> &partition_indexes) {
	auto edge_type_it = edgetype_map.find(type);
	if (edge_type_it != edgetype_map.end()) {
		auto& oids = type_to_partition_index.find(edge_type_it->second)->second;
		for (auto oid : oids) partition_indexes.push_back(oid);
	}
}

void GraphCatalogEntry::GetConnectedEdgeOids(duckdb::ClientContext &context, idx_t src_part_oid, vector<idx_t> &edge_part_oids) {
	auto it = src_part_to_connected_edge_part_index.find(src_part_oid);
	if (it != src_part_to_connected_edge_part_index.end()) {
		auto &oids_vec = it->second;
		for (size_t i = 0; i < oids_vec.size(); i++) {
			edge_part_oids.push_back(oids_vec[i]);
		}
	}
}

LogicalTypeId GraphCatalogEntry::GetTypeIdFromPropertyKeyID(const PropertyKeyID pkid) {
	auto it = propertykey_to_typeid_map.find(pkid);
	D_ASSERT(it != propertykey_to_typeid_map.end());
	D_ASSERT(it->second < std::numeric_limits<uint8_t>::max());
	uint8_t type_id = (uint8_t)it->second;
	return (LogicalTypeId)type_id;
}

VertexLabelID GraphCatalogEntry::GetVertexLabelID() {
	return vertex_label_id_version++;
}

EdgeTypeID GraphCatalogEntry::GetEdgeTypeID() {
	return edge_type_id_version++;
}

PropertyKeyID GraphCatalogEntry::GetPropertyKeyID() {
	return property_key_id_version++;
}

PartitionID GraphCatalogEntry::GetNewPartitionID() {
	return partition_id_version++;
}

PropertyKeyID GraphCatalogEntry::GetPropertyKeyID(duckdb::ClientContext &context, string &property_name) {
    auto property_key_id = propertykey_map.find(property_name);
	if (property_key_id == propertykey_map.end()) {
		return -1;
	}
    return property_key_id->second;
}

PropertyKeyID GraphCatalogEntry::GetPropertyKeyID(duckdb::ClientContext &context, const string &property_name) {
    auto property_key_id = propertykey_map.find(property_name);
    if (property_key_id == propertykey_map.end()) {
        return -1;
    }
    return property_key_id->second;
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

void GraphCatalogEntry::Serialize(duckdb::CatalogSerializer &ser, duckdb::ClientContext &ctx) const {
    // vertex / edge partition OID lists
    ser.WriteVector<uint64_t>(vertex_partitions);
    ser.WriteVector<uint64_t>(edge_partitions);

    // vertexlabel_map: string → VertexLabelID (uint64_t)
    ser.Write(static_cast<uint32_t>(vertexlabel_map.size()));
    for (auto &kv : vertexlabel_map) {
        ser.WriteString(kv.first);
        ser.Write(static_cast<uint64_t>(kv.second));
    }

    // edgetype_map: string → EdgeTypeID (uint64_t)
    ser.Write(static_cast<uint32_t>(edgetype_map.size()));
    for (auto &kv : edgetype_map) {
        ser.WriteString(kv.first);
        ser.Write(static_cast<uint64_t>(kv.second));
    }

    // propertykey_map: string → PropertyKeyID (uint64_t)
    ser.Write(static_cast<uint32_t>(propertykey_map.size()));
    for (auto &kv : propertykey_map) {
        ser.WriteString(kv.first);
        ser.Write(static_cast<uint64_t>(kv.second));
    }

    // propertykey_to_typeid_map: PropertyKeyID → type_id (both uint64_t)
    ser.Write(static_cast<uint32_t>(propertykey_to_typeid_map.size()));
    for (auto &kv : propertykey_to_typeid_map) {
        ser.Write(static_cast<uint64_t>(kv.first));
        ser.Write(static_cast<uint64_t>(kv.second));
    }

    // property_key_id_to_name_vec: string vector
    ser.WriteStringVector(property_key_id_to_name_vec);

    // type_to_partition_index: EdgeTypeID → vector<partition OID>
    // Format v2: write as vector (same as label_to_partition_index)
    ser.Write(static_cast<uint32_t>(type_to_partition_index.size()));
    for (auto &kv : type_to_partition_index) {
        ser.Write(static_cast<uint64_t>(kv.first));
        ser.WriteVector<uint64_t>(kv.second);
    }

    // label_to_partition_index: VertexLabelID → vector<OID>
    ser.Write(static_cast<uint32_t>(label_to_partition_index.size()));
    for (auto &kv : label_to_partition_index) {
        ser.Write(static_cast<uint64_t>(kv.first));
        ser.WriteVector<uint64_t>(kv.second);
    }

    // src_part_to_connected_edge_part_index: src OID → vector<edge OID>
    ser.Write(static_cast<uint32_t>(src_part_to_connected_edge_part_index.size()));
    for (auto &kv : src_part_to_connected_edge_part_index) {
        ser.Write(static_cast<uint64_t>(kv.first));
        ser.WriteVector<uint64_t>(kv.second);
    }

    // Atomic ID counters
    ser.Write(static_cast<uint64_t>(vertex_label_id_version.load()));
    ser.Write(static_cast<uint64_t>(edge_type_id_version.load()));
    ser.Write(static_cast<uint64_t>(property_key_id_version.load()));
    ser.Write(static_cast<uint64_t>(partition_id_version.load()));
}

void GraphCatalogEntry::Deserialize(duckdb::CatalogDeserializer &des, duckdb::ClientContext &ctx) {
    vertex_partitions = des.ReadVector<uint64_t>();
    edge_partitions   = des.ReadVector<uint64_t>();

    uint32_t n = des.ReadU32();
    for (uint32_t i = 0; i < n; i++) {
        auto key = des.ReadString();
        auto val = des.ReadU64();
        vertexlabel_map[key] = val;
    }

    n = des.ReadU32();
    for (uint32_t i = 0; i < n; i++) {
        auto key = des.ReadString();
        auto val = des.ReadU64();
        edgetype_map[key] = val;
    }

    n = des.ReadU32();
    for (uint32_t i = 0; i < n; i++) {
        auto key = des.ReadString();
        auto val = des.ReadU64();
        propertykey_map[key] = val;
    }

    n = des.ReadU32();
    for (uint32_t i = 0; i < n; i++) {
        auto k = des.ReadU64();
        auto v = des.ReadU64();
        propertykey_to_typeid_map[k] = v;
    }

    property_key_id_to_name_vec = des.ReadStringVector();

    // type_to_partition_index: EdgeTypeID → vector<partition OID>
    n = des.ReadU32();
    for (uint32_t i = 0; i < n; i++) {
        auto k   = des.ReadU64();
        auto vec = des.ReadVector<uint64_t>();
        type_to_partition_index[k] = std::move(vec);
    }

    n = des.ReadU32();
    for (uint32_t i = 0; i < n; i++) {
        auto k   = des.ReadU64();
        auto vec = des.ReadVector<uint64_t>();
        label_to_partition_index[k] = std::move(vec);
    }

    n = des.ReadU32();
    for (uint32_t i = 0; i < n; i++) {
        auto k   = des.ReadU64();
        auto vec = des.ReadVector<uint64_t>();
        src_part_to_connected_edge_part_index[k] = std::move(vec);
    }

    vertex_label_id_version.store(des.ReadU64());
    edge_type_id_version.store(des.ReadU64());
    property_key_id_version.store(des.ReadU64());
    partition_id_version.store(des.ReadU64());
}

} // namespace turbolynx
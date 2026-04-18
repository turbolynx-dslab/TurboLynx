#pragma once


#include "catalog/standard_entry.hpp"

#include "common/unordered_map.hpp"
#include "parser/column_definition.hpp"
//#include "parser/constraint.hpp"
//#include "planner/bound_constraint.hpp"
//#include "planner/expression.hpp"
#include "common/case_insensitive_map.hpp"
#include "common/enums/graph_component_type.hpp"
#include "catalog/inverted_index.hpp"
#include "common/boost_typedefs.hpp"

#include <unordered_map>
#include <string>
#include <vector>

namespace duckdb {
struct CreateGraphInfo;
}
namespace turbolynx {
}
namespace duckdb {
    using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

class ColumnStatistics;
struct PartitionCatalogEntry;

//! A graph catalog entry
class GraphCatalogEntry : public StandardEntry {
	using VertexLabelIDUnorderedMap = std::unordered_map<std::string, VertexLabelID>;
	using EdgeTypeIDUnorderedMap = std::unordered_map<std::string, EdgeTypeID>;
	using PropertyKeyIDUnorderedMap = std::unordered_map<std::string, PropertyKeyID>;
	using EdgeTypeToPartitionVecUnorderedMap = std::unordered_map<EdgeTypeID, std::vector<idx_t>>;
	using PropertyKeyIDToTypeIDUnorderedMap = std::unordered_map<EdgeTypeID, idx_t>;
	using VertexLabelToPartitionVecUnorderedMap = std::unordered_map<VertexLabelID, std::vector<idx_t>>;
	using PartitionToPartitionVecUnorderedMap = std::unordered_map<idx_t, std::vector<idx_t>>;

public:
	//! Create a real GraphCatalogEntry
	GraphCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, duckdb::CreateGraphInfo *info);

	idx_t_vector vertex_partitions;
	idx_t_vector edge_partitions;

	// TODO: change map structure into.. what?
	VertexLabelIDUnorderedMap vertexlabel_map;
	EdgeTypeIDUnorderedMap edgetype_map;
	PropertyKeyIDUnorderedMap propertykey_map;
	PropertyKeyIDToTypeIDUnorderedMap propertykey_to_typeid_map;
	string_vector property_key_id_to_name_vec;

	EdgeTypeToPartitionVecUnorderedMap type_to_partition_index;
	VertexLabelToPartitionVecUnorderedMap label_to_partition_index;
	PartitionToPartitionVecUnorderedMap src_part_to_connected_edge_part_index;

	atomic<VertexLabelID> vertex_label_id_version;
	atomic<EdgeTypeID> edge_type_id_version;
	atomic<PropertyKeyID> property_key_id_version;
	atomic<PartitionID> partition_id_version;
public:
	//unique_ptr<CatalogEntry> AlterEntry(duckdb::ClientContext &context, AlterInfo *info) override;
	void AddVertexPartition(duckdb::ClientContext &context, PartitionID pid, idx_t oid, vector<VertexLabelID>& label_ids);
	void AddVertexPartition(duckdb::ClientContext &context, PartitionID pid, idx_t oid, vector<string>& labels);
	void AddEdgePartition(duckdb::ClientContext &context, PartitionID pid, idx_t oid, EdgeTypeID edge_type_id);
	void AddEdgePartition(duckdb::ClientContext &context, PartitionID pid, idx_t oid, string type);
	void AddEdgeConnectionInfo(duckdb::ClientContext &context, idx_t src_part_oid, idx_t edge_part_oid);

	idx_t_vector *GetVertexPartitionOids()
	{
		return &vertex_partitions;
	}

	idx_t_vector *GetEdgePartitionOids()
	{
		return &edge_partitions;
	}

    vector<idx_t> LookupPartition(duckdb::ClientContext &context, vector<string> keys,
                                  GraphComponentType graph_component_type);
    void GetPropertyKeyIDs(duckdb::ClientContext &context,
                           vector<string> &property_names,
                           vector<LogicalType> &property_types,
                           vector<PropertyKeyID> &property_key_ids);
    void GetPropertyNames(duckdb::ClientContext &context,
                          vector<PropertyKeyID> &property_key_ids,
                          vector<string> &property_names);
    string GetPropertyName(duckdb::ClientContext &context,
                           PropertyKeyID property_key_id);
    void GetVertexLabels(vector<string> &label_names);
    void GetEdgeTypes(vector<string> &type_names);
    void GetVertexPartitionIndexesInLabel(
        duckdb::ClientContext &context, string label,
        vector<idx_t> &vertex_partition_indexes);
    void GetEdgePartitionIndexesInType(duckdb::ClientContext &context, string type,
                                       vector<idx_t> &edge_partition_indexes);
    void GetConnectedEdgeOids(duckdb::ClientContext &context, idx_t src_part_oid,
                              vector<idx_t> &edge_part_oids);
    LogicalTypeId GetTypeIdFromPropertyKeyID(const PropertyKeyID pkid);
    string GetLabelFromVertexPartitionIndex(duckdb::ClientContext &context,
                                            idx_t index);
    string GetTypeFromEdgePartitionIndex(duckdb::ClientContext &context, idx_t index);

    vector<idx_t> Intersection(duckdb::ClientContext &context, vector<VertexLabelID>& label_ids);
	VertexLabelID GetVertexLabelID();
	EdgeTypeID GetEdgeTypeID();

	//! Get a new property key id
	PropertyKeyID GetPropertyKeyID();

	//! Get a new partition id
	PartitionID GetNewPartitionID();

	//! Get a property key id from a property name
	PropertyKeyID GetPropertyKeyID(duckdb::ClientContext &context, string &property_name);

	//! Get a property key id from a property name
	PropertyKeyID GetPropertyKeyID(duckdb::ClientContext &context, const string &property_name);

	//! Serialize the meta information of the TableCatalogEntry a serializer
	//virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateTableInfo
	//static unique_ptr<CreateTableInfo> Deserialize(Deserializer &source);

	unique_ptr<CatalogEntry> Copy(duckdb::ClientContext &context) override;

	void Serialize(duckdb::CatalogSerializer &ser, duckdb::ClientContext &ctx) const override;
	void Deserialize(duckdb::CatalogDeserializer &des, duckdb::ClientContext &ctx) override;

	//void CommitAlter(AlterInfo &info);
	//void CommitDrop();

	//! Returns the column index of the specified column name.
	//! If the column does not exist:
	//! If if_exists is true, returns DConstants::INVALID_INDEX
	//! If if_exists is false, throws an exception
	//idx_t GetColumnIndex(string &name, bool if_exists = false);

};
} // namespace turbolynx

namespace duckdb {
using GraphCatalogEntry = turbolynx::GraphCatalogEntry;
}

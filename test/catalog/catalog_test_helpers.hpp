#pragma once
// =============================================================================
// S62GDB Catalog Test Helpers
// =============================================================================
//
// Two layers of helpers are provided:
//
// -- Low-level (single entry creation) ----------------------------------------
//   make_partition()  — create graph + partition (no schema setup)
//   make_ps()         — create a PropertySchema under a partition
//   make_extent()     — create an Extent under a partition + PS
//   make_chunkdef()   — append a ChunkDefinition to an Extent
//
// -- High-level (full schema chain construction) -------------------------------
//   build_vertex_schema() — builds the full graph→partition→PS→extent→ChunkDef chain
//   build_edge_schema()   — builds an edge partition connecting two vertex partitions
//
// Common fixtures:
//   kPersonCols / kPersonTypes — "Person" vertex columns: name VARCHAR, age BIGINT
// =============================================================================

#include "test_helper.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "catalog/catalog_entry/property_schema_catalog_entry.hpp"
#include "catalog/catalog_entry/extent_catalog_entry.hpp"
#include "catalog/catalog_entry/chunkdefinition_catalog_entry.hpp"

#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"

#include "common/types.hpp"
#include "common/constants.hpp"
#include "common/enums/extent_type.hpp"

#include <string>
#include <vector>

namespace s62test {

using namespace duckdb;

// -----------------------------------------------------------------------------
// Common fixture data
// -----------------------------------------------------------------------------

// "Person" vertex: (name VARCHAR, age BIGINT)
static const std::vector<std::string> kPersonCols  = {"name", "age"};
static const std::vector<LogicalType>  kPersonTypes = {LogicalType::VARCHAR,
                                                       LogicalType::BIGINT};

// -----------------------------------------------------------------------------
// Low-level helpers — quick setup for per-entry unit tests
// -----------------------------------------------------------------------------

// Creates a graph and a partition. Uses graph->oid as the pid (test convention).
inline PartitionCatalogEntry *make_partition(TestDB             &db,
                                             const std::string  &gname,
                                             const std::string  &pname)
{
    auto &ctx = db.ctx();
    auto &cat = db.catalog();
    auto *schema = cat.GetSchema(ctx, DEFAULT_SCHEMA);

    if (!cat.GetEntry<GraphCatalogEntry>(ctx, DEFAULT_SCHEMA, gname, true)) {
        CreateGraphInfo gi;
        gi.schema = DEFAULT_SCHEMA; gi.graph = gname; gi.temporary = false;
        cat.CreateGraph(ctx, schema, &gi);
    }
    auto *gcat = cat.GetEntry<GraphCatalogEntry>(ctx, DEFAULT_SCHEMA, gname);

    CreatePartitionInfo pi;
    pi.schema = DEFAULT_SCHEMA; pi.partition = pname;
    pi.pid    = static_cast<PartitionID>(gcat->oid);
    pi.temporary = false;
    cat.CreatePartition(ctx, schema, &pi);
    return cat.GetEntry<PartitionCatalogEntry>(ctx, DEFAULT_SCHEMA, pname);
}

// Creates a PropertySchema under an existing partition.
inline PropertySchemaCatalogEntry *make_ps(TestDB              &db,
                                           PartitionCatalogEntry *part,
                                           const std::string   &ps_name)
{
    auto &ctx = db.ctx();
    auto &cat = db.catalog();
    auto *schema = cat.GetSchema(ctx, DEFAULT_SCHEMA);

    CreatePropertySchemaInfo psi;
    psi.schema        = DEFAULT_SCHEMA;
    psi.propertyschema = ps_name;
    psi.pid           = part->pid;
    psi.partition_oid = part->oid;
    psi.temporary     = false;
    return (PropertySchemaCatalogEntry *)cat.CreatePropertySchema(ctx, schema, &psi);
}

// Creates an Extent under an existing partition + PS.
inline ExtentCatalogEntry *make_extent(TestDB                     &db,
                                       PartitionCatalogEntry      *part,
                                       PropertySchemaCatalogEntry *ps,
                                       size_t                      num_tuples = 100)
{
    auto &ctx = db.ctx();
    auto &cat = db.catalog();
    auto *schema = cat.GetSchema(ctx, DEFAULT_SCHEMA);

    ExtentID eid = part->GetNewExtentID();
    CreateExtentInfo ei(DEFAULT_SCHEMA,
                        "ext_" + std::to_string(eid),
                        ExtentType::EXTENT,
                        eid, part->pid, ps->oid,
                        num_tuples);
    auto *ext = (ExtentCatalogEntry *)cat.CreateExtent(ctx, schema, &ei);
    ext->num_tuples_in_extent = num_tuples;
    return ext;
}

// Appends a ChunkDefinition to an Extent.
inline ChunkDefinitionCatalogEntry *make_chunkdef(TestDB             &db,
                                                   ExtentCatalogEntry *ext,
                                                   LogicalType         type,
                                                   size_t              num_entries = 0)
{
    auto &ctx = db.ctx();
    auto &cat = db.catalog();
    auto *schema = cat.GetSchema(ctx, DEFAULT_SCHEMA);

    std::string name = "cdf_" + std::to_string(ext->eid)
                     + "_"    + std::to_string(ext->chunks.size());
    CreateChunkDefinitionInfo ci(DEFAULT_SCHEMA, name, type);
    auto *cdf = (ChunkDefinitionCatalogEntry *)cat.CreateChunkDefinition(ctx, schema, &ci);
    if (num_entries > 0) cdf->SetNumEntriesInColumn(num_entries);
    ext->AddChunkDefinitionID(cdf->oid);
    return cdf;
}

// -----------------------------------------------------------------------------
// High-level builders — full schema setup for graph integration / persistence tests
// -----------------------------------------------------------------------------

// OIDs returned by build_vertex_schema
struct VertexSchema {
    idx_t       graph_oid;
    idx_t       partition_oid;
    idx_t       ps_oid;
    idx_t       extent_oid;
    PartitionID pid;
    ExtentID    eid;
    std::vector<idx_t> cdf_oids;  // one per column
};

// OIDs returned by build_edge_schema
struct EdgeSchema {
    idx_t       partition_oid;
    PartitionID pid;
    idx_t       src_partition_oid;
    idx_t       dst_partition_oid;
};

// Builds the full graph→partition→PS→extent→ChunkDef vertex schema chain.
// Reuses an existing graph if one with the same name already exists
// (supports multi-label scenarios).
inline VertexSchema build_vertex_schema(
        ClientContext                  &ctx,
        Catalog                        &cat,
        const std::string              &graph_name,
        const std::string              &label,
        const std::vector<std::string> &col_names,
        const std::vector<LogicalType> &col_types,
        size_t                          num_tuples = 100)
{
    VertexSchema vs{};
    auto *schema = cat.GetSchema(ctx, DEFAULT_SCHEMA);

    // 1. Graph (reuse if already exists) --------------------------------------
    auto *graph = cat.GetEntry<GraphCatalogEntry>(ctx, DEFAULT_SCHEMA, graph_name, true);
    if (!graph) {
        CreateGraphInfo gi;
        gi.schema = DEFAULT_SCHEMA; gi.graph = graph_name; gi.temporary = false;
        graph = (GraphCatalogEntry *)cat.CreateGraph(ctx, schema, &gi);
    }
    vs.graph_oid = graph->oid;
    vs.pid       = graph->GetNewPartitionID();

    // 2. Partition ------------------------------------------------------------
    CreatePartitionInfo pi;
    pi.schema    = DEFAULT_SCHEMA;
    pi.partition = "vpart_" + label;
    pi.pid       = vs.pid;
    pi.temporary = false;
    auto *part   = (PartitionCatalogEntry *)cat.CreatePartition(ctx, schema, &pi);
    vs.partition_oid = part->oid;

    // 3. PropertySchema -------------------------------------------------------
    CreatePropertySchemaInfo psi;
    psi.schema        = DEFAULT_SCHEMA;
    psi.propertyschema = "vps_" + label;
    psi.pid           = vs.pid;
    psi.partition_oid = part->oid;
    psi.temporary     = false;
    auto *ps    = (PropertySchemaCatalogEntry *)cat.CreatePropertySchema(ctx, schema, &psi);
    vs.ps_oid   = ps->oid;
    for (const auto &t : col_types)  ps->AppendType(t);
    for (const auto &n : col_names)  ps->AppendKey(ctx, n);

    // 4. Universal schema (register column info on the partition) -------------
    std::vector<std::string>   names_cp(col_names);
    std::vector<LogicalType>   types_cp(col_types);
    std::vector<PropertyKeyID> key_ids;
    graph->GetPropertyKeyIDs(ctx, names_cp, types_cp, key_ids);
    part->SetPartitionID(vs.pid);
    part->SetSchema(ctx, names_cp, types_cp, key_ids);
    part->AddPropertySchema(ctx, ps->oid, key_ids);

    // 5. Register vertex partition with the graph -----------------------------
    std::vector<std::string> labels_v = {label};
    graph->AddVertexPartition(ctx, vs.pid, part->oid, labels_v);

    // 6. Extent ---------------------------------------------------------------
    vs.eid = part->GetNewExtentID();
    CreateExtentInfo ei(DEFAULT_SCHEMA,
                        "ext_" + std::to_string(vs.eid),
                        ExtentType::EXTENT,
                        vs.eid, vs.pid, ps->oid,
                        num_tuples);
    auto *ext            = (ExtentCatalogEntry *)cat.CreateExtent(ctx, schema, &ei);
    vs.extent_oid        = ext->oid;
    ext->num_tuples_in_extent = num_tuples;
    ps->AddExtent(vs.eid, num_tuples);   // sets last_extent_num_tuples correctly

    // 7. ChunkDefinitions (one per column) ------------------------------------
    for (size_t i = 0; i < col_types.size(); ++i) {
        CreateChunkDefinitionInfo ci(DEFAULT_SCHEMA,
            "cdf_" + std::to_string(vs.eid) + "_" + std::to_string(i),
            col_types[i]);
        auto *cdf = (ChunkDefinitionCatalogEntry *)cat.CreateChunkDefinition(ctx, schema, &ci);
        cdf->SetNumEntriesInColumn(num_tuples);
        ext->AddChunkDefinitionID(cdf->oid);
        vs.cdf_oids.push_back(cdf->oid);
    }

    return vs;
}

// Builds an edge partition connecting two vertex partitions.
// The graph must already exist (call build_vertex_schema first).
inline EdgeSchema build_edge_schema(
        ClientContext      &ctx,
        Catalog            &cat,
        const std::string  &graph_name,
        const std::string  &edge_type,
        const VertexSchema &src_vs,
        const VertexSchema &dst_vs)
{
    EdgeSchema es{};
    auto *schema = cat.GetSchema(ctx, DEFAULT_SCHEMA);
    auto *graph  = cat.GetEntry<GraphCatalogEntry>(ctx, DEFAULT_SCHEMA, graph_name);

    es.pid               = graph->GetNewPartitionID();
    es.src_partition_oid = src_vs.partition_oid;
    es.dst_partition_oid = dst_vs.partition_oid;

    CreatePartitionInfo pi;
    pi.schema    = DEFAULT_SCHEMA;
    pi.partition = "epart_" + edge_type;
    pi.pid       = es.pid;
    pi.temporary = false;
    auto *part   = (PartitionCatalogEntry *)cat.CreatePartition(ctx, schema, &pi);
    es.partition_oid = part->oid;

    part->SetPartitionID(es.pid);
    part->SetSrcDstPartOid(src_vs.partition_oid, dst_vs.partition_oid);
    graph->AddEdgePartition(ctx, es.pid, part->oid, std::string(edge_type));
    graph->AddEdgeConnectionInfo(ctx, src_vs.partition_oid, part->oid);

    return es;
}

} // namespace s62test

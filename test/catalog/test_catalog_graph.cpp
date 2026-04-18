// =============================================================================
// [catalog][graph] Graph schema construction and cross-entry relationship tests
// =============================================================================
//
// Verifies that combined entries form the correct structure when assembled.
// Pure in-memory — no restart. Persistence is covered in test_catalog_persistence.cpp.
//
// Sections:
//   [vertex]  — vertex partition chain construction and map registration
//   [edge]    — edge partition construction and src/dst wiring
//   [xref]    — OID cross-reference integrity
//   [counter] — ID counter sequences
//   [multi]   — independence of multiple graphs
// =============================================================================

#include "catch.hpp"
#include "catalog_test_helpers.hpp"

#include "common/enums/graph_component_type.hpp"
#include <algorithm>
#include <string>

using namespace duckdb;
using namespace turbolynxtest;

// =============================================================================
// Vertex — vertex partition chain construction
// =============================================================================

TEST_CASE("Graph: build_vertex_schema creates a complete entry chain",
          "[catalog][graph][vertex]") {
    TestDB db;
    VertexSchema vs = build_vertex_schema(db.ctx(), db.catalog(),
                                          "social", "Person",
                                          kPersonCols, kPersonTypes, 200);

    // Graph exists
    auto *graph = db.catalog().GetEntry<GraphCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "social", true);
    REQUIRE(graph != nullptr);

    // Partition exists
    auto *part = db.catalog().GetEntry<PartitionCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "vpart_Person", true);
    REQUIRE(part != nullptr);
    REQUIRE(part->GetNumberOfColumns() == kPersonCols.size());

    // PropertySchema exists with correct column info
    auto *ps = db.catalog().GetEntry<PropertySchemaCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "vps_Person", true);
    REQUIRE(ps != nullptr);
    REQUIRE(ps->GetNumberOfColumns() == kPersonCols.size());
    for (size_t i = 0; i < kPersonCols.size(); ++i) {
        REQUIRE(ps->GetPropertyKeyName(i) == kPersonCols[i]);
    }

    // Extent exists with correct ChunkDef count
    auto *ext = (ExtentCatalogEntry *)db.catalog().GetEntry(
        db.ctx(), DEFAULT_SCHEMA, vs.extent_oid, true);
    REQUIRE(ext != nullptr);
    REQUIRE(ext->chunks.size() == kPersonCols.size());
    REQUIRE(ext->GetNumTuplesInExtent() == 200);

    // Each ChunkDef has the correct type
    for (size_t i = 0; i < vs.cdf_oids.size(); ++i) {
        auto *cdf = (ChunkDefinitionCatalogEntry *)db.catalog().GetEntry(
            db.ctx(), DEFAULT_SCHEMA, vs.cdf_oids[i], true);
        REQUIRE(cdf != nullptr);
        REQUIRE(cdf->data_type_id == kPersonTypes[i].id());
    }
}

TEST_CASE("Graph: AddVertexPartition registers label in vertexlabel_map and vertex_partitions",
          "[catalog][graph][vertex]") {
    TestDB db;
    VertexSchema vs = build_vertex_schema(db.ctx(), db.catalog(),
                                          "social", "Person",
                                          kPersonCols, kPersonTypes);

    auto *graph = db.catalog().GetEntry<GraphCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "social");

    // vertexlabel_map
    REQUIRE(graph->vertexlabel_map.count("Person") == 1);

    // vertex_partitions OID list
    REQUIRE(graph->vertex_partitions.size() == 1);
    REQUIRE(graph->vertex_partitions[0] == vs.partition_oid);

    // Also verifiable via LookupPartition API
    std::vector<idx_t> found = graph->LookupPartition(
        db.ctx(), {"Person"}, GraphComponentType::VERTEX);
    REQUIRE(found.size() == 1);
    REQUIRE(found[0] == vs.partition_oid);
}

TEST_CASE("Graph: GetPropertyKeyIDs registers keys in propertykey_map",
          "[catalog][graph][vertex]") {
    TestDB db;
    build_vertex_schema(db.ctx(), db.catalog(),
                        "social", "Person",
                        kPersonCols, kPersonTypes);

    auto *graph = db.catalog().GetEntry<GraphCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "social");

    for (const auto &col : kPersonCols) {
        REQUIRE(graph->propertykey_map.count(col) == 1);
    }
    // Reverse lookup via GetPropertyName
    PropertyKeyID pkid = graph->propertykey_map.at("name");
    REQUIRE(graph->GetPropertyName(db.ctx(), pkid) == "name");
}

TEST_CASE("Graph: same graph can hold multiple vertex labels independently",
          "[catalog][graph][vertex]") {
    TestDB db;
    VertexSchema person_vs = build_vertex_schema(db.ctx(), db.catalog(),
                                                 "social", "Person",
                                                 kPersonCols, kPersonTypes, 100);
    VertexSchema company_vs = build_vertex_schema(db.ctx(), db.catalog(),
                                                  "social", "Company",
                                                  {"cname", "founded"},
                                                  {LogicalType::VARCHAR, LogicalType::INTEGER},
                                                  50);

    auto *graph = db.catalog().GetEntry<GraphCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "social");

    // Both labels are registered
    REQUIRE(graph->vertexlabel_map.count("Person")  == 1);
    REQUIRE(graph->vertexlabel_map.count("Company") == 1);
    REQUIRE(graph->vertex_partitions.size() == 2);

    // Partition OIDs are distinct
    REQUIRE(person_vs.partition_oid != company_vs.partition_oid);

    // PS OIDs are also distinct
    REQUIRE(person_vs.ps_oid != company_vs.ps_oid);
}

// =============================================================================
// Edge — edge partition construction and src/dst wiring
// =============================================================================

TEST_CASE("Graph: build_edge_schema registers edge type in edgetype_map",
          "[catalog][graph][edge]") {
    TestDB db;
    VertexSchema person_vs  = build_vertex_schema(db.ctx(), db.catalog(),
                                                   "social", "Person",
                                                   kPersonCols, kPersonTypes);
    VertexSchema company_vs = build_vertex_schema(db.ctx(), db.catalog(),
                                                   "social", "Company",
                                                   {"cname"}, {LogicalType::VARCHAR});
    EdgeSchema es = build_edge_schema(db.ctx(), db.catalog(),
                                      "social", "WORKS_AT",
                                      person_vs, company_vs);

    auto *graph = db.catalog().GetEntry<GraphCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "social");

    REQUIRE(graph->edgetype_map.count("WORKS_AT") == 1);
    REQUIRE(graph->edge_partitions.size() == 1);
    REQUIRE(graph->edge_partitions[0] == es.partition_oid);
}

TEST_CASE("Graph: edge partition stores correct src/dst partition OIDs",
          "[catalog][graph][edge]") {
    TestDB db;
    VertexSchema person_vs  = build_vertex_schema(db.ctx(), db.catalog(),
                                                   "g_e", "Person",
                                                   kPersonCols, kPersonTypes);
    VertexSchema company_vs = build_vertex_schema(db.ctx(), db.catalog(),
                                                   "g_e", "Company",
                                                   {"cname"}, {LogicalType::VARCHAR});
    EdgeSchema es = build_edge_schema(db.ctx(), db.catalog(),
                                      "g_e", "KNOWS",
                                      person_vs, company_vs);

    auto *epart = (PartitionCatalogEntry *)db.catalog().GetEntry(
        db.ctx(), DEFAULT_SCHEMA, es.partition_oid, true);
    REQUIRE(epart != nullptr);
    REQUIRE(epart->GetSrcPartOid() == person_vs.partition_oid);
    REQUIRE(epart->GetDstPartOid() == company_vs.partition_oid);
}

TEST_CASE("Graph: AddEdgeConnectionInfo → GetConnectedEdgeOids returns edge partition",
          "[catalog][graph][edge]") {
    TestDB db;
    VertexSchema src = build_vertex_schema(db.ctx(), db.catalog(),
                                           "g_conn", "Src",
                                           {"val"}, {LogicalType::INTEGER});
    VertexSchema dst = build_vertex_schema(db.ctx(), db.catalog(),
                                           "g_conn", "Dst",
                                           {"val"}, {LogicalType::INTEGER});
    EdgeSchema es = build_edge_schema(db.ctx(), db.catalog(),
                                      "g_conn", "LINKS",
                                      src, dst);

    auto *graph = db.catalog().GetEntry<GraphCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "g_conn");

    std::vector<idx_t> connected;
    graph->GetConnectedEdgeOids(db.ctx(), src.partition_oid, connected);

    REQUIRE(connected.size() >= 1);
    REQUIRE(std::find(connected.begin(), connected.end(), es.partition_oid)
            != connected.end());
}

// =============================================================================
// Cross-reference — OID cross-reference integrity
// =============================================================================

TEST_CASE("Graph: graph→partition→PS→extent→cdf OID chain is intact",
          "[catalog][graph][xref]") {
    TestDB db;
    VertexSchema vs = build_vertex_schema(db.ctx(), db.catalog(),
                                          "g_xref", "Person",
                                          kPersonCols, kPersonTypes);
    auto &ctx = db.ctx();
    auto &cat = db.catalog();

    auto *graph = cat.GetEntry<GraphCatalogEntry>(ctx, DEFAULT_SCHEMA, "g_xref");
    auto *part  = cat.GetEntry<PartitionCatalogEntry>(ctx, DEFAULT_SCHEMA, "vpart_Person");
    auto *ps    = cat.GetEntry<PropertySchemaCatalogEntry>(ctx, DEFAULT_SCHEMA, "vps_Person");
    auto *ext   = (ExtentCatalogEntry *)cat.GetEntry(ctx, DEFAULT_SCHEMA, vs.extent_oid);

    // graph → partition
    REQUIRE(graph->vertex_partitions.size() >= 1);
    REQUIRE(graph->vertex_partitions[0] == part->oid);

    // partition → PS
    REQUIRE((*part->GetPropertySchemaIDs())[0] == ps->oid);

    // PS → extent count
    REQUIRE(ps->GetNumberOfExtents() == 1);

    // extent → ChunkDefs
    REQUIRE(ext->chunks.size() == vs.cdf_oids.size());
    for (size_t i = 0; i < vs.cdf_oids.size(); ++i) {
        REQUIRE(ext->chunks[i] == vs.cdf_oids[i]);
        auto *cdf = cat.GetEntry(ctx, DEFAULT_SCHEMA, vs.cdf_oids[i], true);
        REQUIRE(cdf != nullptr);
    }
}

TEST_CASE("Graph: partition pid matches graph-issued pid",
          "[catalog][graph][xref]") {
    TestDB db;
    VertexSchema vs = build_vertex_schema(db.ctx(), db.catalog(),
                                          "g_pid_check", "Node",
                                          {"val"}, {LogicalType::INTEGER});

    auto *part = db.catalog().GetEntry<PartitionCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "vpart_Node");
    REQUIRE(part->GetPartitionID() == vs.pid);
}

// =============================================================================
// Counters — ID counter sequences
// =============================================================================

TEST_CASE("Graph: GetNewPartitionID is strictly increasing per graph",
          "[catalog][graph][counter]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);
    CreateGraphInfo gi; gi.schema = DEFAULT_SCHEMA; gi.graph = "g_ctr"; gi.temporary = false;
    auto *graph = (GraphCatalogEntry *)db.catalog().CreateGraph(db.ctx(), schema, &gi);

    PartitionID p0 = graph->GetNewPartitionID();
    PartitionID p1 = graph->GetNewPartitionID();
    PartitionID p2 = graph->GetNewPartitionID();

    REQUIRE(p1 > p0);
    REQUIRE(p2 > p1);
}

TEST_CASE("Graph: GetNewExtentID is strictly increasing per partition",
          "[catalog][graph][counter]") {
    TestDB db;
    auto *pcat = make_partition(db, "g_eid_ctr", "part_eid");

    ExtentID e0 = pcat->GetNewExtentID();
    ExtentID e1 = pcat->GetNewExtentID();
    ExtentID e2 = pcat->GetNewExtentID();

    REQUIRE(e1 > e0);
    REQUIRE(e2 > e1);
}

TEST_CASE("Graph: GetPropertyKeyID (no-arg) is strictly increasing per graph",
          "[catalog][graph][counter]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);
    CreateGraphInfo gi; gi.schema = DEFAULT_SCHEMA; gi.graph = "g_keyid"; gi.temporary = false;
    auto *graph = (GraphCatalogEntry *)db.catalog().CreateGraph(db.ctx(), schema, &gi);

    PropertyKeyID k0 = graph->GetPropertyKeyID();
    PropertyKeyID k1 = graph->GetPropertyKeyID();
    PropertyKeyID k2 = graph->GetPropertyKeyID();

    REQUIRE(k1 > k0);
    REQUIRE(k2 > k1);
}


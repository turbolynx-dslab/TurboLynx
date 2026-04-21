// =============================================================================
// [catalog][persistence] Catalog persistence roundtrip tests
// =============================================================================
//
// TDD-style — written assuming SaveCatalog / LoadCatalog will be implemented.
//
// Assumed API:
//   Catalog::SaveCatalog()
//     → serializes the full catalog to {workspace}/catalog.bin
//   Catalog::GetCatalogVersion()
//     → returns the current catalog_version (accessor to be added)
//   TestDB(path) reopen
//     → DatabaseInstance::Initialize() auto-loads catalog.bin
//
// Pattern:
//   Phase 1: TestDB db1(tmp.path()) — build catalog → SaveCatalog()
//   Phase 2: TestDB db2(tmp.path()) — reopen → verify restored state
//
// Sections:
//   [entry]     — per-entry-type field roundtrip
//   [integrity] — OID cross-references and edge partition restoration
//   [counter]   — ID counter continuity (no collisions after restart)
//   [safety]    — catalog_version monotonicity, entries lost without save
// =============================================================================

#include "catch.hpp"
#include "catalog_test_helpers.hpp"
#include "catalog/catalog_entry/index_catalog_entry.hpp"
#include "parser/parsed_data/create_index_info.hpp"
#include "common/enums/index_type.hpp"

#include <algorithm>
#include <string>

using namespace duckdb;
using namespace turbolynxtest;

// =============================================================================
// Entry roundtrip — per-entry-type field restoration
// =============================================================================

TEST_CASE("Persistence: GraphCatalogEntry roundtrip",
          "[catalog][persistence][entry]") {
    ScopedTempDir tmp;
    VertexSchema vs;

    // Phase 1: build and save --------------------------------------------------
    {
        TestDB db1(tmp.path());
        vs = build_vertex_schema(db1.ctx(), db1.catalog(),
                                 "social", "Person",
                                 kPersonCols, kPersonTypes, 300);

        // Register an edge type so that edgetype_map restoration can be verified
        auto *graph = db1.catalog().GetEntry<GraphCatalogEntry>(
            db1.ctx(), DEFAULT_SCHEMA, "social");
        PartitionID edge_pid = graph->GetNewPartitionID();
        graph->AddEdgePartition(db1.ctx(), edge_pid, /*dummy oid*/9999,
                                std::string("KNOWS"));

        REQUIRE_NOTHROW(db1.catalog().SaveCatalog());
    }

    // Phase 2: reopen and verify -----------------------------------------------
    {
        TestDB db2(tmp.path());
        auto *graph = db2.catalog().GetEntry<GraphCatalogEntry>(
            db2.ctx(), DEFAULT_SCHEMA, "social", /*if_exists=*/true);

        REQUIRE(graph != nullptr);
        REQUIRE(graph->name == "social");
        REQUIRE(graph->vertexlabel_map.count("Person") == 1);
        REQUIRE(graph->edgetype_map.count("KNOWS")     == 1);
        REQUIRE(graph->propertykey_map.count("name")   == 1);
        REQUIRE(graph->propertykey_map.count("age")    == 1);
        REQUIRE(graph->vertex_partitions.size() >= 1);
        REQUIRE(graph->vertex_partitions[0] == vs.partition_oid);

        // Also verifiable via GetVertexLabels / GetEdgeTypes API
        std::vector<std::string> labels, types;
        graph->GetVertexLabels(labels);
        graph->GetEdgeTypes(types);
        REQUIRE(std::find(labels.begin(), labels.end(), "Person") != labels.end());
        REQUIRE(std::find(types.begin(),  types.end(),  "KNOWS")  != types.end());
    }
}

TEST_CASE("Persistence: PartitionCatalogEntry roundtrip",
          "[catalog][persistence][entry]") {
    ScopedTempDir tmp;
    VertexSchema vs;

    {
        TestDB db1(tmp.path());
        vs = build_vertex_schema(db1.ctx(), db1.catalog(),
                                 "g_part", "Person",
                                 kPersonCols, kPersonTypes);
        REQUIRE_NOTHROW(db1.catalog().SaveCatalog());
    }

    {
        TestDB db2(tmp.path());
        auto *part = db2.catalog().GetEntry<PartitionCatalogEntry>(
            db2.ctx(), DEFAULT_SCHEMA, "vpart_Person", /*if_exists=*/true);

        REQUIRE(part != nullptr);
        REQUIRE(part->name == "vpart_Person");
        REQUIRE(part->GetPartitionID() == vs.pid);
        REQUIRE(part->GetNumberOfColumns() == kPersonCols.size());

        // Column name list (order may vary)
        auto *names = part->GetUniversalPropertyKeyNames();
        REQUIRE(names != nullptr);
        for (const auto &col : kPersonCols) {
            REQUIRE(std::find(names->begin(), names->end(), col) != names->end());
        }

        // Column type IDs
        auto *typeids = part->GetUniversalPropertyTypeIds();
        REQUIRE(typeids != nullptr);
        REQUIRE((*typeids)[0] == LogicalTypeId::VARCHAR);
        REQUIRE((*typeids)[1] == LogicalTypeId::BIGINT);

        // PS OID list
        REQUIRE((*part->GetPropertySchemaIDs())[0] == vs.ps_oid);
    }
}

TEST_CASE("Persistence: multi-label partition representative label is stable",
          "[catalog][persistence][entry]") {
    ScopedTempDir tmp;

    {
        TestDB db1(tmp.path());
        auto &ctx = db1.ctx();
        auto &cat = db1.catalog();
        auto *schema = cat.GetSchema(ctx, DEFAULT_SCHEMA);

        CreateGraphInfo gi;
        gi.schema = DEFAULT_SCHEMA;
        gi.graph = "g_multi_label";
        gi.temporary = false;
        auto *graph = (GraphCatalogEntry *)cat.CreateGraph(ctx, schema, &gi);

        PartitionID pid = graph->GetNewPartitionID();
        CreatePartitionInfo pi;
        pi.schema = DEFAULT_SCHEMA;
        pi.partition = "vpart_City:Place";
        pi.pid = pid;
        pi.temporary = false;
        auto *part = (PartitionCatalogEntry *)cat.CreatePartition(ctx, schema, &pi);

        std::vector<std::string> labels = {"Place", "City"};
        graph->AddVertexPartition(ctx, pid, part->oid, labels);
        REQUIRE(graph->GetLabelFromVertexPartitionIndex(ctx, part->oid) == "City");

        REQUIRE_NOTHROW(db1.catalog().SaveCatalog());
    }

    {
        TestDB db2(tmp.path());
        auto *graph = db2.catalog().GetEntry<GraphCatalogEntry>(
            db2.ctx(), DEFAULT_SCHEMA, "g_multi_label", /*if_exists=*/true);
        auto *part = db2.catalog().GetEntry<PartitionCatalogEntry>(
            db2.ctx(), DEFAULT_SCHEMA, "vpart_City:Place", /*if_exists=*/true);

        REQUIRE(graph != nullptr);
        REQUIRE(part != nullptr);
        REQUIRE(graph->GetLabelFromVertexPartitionIndex(db2.ctx(), part->oid) ==
                "City");
    }
}

TEST_CASE("Persistence: IndexCatalogEntry roundtrip",
          "[catalog][persistence][entry]") {
    ScopedTempDir tmp;
    idx_t idx_oid;

    {
        TestDB db1(tmp.path());
        auto &ctx = db1.ctx();
        auto &cat = db1.catalog();
        auto *schema = cat.GetSchema(ctx, DEFAULT_SCHEMA);

        // Create a minimal partition + PS to give the index realistic OIDs
        auto *part = make_partition(db1, "g_idx", "epart_KNOWS");
        auto *ps   = make_ps(db1, part, "eps_KNOWS");

        // Create an adj list index (FORWARD_CSR)
        CreateIndexInfo ii(DEFAULT_SCHEMA, "KNOWS_fwd", IndexType::FORWARD_CSR,
                           part->GetOid(), ps->GetOid(),
                           /*adj_col_idx=*/3,
                           {1, 2} /*key_cols*/);
        auto *idx = (IndexCatalogEntry *)cat.CreateIndex(ctx, schema, &ii);
        idx_oid = idx->GetOid();

        REQUIRE_NOTHROW(cat.SaveCatalog());
    }

    {
        TestDB db2(tmp.path());
        auto &ctx = db2.ctx();
        auto &cat = db2.catalog();

        auto *idx = (IndexCatalogEntry *)cat.GetEntry(ctx, DEFAULT_SCHEMA, idx_oid);
        REQUIRE(idx != nullptr);
        REQUIRE(idx->name == "KNOWS_fwd");
        REQUIRE(idx->index_type == IndexType::FORWARD_CSR);
        REQUIRE(idx->GetAdjColIdx() == 3);

        auto *key_cols = idx->GetIndexKeyColumns();
        REQUIRE(key_cols != nullptr);
        REQUIRE(key_cols->size() == 2);
        REQUIRE((*key_cols)[0] == 1);
        REQUIRE((*key_cols)[1] == 2);

        // partition OID and PS OID are also restored
        REQUIRE(idx->GetPartitionID() > 0);
        REQUIRE(idx->GetPropertySchemaID() > 0);
    }
}

TEST_CASE("Persistence: PropertySchemaCatalogEntry roundtrip",
          "[catalog][persistence][entry]") {
    ScopedTempDir tmp;
    VertexSchema vs;

    {
        TestDB db1(tmp.path());
        vs = build_vertex_schema(db1.ctx(), db1.catalog(),
                                 "g_ps", "Person",
                                 kPersonCols, kPersonTypes, 200);
        REQUIRE_NOTHROW(db1.catalog().SaveCatalog());
    }

    {
        TestDB db2(tmp.path());
        auto *ps = db2.catalog().GetEntry<PropertySchemaCatalogEntry>(
            db2.ctx(), DEFAULT_SCHEMA, "vps_Person", /*if_exists=*/true);

        REQUIRE(ps != nullptr);
        REQUIRE(ps->name == "vps_Person");
        REQUIRE(ps->GetPartitionID()  == vs.pid);
        REQUIRE(ps->GetPartitionOID() == vs.partition_oid);

        // Column names — order must be preserved
        REQUIRE(ps->GetNumberOfColumns() == kPersonCols.size());
        for (size_t i = 0; i < kPersonCols.size(); ++i) {
            REQUIRE(ps->GetPropertyKeyName(i) == kPersonCols[i]);
        }

        REQUIRE(ps->GetNumberOfExtents() == 1);
        REQUIRE(ps->GetNumberOfRowsApproximately() == 200);
    }
}

TEST_CASE("Persistence: ExtentCatalogEntry and ChunkDefs roundtrip",
          "[catalog][persistence][entry]") {
    ScopedTempDir tmp;
    VertexSchema vs;

    {
        TestDB db1(tmp.path());
        vs = build_vertex_schema(db1.ctx(), db1.catalog(),
                                 "g_ext", "Person",
                                 kPersonCols, kPersonTypes, 512);
        REQUIRE_NOTHROW(db1.catalog().SaveCatalog());
    }

    {
        TestDB db2(tmp.path());
        auto &ctx = db2.ctx();
        auto &cat = db2.catalog();

        // Look up Extent by OID
        auto *ext = (ExtentCatalogEntry *)cat.GetEntry(
            ctx, DEFAULT_SCHEMA, vs.extent_oid, /*if_exists=*/true);
        REQUIRE(ext != nullptr);
        REQUIRE(ext->eid            == vs.eid);
        REQUIRE(ext->pid            == vs.pid);
        REQUIRE(ext->ps_oid         == vs.ps_oid);
        REQUIRE(ext->GetNumTuplesInExtent() == 512);

        // Chunk list — order must be preserved
        REQUIRE(ext->chunks.size() == kPersonCols.size());
        for (size_t i = 0; i < vs.cdf_oids.size(); ++i) {
            REQUIRE(ext->chunks[i] == vs.cdf_oids[i]);
        }

        // Each ChunkDefinition
        for (size_t i = 0; i < vs.cdf_oids.size(); ++i) {
            auto *cdf = (ChunkDefinitionCatalogEntry *)cat.GetEntry(
                ctx, DEFAULT_SCHEMA, vs.cdf_oids[i], /*if_exists=*/true);
            REQUIRE(cdf != nullptr);
            REQUIRE(cdf->data_type_id           == kPersonTypes[i].id());
            REQUIRE(cdf->GetNumEntriesInColumn() == 512);
        }
    }
}

// =============================================================================
// Integrity — OID cross-references and edge partition restoration
// =============================================================================

TEST_CASE("Persistence: OID cross-reference chain is intact after reload",
          "[catalog][persistence][integrity]") {
    ScopedTempDir tmp;
    VertexSchema vs;

    {
        TestDB db1(tmp.path());
        vs = build_vertex_schema(db1.ctx(), db1.catalog(),
                                 "g_xref", "Person",
                                 kPersonCols, kPersonTypes);
        REQUIRE_NOTHROW(db1.catalog().SaveCatalog());
    }

    {
        TestDB db2(tmp.path());
        auto &ctx = db2.ctx();
        auto &cat = db2.catalog();

        auto *graph = cat.GetEntry<GraphCatalogEntry>(ctx, DEFAULT_SCHEMA, "g_xref");
        auto *part  = cat.GetEntry<PartitionCatalogEntry>(ctx, DEFAULT_SCHEMA, "vpart_Person");
        auto *ps    = cat.GetEntry<PropertySchemaCatalogEntry>(ctx, DEFAULT_SCHEMA, "vps_Person");
        auto *ext   = (ExtentCatalogEntry *)cat.GetEntry(ctx, DEFAULT_SCHEMA, vs.extent_oid);

        REQUIRE(graph != nullptr);
        REQUIRE(part  != nullptr);
        REQUIRE(ps    != nullptr);
        REQUIRE(ext   != nullptr);

        // graph → partition
        REQUIRE(graph->vertex_partitions[0] == part->oid);
        // partition → PS
        REQUIRE((*part->GetPropertySchemaIDs())[0] == ps->oid);
        // PS extent count
        REQUIRE(ps->GetNumberOfExtents() == 1);
        // extent → ChunkDefs
        REQUIRE(ext->chunks.size() == vs.cdf_oids.size());
        for (size_t i = 0; i < vs.cdf_oids.size(); ++i) {
            REQUIRE(ext->chunks[i] == vs.cdf_oids[i]);
            REQUIRE(cat.GetEntry(ctx, DEFAULT_SCHEMA, vs.cdf_oids[i], true) != nullptr);
        }
    }
}

TEST_CASE("Persistence: edge partition src/dst OIDs restored correctly",
          "[catalog][persistence][integrity]") {
    ScopedTempDir tmp;
    VertexSchema src_vs, dst_vs;
    EdgeSchema   es;

    {
        TestDB db1(tmp.path());
        src_vs = build_vertex_schema(db1.ctx(), db1.catalog(),
                                     "g_edge", "Person",
                                     kPersonCols, kPersonTypes);
        dst_vs = build_vertex_schema(db1.ctx(), db1.catalog(),
                                     "g_edge", "Company",
                                     {"cname"}, {LogicalType::VARCHAR});
        es = build_edge_schema(db1.ctx(), db1.catalog(),
                               "g_edge", "WORKS_AT",
                               src_vs, dst_vs);
        REQUIRE_NOTHROW(db1.catalog().SaveCatalog());
    }

    {
        TestDB db2(tmp.path());
        auto &ctx = db2.ctx();
        auto &cat = db2.catalog();

        auto *graph = cat.GetEntry<GraphCatalogEntry>(ctx, DEFAULT_SCHEMA, "g_edge", true);
        REQUIRE(graph != nullptr);
        REQUIRE(graph->edgetype_map.count("WORKS_AT") == 1);
        REQUIRE(graph->edge_partitions.size() >= 1);

        auto *epart = (PartitionCatalogEntry *)cat.GetEntry(
            ctx, DEFAULT_SCHEMA, es.partition_oid, true);
        REQUIRE(epart != nullptr);
        REQUIRE(epart->GetSrcPartOid() == src_vs.partition_oid);
        REQUIRE(epart->GetDstPartOid() == dst_vs.partition_oid);

        // Look up connected edges from src
        std::vector<idx_t> connected;
        graph->GetConnectedEdgeOids(ctx, src_vs.partition_oid, connected);
        REQUIRE(std::find(connected.begin(), connected.end(), es.partition_oid)
                != connected.end());
    }
}

// =============================================================================
// Counter — ID counter continuity (no collisions after restart)
// =============================================================================

TEST_CASE("Persistence: ExtentID counter continues after reload",
          "[catalog][persistence][counter]") {
    ScopedTempDir tmp;
    VertexSchema vs;
    ExtentID     last_eid;

    {
        TestDB db1(tmp.path());
        vs = build_vertex_schema(db1.ctx(), db1.catalog(),
                                 "g_ctr", "Person",
                                 kPersonCols, kPersonTypes);
        auto *part = db1.catalog().GetEntry<PartitionCatalogEntry>(
            db1.ctx(), DEFAULT_SCHEMA, "vpart_Person");
        last_eid = vs.eid;   // the actually allocated eid; GetNewExtentID() after reload returns eid+1
        REQUIRE_NOTHROW(db1.catalog().SaveCatalog());
    }

    {
        TestDB db2(tmp.path());
        auto *part = db2.catalog().GetEntry<PartitionCatalogEntry>(
            db2.ctx(), DEFAULT_SCHEMA, "vpart_Person");
        REQUIRE(part != nullptr);
        REQUIRE(part->GetNewExtentID() > last_eid);
    }
}

TEST_CASE("Persistence: PartitionID counter continues after reload",
          "[catalog][persistence][counter]") {
    ScopedTempDir tmp;
    VertexSchema vs;

    {
        TestDB db1(tmp.path());
        vs = build_vertex_schema(db1.ctx(), db1.catalog(),
                                 "g_pctr", "Person",
                                 kPersonCols, kPersonTypes);
        REQUIRE_NOTHROW(db1.catalog().SaveCatalog());
    }

    {
        TestDB db2(tmp.path());
        auto *graph = db2.catalog().GetEntry<GraphCatalogEntry>(
            db2.ctx(), DEFAULT_SCHEMA, "g_pctr");
        REQUIRE(graph != nullptr);
        REQUIRE(graph->GetNewPartitionID() > vs.pid);
    }
}

// =============================================================================
// Safety — catalog_version monotonicity, entries lost without save
// =============================================================================

TEST_CASE("Persistence: catalog_version is non-decreasing across restarts",
          "[catalog][persistence][safety]") {
    ScopedTempDir tmp;
    idx_t version_before;

    {
        TestDB db1(tmp.path());
        build_vertex_schema(db1.ctx(), db1.catalog(),
                            "g_ver", "Node",
                            {"val"}, {LogicalType::INTEGER});
        db1.catalog().SaveCatalog();
        version_before = db1.catalog().GetCatalogVersion();
        REQUIRE(version_before > 0);
    }

    {
        TestDB db2(tmp.path());
        REQUIRE(db2.catalog().GetCatalogVersion() >= version_before);
    }
}

TEST_CASE("Persistence: without SaveCatalog entries are lost on restart",
          "[catalog][persistence][safety]") {
    ScopedTempDir tmp;

    {
        TestDB db1(tmp.path());
        build_vertex_schema(db1.ctx(), db1.catalog(),
                            "g_nosave", "Person",
                            kPersonCols, kPersonTypes);
        // SaveCatalog() intentionally not called
    }

    {
        TestDB db2(tmp.path());
        // Entries must be absent — if this fails, an implicit save is occurring
        auto *graph = db2.catalog().GetEntry<GraphCatalogEntry>(
            db2.ctx(), DEFAULT_SCHEMA, "g_nosave", /*if_exists=*/true);
        REQUIRE(graph == nullptr);
    }
}

TEST_CASE("Persistence: SaveCatalog fails on missing extent chunk definition and preserves prior catalog",
          "[catalog][persistence][safety]") {
    ScopedTempDir tmp;
    VertexSchema vs;

    {
        TestDB db1(tmp.path());
        vs = build_vertex_schema(db1.ctx(), db1.catalog(),
                                 "g_bad_cdf", "Person",
                                 kPersonCols, kPersonTypes, 128);
        REQUIRE_NOTHROW(db1.catalog().SaveCatalog());

        auto *ext = (ExtentCatalogEntry *)db1.catalog().GetEntry(
            db1.ctx(), DEFAULT_SCHEMA, vs.extent_oid, /*if_exists=*/true);
        REQUIRE(ext != nullptr);

        ext->AddChunkDefinitionID(999999999);
        REQUIRE_THROWS_AS(db1.catalog().SaveCatalog(), InternalException);
    }

    {
        TestDB db2(tmp.path());
        auto &ctx = db2.ctx();
        auto &cat = db2.catalog();

        auto *ext = (ExtentCatalogEntry *)cat.GetEntry(
            ctx, DEFAULT_SCHEMA, vs.extent_oid, /*if_exists=*/true);
        REQUIRE(ext != nullptr);
        REQUIRE(ext->chunks.size() == vs.cdf_oids.size());

        for (size_t i = 0; i < vs.cdf_oids.size(); ++i) {
            REQUIRE(ext->chunks[i] == vs.cdf_oids[i]);
            REQUIRE(cat.GetEntry(ctx, DEFAULT_SCHEMA, vs.cdf_oids[i], /*if_exists=*/true) != nullptr);
        }
    }
}

// =============================================================================
// [catalog][entry] Per-entry-type unit tests
// =============================================================================
//
// Verifies that each entry type is correctly created, retrieved, and that
// its fields are stored as expected.
// Cross-entry relationships and schema chains are covered in test_catalog_graph.cpp.
//
// Sections:
//   [schema]         — SchemaCatalogEntry
//   [graph]          — GraphCatalogEntry
//   [partition]      — PartitionCatalogEntry
//   [propertyschema] — PropertySchemaCatalogEntry
//   [extent]         — ExtentCatalogEntry
//   [chunkdef]       — ChunkDefinitionCatalogEntry
// =============================================================================

#include "catch.hpp"
#include "catalog_test_helpers.hpp"

#include "catalog/catalog_wrapper.hpp"
#include "catalog/catalog_entry/index_catalog_entry.hpp"
#include "catalog/catalog_entry/schema_catalog_entry.hpp"
#include "optimizer/orca/gpopt/tbgppdbwrappers.hpp"
#include "parser/parsed_data/create_index_info.hpp"
#include "parser/parsed_data/create_schema_info.hpp"

#include <algorithm>
#include <string>

using namespace duckdb;
using namespace turbolynxtest;

namespace {

struct ScopedOrcaCatalogWrapper {
    explicit ScopedOrcaCatalogWrapper(TestDB &db) {
        SetClientWrapper(db.ctx_ptr(), make_shared<CatalogWrapper>(*db.db().instance));
    }

    ~ScopedOrcaCatalogWrapper() {
        ReleaseClientWrapper();
    }
};

} // namespace

// =============================================================================
// Schema
// =============================================================================

TEST_CASE("Entry: default 'main' schema exists after init", "[catalog][entry][schema]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA, /*if_exists=*/true);
    REQUIRE(schema != nullptr);
    REQUIRE(schema->name == DEFAULT_SCHEMA);
}

TEST_CASE("Entry: create and retrieve a custom schema", "[catalog][entry][schema]") {
    TestDB db;
    CreateSchemaInfo info;
    info.schema = "analytics";

    auto *entry = db.catalog().CreateSchema(db.ctx(), &info);
    REQUIRE(entry != nullptr);

    auto *found = db.catalog().GetSchema(db.ctx(), "analytics", /*if_exists=*/true);
    REQUIRE(found != nullptr);
    REQUIRE(found->name == "analytics");
}

TEST_CASE("Entry: GetSchema returns nullptr for missing schema (if_exists=true)", "[catalog][entry][schema]") {
    TestDB db;
    auto *found = db.catalog().GetSchema(db.ctx(), "no_such_schema", /*if_exists=*/true);
    REQUIRE(found == nullptr);
}

TEST_CASE("Entry: ScanSchemas visits all created schemas", "[catalog][entry][schema]") {
    TestDB db;
    for (auto &name : {"alpha", "beta", "gamma"}) {
        CreateSchemaInfo info;
        info.schema = name;
        db.catalog().CreateSchema(db.ctx(), &info);
    }

    std::vector<std::string> seen;
    db.catalog().ScanSchemas(db.ctx(), [&](CatalogEntry *e) {
        seen.push_back(e->name);
    });

    REQUIRE(seen.size() >= 4);  // main + alpha + beta + gamma
    for (auto &name : {"main", "alpha", "beta", "gamma"}) {
        REQUIRE(std::find(seen.begin(), seen.end(), name) != seen.end());
    }
}

// =============================================================================
// Graph
// =============================================================================

TEST_CASE("Entry: create and retrieve a graph", "[catalog][entry][graph]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);

    CreateGraphInfo info;
    info.schema = DEFAULT_SCHEMA; info.graph = "social"; info.temporary = false;

    auto *entry = db.catalog().CreateGraph(db.ctx(), schema, &info);
    REQUIRE(entry != nullptr);

    auto *gcat = db.catalog().GetEntry<GraphCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "social", /*if_exists=*/true);
    REQUIRE(gcat != nullptr);
    REQUIRE(gcat->name == "social");
}

TEST_CASE("Entry: GetEntry returns nullptr for missing graph (if_exists=true)", "[catalog][entry][graph]") {
    TestDB db;
    auto *gcat = db.catalog().GetEntry<GraphCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "nonexistent", /*if_exists=*/true);
    REQUIRE(gcat == nullptr);
}

TEST_CASE("Entry: each catalog entry receives a unique OID", "[catalog][entry][graph]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);

    CreateGraphInfo gi1; gi1.schema = DEFAULT_SCHEMA; gi1.graph = "g1"; gi1.temporary = false;
    CreateGraphInfo gi2; gi2.schema = DEFAULT_SCHEMA; gi2.graph = "g2"; gi2.temporary = false;
    auto *e1 = db.catalog().CreateGraph(db.ctx(), schema, &gi1);
    auto *e2 = db.catalog().CreateGraph(db.ctx(), schema, &gi2);

    REQUIRE(e1 != nullptr);
    REQUIRE(e2 != nullptr);
    REQUIRE(e1->oid != e2->oid);
}

TEST_CASE("Entry: GetVertexLabels returns empty before any partition is registered", "[catalog][entry][graph]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);
    CreateGraphInfo gi; gi.schema = DEFAULT_SCHEMA; gi.graph = "empty_g"; gi.temporary = false;
    auto *graph = (GraphCatalogEntry *)db.catalog().CreateGraph(db.ctx(), schema, &gi);

    std::vector<std::string> labels;
    graph->GetVertexLabels(labels);
    REQUIRE(labels.empty());

    std::vector<std::string> types;
    graph->GetEdgeTypes(types);
    REQUIRE(types.empty());
}

TEST_CASE("Entry: GetNewPartitionID increments monotonically", "[catalog][entry][graph]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);
    CreateGraphInfo gi; gi.schema = DEFAULT_SCHEMA; gi.graph = "g_pid"; gi.temporary = false;
    auto *graph = (GraphCatalogEntry *)db.catalog().CreateGraph(db.ctx(), schema, &gi);

    PartitionID p0 = graph->GetNewPartitionID();
    PartitionID p1 = graph->GetNewPartitionID();
    PartitionID p2 = graph->GetNewPartitionID();

    REQUIRE(p1 == p0 + 1);
    REQUIRE(p2 == p0 + 2);
}

TEST_CASE("Entry: CreateIndexInfo copy preserves graph index metadata",
          "[catalog][entry][index]") {
    CreateIndexInfo info(DEFAULT_SCHEMA, "KNOWS_fwd", IndexType::FORWARD_CSR,
                         /*partition_oid=*/11, /*propertyschema_oid=*/22,
                         /*adj_col_idx=*/3, {1, 2});
    info.temporary = true;

    auto copy = info.Copy();
    auto &copied = (CreateIndexInfo &)*copy;

    REQUIRE(copied.schema == DEFAULT_SCHEMA);
    REQUIRE(copied.temporary == true);
    REQUIRE(copied.index_name == "KNOWS_fwd");
    REQUIRE(copied.index_type == IndexType::FORWARD_CSR);
    REQUIRE(copied.partition_oid == 11);
    REQUIRE(copied.propertyschema_oid == 22);
    REQUIRE(copied.adj_col_idx == 3);
    REQUIRE(copied.column_ids.size() == 2);
    REQUIRE(copied.column_ids[0] == 1);
    REQUIRE(copied.column_ids[1] == 2);
}

TEST_CASE("Entry: CreateIndexInfo default metadata uses safe sentinels",
          "[catalog][entry][index]") {
    CreateIndexInfo info;

    REQUIRE(info.partition_oid == 0);
    REQUIRE(info.propertyschema_oid == 0);
    REQUIRE(info.adj_col_idx == DConstants::INVALID_INDEX);
}

TEST_CASE("Entry: ORCA index metadata wrapper returns catalog index type",
          "[catalog][entry][index][orca]") {
    TestDB db;
    ScopedOrcaCatalogWrapper wrapper(db);

    auto &ctx = db.ctx();
    auto &cat = db.catalog();
    auto *schema = cat.GetSchema(ctx, DEFAULT_SCHEMA);

    auto *part = make_partition(db, "g_orca_idx", "epart_KNOWS");
    auto *ps   = make_ps(db, part, "eps_KNOWS");

    CreateIndexInfo csr_info(DEFAULT_SCHEMA, "KNOWS_fwd", IndexType::FORWARD_CSR,
                             part->GetOid(), ps->GetOid(),
                             /*adj_col_idx=*/3, {1, 2});
    auto *csr_index =
        (IndexCatalogEntry *)cat.CreateIndex(ctx, schema, &csr_info);
    REQUIRE(GetLogicalIndexType(csr_index->GetOid()) == IndexType::FORWARD_CSR);

    CreateIndexInfo pid_info(DEFAULT_SCHEMA, "KNOWS_pid", IndexType::PHYSICAL_ID,
                             part->GetOid(), ps->GetOid(),
                             DConstants::INVALID_INDEX, {1});
    auto *pid_index =
        (IndexCatalogEntry *)cat.CreateIndex(ctx, schema, &pid_info);
    REQUIRE(GetLogicalIndexType(pid_index->GetOid()) == IndexType::PHYSICAL_ID);
}

TEST_CASE("Entry: ORCA index metadata wrapper errors on unknown index oid",
          "[catalog][entry][index][orca]") {
    TestDB db;
    ScopedOrcaCatalogWrapper wrapper(db);

    REQUIRE_THROWS_AS(GetLogicalIndexType(999999999), InternalException);
}

// =============================================================================
// Partition
// =============================================================================

TEST_CASE("Entry: create and retrieve a partition", "[catalog][entry][partition]") {
    TestDB db;
    auto *pcat = make_partition(db, "g_part", "person");
    REQUIRE(pcat != nullptr);
    REQUIRE(pcat->name == "person");
}

TEST_CASE("Entry: multiple partitions are independently retrievable", "[catalog][entry][partition]") {
    TestDB db;
    auto *p1 = make_partition(db, "g_multi", "part_a");
    auto *p2 = make_partition(db, "g_multi", "part_b");
    auto *p3 = make_partition(db, "g_multi", "part_c");

    REQUIRE(p1 != nullptr); REQUIRE(p1->name == "part_a");
    REQUIRE(p2 != nullptr); REQUIRE(p2->name == "part_b");
    REQUIRE(p3 != nullptr); REQUIRE(p3->name == "part_c");
    REQUIRE(p1->oid != p2->oid);
    REQUIRE(p2->oid != p3->oid);
}

TEST_CASE("Entry: Partition GetNewExtentID increments sequentially", "[catalog][entry][partition]") {
    TestDB db;
    auto *pcat = make_partition(db, "g_eid", "part_eid");
    REQUIRE(pcat != nullptr);

    ExtentID e0 = pcat->GetNewExtentID();
    ExtentID e1 = pcat->GetNewExtentID();
    ExtentID e2 = pcat->GetNewExtentID();

    REQUIRE(e1 == e0 + 1);
    REQUIRE(e2 == e0 + 2);
}

TEST_CASE("Entry: Partition SetPartitionID / GetPartitionID roundtrip", "[catalog][entry][partition]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);
    CreateGraphInfo gi; gi.schema = DEFAULT_SCHEMA; gi.graph = "g_pid2"; gi.temporary = false;
    db.catalog().CreateGraph(db.ctx(), schema, &gi);

    CreatePartitionInfo pi;
    pi.schema = DEFAULT_SCHEMA; pi.partition = "part_pid"; pi.temporary = false;
    pi.pid = 42;   // explicit pid
    db.catalog().CreatePartition(db.ctx(), schema, &pi);

    auto *pcat = db.catalog().GetEntry<PartitionCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "part_pid");
    REQUIRE(pcat != nullptr);
    REQUIRE(pcat->GetPartitionID() == 42);
}

TEST_CASE("Entry: Partition GetNumberOfColumns after SetSchema", "[catalog][entry][partition]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);

    // Create graph + partition
    CreateGraphInfo gi; gi.schema = DEFAULT_SCHEMA; gi.graph = "g_cols"; gi.temporary = false;
    auto *graph = (GraphCatalogEntry *)db.catalog().CreateGraph(db.ctx(), schema, &gi);
    auto *pcat  = make_partition(db, "g_cols", "part_cols");
    pcat->SetPartitionID(graph->GetNewPartitionID());

    // Call SetSchema
    std::vector<std::string>   names_cp(kPersonCols);
    std::vector<LogicalType>   types_cp(kPersonTypes);
    std::vector<PropertyKeyID> key_ids;
    graph->GetPropertyKeyIDs(db.ctx(), names_cp, types_cp, key_ids);
    pcat->SetSchema(db.ctx(), names_cp, types_cp, key_ids);

    REQUIRE(pcat->GetNumberOfColumns() == kPersonCols.size());
}

// =============================================================================
// PropertySchema
// =============================================================================

TEST_CASE("Entry: create and retrieve a PropertySchema", "[catalog][entry][propertyschema]") {
    TestDB db;
    auto *pcat = make_partition(db, "g_ps", "part_ps");
    auto *ps   = make_ps(db, pcat, "ps_0");

    REQUIRE(ps != nullptr);
    REQUIRE(ps->name == "ps_0");
    REQUIRE(ps->GetPartitionID()  == pcat->pid);
    REQUIRE(ps->GetPartitionOID() == pcat->oid);
}

TEST_CASE("Entry: PropertySchema AppendType increments num_columns", "[catalog][entry][propertyschema]") {
    TestDB db;
    auto *pcat = make_partition(db, "g_pst", "part_pst");
    auto *ps   = make_ps(db, pcat, "ps_types");

    REQUIRE(ps->GetNumberOfColumns() == 0);

    ps->AppendType(LogicalType::INTEGER);
    REQUIRE(ps->GetNumberOfColumns() == 1);

    ps->AppendType(LogicalType::VARCHAR);
    ps->AppendType(LogicalType::DOUBLE);
    REQUIRE(ps->GetNumberOfColumns() == 3);

    // AppendKey only adds to the name list — does NOT change num_columns
    ps->AppendKey(db.ctx(), "id");
    REQUIRE(ps->GetNumberOfColumns() == 3);
}

TEST_CASE("Entry: PropertySchema AppendKey stores names in order", "[catalog][entry][propertyschema]") {
    TestDB db;
    auto *pcat = make_partition(db, "g_pskeys", "part_pskeys");
    auto *ps   = make_ps(db, pcat, "ps_keys");

    ps->AppendKey(db.ctx(), "first");
    ps->AppendKey(db.ctx(), "second");
    ps->AppendKey(db.ctx(), "third");

    REQUIRE(ps->GetPropertyKeyName(0) == "first");
    REQUIRE(ps->GetPropertyKeyName(1) == "second");
    REQUIRE(ps->GetPropertyKeyName(2) == "third");
}

TEST_CASE("Entry: PropertySchema AddExtent tracks count correctly", "[catalog][entry][propertyschema]") {
    TestDB db;
    auto *pcat = make_partition(db, "g_psext", "part_psext");
    auto *ps   = make_ps(db, pcat, "ps_ext");

    REQUIRE(ps->GetNumberOfExtents() == 0);

    auto *ext0 = make_extent(db, pcat, ps, 1024);
    ps->AddExtent(ext0->eid, ext0->num_tuples_in_extent);
    REQUIRE(ps->GetNumberOfExtents() == 1);

    auto *ext1 = make_extent(db, pcat, ps, 2048);
    ps->AddExtent(ext1->eid, ext1->num_tuples_in_extent);
    auto *ext2 = make_extent(db, pcat, ps, 512);
    ps->AddExtent(ext2->eid, ext2->num_tuples_in_extent);
    REQUIRE(ps->GetNumberOfExtents() == 3);
}

TEST_CASE("Entry: PropertySchema GetNumberOfRowsApproximately", "[catalog][entry][propertyschema]") {
    TestDB db;
    auto *pcat = make_partition(db, "g_psrows", "part_psrows");
    auto *ps   = make_ps(db, pcat, "ps_rows");

    REQUIRE(ps->GetNumberOfRowsApproximately() == 0);

    // 1 extent with 500 tuples
    auto *ext = make_extent(db, pcat, ps, 500);
    ps->AddExtent(ext->eid, ext->num_tuples_in_extent);
    REQUIRE(ps->GetNumberOfExtents() == 1);
    REQUIRE(ps->GetNumberOfRowsApproximately() == 500);
}

// =============================================================================
// Extent
// =============================================================================

TEST_CASE("Entry: create an extent and retrieve by OID", "[catalog][entry][extent]") {
    TestDB db;
    auto *pcat = make_partition(db, "g_ext", "part_ext");
    auto *ps   = make_ps(db, pcat, "ps_ext_e");
    auto *ext  = make_extent(db, pcat, ps, 256);

    REQUIRE(ext != nullptr);

    // Look up by OID
    auto *found = db.catalog().GetEntry(
        db.ctx(), DEFAULT_SCHEMA, ext->oid, /*if_exists=*/true);
    REQUIRE(found != nullptr);
    REQUIRE(found->oid == ext->oid);
}

TEST_CASE("Entry: Extent stores eid, pid, ps_oid and num_tuples", "[catalog][entry][extent]") {
    TestDB db;
    auto *pcat = make_partition(db, "g_ext2", "part_ext2");
    auto *ps   = make_ps(db, pcat, "ps_ext2");
    auto *ext  = make_extent(db, pcat, ps, 999);

    REQUIRE(ext->pid           == pcat->pid);
    REQUIRE(ext->ps_oid        == ps->oid);
    REQUIRE(ext->GetNumTuplesInExtent() == 999);
    // eid is issued by the partition-local counter and must be nonzero
    REQUIRE(ext->eid != 0);
}

TEST_CASE("Entry: Extent AddChunkDefinitionID populates chunks list", "[catalog][entry][extent]") {
    TestDB db;
    auto *pcat = make_partition(db, "g_ext3", "part_ext3");
    auto *ps   = make_ps(db, pcat, "ps_ext3");
    auto *ext  = make_extent(db, pcat, ps);

    REQUIRE(ext->chunks.empty());

    auto *cdf0 = make_chunkdef(db, ext, LogicalType::INTEGER, 100);
    REQUIRE(ext->chunks.size() == 1);
    REQUIRE(ext->chunks[0] == cdf0->oid);

    auto *cdf1 = make_chunkdef(db, ext, LogicalType::VARCHAR, 100);
    REQUIRE(ext->chunks.size() == 2);
    REQUIRE(ext->chunks[1] == cdf1->oid);
}

// =============================================================================
// ChunkDefinition
// =============================================================================

TEST_CASE("Entry: create a ChunkDefinition and retrieve by OID", "[catalog][entry][chunkdef]") {
    TestDB db;
    auto *pcat = make_partition(db, "g_cdf", "part_cdf");
    auto *ps   = make_ps(db, pcat, "ps_cdf");
    auto *ext  = make_extent(db, pcat, ps);
    auto *cdf  = make_chunkdef(db, ext, LogicalType::BIGINT, 512);

    REQUIRE(cdf != nullptr);

    auto *found = db.catalog().GetEntry(
        db.ctx(), DEFAULT_SCHEMA, cdf->oid, /*if_exists=*/true);
    REQUIRE(found != nullptr);
    REQUIRE(found->oid == cdf->oid);
}

TEST_CASE("Entry: ChunkDefinition stores data_type_id from LogicalType", "[catalog][entry][chunkdef]") {
    TestDB db;
    auto *pcat = make_partition(db, "g_cdf2", "part_cdf2");
    auto *ps   = make_ps(db, pcat, "ps_cdf2");
    auto *ext  = make_extent(db, pcat, ps);

    auto *cdf_i = make_chunkdef(db, ext, LogicalType::INTEGER);
    auto *cdf_v = make_chunkdef(db, ext, LogicalType::VARCHAR);
    auto *cdf_d = make_chunkdef(db, ext, LogicalType::DOUBLE);

    REQUIRE(cdf_i->data_type_id == LogicalTypeId::INTEGER);
    REQUIRE(cdf_v->data_type_id == LogicalTypeId::VARCHAR);
    REQUIRE(cdf_d->data_type_id == LogicalTypeId::DOUBLE);
}

TEST_CASE("Entry: ChunkDefinition SetNumEntriesInColumn / GetNumEntriesInColumn", "[catalog][entry][chunkdef]") {
    TestDB db;
    auto *pcat = make_partition(db, "g_cdf3", "part_cdf3");
    auto *ps   = make_ps(db, pcat, "ps_cdf3");
    auto *ext  = make_extent(db, pcat, ps);
    auto *cdf  = make_chunkdef(db, ext, LogicalType::INTEGER);

    cdf->SetNumEntriesInColumn(4096);
    REQUIRE(cdf->GetNumEntriesInColumn() == 4096);
}

TEST_CASE("Entry: ChunkDefinition default compression is COMPRESSION_AUTO", "[catalog][entry][chunkdef]") {
    TestDB db;
    auto *pcat = make_partition(db, "g_cdf4", "part_cdf4");
    auto *ps   = make_ps(db, pcat, "ps_cdf4");
    auto *ext  = make_extent(db, pcat, ps);
    auto *cdf  = make_chunkdef(db, ext, LogicalType::BIGINT);

    REQUIRE(cdf->compression_type == CompressionType::COMPRESSION_AUTO);
}

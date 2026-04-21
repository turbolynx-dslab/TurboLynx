// =============================================================================
// [storage] Storage catalog entry unit tests
// =============================================================================
// Tag: [storage]
//
// Tests extent and chunk definition catalog entries. These sit at the boundary
// between the catalog (metadata) and actual disk I/O — so tests here focus on
// the metadata/catalog layer; actual AIO/disk tests belong in integration tests.
//
// Uses TestDB for in-memory catalog setup.
// =============================================================================

#include "catch.hpp"
#include "test_helper.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/extent_catalog_entry.hpp"
#include "catalog/catalog_entry/chunkdefinition_catalog_entry.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "catalog/catalog_entry/property_schema_catalog_entry.hpp"
#define private public
#include "storage/extent/extent_iterator.hpp"
#undef private
#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "common/types.hpp"
#include "common/constants.hpp"

#include <limits>
#include <string>

using namespace duckdb;
using namespace turbolynxtest;

// ---------------------------------------------------------------------------
// Helper: create a minimal graph + partition in catalog
// ---------------------------------------------------------------------------
static PartitionCatalogEntry *setup_partition(TestDB &db, const std::string &graph_name,
                                              const std::string &part_name) {
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);

    CreateGraphInfo ginfo;
    ginfo.schema    = DEFAULT_SCHEMA;
    ginfo.graph     = graph_name;
    ginfo.temporary = false;
    db.catalog().CreateGraph(db.ctx(), schema, &ginfo);

    auto *gcat = db.catalog().GetEntry<GraphCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, graph_name);

    CreatePartitionInfo pinfo;
    pinfo.schema              = DEFAULT_SCHEMA;
    pinfo.partition           = part_name;
    pinfo.pid                 = static_cast<PartitionID>(gcat->oid);
    pinfo.temporary           = false;
    db.catalog().CreatePartition(db.ctx(), schema, &pinfo);

    return db.catalog().GetEntry<PartitionCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, part_name);
}

// Helper: create an extent in the given partition
static ExtentCatalogEntry *make_extent(TestDB &db, PartitionCatalogEntry *pcat,
                                       const std::string &name, ExtentID eid,
                                       size_t num_tuples = 0,
                                       ExtentType etype  = ExtentType::EXTENT) {
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);
    CreateExtentInfo einfo;
    einfo.schema               = DEFAULT_SCHEMA;
    einfo.extent               = name;
    einfo.temporary            = false;
    einfo.extent_type          = etype;
    einfo.eid                  = eid;
    einfo.pid                  = pcat->oid;
    einfo.ps_oid               = 0;
    einfo.num_tuples_in_extent = num_tuples;
    db.catalog().CreateExtent(db.ctx(), schema, &einfo);
    return db.catalog().GetEntry<ExtentCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, name);
}

// ===========================================================================
// Original tests (kept intact)
// ===========================================================================

TEST_CASE("Storage: create an extent catalog entry", "[storage][extent]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);
    auto *pcat   = setup_partition(db, "g_ext", "person_ext");
    REQUIRE(pcat != nullptr);

    CreateExtentInfo einfo;
    einfo.schema      = DEFAULT_SCHEMA;
    einfo.extent      = "ext_0";
    einfo.temporary   = false;
    einfo.extent_type = ExtentType::EXTENT;
    einfo.eid         = 0;
    einfo.pid         = pcat->oid;
    einfo.ps_oid      = 0;
    einfo.num_tuples_in_extent = 0;

    auto *eentry = db.catalog().CreateExtent(db.ctx(), schema, &einfo);
    REQUIRE(eentry != nullptr);

    auto *ecat = db.catalog().GetEntry<ExtentCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "ext_0", /*if_exists=*/true);
    REQUIRE(ecat != nullptr);
    REQUIRE(ecat->name == "ext_0");
    REQUIRE(ecat->extent_type == ExtentType::EXTENT);
    REQUIRE(ecat->pid == pcat->oid);
}

TEST_CASE("Storage: ExtentCatalogEntry chunk id tracking", "[storage][extent]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);
    auto *pcat   = setup_partition(db, "g_cid", "person_cid");

    CreateExtentInfo einfo;
    einfo.schema      = DEFAULT_SCHEMA;
    einfo.extent      = "ext_cid";
    einfo.temporary   = false;
    einfo.extent_type = ExtentType::EXTENT;
    einfo.eid         = 1;
    einfo.pid         = pcat->oid;
    einfo.ps_oid      = 0;
    einfo.num_tuples_in_extent = 0;
    db.catalog().CreateExtent(db.ctx(), schema, &einfo);

    auto *ecat = db.catalog().GetEntry<ExtentCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, "ext_cid");
    REQUIRE(ecat != nullptr);

    // GetNextChunkDefinitionID should increment from 0
    REQUIRE(ecat->GetNextChunkDefinitionID() == 0);
    REQUIRE(ecat->GetNextChunkDefinitionID() == 1);
    REQUIRE(ecat->GetNextChunkDefinitionID() == 2);
}

TEST_CASE("Storage: create a ChunkDefinition catalog entry", "[storage][chunk]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);
    auto *pcat   = setup_partition(db, "g_cdf", "person_cdf");

    // Create extent first
    CreateExtentInfo einfo;
    einfo.schema      = DEFAULT_SCHEMA;
    einfo.extent      = "ext_for_cdf";
    einfo.temporary   = false;
    einfo.extent_type = ExtentType::EXTENT;
    einfo.eid         = 0;
    einfo.pid         = pcat->oid;
    einfo.ps_oid      = 0;
    einfo.num_tuples_in_extent = 0;
    db.catalog().CreateExtent(db.ctx(), schema, &einfo);

    CreateChunkDefinitionInfo cinfo;
    cinfo.schema          = DEFAULT_SCHEMA;
    cinfo.chunkdefinition = "cdf_0";
    cinfo.temporary       = false;
    cinfo.l_type          = LogicalType::INTEGER;

    auto *centry = db.catalog().CreateChunkDefinition(db.ctx(), schema, &cinfo);
    REQUIRE(centry != nullptr);

    auto *ccat = db.catalog().GetEntry<ChunkDefinitionCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "cdf_0", /*if_exists=*/true);
    REQUIRE(ccat != nullptr);
    REQUIRE(ccat->name == "cdf_0");
}

// ===========================================================================
// Extent — new tests
// ===========================================================================

TEST_CASE("Storage: GetNextAdjListChunkDefinitionID is backward-growing from uint32_t max",
          "[storage][extent]") {
    // AdjList chunk IDs grow downward from UINT32_MAX so they never collide
    // with forward chunk IDs (which grow upward from 0).
    TestDB db;
    auto *pcat = setup_partition(db, "g_adj", "part_adj");
    auto *ecat = make_extent(db, pcat, "ext_adj", 0);
    REQUIRE(ecat != nullptr);

    LocalChunkDefinitionID max_val = std::numeric_limits<LocalChunkDefinitionID>::max();

    REQUIRE(ecat->GetNextAdjListChunkDefinitionID() == max_val);
    REQUIRE(ecat->GetNextAdjListChunkDefinitionID() == max_val - 1);
    REQUIRE(ecat->GetNextAdjListChunkDefinitionID() == max_val - 2);
}

TEST_CASE("Storage: forward and adjlist chunk ID spaces never overlap for small counts",
          "[storage][extent]") {
    // Forward IDs start at 0 and go up; adjlist IDs start at MAX and go down.
    // They should stay far apart for any realistic workload.
    TestDB db;
    auto *pcat = setup_partition(db, "g_nooverlap", "part_nooverlap");
    auto *ecat = make_extent(db, pcat, "ext_nooverlap", 0);

    for (int i = 0; i < 100; ++i) ecat->GetNextChunkDefinitionID();
    LocalChunkDefinitionID adj0 = ecat->GetNextAdjListChunkDefinitionID();

    // After 100 forward IDs the adjlist ID is still far from 100
    REQUIRE(adj0 > 1000000u);
}

TEST_CASE("Storage: AddChunkDefinitionID appends to chunks vector", "[storage][extent]") {
    TestDB db;
    auto *pcat = setup_partition(db, "g_addcdf", "part_addcdf");
    auto *ecat = make_extent(db, pcat, "ext_addcdf", 0);

    REQUIRE(ecat->chunks.empty());

    ecat->AddChunkDefinitionID(100);
    REQUIRE(ecat->chunks.size() == 1);
    REQUIRE(ecat->chunks[0] == 100);

    ecat->AddChunkDefinitionID(200);
    ecat->AddChunkDefinitionID(300);
    REQUIRE(ecat->chunks.size() == 3);
    REQUIRE(ecat->chunks[1] == 200);
    REQUIRE(ecat->chunks[2] == 300);
}

TEST_CASE("Storage: AddAdjListChunkDefinitionID appends to adjlist_chunks vector", "[storage][extent]") {
    TestDB db;
    auto *pcat = setup_partition(db, "g_addadj", "part_addadj");
    auto *ecat = make_extent(db, pcat, "ext_addadj", 0);

    REQUIRE(ecat->adjlist_chunks.empty());

    ecat->AddAdjListChunkDefinitionID(999);
    REQUIRE(ecat->adjlist_chunks.size() == 1);
    REQUIRE(ecat->adjlist_chunks[0] == 999);

    ecat->AddAdjListChunkDefinitionID(998);
    ecat->AddAdjListChunkDefinitionID(997);
    REQUIRE(ecat->adjlist_chunks.size() == 3);
}

TEST_CASE("Storage: SetExtentType changes the extent_type field", "[storage][extent]") {
    TestDB db;
    auto *pcat = setup_partition(db, "g_setext", "part_setext");
    auto *ecat = make_extent(db, pcat, "ext_setext", 0, 0, ExtentType::EXTENT);

    REQUIRE(ecat->extent_type == ExtentType::EXTENT);

    ecat->SetExtentType(ExtentType::DELTA);
    REQUIRE(ecat->extent_type == ExtentType::DELTA);

    // Can change back
    ecat->SetExtentType(ExtentType::EXTENT);
    REQUIRE(ecat->extent_type == ExtentType::EXTENT);
}

TEST_CASE("Storage: Extent created with DELTA type is stored correctly", "[storage][extent]") {
    TestDB db;
    auto *pcat = setup_partition(db, "g_delta", "part_delta");
    auto *ecat = make_extent(db, pcat, "ext_delta", 0, 0, ExtentType::DELTA);

    REQUIRE(ecat->extent_type == ExtentType::DELTA);
}

TEST_CASE("Storage: GetNumTuplesInExtent returns value set at creation", "[storage][extent]") {
    TestDB db;
    auto *pcat = setup_partition(db, "g_tuples", "part_tuples");

    auto *e0 = make_extent(db, pcat, "ext_tuples_0", 0, 0);
    REQUIRE(e0->GetNumTuplesInExtent() == 0);

    auto *e1 = make_extent(db, pcat, "ext_tuples_1", 1, 512);
    REQUIRE(e1->GetNumTuplesInExtent() == 512);

    auto *e2 = make_extent(db, pcat, "ext_tuples_2", 2, 65536);
    REQUIRE(e2->GetNumTuplesInExtent() == 65536);
}

TEST_CASE("Storage: multiple extents under same partition are independently retrievable",
          "[storage][extent]") {
    TestDB db;
    auto *pcat = setup_partition(db, "g_multiext", "part_multiext");

    // Create 5 extents with different eids and tuple counts
    for (int i = 0; i < 5; ++i) {
        make_extent(db, pcat, "mext_" + std::to_string(i), (ExtentID)i, (size_t)i * 100);
    }

    for (int i = 0; i < 5; ++i) {
        auto *ecat = db.catalog().GetEntry<ExtentCatalogEntry>(
            db.ctx(), DEFAULT_SCHEMA, "mext_" + std::to_string(i), /*if_exists=*/true);
        REQUIRE(ecat != nullptr);
        REQUIRE(ecat->eid == (ExtentID)i);
        REQUIRE(ecat->GetNumTuplesInExtent() == (size_t)i * 100);
        REQUIRE(ecat->pid == pcat->oid);
    }
}

TEST_CASE("Storage: ps_oid is stored correctly in ExtentCatalogEntry", "[storage][extent]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);
    auto *pcat   = setup_partition(db, "g_psoid", "part_psoid");

    CreateExtentInfo einfo;
    einfo.schema = DEFAULT_SCHEMA; einfo.extent = "ext_psoid"; einfo.temporary = false;
    einfo.extent_type = ExtentType::EXTENT; einfo.eid = 0;
    einfo.pid = pcat->oid; einfo.ps_oid = 42; einfo.num_tuples_in_extent = 0;
    db.catalog().CreateExtent(db.ctx(), schema, &einfo);

    auto *ecat = db.catalog().GetEntry<ExtentCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, "ext_psoid");
    REQUIRE(ecat->ps_oid == 42);
}

// ===========================================================================
// ChunkDefinition — new tests
// ===========================================================================

TEST_CASE("Storage: ChunkDefinition stores correct data_type_id for various types",
          "[storage][chunk]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);

    struct TC { const char *name; LogicalType type; LogicalTypeId expected; };
    TC cases[] = {
        {"cdf_int",    LogicalType::INTEGER, LogicalTypeId::INTEGER},
        {"cdf_bigint", LogicalType::BIGINT,  LogicalTypeId::BIGINT},
        {"cdf_float",  LogicalType::FLOAT,   LogicalTypeId::FLOAT},
        {"cdf_double", LogicalType::DOUBLE,  LogicalTypeId::DOUBLE},
    };

    for (auto &c : cases) {
        CreateChunkDefinitionInfo cinfo;
        cinfo.schema = DEFAULT_SCHEMA; cinfo.chunkdefinition = c.name;
        cinfo.temporary = false; cinfo.l_type = c.type;
        db.catalog().CreateChunkDefinition(db.ctx(), schema, &cinfo);

        auto *ccat = db.catalog().GetEntry<ChunkDefinitionCatalogEntry>(
            db.ctx(), DEFAULT_SCHEMA, c.name, /*if_exists=*/true);
        REQUIRE(ccat != nullptr);
        REQUIRE(ccat->data_type_id == c.expected);
    }
}

TEST_CASE("Storage: ChunkDefinition SetNumEntriesInColumn / GetNumEntriesInColumn round-trip",
          "[storage][chunk]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);

    CreateChunkDefinitionInfo cinfo;
    cinfo.schema = DEFAULT_SCHEMA; cinfo.chunkdefinition = "cdf_numentries";
    cinfo.temporary = false; cinfo.l_type = LogicalType::BIGINT;
    db.catalog().CreateChunkDefinition(db.ctx(), schema, &cinfo);

    auto *ccat = db.catalog().GetEntry<ChunkDefinitionCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "cdf_numentries");
    REQUIRE(ccat != nullptr);

    ccat->SetNumEntriesInColumn(1024);
    REQUIRE(ccat->GetNumEntriesInColumn() == 1024);

    ccat->SetNumEntriesInColumn(65536);
    REQUIRE(ccat->GetNumEntriesInColumn() == 65536);

    ccat->SetNumEntriesInColumn(0);
    REQUIRE(ccat->GetNumEntriesInColumn() == 0);
}

TEST_CASE("Storage: ChunkDefinition IsMinMaxArrayExist is false before creation",
          "[storage][chunk]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);

    CreateChunkDefinitionInfo cinfo;
    cinfo.schema = DEFAULT_SCHEMA; cinfo.chunkdefinition = "cdf_minmax";
    cinfo.temporary = false; cinfo.l_type = LogicalType::INTEGER;
    db.catalog().CreateChunkDefinition(db.ctx(), schema, &cinfo);

    auto *ccat = db.catalog().GetEntry<ChunkDefinitionCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "cdf_minmax");
    REQUIRE(ccat != nullptr);
    // Min/max array is created lazily; should not exist yet
    REQUIRE(ccat->IsMinMaxArrayExist() == false);
}

TEST_CASE("Storage: signed equality pruning keeps mixed-sign block",
          "[storage][chunk][pruning]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);

    CreateChunkDefinitionInfo cinfo;
    cinfo.schema = DEFAULT_SCHEMA;
    cinfo.chunkdefinition = "cdf_0";
    cinfo.temporary = false;
    cinfo.l_type = LogicalType::BIGINT;
    db.catalog().CreateChunkDefinition(db.ctx(), schema, &cinfo);

    auto *ccat = db.catalog().GetEntry<ChunkDefinitionCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "cdf_0");
    REQUIRE(ccat != nullptr);
    ccat->SetNumEntriesInColumn(2);
    ccat->min_max_array.push_back({-5, 5});
    ccat->is_min_max_array_exist = true;

    ExtentIterator it;
    it.current_idx_in_this_extent = 0;
    it.toggle = 0;
    it.num_tuples_in_current_extent[0] = 2;

    idx_t scan_start = 0, scan_end = 0;
    auto filter = Value::BIGINT(3);
    REQUIRE(it.getScanRange(db.ctx(), 0, filter, MIN_MAX_ARRAY_SIZE,
                            scan_start, scan_end));
    CHECK(scan_start == 0);
    CHECK(scan_end == 2);
}

TEST_CASE("Storage: signed range pruning keeps mixed-sign block",
          "[storage][chunk][pruning]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);

    CreateChunkDefinitionInfo cinfo;
    cinfo.schema = DEFAULT_SCHEMA;
    cinfo.chunkdefinition = "cdf_0";
    cinfo.temporary = false;
    cinfo.l_type = LogicalType::BIGINT;
    db.catalog().CreateChunkDefinition(db.ctx(), schema, &cinfo);

    auto *ccat = db.catalog().GetEntry<ChunkDefinitionCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "cdf_0");
    REQUIRE(ccat != nullptr);
    ccat->SetNumEntriesInColumn(2);
    ccat->min_max_array.push_back({-5, 5});
    ccat->is_min_max_array_exist = true;

    ExtentIterator it;
    it.current_idx_in_this_extent = 0;
    it.toggle = 0;
    it.num_tuples_in_current_extent[0] = 2;

    idx_t scan_start = 0, scan_end = 0;
    auto lower = Value::BIGINT(-3);
    auto upper = Value::BIGINT(4);
    REQUIRE(it.getScanRange(db.ctx(), 0, lower, upper, true, true,
                            MIN_MAX_ARRAY_SIZE, scan_start, scan_end));
    CHECK(scan_start == 0);
    CHECK(scan_end == 2);
}

TEST_CASE("Storage: signed INTEGER equality pruning keeps mixed-sign block",
          "[storage][chunk][pruning]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);

    CreateChunkDefinitionInfo cinfo;
    cinfo.schema = DEFAULT_SCHEMA;
    cinfo.chunkdefinition = "cdf_0";
    cinfo.temporary = false;
    cinfo.l_type = LogicalType::INTEGER;
    db.catalog().CreateChunkDefinition(db.ctx(), schema, &cinfo);

    auto *ccat = db.catalog().GetEntry<ChunkDefinitionCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "cdf_0");
    REQUIRE(ccat != nullptr);
    ccat->SetNumEntriesInColumn(2);
    ccat->min_max_array.push_back({-5, 5});
    ccat->is_min_max_array_exist = true;

    ExtentIterator it;
    it.current_idx_in_this_extent = 0;
    it.toggle = 0;
    it.num_tuples_in_current_extent[0] = 2;

    idx_t scan_start = 0, scan_end = 0;
    auto filter = Value::INTEGER(-3);
    REQUIRE(it.getScanRange(db.ctx(), 0, filter, MIN_MAX_ARRAY_SIZE,
                            scan_start, scan_end));
    CHECK(scan_start == 0);
    CHECK(scan_end == 2);
}

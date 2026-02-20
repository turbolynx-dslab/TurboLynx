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
#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "common/types.hpp"

using namespace duckdb;
using namespace s62test;

// ---------------------------------------------------------------------------
// Helper: create a minimal graph + partition + property_schema in catalog
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

// ---------------------------------------------------------------------------
TEST_CASE("Storage: create an extent catalog entry", "[storage][extent]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);
    auto *pcat   = setup_partition(db, "g_ext", "person_ext");
    REQUIRE(pcat != nullptr);

    // GIVEN: an extent info
    CreateExtentInfo einfo;
    einfo.schema      = DEFAULT_SCHEMA;
    einfo.extent      = "ext_0";
    einfo.temporary   = false;
    einfo.extent_type = ExtentType::EXTENT;
    einfo.eid         = 0;
    einfo.pid         = pcat->oid;
    einfo.ps_oid      = 0;
    einfo.num_tuples_in_extent = 0;

    // WHEN
    auto *eentry = db.catalog().CreateExtent(db.ctx(), schema, &einfo);
    REQUIRE(eentry != nullptr);

    // THEN: retrieve by name
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

    // GIVEN: a chunk definition
    CreateChunkDefinitionInfo cinfo;
    cinfo.schema          = DEFAULT_SCHEMA;
    cinfo.chunkdefinition = "cdf_0";
    cinfo.temporary       = false;
    cinfo.l_type          = LogicalType::INTEGER;

    // WHEN
    auto *centry = db.catalog().CreateChunkDefinition(db.ctx(), schema, &cinfo);
    REQUIRE(centry != nullptr);

    // THEN
    auto *ccat = db.catalog().GetEntry<ChunkDefinitionCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "cdf_0", /*if_exists=*/true);
    REQUIRE(ccat != nullptr);
    REQUIRE(ccat->name == "cdf_0");
}

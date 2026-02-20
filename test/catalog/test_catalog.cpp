// =============================================================================
// [catalog] Catalog & Schema unit tests
// =============================================================================
// Tag: [catalog]
//
// Tests the S62GDB catalog layer: schema management, graph/partition/property
// schema creation and retrieval. Uses TestDB (in-memory DuckDB instance).
//
// Each TEST_CASE creates a fresh TestDB → complete isolation, no shared state.
// =============================================================================

#include "catch.hpp"
#include "test_helper.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/schema_catalog_entry.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "catalog/catalog_entry/property_schema_catalog_entry.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"

using namespace duckdb;
using namespace s62test;

// ---------------------------------------------------------------------------
// Schema tests
// ---------------------------------------------------------------------------
TEST_CASE("Catalog: default 'main' schema exists after init", "[catalog][schema]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA, /*if_exists=*/true);
    REQUIRE(schema != nullptr);
    REQUIRE(schema->name == DEFAULT_SCHEMA);
}

TEST_CASE("Catalog: create and retrieve a custom schema", "[catalog][schema]") {
    TestDB db;

    // GIVEN: a new schema info
    CreateSchemaInfo info;
    info.schema = "test_schema";

    // WHEN: we create it
    auto *entry = db.catalog().CreateSchema(db.ctx(), &info);
    REQUIRE(entry != nullptr);

    // THEN: we can retrieve it by name
    auto *found = db.catalog().GetSchema(db.ctx(), "test_schema", /*if_exists=*/true);
    REQUIRE(found != nullptr);
    REQUIRE(found->name == "test_schema");
}

TEST_CASE("Catalog: GetSchema returns nullptr for missing schema (if_exists=true)", "[catalog][schema]") {
    TestDB db;
    auto *found = db.catalog().GetSchema(db.ctx(), "nonexistent_xyz", /*if_exists=*/true);
    REQUIRE(found == nullptr);
}

TEST_CASE("Catalog: ScanSchemas visits all schemas", "[catalog][schema]") {
    TestDB db;

    // Create two extra schemas
    for (auto &name : {"s1", "s2"}) {
        CreateSchemaInfo info;
        info.schema = name;
        db.catalog().CreateSchema(db.ctx(), &info);
    }

    std::vector<std::string> seen;
    db.catalog().ScanSchemas(db.ctx(), [&](CatalogEntry *e) {
        seen.push_back(e->name);
    });

    // Should contain at least 'main', 's1', 's2'
    REQUIRE(seen.size() >= 3);
    REQUIRE(std::find(seen.begin(), seen.end(), "main") != seen.end());
    REQUIRE(std::find(seen.begin(), seen.end(), "s1")   != seen.end());
    REQUIRE(std::find(seen.begin(), seen.end(), "s2")   != seen.end());
}

// ---------------------------------------------------------------------------
// Graph tests
// ---------------------------------------------------------------------------
TEST_CASE("Catalog: create and retrieve a graph", "[catalog][graph]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);

    // GIVEN: a CreateGraphInfo
    CreateGraphInfo info;
    info.schema   = DEFAULT_SCHEMA;
    info.graph    = "test_graph";
    info.temporary = false;

    // WHEN: create graph
    auto *entry = db.catalog().CreateGraph(db.ctx(), schema, &info);
    REQUIRE(entry != nullptr);

    // THEN: retrieve by name
    auto *gcat = db.catalog().GetEntry<GraphCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, "test_graph", /*if_exists=*/true);
    REQUIRE(gcat != nullptr);
    REQUIRE(gcat->name == "test_graph");
}

TEST_CASE("Catalog: GetEntry returns nullptr for missing graph (if_exists=true)", "[catalog][graph]") {
    TestDB db;
    auto *gcat = db.catalog().GetEntry<GraphCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "no_such_graph", /*if_exists=*/true);
    REQUIRE(gcat == nullptr);
}

// ---------------------------------------------------------------------------
// Partition tests
// ---------------------------------------------------------------------------
TEST_CASE("Catalog: create a partition under a graph", "[catalog][partition]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);

    // Create graph first
    CreateGraphInfo ginfo;
    ginfo.schema   = DEFAULT_SCHEMA;
    ginfo.graph    = "g1";
    ginfo.temporary = false;
    db.catalog().CreateGraph(db.ctx(), schema, &ginfo);

    auto *gcat = db.catalog().GetEntry<GraphCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, "g1");
    REQUIRE(gcat != nullptr);

    // Create partition
    CreatePartitionInfo pinfo;
    pinfo.schema         = DEFAULT_SCHEMA;
    pinfo.partition      = "person";
    pinfo.pid            = static_cast<PartitionID>(gcat->oid);
    pinfo.temporary      = false;

    auto *pentry = db.catalog().CreatePartition(db.ctx(), schema, &pinfo);
    REQUIRE(pentry != nullptr);

    auto *pcat = db.catalog().GetEntry<PartitionCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "person", /*if_exists=*/true);
    REQUIRE(pcat != nullptr);
    REQUIRE(pcat->name == "person");
}

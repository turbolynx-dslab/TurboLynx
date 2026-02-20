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
#include "common/types.hpp"

#include <algorithm>

using namespace duckdb;
using namespace s62test;

// ---------------------------------------------------------------------------
// Internal helper: create graph + partition in one shot
// ---------------------------------------------------------------------------
static PartitionCatalogEntry *make_partition(TestDB &db, const std::string &gname,
                                             const std::string &pname) {
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);
    CreateGraphInfo gi;
    gi.schema = DEFAULT_SCHEMA; gi.graph = gname; gi.temporary = false;
    db.catalog().CreateGraph(db.ctx(), schema, &gi);
    auto *gcat = db.catalog().GetEntry<GraphCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, gname);

    CreatePartitionInfo pi;
    pi.schema = DEFAULT_SCHEMA; pi.partition = pname; pi.temporary = false;
    pi.pid = static_cast<PartitionID>(gcat->oid);
    db.catalog().CreatePartition(db.ctx(), schema, &pi);
    return db.catalog().GetEntry<PartitionCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, pname);
}

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

TEST_CASE("Catalog: each catalog entry gets a unique OID", "[catalog][graph]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);

    // Create two separate graphs
    CreateGraphInfo gi1; gi1.schema = DEFAULT_SCHEMA; gi1.graph = "g_oid1"; gi1.temporary = false;
    CreateGraphInfo gi2; gi2.schema = DEFAULT_SCHEMA; gi2.graph = "g_oid2"; gi2.temporary = false;
    auto *e1 = db.catalog().CreateGraph(db.ctx(), schema, &gi1);
    auto *e2 = db.catalog().CreateGraph(db.ctx(), schema, &gi2);

    REQUIRE(e1 != nullptr);
    REQUIRE(e2 != nullptr);
    // OIDs must be distinct across all catalog entries
    REQUIRE(e1->oid != e2->oid);
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

TEST_CASE("Catalog: multiple partitions under one graph are independently retrievable", "[catalog][partition]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);

    CreateGraphInfo ginfo;
    ginfo.schema = DEFAULT_SCHEMA; ginfo.graph = "g_multi"; ginfo.temporary = false;
    db.catalog().CreateGraph(db.ctx(), schema, &ginfo);
    auto *gcat = db.catalog().GetEntry<GraphCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, "g_multi");

    // Create 3 partitions with distinct names
    for (auto &pname : {"person", "comment", "post"}) {
        CreatePartitionInfo pi;
        pi.schema = DEFAULT_SCHEMA; pi.partition = pname; pi.temporary = false;
        pi.pid = static_cast<PartitionID>(gcat->oid);
        db.catalog().CreatePartition(db.ctx(), schema, &pi);
    }

    auto *p1 = db.catalog().GetEntry<PartitionCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, "person",  true);
    auto *p2 = db.catalog().GetEntry<PartitionCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, "comment", true);
    auto *p3 = db.catalog().GetEntry<PartitionCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, "post",    true);

    REQUIRE(p1 != nullptr); REQUIRE(p1->name == "person");
    REQUIRE(p2 != nullptr); REQUIRE(p2->name == "comment");
    REQUIRE(p3 != nullptr); REQUIRE(p3->name == "post");
    // Each gets a distinct catalog OID
    REQUIRE(p1->oid != p2->oid);
    REQUIRE(p2->oid != p3->oid);
}

TEST_CASE("Catalog: Partition GetNewExtentID increments sequentially", "[catalog][partition]") {
    TestDB db;
    auto *pcat = make_partition(db, "g_eid", "part_eid");
    REQUIRE(pcat != nullptr);

    // GetNewExtentID = (pid << 16) + local_counter++
    // Successive calls differ by exactly 1
    ExtentID e0 = pcat->GetNewExtentID();
    ExtentID e1 = pcat->GetNewExtentID();
    ExtentID e2 = pcat->GetNewExtentID();

    REQUIRE(e1 == e0 + 1);
    REQUIRE(e2 == e0 + 2);
}

TEST_CASE("Catalog: Partition GetPartitionID returns its pid", "[catalog][partition]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);

    CreateGraphInfo gi; gi.schema = DEFAULT_SCHEMA; gi.graph = "g_pid"; gi.temporary = false;
    db.catalog().CreateGraph(db.ctx(), schema, &gi);

    CreatePartitionInfo pi;
    pi.schema = DEFAULT_SCHEMA; pi.partition = "part_pid"; pi.temporary = false;
    pi.pid = 42;   // explicit, known pid
    db.catalog().CreatePartition(db.ctx(), schema, &pi);

    auto *pcat = db.catalog().GetEntry<PartitionCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, "part_pid");
    REQUIRE(pcat != nullptr);
    REQUIRE(pcat->GetPartitionID() == 42);
}

// ---------------------------------------------------------------------------
// PropertySchema tests
// ---------------------------------------------------------------------------
TEST_CASE("Catalog: create and retrieve a PropertySchema", "[catalog][propertyschema]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);
    auto *pcat   = make_partition(db, "g_ps", "person_ps");

    CreatePropertySchemaInfo psinfo;
    psinfo.schema         = DEFAULT_SCHEMA;
    psinfo.propertyschema = "ps_0";
    psinfo.pid            = pcat->pid;
    psinfo.partition_oid  = pcat->oid;
    psinfo.temporary      = false;

    auto *psentry = db.catalog().CreatePropertySchema(db.ctx(), schema, &psinfo);
    REQUIRE(psentry != nullptr);

    auto *pscat = db.catalog().GetEntry<PropertySchemaCatalogEntry>(
        db.ctx(), DEFAULT_SCHEMA, "ps_0", /*if_exists=*/true);
    REQUIRE(pscat != nullptr);
    REQUIRE(pscat->name == "ps_0");
    // Must remember which partition it belongs to
    REQUIRE(pscat->GetPartitionID()  == pcat->pid);
    REQUIRE(pscat->GetPartitionOID() == pcat->oid);
}

TEST_CASE("Catalog: PropertySchema.AppendType increments num_columns; AppendKey does not", "[catalog][propertyschema]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);
    auto *pcat   = make_partition(db, "g_pst", "part_pst");

    CreatePropertySchemaInfo psinfo;
    psinfo.schema = DEFAULT_SCHEMA; psinfo.propertyschema = "ps_types";
    psinfo.pid = pcat->pid; psinfo.partition_oid = pcat->oid; psinfo.temporary = false;
    db.catalog().CreatePropertySchema(db.ctx(), schema, &psinfo);
    auto *pscat = db.catalog().GetEntry<PropertySchemaCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, "ps_types");

    // Fresh: no columns
    REQUIRE(pscat->GetNumberOfColumns() == 0);

    // Each AppendType adds one column
    pscat->AppendType(LogicalType::INTEGER);
    REQUIRE(pscat->GetNumberOfColumns() == 1);

    pscat->AppendType(LogicalType::BIGINT);
    pscat->AppendType(LogicalType::FLOAT);
    REQUIRE(pscat->GetNumberOfColumns() == 3);

    // AppendKey only adds to the name list — does NOT change num_columns
    pscat->AppendKey(db.ctx(), "id");
    pscat->AppendKey(db.ctx(), "age");
    REQUIRE(pscat->GetNumberOfColumns() == 3);

    // Key names are stored in order
    REQUIRE(pscat->GetPropertyKeyName(0) == "id");
    REQUIRE(pscat->GetPropertyKeyName(1) == "age");
}

TEST_CASE("Catalog: PropertySchema.AddExtent tracks extent count correctly", "[catalog][propertyschema]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);
    auto *pcat   = make_partition(db, "g_psext", "part_psext");

    CreatePropertySchemaInfo psinfo;
    psinfo.schema = DEFAULT_SCHEMA; psinfo.propertyschema = "ps_ext";
    psinfo.pid = pcat->pid; psinfo.partition_oid = pcat->oid; psinfo.temporary = false;
    db.catalog().CreatePropertySchema(db.ctx(), schema, &psinfo);
    auto *pscat = db.catalog().GetEntry<PropertySchemaCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, "ps_ext");

    REQUIRE(pscat->GetNumberOfExtents() == 0);

    pscat->AddExtent(/*eid=*/0, /*num_tuples=*/1024);
    REQUIRE(pscat->GetNumberOfExtents() == 1);

    pscat->AddExtent(1, 2048);
    pscat->AddExtent(2, 512);
    REQUIRE(pscat->GetNumberOfExtents() == 3);
}

TEST_CASE("Catalog: PropertySchema GetNumberOfRowsApproximately", "[catalog][propertyschema]") {
    TestDB db;
    auto *schema = db.catalog().GetSchema(db.ctx(), DEFAULT_SCHEMA);
    auto *pcat   = make_partition(db, "g_psrows", "part_psrows");

    CreatePropertySchemaInfo psinfo;
    psinfo.schema = DEFAULT_SCHEMA; psinfo.propertyschema = "ps_rows";
    psinfo.pid = pcat->pid; psinfo.partition_oid = pcat->oid; psinfo.temporary = false;
    db.catalog().CreatePropertySchema(db.ctx(), schema, &psinfo);
    auto *pscat = db.catalog().GetEntry<PropertySchemaCatalogEntry>(db.ctx(), DEFAULT_SCHEMA, "ps_rows");

    // No extents: 0 rows
    REQUIRE(pscat->GetNumberOfRowsApproximately() == 0);

    // With one extent of 500 tuples — last_extent_num_tuples is used directly
    pscat->AddExtent(0, 500);
    REQUIRE(pscat->GetNumberOfExtents() == 1);
    // Single extent: all rows come from last_extent_num_tuples
    REQUIRE(pscat->GetNumberOfRowsApproximately() == 500);
}

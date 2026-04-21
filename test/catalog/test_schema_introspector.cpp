#include "catch.hpp"
#include "catalog_test_helpers.hpp"
#include "nl2cypher/schema_introspector.hpp"
#include "parser/parsed_data/create_partition_info.hpp"

using namespace duckdb;
using namespace turbolynxtest;
using namespace turbolynx::nl2cypher;

namespace {

PartitionCatalogEntry* CreateNamedEdgePartition(TestDB& db, GraphCatalogEntry* graph,
                                                const std::string& partition_name,
                                                const std::string& edge_type,
                                                idx_t src_partition_oid,
                                                idx_t dst_partition_oid) {
    auto& ctx = db.ctx();
    auto& cat = db.catalog();
    auto* schema = cat.GetSchema(ctx, DEFAULT_SCHEMA);

    auto pid = graph->GetNewPartitionID();
    CreatePartitionInfo pi;
    pi.schema = DEFAULT_SCHEMA;
    pi.partition = partition_name;
    pi.pid = pid;
    pi.temporary = false;

    auto* part = (PartitionCatalogEntry*)cat.CreatePartition(ctx, schema, &pi);
    part->SetPartitionID(pid);
    part->SetSrcDstPartOid(src_partition_oid, dst_partition_oid);
    graph->AddEdgePartition(ctx, pid, part->oid, edge_type);
    graph->AddEdgeConnectionInfo(ctx, src_partition_oid, part->oid);
    return part;
}

} // namespace

TEST_CASE("Schema introspector keeps Message endpoints for real labels",
          "[catalog][nl2cypher][schema]") {
    TestDB db;
    auto& ctx = db.ctx();
    auto& cat = db.catalog();

    auto message = build_vertex_schema(
        ctx, cat, DEFAULT_GRAPH, "Message", {"id"}, {LogicalType::BIGINT}, 0);
    auto person = build_vertex_schema(
        ctx, cat, DEFAULT_GRAPH, "Person", {"id"}, {LogicalType::BIGINT}, 0);

    auto* graph = cat.GetEntry<GraphCatalogEntry>(ctx, DEFAULT_SCHEMA, DEFAULT_GRAPH);
    REQUIRE(graph != nullptr);

    auto* edge = CreateNamedEdgePartition(
        db, graph, "epart_HAS_CREATOR@Message@Person", "HAS_CREATOR",
        message.partition_oid, person.partition_oid);
    REQUIRE(edge != nullptr);

    auto schema = IntrospectGraphSchema(ctx);
    auto edge_it = std::find_if(schema.edges.begin(), schema.edges.end(),
        [](const EdgeInfo& info) { return info.type == "HAS_CREATOR"; });
    REQUIRE(edge_it != schema.edges.end());

    auto endpoint_it = std::find_if(edge_it->endpoints.begin(), edge_it->endpoints.end(),
        [](const EdgeInfo::Endpoint& ep) {
            return ep.src_label == "Message" && ep.dst_label == "Person";
        });
    REQUIRE(endpoint_it != edge_it->endpoints.end());

    auto prompt = schema.ToPromptText();
    CHECK(prompt.find("(:Message)-[:HAS_CREATOR]->(:Person)") != std::string::npos);
}

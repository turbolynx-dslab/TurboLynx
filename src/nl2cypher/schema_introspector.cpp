#include "nl2cypher/schema_introspector.hpp"

#include <sstream>
#include <stdexcept>

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "common/constants.hpp"
#include "common/types.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"

namespace turbolynx {
namespace nl2cypher {

namespace {

// Mirrors `tools/shell/commands.cpp::TypeName`. Kept local so this module
// has no dependency on the shell binary.
const char* TypeNameOf(duckdb::LogicalTypeId tid) {
    using duckdb::LogicalTypeId;
    switch (tid) {
        case LogicalTypeId::BIGINT:    return "BIGINT";
        case LogicalTypeId::UBIGINT:   return "UBIGINT";
        case LogicalTypeId::INTEGER:   return "INTEGER";
        case LogicalTypeId::HUGEINT:   return "HUGEINT";
        case LogicalTypeId::FLOAT:     return "FLOAT";
        case LogicalTypeId::DOUBLE:    return "DOUBLE";
        case LogicalTypeId::VARCHAR:   return "VARCHAR";
        case LogicalTypeId::BOOLEAN:   return "BOOLEAN";
        case LogicalTypeId::DATE:      return "DATE";
        case LogicalTypeId::TIMESTAMP: return "TIMESTAMP";
        case LogicalTypeId::ID:        return "ID";
        default:                       return "UNKNOWN";
    }
}

duckdb::GraphCatalogEntry* GetGraphCatalog(duckdb::ClientContext& ctx) {
    auto& catalog = ctx.db->GetCatalog();
    return (duckdb::GraphCatalogEntry*)catalog.GetEntry(
        ctx, duckdb::CatalogType::GRAPH_ENTRY,
        DEFAULT_SCHEMA, DEFAULT_GRAPH);
}

duckdb::PartitionCatalogEntry* GetPartition(duckdb::ClientContext& ctx,
                                            duckdb::idx_t oid) {
    auto& catalog = ctx.db->GetCatalog();
    return (duckdb::PartitionCatalogEntry*)catalog.GetEntry(
        ctx, DEFAULT_SCHEMA, oid);
}

// Walk the partitions for one label/type and merge their property lists
// into a single ordered, de-duplicated list. Multi-partition labels can
// occur for inheritance hierarchies; the union is what the LLM should
// see.
std::vector<PropertyInfo> CollectUnionProperties(
    duckdb::ClientContext& ctx,
    const std::vector<duckdb::idx_t>& partition_oids) {
    std::vector<PropertyInfo> out;
    std::vector<bool> seen_internal;  // skip "_id", "_sid", "_tid"

    for (auto oid : partition_oids) {
        auto* part = GetPartition(ctx, oid);
        if (!part) continue;
        auto* names = part->GetUniversalPropertyKeyNames();
        auto* types = part->GetUniversalPropertyTypeIds();
        if (!names || !types) continue;

        for (size_t i = 0; i < names->size(); ++i) {
            const std::string& nm = (*names)[i];
            // Skip internal columns (_id, _sid, _tid). The LLM doesn't
            // need to see these — Cypher exposes them implicitly via
            // pattern variables.
            if (!nm.empty() && nm[0] == '_') continue;

            bool dup = false;
            for (const auto& p : out) {
                if (p.name == nm) { dup = true; break; }
            }
            if (dup) continue;

            PropertyInfo info;
            info.name = nm;
            info.type_name = (i < types->size())
                ? TypeNameOf((*types)[i])
                : "UNKNOWN";
            out.push_back(std::move(info));
        }
    }
    return out;
}

}  // namespace

GraphSchema IntrospectGraphSchema(duckdb::ClientContext& client) {
    auto* graph = GetGraphCatalog(client);
    if (!graph) {
        throw std::runtime_error(
            "NL2Cypher: no graph catalog loaded in this client context");
    }

    GraphSchema out;
    out.graph_name = DEFAULT_GRAPH;

    std::vector<std::string> label_names;
    std::vector<std::string> edge_type_names;
    graph->GetVertexLabels(label_names);
    graph->GetEdgeTypes(edge_type_names);

    // --- Vertex labels ---
    for (const auto& lbl : label_names) {
        std::vector<duckdb::idx_t> oids;
        try {
            graph->GetVertexPartitionIndexesInLabel(client, lbl, oids);
        } catch (...) {
            continue;
        }
        if (oids.empty()) continue;

        LabelInfo li;
        li.label = lbl;
        li.n_partitions = static_cast<int>(oids.size());
        li.properties = CollectUnionProperties(client, oids);
        out.labels.push_back(std::move(li));
    }

    // --- Edge types ---
    for (const auto& et : edge_type_names) {
        std::vector<duckdb::idx_t> oids;
        try {
            graph->GetEdgePartitionIndexesInType(client, et, oids);
        } catch (...) {
            continue;
        }
        if (oids.empty()) continue;

        EdgeInfo ei;
        ei.type = et;
        ei.n_partitions = static_cast<int>(oids.size());
        ei.properties = CollectUnionProperties(client, oids);
        out.edges.push_back(std::move(ei));
    }

    return out;
}

std::string GraphSchema::ToPromptText() const {
    std::ostringstream os;
    os << "Vertex labels (" << labels.size() << "):\n";
    for (const auto& l : labels) {
        os << "  (:" << l.label;
        if (!l.properties.empty()) {
            os << " {";
            for (size_t i = 0; i < l.properties.size(); ++i) {
                if (i) os << ", ";
                os << l.properties[i].name << ": " << l.properties[i].type_name;
            }
            os << "}";
        }
        os << ")\n";
    }
    os << "\nEdge types (" << edges.size() << "):\n";
    for (const auto& e : edges) {
        os << "  [:" << e.type;
        if (!e.properties.empty()) {
            os << " {";
            for (size_t i = 0; i < e.properties.size(); ++i) {
                if (i) os << ", ";
                os << e.properties[i].name << ": " << e.properties[i].type_name;
            }
            os << "}";
        }
        os << "]\n";
    }
    return os.str();
}

nlohmann::json GraphSchema::ToJson() const {
    nlohmann::json j;
    j["graph_name"] = graph_name;
    j["labels"] = nlohmann::json::array();
    for (const auto& l : labels) {
        nlohmann::json lj;
        lj["label"] = l.label;
        lj["n_partitions"] = l.n_partitions;
        lj["properties"] = nlohmann::json::array();
        for (const auto& p : l.properties) {
            lj["properties"].push_back({
                {"name", p.name},
                {"type", p.type_name},
            });
        }
        j["labels"].push_back(std::move(lj));
    }
    j["edges"] = nlohmann::json::array();
    for (const auto& e : edges) {
        nlohmann::json ej;
        ej["type"] = e.type;
        ej["n_partitions"] = e.n_partitions;
        ej["properties"] = nlohmann::json::array();
        for (const auto& p : e.properties) {
            ej["properties"].push_back({
                {"name", p.name},
                {"type", p.type_name},
            });
        }
        j["edges"].push_back(std::move(ej));
    }
    return j;
}

}  // namespace nl2cypher
}  // namespace turbolynx

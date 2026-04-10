// Schema introspection for NL2Cypher.
//
// Reads the TurboLynx catalog into a portable, JSON-serialisable
// `GraphSchema` struct that downstream NL2Cypher components (prompt
// builder, schema linker, profile collector) can consume without taking
// a dependency on the catalog API.
//
// The introspector is read-only and stateless; it walks
// `GraphCatalogEntry` once per call.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "nlohmann/json.hpp"

namespace duckdb { class ClientContext; }

namespace turbolynx {
namespace nl2cypher {

struct PropertyInfo {
    std::string name;
    std::string type_name;   // canonical TurboLynx type name (BIGINT, VARCHAR, ...)
};

struct LabelInfo {
    std::string label;
    // Number of partitions (graphlets) under this label. Multi-graphlet
    // labels exist for inheritance / wide-row layouts; the property list
    // is the union across all graphlets in this label.
    int n_partitions = 0;
    std::vector<PropertyInfo> properties;
};

struct EdgeInfo {
    std::string type;
    int n_partitions = 0;
    std::vector<PropertyInfo> properties;
    // Endpoint pairs derived from partition naming
    // (`eps_<Type>@<Src>@<Dst>`). Lets the LLM see edge directionality
    // without us having to run a profiling Cypher.
    struct Endpoint { std::string src_label; std::string dst_label; };
    std::vector<Endpoint> endpoints;
};

struct GraphSchema {
    std::string graph_name;
    std::vector<LabelInfo> labels;
    std::vector<EdgeInfo>  edges;

    // Concise text dump suitable for embedding directly in an LLM prompt.
    // Format (one label/edge per line block):
    //
    //     (:Person {id: BIGINT, firstName: VARCHAR, ...})
    //     (:Forum {id: BIGINT, title: VARCHAR, ...})
    //     ...
    //     [:KNOWS {creationDate: TIMESTAMP}]
    //     [:HAS_CREATOR]
    //
    // Endpoint label inference is intentionally omitted at this stage;
    // S1 (profiling) fills it in by sampling actual edges.
    std::string ToPromptText() const;

    // JSON form, used by the metadata store and unit tests.
    nlohmann::json ToJson() const;
};

// Walk the catalog of `client` and return a populated GraphSchema.
// Throws std::runtime_error on missing/empty catalog.
GraphSchema IntrospectGraphSchema(duckdb::ClientContext& client);

}  // namespace nl2cypher
}  // namespace turbolynx

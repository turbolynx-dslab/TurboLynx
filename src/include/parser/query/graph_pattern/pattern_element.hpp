#pragma once

#include "parser/query/graph_pattern/rel_pattern.hpp"
#include <vector>
#include <utility>

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
	using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

// One hop in a chain: "-[rel]-(node)"
struct PatternElementChain {
    unique_ptr<RelPattern>  rel;
    unique_ptr<NodePattern> node;

    PatternElementChain(unique_ptr<RelPattern> rel, unique_ptr<NodePattern> node)
        : rel(std::move(rel)), node(std::move(node)) {}
};

enum class PatternPathType : uint8_t { NONE = 0, SHORTEST = 1, ALL_SHORTEST = 2 };

// A full path pattern: NodePattern ( - RelPattern - NodePattern )*
// Optionally named (p = (a)-[]->(b)).
class PatternElement {
public:
    explicit PatternElement(unique_ptr<NodePattern> first_node)
        : first_node(std::move(first_node)) {}

    void AddChain(unique_ptr<RelPattern> rel, unique_ptr<NodePattern> node) {
        chains.emplace_back(std::move(rel), std::move(node));
    }

    const NodePattern& GetFirstNode() const { return *first_node; }
    idx_t              GetNumChains() const { return chains.size(); }
    const PatternElementChain& GetChain(idx_t i) const { return chains[i]; }

    void             SetPathName(string name) { path_name = std::move(name); }
    const string&    GetPathName()  const { return path_name; }
    bool             HasPathName()  const { return !path_name.empty(); }

    void             SetPathType(PatternPathType t) { path_type = t; }
    PatternPathType  GetPathType()  const { return path_type; }

private:
    string path_name;
    PatternPathType path_type = PatternPathType::NONE;
    unique_ptr<NodePattern> first_node;
    vector<PatternElementChain> chains;
};

} // namespace turbolynx

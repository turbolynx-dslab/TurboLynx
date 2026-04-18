#pragma once

#include "parser/query/graph_pattern/node_pattern.hpp"

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
	using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

enum class RelDirection : uint8_t { LEFT = 0, RIGHT = 1, BOTH = 2 };
enum class RelPatternType : uint8_t { SIMPLE = 0, VARIABLE_LENGTH = 1, SHORTEST = 2, ALL_SHORTEST = 3 };

// Represents "-[varName:Type1|Type2*lower..upper]->" in MATCH patterns.
class RelPattern {
public:
    RelPattern(string var_name, vector<string> types, RelDirection direction,
               RelPatternType pattern_type,
               vector<pair<string, unique_ptr<ParsedExpression>>> properties,
               string lower_bound = "1", string upper_bound = "1")
        : var_name(std::move(var_name)),
          types(std::move(types)),
          direction(direction),
          pattern_type(pattern_type),
          properties(std::move(properties)),
          lower_bound(std::move(lower_bound)),
          upper_bound(std::move(upper_bound)) {}

    const string&              GetVarName()     const { return var_name; }
    const vector<string>&      GetTypes()       const { return types; }
    RelDirection               GetDirection()   const { return direction; }
    RelPatternType             GetPatternType() const { return pattern_type; }
    const string&              GetLowerBound()  const { return lower_bound; }
    const string&              GetUpperBound()  const { return upper_bound; }
    bool                       HasVarName()     const { return !var_name.empty(); }

    idx_t GetNumProperties() const { return properties.size(); }
    const string& GetPropertyKey(idx_t i) const { return properties[i].first; }
    ParsedExpression* GetPropertyValue(idx_t i) const { return properties[i].second.get(); }

private:
    string var_name;
    vector<string> types;
    RelDirection direction;
    RelPatternType pattern_type;
    vector<pair<string, unique_ptr<ParsedExpression>>> properties;
    string lower_bound;
    string upper_bound;
};

} // namespace turbolynx

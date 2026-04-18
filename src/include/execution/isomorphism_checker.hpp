//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/isomorphism_checker.hpp
//
//
//===----------------------------------------------------------------------===//

#ifndef ISOMORPHISM_CHECKER
#define ISOMORPHISM_CHECKER

#include <cstdint>
#include <unordered_set>

class IsoMorphismChecker {
public:
    virtual ~IsoMorphismChecker() = default;

    virtual void initialize(uint64_t num_max_items) = 0;
    virtual void addToSet(uint64_t edge_id) = 0;
    virtual bool checkIsoMorphism(uint64_t edge_id) = 0;
    virtual void removeFromSet(uint64_t edge_id) = 0;
    virtual bool isProbabilistic() = 0;
};

class ExactIsoChecker : public IsoMorphismChecker {
public:
    void initialize(uint64_t num_max_items) override {
        visited_edge_ids.clear();
        visited_edge_ids.reserve(num_max_items);
    }

    void addToSet(uint64_t edge_id) override {
        visited_edge_ids.insert(edge_id);
    }

    bool checkIsoMorphism(uint64_t edge_id) override {
        return visited_edge_ids.find(edge_id) != visited_edge_ids.end();
    }

    void removeFromSet(uint64_t edge_id) override {
        visited_edge_ids.erase(edge_id);
    }

    bool isProbabilistic() override {
        return false;
    }

private:
    std::unordered_set<uint64_t> visited_edge_ids;
};

#endif // ISOMORPHISM_CHECKER

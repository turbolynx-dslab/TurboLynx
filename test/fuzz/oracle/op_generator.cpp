#include "op_generator.hpp"

namespace tl_fuzz_oracle {

namespace {

// All fuzz state lives under a single namespace label `:Fz` so the
// wipe is scoped and unrelated host data is untouched.  The
// would-be-label (Person, Movie, …) is carried as a `kind` property
// — TurboLynx's empty-bootstrap multi-label CREATE has its own
// quirks that the L4 oracle should not fight in v1.

constexpr const char* kNs = "Fz";

// ---- l4.create-node-stream ------------------------------------------------
//
// CREATE (:Fz {id, kind:'Person', name, age}) followed by `count(*)`
// and `properties(n)` probes.  Simplest possible state-evolution op.
class CreateNodeStream final : public OpGenerator {
public:
    const char* name() const override { return "create-node-stream"; }

    std::vector<Op> emit(std::mt19937& /*rng*/, size_t n) const override {
        std::vector<Op> ops;
        ops.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            int64_t id = static_cast<int64_t>(1000 + i);
            Op op;
            op.write =
                std::string("CREATE (n:") + kNs +
                " {id: " + std::to_string(id) +
                ", kind: 'Person'" +
                ", name: 'P_" + std::to_string(id) +
                "', age: " + std::to_string(id % 80) + "})";
            op.probes = {
                std::string("MATCH (n:") + kNs +
                    " {kind:'Person'}) RETURN count(n) AS cnt",
                std::string("MATCH (n:") + kNs +
                    " {id:" + std::to_string(id) +
                    "}) RETURN n.name, n.age",
            };
            ops.push_back(std::move(op));
        }
        return ops;
    }
};

// ---- l4.create-edge-stream ------------------------------------------------
class CreateEdgeStream final : public OpGenerator {
public:
    const char* name() const override { return "create-edge-stream"; }

    std::vector<Op> emit(std::mt19937& /*rng*/, size_t n) const override {
        std::vector<Op> ops;
        ops.reserve(n + 2);
        auto create_person = [](int64_t id) -> Op {
            Op op;
            op.write =
                std::string("CREATE (n:") + kNs +
                " {id:" + std::to_string(id) +
                ", kind:'Person'" +
                ", name:'Anchor" + std::to_string(id) + "'})";
            op.probes = {
                std::string("MATCH (n:") + kNs +
                    " {kind:'Person'}) RETURN count(n) AS cnt"
            };
            return op;
        };
        ops.push_back(create_person(1));
        ops.push_back(create_person(2));
        for (size_t i = 0; i < n; ++i) {
            int64_t src_id = static_cast<int64_t>(1 + i);
            int64_t dst_id = static_cast<int64_t>(2 + i);
            if (i > 0) {
                ops.push_back(create_person(dst_id));
            }
            Op op;
            op.write =
                std::string("MATCH (a:") + kNs +
                " {id:" + std::to_string(src_id) +
                "}), (b:" + kNs +
                " {id:" + std::to_string(dst_id) +
                "}) CREATE (a)-[:KNOWS]->(b)";
            op.probes = {
                std::string("MATCH (:") + kNs +
                    ")-[r:KNOWS]->(:" + kNs +
                    ") RETURN count(r) AS cnt",
                std::string("MATCH (a:") + kNs +
                    " {id:" + std::to_string(src_id) +
                    "})-[:KNOWS]->(b:" + kNs +
                    ") RETURN b.id ORDER BY b.id",
            };
            ops.push_back(std::move(op));
        }
        return ops;
    }
};

// ---- l4.create-then-set-prop ----------------------------------------------
class CreateThenSetProp final : public OpGenerator {
public:
    const char* name() const override { return "create-then-set-prop"; }

    std::vector<Op> emit(std::mt19937& /*rng*/, size_t n) const override {
        std::vector<Op> ops;
        ops.reserve(n * 2);
        for (size_t i = 0; i < n; ++i) {
            int64_t id      = static_cast<int64_t>(2000 + i);
            int64_t new_age = static_cast<int64_t>(50 + (i % 30));
            ops.push_back({
                std::string("CREATE (n:") + kNs +
                    " {id:" + std::to_string(id) +
                    ", kind:'Person'" +
                    ", name:'Q_" + std::to_string(id) +
                    "', age:" + std::to_string(id % 80) + "})",
                {std::string("MATCH (n:") + kNs +
                    " {id:" + std::to_string(id) +
                    "}) RETURN n.age"}
            });
            ops.push_back({
                std::string("MATCH (n:") + kNs +
                    " {id:" + std::to_string(id) +
                    "}) SET n.age = " + std::to_string(new_age),
                {std::string("MATCH (n:") + kNs +
                    " {id:" + std::to_string(id) +
                    "}) RETURN n.age"}
            });
        }
        return ops;
    }
};

}  // namespace

const OpGenerator& create_node_stream_gen() {
    static CreateNodeStream g;
    return g;
}

const OpGenerator& create_edge_stream_gen() {
    static CreateEdgeStream g;
    return g;
}

const OpGenerator& create_then_set_prop_gen() {
    static CreateThenSetProp g;
    return g;
}

}  // namespace tl_fuzz_oracle

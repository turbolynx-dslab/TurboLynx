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

// ---- l4.create-then-detach-delete -----------------------------------------
//
// Interleaved CREATE + DETACH DELETE: a sliding window of three nodes
// where each step creates a new id and deletes the oldest.  Bugs of the
// "deleted id resurfaces in a subsequent probe" class (past Bug C) show
// up here as a row-count mismatch on the probe.
class CreateThenDetachDelete final : public OpGenerator {
public:
    const char* name() const override { return "create-then-detach-delete"; }

    std::vector<Op> emit(std::mt19937& /*rng*/, size_t n) const override {
        std::vector<Op> ops;
        ops.reserve(n * 2);
        for (size_t i = 0; i < n; ++i) {
            int64_t new_id = static_cast<int64_t>(3000 + i);
            int64_t old_id = static_cast<int64_t>(3000 + i - 3);
            ops.push_back({
                std::string("CREATE (n:") + kNs +
                    " {id:" + std::to_string(new_id) +
                    ", kind:'Person'" +
                    ", name:'D_" + std::to_string(new_id) + "'})",
                {std::string("MATCH (n:") + kNs +
                    " {kind:'Person'}) RETURN count(n) AS cnt"}
            });
            if (i >= 3) {
                ops.push_back({
                    std::string("MATCH (n:") + kNs +
                        " {id:" + std::to_string(old_id) +
                        "}) DETACH DELETE n",
                    {std::string("MATCH (n:") + kNs +
                        " {kind:'Person'}) RETURN count(n) AS cnt",
                     std::string("MATCH (n:") + kNs +
                        " {id:" + std::to_string(old_id) +
                        "}) RETURN count(n) AS cnt"}
                });
            }
        }
        return ops;
    }
};

// ---- l4.merge-stream ------------------------------------------------------
//
// MERGE with a 50/50 mix of new and existing ids.  After each MERGE the
// count probe must agree — if MERGE accidentally re-creates an already-
// present node it surfaces as a `count(:Fz) > N` divergence.
class MergeStream final : public OpGenerator {
public:
    const char* name() const override { return "merge-stream"; }

    std::vector<Op> emit(std::mt19937& /*rng*/, size_t n) const override {
        std::vector<Op> ops;
        ops.reserve(n);
        std::mt19937 rng(0x4242);   // local — keeps the test deterministic
        std::uniform_int_distribution<int> coin(0, 1);
        int64_t next_id = 4000;
        std::vector<int64_t> live;
        for (size_t i = 0; i < n; ++i) {
            int64_t id;
            if (!live.empty() && coin(rng) == 0) {
                std::uniform_int_distribution<size_t> pick(0, live.size() - 1);
                id = live[pick(rng)];
            } else {
                id = next_id++;
                live.push_back(id);
            }
            ops.push_back({
                std::string("MERGE (n:") + kNs +
                    " {id:" + std::to_string(id) +
                    ", kind:'Person'}) "
                    "ON CREATE SET n.created = true",
                {std::string("MATCH (n:") + kNs +
                    " {kind:'Person'}) RETURN count(n) AS cnt",
                 std::string("MATCH (n:") + kNs +
                    " {id:" + std::to_string(id) +
                    "}) RETURN count(n) AS cnt"}
            });
        }
        return ops;
    }
};

// ---- l4.ryw-match-by-id ---------------------------------------------------
//
// CREATE a node and immediately MATCH it by id in the same session.
// Past Bug B / RYW correctness class.  The match must see the
// just-created node; the count probe must agree across backends.
class RywMatchById final : public OpGenerator {
public:
    const char* name() const override { return "ryw-match-by-id"; }

    std::vector<Op> emit(std::mt19937& /*rng*/, size_t n) const override {
        std::vector<Op> ops;
        ops.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            int64_t id = static_cast<int64_t>(5000 + i);
            ops.push_back({
                std::string("CREATE (n:") + kNs +
                    " {id:" + std::to_string(id) +
                    ", kind:'Person'" +
                    ", name:'R_" + std::to_string(id) + "'})",
                {std::string("MATCH (n:") + kNs +
                    " {id:" + std::to_string(id) +
                    "}) RETURN n.name",
                 std::string("MATCH (n:") + kNs +
                    " {kind:'Person'}) RETURN count(n) AS cnt"}
            });
        }
        return ops;
    }
};

// ---- l4.varlen-on-delta ---------------------------------------------------
//
// Build a chain of Persons connected by KNOWS, one edge per op, and
// probe with VLE `*1..K` after each insert. Exercises the wrapper's
// delta-merge path through `graph_storage_wrapper.getAdjListFromVid` —
// the iterators (DFS / SP) hit `AdjListDelta` on every probe before
// any checkpoint flushes the edges to base CSR (#278 will land that
// flush). Same code path that #270 fixed for the SEGV on PID-0
// disk slot; keep this category gating future delta refactors.
class VarlenOnDelta final : public OpGenerator {
public:
    const char* name() const override { return "varlen-on-delta"; }

    std::vector<Op> emit(std::mt19937& /*rng*/, size_t n) const override {
        std::vector<Op> ops;
        ops.reserve(n + 1);
        auto create_person = [](int64_t id) -> Op {
            Op op;
            op.write =
                std::string("CREATE (n:") + kNs +
                " {id:" + std::to_string(id) +
                ", kind:'Person'" +
                ", name:'V_" + std::to_string(id) + "'})";
            op.probes = {
                std::string("MATCH (n:") + kNs +
                    " {kind:'Person'}) RETURN count(n) AS cnt"
            };
            return op;
        };
        ops.push_back(create_person(1));
        for (size_t i = 0; i < n; ++i) {
            int64_t src_id = static_cast<int64_t>(1 + i);
            int64_t dst_id = static_cast<int64_t>(2 + i);
            ops.push_back(create_person(dst_id));
            // Extend the chain: src → dst.
            Op op;
            op.write =
                std::string("MATCH (a:") + kNs +
                " {id:" + std::to_string(src_id) +
                "}), (b:" + kNs +
                " {id:" + std::to_string(dst_id) +
                "}) CREATE (a)-[:KNOWS]->(b)";
            // Probes hit the wrapper's delta-merge path: VLE reads
            // both the (empty) base CSR and the AdjListDelta inserts.
            op.probes = {
                // 1-hop neighbours of head — sanity check via VLE.
                std::string("MATCH (a:") + kNs +
                    " {id:1})-[:KNOWS*1..1]->(b:" + kNs +
                    ") RETURN b.id ORDER BY b.id",
                // K-hop reachability from head — grows with each edge.
                std::string("MATCH (a:") + kNs +
                    " {id:1})-[:KNOWS*1..3]->(b:" + kNs +
                    ") RETURN b.id ORDER BY b.id",
                // Reverse direction sanity.
                std::string("MATCH (b:") + kNs +
                    ")<-[:KNOWS*1..2]-(a:" + kNs +
                    " {id:1}) RETURN b.id ORDER BY b.id",
            };
            ops.push_back(std::move(op));
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

const OpGenerator& create_then_detach_delete_gen() {
    static CreateThenDetachDelete g;
    return g;
}

const OpGenerator& merge_stream_gen() {
    static MergeStream g;
    return g;
}

const OpGenerator& ryw_match_by_id_gen() {
    static RywMatchById g;
    return g;
}

const OpGenerator& varlen_on_delta_gen() {
    static VarlenOnDelta g;
    return g;
}

}  // namespace tl_fuzz_oracle

#include "catch.hpp"
#include "test_config.hpp"

#include "execution/physical_operator/physical_projection.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/vector.hpp"
#include "planner/expression/bound_reference_expression.hpp"
#include "planner/expression/bound_operator_expression.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "execution/execution_context.hpp"
#include "function/function.hpp"
#include "function/scalar_function.hpp"
#include "function/scalar/operators.hpp"

using namespace duckdb;

TEST_CASE("Test projection with BoundReferenceExpression (col0 + col1)") {
    // 1. Set up input types: 2 integers
    vector<LogicalType> input_types = {LogicalType::INTEGER, LogicalType::INTEGER};

    // 2. Create input DataChunk
    DataChunk input;
    input.Initialize(input_types);
    input.SetCardinality(1);
    input.SetValue(0, 0, Value::INTEGER(10));  // col0 = 10
    input.SetValue(1, 0, Value::INTEGER(20));  // col1 = 20

    // 3. Create projection expression: col0 + col1
    vector<unique_ptr<Expression>> children;
    children.push_back(std::make_unique<BoundReferenceExpression>(LogicalType::INTEGER, 0));  // ref col 0
    children.push_back(std::make_unique<BoundReferenceExpression>(LogicalType::INTEGER, 1));  // ref col 1
    ScalarFunction add_func = AddFun::GetFunction(LogicalType::INTEGER, LogicalType::INTEGER);

    vector<unique_ptr<Expression>> expressions;
    expressions.push_back(
        std::make_unique<BoundFunctionExpression>(
            LogicalType::INTEGER,
            std::move(add_func),
            std::move(children),
            nullptr,
            false
        )
    );

    // 4. Set up schema
    Schema schema;
    schema.setStoredTypes({LogicalType::INTEGER});

    // 5. Create operator
    PhysicalProjection projection(schema, std::move(expressions));

    // 6. Output chunk
    DataChunk output;
    output.Initialize(schema.getStoredTypes(), 1);

    // 7. Execution context/state
    ExecutionContext exec_context(nullptr);
    auto state = projection.GetOperatorState(exec_context);

    // 8. Run projection
    auto result = projection.Execute(exec_context, input, output, *state);

    // 9. Check result
    REQUIRE(result == OperatorResultType::NEED_MORE_INPUT);
    REQUIRE(output.size() == 1);
    REQUIRE(output.GetValue(0, 0) == Value::INTEGER(30));
}

// ═══════════════════════════════════════════════════════════════
// VLE isomorphism level-desync regression tests
//
// Bug: getNextEdge() calls changeLevel(true) before returning.
// When the isomorphism checker rejects the edge, the old code did
// `continue` without calling reduceLevel(), so DFS current_lv
// drifted ahead of outer cur_lv.  Adj lists were then set up at
// wrong levels, causing the DFS to silently reuse stale data from
// the first neighbor instead of loading subsequent neighbors.
// ═══════════════════════════════════════════════════════════════

namespace {

using AdjEntry = std::pair<uint64_t, uint64_t>;  // (target, edge_id)
using AdjMap   = std::vector<std::vector<AdjEntry>>;

// Mock DFS that replicates the real DFSIterator's level tracking.
struct MockDFS {
    const AdjMap &adj;
    int current_lv = 0;
    int max_lv     = 0;

    struct LevelState { uint64_t src = UINT64_MAX; std::size_t cursor = 0; };
    std::vector<LevelState> lv_state;

    explicit MockDFS(const AdjMap &a) : adj(a) {}

    void initialize(uint64_t src) {
        current_lv = 0; max_lv = 0;
        lv_state.assign(1, {src, 0});
    }

    bool getNextEdge(int lv, uint64_t &tgt, uint64_t &edge) {
        auto &st = lv_state[lv];
        if (st.src >= adj.size() || st.cursor >= adj[st.src].size()) {
            st.cursor = 0;
            changeLevel(false);
            return false;
        }
        tgt  = adj[st.src][st.cursor].first;
        edge = adj[st.src][st.cursor].second;
        st.cursor++;
        changeLevel(true, tgt);
        return true;
    }

    void reduceLevel() { current_lv--; }

private:
    void changeLevel(bool child, uint64_t tgt = 0) {
        if (child) {
            current_lv++;
            if (current_lv > max_lv) { max_lv = current_lv; lv_state.push_back({}); }
            lv_state[current_lv] = {tgt, 0};
        } else {
            current_lv--;
        }
    }
};

// VLE loop WITH the reduceLevel() fix.
std::unordered_set<uint64_t>
run_vle_with_fix(const AdjMap &adj, uint64_t src, int min_len, int max_len) {
    MockDFS dfs(adj);
    dfs.initialize(src);
    std::unordered_set<uint64_t> result, iso_set;
    std::vector<uint64_t> path;
    int cur_lv = 0;
    while (true) {
        if (cur_lv >= max_len) {
            iso_set.erase(path.back()); path.pop_back();
            cur_lv--; dfs.reduceLevel(); continue;
        }
        if (cur_lv < 0) break;
        uint64_t tgt, edge;
        if (!dfs.getNextEdge(cur_lv, tgt, edge)) {
            if (cur_lv == 0) break;
            iso_set.erase(path.back()); path.pop_back();
            cur_lv--; continue;
        }
        if (iso_set.count(edge)) { dfs.reduceLevel(); continue; }
        iso_set.insert(edge);
        cur_lv++; path.push_back(edge);
        if (cur_lv >= min_len && tgt != src) result.insert(tgt);
    }
    return result;
}

// Same loop WITHOUT the fix — reproduces the old bug.
std::unordered_set<uint64_t>
run_vle_without_fix(const AdjMap &adj, uint64_t src, int min_len, int max_len) {
    MockDFS dfs(adj);
    dfs.initialize(src);
    std::unordered_set<uint64_t> result, iso_set;
    std::vector<uint64_t> path;
    int cur_lv = 0;
    while (true) {
        if (cur_lv >= max_len) {
            iso_set.erase(path.back()); path.pop_back();
            cur_lv--; dfs.reduceLevel(); continue;
        }
        if (cur_lv < 0) break;
        uint64_t tgt, edge;
        if (!dfs.getNextEdge(cur_lv, tgt, edge)) {
            if (cur_lv == 0) break;
            iso_set.erase(path.back()); path.pop_back();
            cur_lv--; continue;
        }
        if (iso_set.count(edge)) { /* BUG: no reduceLevel() */ continue; }
        iso_set.insert(edge);
        cur_lv++; path.push_back(edge);
        if (cur_lv >= min_len && tgt != src) result.insert(tgt);
    }
    return result;
}

} // anon namespace

TEST_CASE("VLE isomorphism level desync — diamond graph", "[execution]") {
    // S─A─C─E
    // S─B─D─F   (undirected: same edge_id both ways → triggers iso rejection)
    // At *1..2 from S: should find {A,B,C,D}.
    // Bug: only found {A,B,C}, missing D.
    AdjMap adj(7);
    auto add = [&](uint64_t u, uint64_t v, uint64_t eid) {
        adj[u].push_back({v, eid}); adj[v].push_back({u, eid});
    };
    add(0,1,100); add(0,2,101); add(1,3,102); add(2,4,103);
    add(3,5,104); add(4,6,105);

    SECTION("Fixed code finds all 4 vertices at *1..2") {
        auto found = run_vle_with_fix(adj, 0, 1, 2);
        CHECK(found.count(1)); CHECK(found.count(2));
        CHECK(found.count(3)); CHECK(found.count(4));
        CHECK(found.size() == 4);
    }
    SECTION("Buggy code misses second neighbor's 2-hop") {
        auto found = run_vle_without_fix(adj, 0, 1, 2);
        CHECK(found.count(1)); CHECK(found.count(2)); CHECK(found.count(3));
        CHECK(found.count(4) == 0);  // D missed due to the bug
    }
}

TEST_CASE("VLE isomorphism level desync — star graph", "[execution]") {
    // S has 4 neighbors (A,B,C,D); each has unique leaves.
    // At *2..2 from S: should find all 6 leaves.
    // Bug: only found A's 2 leaves, missed B/C/D's leaves.
    enum { S=0,A=1,B=2,C=3,D=4, LA0=5,LA1=6,LB0=7,LB1=8,LC0=9,LD0=10 };
    AdjMap adj(11);
    auto add = [&](uint64_t u, uint64_t v, uint64_t eid) {
        adj[u].push_back({v, eid}); adj[v].push_back({u, eid});
    };
    add(S,A,200); add(S,B,201); add(S,C,202); add(S,D,203);
    add(A,LA0,300); add(A,LA1,301); add(B,LB0,302);
    add(B,LB1,303); add(C,LC0,304); add(D,LD0,305);

    SECTION("Fixed code finds all 6 leaves at *2..2") {
        auto found = run_vle_with_fix(adj, S, 2, 2);
        CHECK(found.size() == 6);
        for (auto v : {LA0,LA1,LB0,LB1,LC0,LD0}) CHECK(found.count(v));
    }
    SECTION("Buggy code only finds first neighbor's leaves") {
        auto found = run_vle_without_fix(adj, S, 2, 2);
        CHECK(found.count(LA0)); CHECK(found.count(LA1));
        CHECK(found.size() == 2);
    }
}

TEST_CASE("VLE no iso rejection — both paths agree", "[execution]") {
    // Directed graph, unique edge IDs → no rejection → both agree.
    AdjMap adj(5);
    adj[0] = {{1,10},{2,20}};
    adj[1] = {{3,30}};
    adj[2] = {{4,40}};

    auto fixed = run_vle_with_fix(adj, 0, 1, 2);
    auto buggy = run_vle_without_fix(adj, 0, 1, 2);
    CHECK(fixed == buggy);
    CHECK(fixed.size() == 4);
}

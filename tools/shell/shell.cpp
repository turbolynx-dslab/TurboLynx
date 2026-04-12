// Signal recovery for crash-proof operation
#include <signal.h>
#include <setjmp.h>
static sigjmp_buf g_crash_jmpbuf;
static volatile sig_atomic_t g_in_query = 0;

static void crash_signal_handler(int sig) {
    if (g_in_query) {
        g_in_query = 0;
        siglongjmp(g_crash_jmpbuf, sig);
    }
    // Not in query context — fall back to default handler
    signal(sig, SIG_DFL);
    raise(sig);
}

// antlr4 headers must come before ORCA (c.h postgres macros)
#include "CypherLexer.h"
#include "CypherParser.h"
#include "parser/cypher_transformer.hpp"
#include "binder/binder.hpp"
#include "common/logger.hpp"
#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "main/client_config.hpp"
#include <regex>
#include "storage/cache/chunk_cache_manager.h"
#include "planner/planner.hpp"
#include "catalog/catalog_wrapper.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "catalog/catalog_entry/index_catalog_entry.hpp"
#include "common/constants.hpp"
#include "storage/delta_store.hpp"
#include "storage/wal.hpp"
#include "storage/extent/adjlist_iterator.hpp"
#include "binder/query/updating_clause/bound_create_clause.hpp"
#include "binder/query/updating_clause/bound_set_clause.hpp"
#include "binder/query/updating_clause/bound_delete_clause.hpp"
#include "optimizer/orca/gpopt/tbgppdbwrappers.hpp"
#include "main/capi/turbolynx.h"
#include "replxx.hxx"

#include "include/shell.hpp"
#include "include/commands.hpp"
#include "include/renderer.hpp"
#include "include/completion.hpp"

#include "nl2cypher/nl2cypher_engine.hpp"
#include "nl2cypher/cypher_executor.hpp"
#include "nl2cypher/profile_collector.hpp"

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <getopt.h>
#include <unistd.h>
#include <chrono>
#include <iostream>
#include <numeric>
#include <optional>
#include <string>

using namespace antlr4;
using namespace gpopt;
using namespace duckdb;

// ------------------------------------------------------------------ ANTLR error listener

// Replaces ANTLR's default stderr printer with an exception throw.
// This prevents the "extraneous input ..." message and stops execution
// before transform() walks a malformed parse tree.
class ThrowingErrorListener : public BaseErrorListener {
public:
    void syntaxError(Recognizer*, Token*, size_t line, size_t col,
                     const std::string& msg, std::exception_ptr) override {
        throw std::runtime_error(
            "Syntax error at " + std::to_string(line) + ":" +
            std::to_string(col) + " — " + msg);
    }
};

// ------------------------------------------------------------------ ANSI helpers

static void PrintError(const std::string& msg) {
    if (isatty(STDERR_FILENO))
        std::cerr << "\033[1;31mError:\033[0m " << msg << '\n';
    else
        std::cerr << "Error: " << msg << '\n';
}

// ------------------------------------------------------------------ options

struct ShellCliOptions {
    std::string workspace;
    std::string query;
    std::string query_file;
    std::string output_mode;    // CLI --mode flag (applied to ShellState at init)
    bool        standalone     = false;
    bool        compile_only   = false;
    bool        enable_profile = false;
    int         iterations     = 1;
    bool        warmup         = false;
    turbolynx::PlannerConfig planner_config;
};

static void ParseShellOptions(int argc, char** argv, ShellCliOptions& opts) {
    optind = 1;
    static struct option long_options[] = {
        {"help",                no_argument,       0, 'h'},
        {"workspace",           required_argument, 0, 'w'},
        {"ws",                  required_argument, 0, 'w'},  // alias for --workspace
        {"log-level",           required_argument, 0, 'L'},
        {"standalone",          no_argument,       0, 'S'},
        {"compile-only",        no_argument,       0, 'c'},
        {"orca-compile-only",   no_argument,       0, 'O'},
        {"index-join-only",     no_argument,       0, 1001},
        {"hash-join-only",      no_argument,       0, 1002},
        {"merge-join-only",     no_argument,       0, 1003},
        {"disable-merge-join",  no_argument,       0, 1004},
        {"disable-index-join",  no_argument,       0, 1005},
        {"join-order-optimizer",required_argument, 0, 'j'},
        {"debug-orca",          no_argument,       0, 1006},
        {"query",               required_argument, 0, 'q'},
        {"q",                   required_argument, 0, 'q'},  // alias for --query
        {"query-file",          required_argument, 0, 'f'},
        {"qf",                  required_argument, 0, 'f'},  // alias for --query-file
        {"iterations",          required_argument, 0, 'i'},
        {"warmup",              no_argument,       0, 1007},
        {"profile",             no_argument,       0, 1008},
        {"explain",             no_argument,       0, 1009},
        {"mode",                required_argument, 0, 'm'},
        {0, 0, 0, 0}
    };

    int opt, option_index = 0;
    while ((opt = getopt_long(argc, argv, "hw:L:ScOj:q:f:i:m:", long_options, &option_index)) != -1) {
        switch (opt) {
        case 'h':
            std::cout << "Usage: turbolynx shell [options]\n"
                      << "  -w, --workspace <path>         Workspace directory (alias: --ws)\n"
                      << "  -q, --query <string>           Execute a single query (alias: --q)\n"
                      << "  -f, --query-file <path>        Execute all ';'-terminated queries in a file (alias: --qf)\n"
                      << "  -m, --mode <table|box|csv|json|...> Output format\n"
                      << "  -i, --iterations <n>           Repeat query N times\n"
                      << "      --warmup                   First iteration is warmup\n"
                      << "  -S, --standalone               Standalone mode\n"
                      << "  -c, --compile-only             Compile without execution\n"
                      << "      --profile                  Enable profiling\n"
                      << "      --explain                  Print physical plan (no execution)\n"
                      << "  -L, --log-level <level>        Log level\n";
            exit(0);
        case 'w': opts.workspace = optarg; break;
        case 'L': setLogLevel(getLogLevel(optarg)); break;
        case 'S': opts.standalone = true; break;
        case 'c': opts.compile_only = true; break;
        case 'O': opts.compile_only = true; opts.planner_config.ORCA_COMPILE_ONLY = true; break;
        case 1001: opts.planner_config.INDEX_JOIN_ONLY = true; break;
        case 1002: opts.planner_config.HASH_JOIN_ONLY = true; break;
        case 1003: opts.planner_config.MERGE_JOIN_ONLY = true; break;
        case 1004: opts.planner_config.DISABLE_MERGE_JOIN = true; break;
        case 1005: opts.planner_config.DISABLE_INDEX_JOIN = true; break;
        case 'j': {
            std::string o = optarg;
            using JOT = turbolynx::PlannerConfig::JoinOrderType;
            if      (o == "query")       opts.planner_config.JOIN_ORDER_TYPE = JOT::JOIN_ORDER_IN_QUERY;
            else if (o == "greedy")      opts.planner_config.JOIN_ORDER_TYPE = JOT::JOIN_ORDER_GREEDY_SEARCH;
            else if (o == "exhaustive")  opts.planner_config.JOIN_ORDER_TYPE = JOT::JOIN_ORDER_EXHAUSTIVE_SEARCH;
            else if (o == "exhaustive2") opts.planner_config.JOIN_ORDER_TYPE = JOT::JOIN_ORDER_EXHAUSTIVE2_SEARCH;
            else if (o == "gem")         opts.planner_config.JOIN_ORDER_TYPE = JOT::JOIN_ORDER_GEM;
            break;
        }
        case 1006: opts.planner_config.ORCA_DEBUG_PRINT = true; break;
        case 'q': opts.query = optarg; break;
        case 'f': opts.query_file = optarg; break;
        case 'i': opts.iterations = std::stoi(optarg); break;
        case 1007: opts.warmup = true; break;
        case 1008: opts.enable_profile = true; break;
        case 1009: opts.planner_config.DEBUG_PRINT = true; break;
        case 'm': opts.output_mode = optarg; break;
        default: break;
        }
    }
}

// ------------------------------------------------------------------ query execution

struct ExecContext {
    std::shared_ptr<duckdb::ClientContext> client;
    ShellCliOptions&                       cli;
    turbolynx::ShellState&                 state;
    turbolynx::Planner&                    planner;
    // NL2Cypher engine — lazily constructed on the first NL prompt so
    // shells that never use `.nl on` don't pay the LLM-pool warmup cost.
    std::unique_ptr<turbolynx::nl2cypher::NL2CypherEngine> nl_engine;
};

// Query plan cache: query string → parsed AST (skips lexing, parsing, transform)
static std::unordered_map<std::string, std::shared_ptr<RegularQuery>> g_query_cache;

static std::shared_ptr<RegularQuery> ParseAndTransform(
    const std::string& query, ScopedTimer& timer) {
    // Check cache
    auto it = g_query_cache.find(query);
    if (it != g_query_cache.end()) {
        spdlog::debug("[QueryCache] hit");
        return it->second;
    }

    ThrowingErrorListener error_listener;

    timer.start("Lexing");
    auto inputStream = ANTLRInputStream(query);
    CypherLexer lexer(&inputStream);
    lexer.removeErrorListeners();
    lexer.addErrorListener(&error_listener);
    CommonTokenStream tokens(&lexer);
    tokens.fill();
    timer.stop("Lexing");

    timer.start("Parse");
    CypherParser parser(&tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(&error_listener);
    timer.stop("Parse");

    timer.start("ANTLR Parse");
    auto* cypher_ctx = parser.oC_Cypher();
    if (!cypher_ctx || parser.getNumberOfSyntaxErrors() > 0)
        throw std::runtime_error("Parser returned no tree — check Cypher syntax");
    timer.stop("ANTLR Parse");

    timer.start("Transform");
    CypherTransformer transformer(*cypher_ctx);
    auto stmt = transformer.transform();
    if (!stmt)
        throw std::runtime_error("Transformer returned null — check Cypher syntax");
    timer.stop("Transform");

    auto shared_stmt = std::shared_ptr<RegularQuery>(std::move(stmt));
    g_query_cache[query] = shared_stmt;
    return shared_stmt;
}

// Returns true if the bound query contains any updating clause (CREATE / SET /
// DELETE / MERGE). Used to reject write queries in the shell with a clean
// error instead of letting the Cypher→ORCA converter assert at
// `qp.GetNumUpdatingClauses() == 0` (cypher2orca_converter.cpp:413).
//
// CRUD is supported via the embedded C API (see test/query/test_q7_crud.cpp),
// which maintains a ConnectionHandle with mutation state and runs
// CREATE/SET/DELETE directly against DeltaStore. The shell does not yet share
// that mutation-bypass path, so for now writes must go through the C API.
// Check on the *unbound* AST so we can reject writes BEFORE running the
// binder. The binder requires matching vertex/edge partitions to exist,
// which gives a confusing "no vertex with label X" error on an empty
// workspace instead of the real "writes go through the C API" reason.
static bool ShellQueryHasUpdatingClause(duckdb::RegularQuery* stmt) {
    if (!stmt) return false;
    for (duckdb::idx_t si = 0; si < stmt->GetNumSingleQueries(); si++) {
        auto* sq = stmt->GetSingleQuery(si);
        if (sq->GetNumUpdatingClauses() > 0) return true;
        for (duckdb::idx_t pi = 0; pi < sq->GetNumQueryParts(); pi++) {
            if (sq->GetQueryPart(pi)->GetNumUpdatingClauses() > 0) return true;
        }
    }
    return false;
}

// Track per-query mutation state so RunOneIteration can branch.
struct MutationState {
    bool is_mutation_only = false;    // CREATE-only (no RETURN)
    std::unique_ptr<BoundRegularQuery> bound_mutation;
    // For MATCH+SET/DELETE: pending items extracted before ORCA
    std::vector<duckdb::BoundSetItem> pending_set_items;
    bool pending_delete = false;
    bool pending_detach_delete = false;
};

static thread_local MutationState g_mutation;

// Execute a CREATE-only mutation against DeltaStore (no ORCA pipeline).
static void ExecuteMutationDirect(ExecContext& ctx) {
    auto& bound_query = g_mutation.bound_mutation;
    if (!bound_query || bound_query->GetNumSingleQueries() == 0)
        throw std::runtime_error("No bound mutation query");

    auto& delta_store = ctx.client->db->delta_store;
    auto* sq = bound_query->GetSingleQuery(0);

    for (idx_t pi = 0; pi < sq->GetNumQueryParts(); pi++) {
        auto* qp = sq->GetQueryPart(pi);
        for (idx_t ui = 0; ui < qp->GetNumUpdatingClauses(); ui++) {
            auto* uc = qp->GetUpdatingClause(ui);
            if (uc->GetClauseType() == duckdb::BoundUpdatingClauseType::CREATE) {
                auto* create = static_cast<const duckdb::BoundCreateClause*>(uc);
                for (auto& node_info : create->GetNodes()) {
                    duckdb::idx_t part_oid = 0;
                    if (!node_info.partition_ids.empty())
                        part_oid = node_info.partition_ids[0];
                    auto& catalog = ctx.client->db->GetCatalog();
                    auto* part_cat = (PartitionCatalogEntry*)catalog.GetEntry(
                        *ctx.client, DEFAULT_SCHEMA, part_oid);
                    uint16_t logical_pid = part_cat ? part_cat->GetPartitionID() : (uint16_t)(part_oid & 0xFFFF);
                    auto inmem_eids = delta_store.GetInMemoryExtentIDs(logical_pid);
                    uint32_t inmem_eid;
                    if (inmem_eids.empty()) {
                        inmem_eid = delta_store.AllocateInMemoryExtentID(logical_pid);
                    } else {
                        inmem_eid = inmem_eids[0];
                    }
                    duckdb::vector<std::string> keys;
                    duckdb::vector<duckdb::Value> row;
                    for (auto& [key, val] : node_info.properties) {
                        keys.push_back(key);
                        row.push_back(val);
                    }
                    if (ctx.client->db->wal_writer)
                        ctx.client->db->wal_writer->LogInsertNode(logical_pid, inmem_eid, keys, row);
                    delta_store.GetInsertBuffer(inmem_eid).AppendRow(std::move(keys), std::move(row));
                    spdlog::info("[CREATE] Inserted node label='{}' with {} properties (partition {}, oid {})",
                                 node_info.label, node_info.properties.size(), logical_pid, part_oid);
                }
            }
        }
    }
    g_mutation.bound_mutation.reset();
}

// ------------------------------------------------------------------ query rewriting
// Ported from C API (turbolynx-c.cpp) — regex rewrites for DETACH DELETE, REMOVE, MERGE

// Rewrite DETACH DELETE → DELETE and set is_detach flag
static std::string RewriteDetachDelete(const std::string& query, bool& is_detach) {
    std::regex detach_re(R"(\bDETACH\s+DELETE\b)", std::regex::icase);
    is_detach = std::regex_search(query, detach_re);
    if (is_detach) return std::regex_replace(query, detach_re, "DELETE");
    return query;
}

// Rewrite REMOVE n.prop → SET n.prop = NULL
static std::string RewriteRemoveToSetNull(const std::string& query) {
    std::regex remove_re(R"(\bREMOVE\s+)", std::regex::icase);
    std::smatch m;
    std::string q = query;
    if (!std::regex_search(q, m, remove_re)) return query;
    std::string prefix = m.prefix().str();
    std::string rest = m.suffix().str();
    std::regex end_re(R"(\s+(?:RETURN|WITH|DELETE|SET|CREATE|MATCH|$))", std::regex::icase);
    std::smatch end_m;
    std::string items_part;
    std::string after;
    if (std::regex_search(rest, end_m, end_re)) {
        items_part = rest.substr(0, end_m.position());
        after = rest.substr(end_m.position());
    } else {
        items_part = rest;
    }
    std::regex prop_re(R"((\w+\.\w+))");
    std::string set_clause = "SET ";
    bool first = true;
    auto pbegin = std::sregex_iterator(items_part.begin(), items_part.end(), prop_re);
    for (auto it = pbegin; it != std::sregex_iterator(); ++it) {
        if (!first) set_clause += ", ";
        set_clause += it->str() + " = NULL";
        first = false;
    }
    return prefix + set_clause + after;
}

// Check if query is a MERGE statement
static bool IsMergeQuery(const std::string& query) {
    std::regex merge_re(R"(^\s*MERGE\s+)", std::regex::icase);
    return std::regex_search(query, merge_re);
}

// ------------------------------------------------------------------ post-pipeline mutations

// Apply pending SET items to DeltaStore after pipeline execution
static void ApplyPendingSet(ExecContext& ctx,
                            std::vector<std::shared_ptr<duckdb::DataChunk>>& results) {
    if (g_mutation.pending_set_items.empty()) return;

    // Validate property keys against catalog
    auto& catalog = ctx.client->db->GetCatalog();
    auto* gcat = (duckdb::GraphCatalogEntry*)catalog.GetEntry(
        *ctx.client, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH, true);
    if (gcat) {
        std::unordered_set<std::string> known_keys;
        for (auto vp_oid : *gcat->GetVertexPartitionOids()) {
            auto* vp = (duckdb::PartitionCatalogEntry*)catalog.GetEntry(
                *ctx.client, DEFAULT_SCHEMA, vp_oid, true);
            if (!vp) continue;
            auto* key_names = vp->GetUniversalPropertyKeyNames();
            if (key_names) for (auto& key : *key_names) known_keys.insert(key);
        }
        std::vector<duckdb::BoundSetItem> valid;
        for (auto& item : g_mutation.pending_set_items) {
            if (known_keys.find(item.property_key) == known_keys.end()) {
                if (item.value.IsNull()) continue; // REMOVE non-existent → no-op
                throw std::runtime_error(
                    "Unsupported: SET with new property '" + item.property_key +
                    "' (schema evolution not yet supported).");
            }
            valid.push_back(item);
        }
        g_mutation.pending_set_items = std::move(valid);
    }

    auto& delta_store = ctx.client->db->delta_store;
    for (auto& chunk : results) {
        if (!chunk || chunk->ColumnCount() == 0 || chunk->size() == 0) continue;
        // Find user 'id' column (UBIGINT/BIGINT)
        idx_t id_col = duckdb::DConstants::INVALID_INDEX;
        for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
            auto tid = chunk->data[c].GetType().id();
            if (tid == duckdb::LogicalTypeId::UBIGINT || tid == duckdb::LogicalTypeId::BIGINT) {
                id_col = c; break;
            }
        }
        if (id_col == duckdb::DConstants::INVALID_INDEX) continue;
        for (idx_t row = 0; row < chunk->size(); row++) {
            uint64_t user_id = ((uint64_t*)chunk->data[id_col].GetData())[row];
            for (auto& item : g_mutation.pending_set_items) {
                delta_store.SetPropertyByUserId(user_id, item.property_key, item.value);
                if (ctx.client->db->wal_writer)
                    ctx.client->db->wal_writer->LogUpdateProp(user_id, item.property_key, item.value);
            }
            spdlog::info("[SET] user_id={} props={}", user_id, g_mutation.pending_set_items.size());
        }
    }
    g_mutation.pending_set_items.clear();
}

// Apply pending DELETE to DeltaStore after pipeline execution.
// Uses user-id based deletion (simpler, works without VID in output).
// For DETACH DELETE with adjacency cascade, the full VID-based path from C API would be needed.
static void ApplyPendingDelete(ExecContext& ctx,
                               std::vector<std::shared_ptr<duckdb::DataChunk>>& results) {
    if (!g_mutation.pending_delete) return;

    auto& delta_store = ctx.client->db->delta_store;

    for (auto& chunk : results) {
        if (!chunk || chunk->ColumnCount() == 0 || chunk->size() == 0) continue;
        // Find user 'id' column (UBIGINT/BIGINT) for user-id based deletion
        idx_t uid_col = duckdb::DConstants::INVALID_INDEX;
        for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
            auto tid = chunk->data[c].GetType().id();
            if (tid == duckdb::LogicalTypeId::UBIGINT || tid == duckdb::LogicalTypeId::BIGINT) {
                uid_col = c; break;
            }
        }
        if (uid_col == duckdb::DConstants::INVALID_INDEX) continue;

        for (idx_t row = 0; row < chunk->size(); row++) {
            uint64_t user_id = ((uint64_t*)chunk->data[uid_col].GetData())[row];
            delta_store.DeleteByUserId(user_id);
            if (ctx.client->db->wal_writer)
                ctx.client->db->wal_writer->LogDeleteNode(0, 0, user_id);
            spdlog::info("[{}] user_id={}",
                         g_mutation.pending_detach_delete ? "DETACH DELETE" : "DELETE", user_id);
        }
    }
    g_mutation.pending_delete = false;
    g_mutation.pending_detach_delete = false;
}

// Rewrite MATCH+SET/DELETE queries: strip SET/DELETE clause, inject RETURN <var>.id
// so the pipeline produces user-id columns needed for DeltaStore mutations.
static std::string RewriteMutationQuery(const std::string& query, bool& is_set, bool& is_delete) {
    is_set = false;
    is_delete = false;
    std::string upper;
    upper.reserve(query.size());
    for (char c : query) upper.push_back(std::toupper(c));
    bool has_match = upper.find("MATCH") != std::string::npos;
    bool has_set = upper.find(" SET ") != std::string::npos || upper.find("\nSET ") != std::string::npos;
    bool has_delete = upper.find("DELETE") != std::string::npos;
    bool has_return = upper.find("RETURN") != std::string::npos;
    if (!has_match || has_return) return query; // has RETURN already, or no MATCH — pass through
    if (!has_set && !has_delete) return query;

    // Extract variable name from MATCH pattern: MATCH (var:Label ...)
    std::regex var_re(R"(\bMATCH\s*\(\s*(\w+)\s*:)", std::regex::icase);
    std::smatch vm;
    std::string var = "n";
    if (std::regex_search(query, vm, var_re)) var = vm[1].str();

    // Strip SET clause(s) and extract items
    std::string q = query;
    if (has_set) {
        is_set = true;
        // Store original SET text for g_mutation extraction (done in CompileQuery via bound query)
    }
    if (has_delete) {
        is_delete = true;
    }

    // Remove SET ... and DELETE ... from the query, then append RETURN
    std::string clean = q;
    // Remove SET clause: SET <var>.<prop> = <val> [, ...]
    clean = std::regex_replace(clean, std::regex(R"(\bSET\s+[^;]*)", std::regex::icase), "");
    // Remove DELETE clause: DELETE <var>
    clean = std::regex_replace(clean, std::regex(R"(\bDELETE\s+\w+)", std::regex::icase), "");
    // Trim trailing whitespace
    while (!clean.empty() && (clean.back() == ' ' || clean.back() == '\t' || clean.back() == '\n'))
        clean.pop_back();
    // Append RETURN with user id for DeltaStore lookup
    clean += " RETURN " + var + ".id";
    return clean;
}

static void CompileQuery(const std::string& query, ExecContext& ctx, double& compile_ms) {
    SCOPED_TIMER(CompileQuery, spdlog::level::info, spdlog::level::debug, compile_ms);

    auto stmt = ParseAndTransform(query, CompileQuery_timer);

    bool has_updating = ShellQueryHasUpdatingClause(stmt.get());

    SUBTIMER_START(CompileQuery, "Bind");
    Binder binder(ctx.client.get());
    auto bound = binder.Bind(*stmt);
    SUBTIMER_STOP(CompileQuery, "Bind");

    // Detect mutation-only queries (CREATE without RETURN)
    g_mutation = MutationState{};
    if (bound->GetNumSingleQueries() == 1) {
        auto* sq_chk = bound->GetSingleQuery(0);
        bool chk_reading = false, chk_projection = false, chk_updating = false;
        for (idx_t i = 0; i < sq_chk->GetNumQueryParts(); i++) {
            auto* qp = sq_chk->GetQueryPart(i);
            if (qp->HasReadingClause()) chk_reading = true;
            if (qp->HasProjectionBody()) chk_projection = true;
            if (qp->HasUpdatingClause()) chk_updating = true;
        }
        if (chk_reading && !chk_projection && !chk_updating)
            throw std::runtime_error("Query must end with a RETURN clause");
    }
    if (has_updating && bound->GetNumSingleQueries() == 1) {
        auto* sq = bound->GetSingleQuery(0);
        bool has_reading = false, has_projection = false;
        for (idx_t i = 0; i < sq->GetNumQueryParts(); i++) {
            auto* qp = sq->GetQueryPart(i);
            if (qp->HasReadingClause()) has_reading = true;
            if (qp->HasProjectionBody()) has_projection = true;
        }
        if (!has_reading && !has_projection) {
            g_mutation.is_mutation_only = true;
            g_mutation.bound_mutation = std::move(bound);
            return;
        }
        // MATCH + SET/DELETE: strip updating clauses before ORCA
        for (idx_t pi = 0; pi < sq->GetNumQueryParts(); pi++) {
            auto* qp = sq->GetQueryPart(pi);
            for (idx_t ui = 0; ui < qp->GetNumUpdatingClauses(); ui++) {
                auto* uc = qp->GetUpdatingClause(ui);
                if (uc->GetClauseType() == duckdb::BoundUpdatingClauseType::SET) {
                    auto* sc = static_cast<const duckdb::BoundSetClause*>(uc);
                    for (auto& item : sc->GetItems())
                        g_mutation.pending_set_items.push_back(item);
                } else if (uc->GetClauseType() == duckdb::BoundUpdatingClauseType::DELETE_CLAUSE) {
                    g_mutation.pending_delete = true;
                }
            }
            qp->ClearUpdatingClauses();
        }
    }

    // Reject MATCH queries against an empty workspace BEFORE handing off to
    // ORCA.
    {
        auto &catalog = ctx.client->db->GetCatalog();
        auto *graph_entry = (duckdb::GraphCatalogEntry *)catalog.GetEntry(
            *ctx.client, duckdb::CatalogType::GRAPH_ENTRY,
            DEFAULT_SCHEMA, DEFAULT_GRAPH);
        if (graph_entry &&
            graph_entry->vertex_partitions.empty() &&
            graph_entry->edge_partitions.empty()) {
            throw std::runtime_error(
                "Workspace is empty — no vertex or edge partitions exist yet. "
                "Import data with 'turbolynx import' or create partitions via "
                "the embedded C API before running queries.");
        }
    }

    SUBTIMER_START(CompileQuery, "Orca Compile");
    ctx.planner.execute(bound.get());
    SUBTIMER_STOP(CompileQuery, "Orca Compile");
}

// Build RenderOptions from the current ShellState, consuming output_once if set.
static turbolynx::RenderOptions BuildRenderOptions(turbolynx::ShellState& state) {
    turbolynx::RenderOptions opts;
    opts.show_headers  = state.show_headers;
    opts.null_value    = state.null_value;
    opts.col_sep       = state.col_sep;
    opts.max_rows      = state.max_rows;
    opts.min_col_width = state.min_col_width;
    opts.insert_label  = state.insert_label;
    if (!state.output_once.empty()) {
        opts.output_file   = state.output_once;
        state.output_once.clear();
    } else {
        opts.output_file = state.output_file;
    }
    return opts;
}

static void LogQuery(const std::string& query, double compile_ms, double exec_ms,
                     const std::string& log_file) {
    if (log_file.empty()) return;
    std::ofstream f(log_file, std::ios::app);
    if (!f) return;
    f << "-- " << query << '\n';
    f << "-- compile: " << compile_ms << " ms, execute: " << exec_ms << " ms\n";
}

// Session config statement: PRAGMA threads = N
// Returns parsed N if matched, -1 otherwise.
static int64_t ParseSetThreadsStmt(const std::string& q) {
    std::regex re(R"(^\s*PRAGMA\s+threads\s*=\s*(\d+)\s*;?\s*$)",
                  std::regex::icase);
    std::smatch m;
    if (std::regex_match(q, m, re)) {
        try { return std::stoll(m[1].str()); } catch (...) { return -1; }
    }
    return -1;
}

static void RunOneIteration(const std::string& query, ExecContext& ctx,
                            double& compile_ms, double& exec_ms) {
    // Session config: PRAGMA threads = N  /  SET parallel_threads = N
    {
        int64_t n = ParseSetThreadsStmt(query);
        if (n >= 0) {
            duckdb::ClientConfig::GetConfig(*ctx.client).maximum_threads = (idx_t)n;
            std::cout << "parallel_threads = " << n
                      << (n == 0 ? " (auto)" : "") << "\n";
            compile_ms = 0;
            exec_ms = 0;
            return;
        }
    }

    // Pre-compile rewrites: DETACH DELETE, REMOVE, MERGE
    std::string rewritten = query;
    bool is_detach = false;
    rewritten = RewriteDetachDelete(rewritten, is_detach);
    rewritten = RewriteRemoveToSetNull(rewritten);

    // MERGE: decompose into MATCH check + conditional CREATE
    if (IsMergeQuery(rewritten)) {
        std::regex merge_re(R"(\bMERGE\s*\(\s*(\w+)\s*:\s*(\w+)\s*(?:\{([^}]*)\})?\s*\))", std::regex::icase);
        std::smatch mm;
        if (std::regex_search(rewritten, mm, merge_re)) {
            std::string var = mm[1].str();
            std::string label = mm[2].str();
            std::string props_str = mm[3].str();
            // Step 1: MATCH check (silent — no rendering)
            std::string match_q;
            if (!props_str.empty()) {
                match_q = "MATCH (" + var + ":" + label + " {" + props_str + "}) RETURN count(" + var + ") AS cnt;";
            } else {
                match_q = "MATCH (" + var + ":" + label + ") RETURN count(" + var + ") AS cnt;";
            }
            double mc = 0;
            CompileQuery(match_q, ctx, mc);
            auto mexecs = ctx.planner.genPipelineExecutors();
            int64_t cnt = 0;
            if (!mexecs.empty() && mexecs.back() && mexecs.back()->pipeline && mexecs.back()->pipeline->GetSink()) {
                for (auto& ex : mexecs) ex->ExecutePipeline();
                auto& mresults = mexecs.back()->context->query_results;
                for (auto& chunk : *mresults) {
                    if (chunk && chunk->size() > 0 && chunk->ColumnCount() > 0) {
                        auto tid = chunk->data[0].GetType().id();
                        if (tid == duckdb::LogicalTypeId::BIGINT)
                            cnt = ((int64_t*)chunk->data[0].GetData())[0];
                        else if (tid == duckdb::LogicalTypeId::UBIGINT)
                            cnt = (int64_t)((uint64_t*)chunk->data[0].GetData())[0];
                    }
                }
                for (auto& chunk : *mresults) chunk.reset();
                mresults->clear();
            }
            for (auto* e : mexecs) delete e;
            // Step 2: CREATE if not exists
            if (cnt == 0) {
                std::string create_q = "CREATE (" + var + ":" + label;
                if (!props_str.empty()) create_q += " {" + props_str + "}";
                create_q += ");";
                RunOneIteration(create_q, ctx, compile_ms, exec_ms);
            } else {
                std::cout << "(no changes, " << cnt << " row(s) already exist)\n";
                compile_ms = 0; exec_ms = 0;
            }
            return;
        }
        throw std::runtime_error("Cannot parse MERGE query");
    }

    if (is_detach) g_mutation.pending_detach_delete = true;

    // For MATCH+SET/DELETE without RETURN: rewrite to inject RETURN <var>.id, <var>
    // so the pipeline produces user-id columns needed by DeltaStore mutations.
    bool is_set_rewrite = false, is_delete_rewrite = false;
    std::string mutation_rewritten = RewriteMutationQuery(rewritten, is_set_rewrite, is_delete_rewrite);
    if (is_set_rewrite || is_delete_rewrite) {
        // First compile original to extract SET/DELETE bound items
        CompileQuery(rewritten, ctx, compile_ms);
        // Save mutation state (CompileQuery populates g_mutation)
        auto saved_set = std::move(g_mutation.pending_set_items);
        bool saved_del = g_mutation.pending_delete;
        bool saved_detach = g_mutation.pending_detach_delete;
        // Re-compile with the rewritten query that has RETURN <var>.id
        CompileQuery(mutation_rewritten, ctx, compile_ms);
        // Restore mutation state
        g_mutation.pending_set_items = std::move(saved_set);
        g_mutation.pending_delete = saved_del;
        g_mutation.pending_detach_delete = saved_detach;
    } else {
        CompileQuery(rewritten, ctx, compile_ms);
    }
    if (ctx.cli.compile_only) return;

    // Mutation-only (CREATE without RETURN): execute directly, no pipeline
    if (g_mutation.is_mutation_only) {
        ExecuteMutationDirect(ctx);
        std::cout << "(node(s) created)\n";
        exec_ms = 0;
        return;
    }

    auto executors = ctx.planner.genPipelineExecutors();
    if (executors.empty()) { spdlog::error("Plan empty"); return; }
    if (!executors.back() || !executors.back()->pipeline ||
        !executors.back()->pipeline->GetSink())
        throw std::runtime_error("Pipeline executor is incomplete");

    bool do_profile = ctx.state.profile || ctx.cli.enable_profile;
    auto& profiler = QueryProfiler::Get(*ctx.client);
    profiler.StartQuery(query, do_profile);
    profiler.Initialize(executors.back()->pipeline->GetSink());

    {
        SCOPED_TIMER(Execute, spdlog::level::info, spdlog::level::info, exec_ms);
        for (size_t i = 0; i < executors.size(); i++) {
            SUBTIMER_START(Execute, "Pipeline " + std::to_string(i));
            executors[i]->ExecutePipeline();
            SUBTIMER_STOP(Execute, "Pipeline " + std::to_string(i));
        }
    }
    profiler.EndQuery();

    auto& results   = executors.back()->context->query_results;
    auto& schema    = executors.back()->pipeline->GetSink()->schema;
    auto  col_names = ctx.planner.getQueryOutputColNames();

    // Apply pending SET/DELETE mutations BEFORE rendering
    bool had_mutation = !g_mutation.pending_set_items.empty() || g_mutation.pending_delete;
    ApplyPendingSet(ctx, *results);
    ApplyPendingDelete(ctx, *results);

    if (had_mutation) {
        // For mutation queries, show a confirmation message instead of dumping columns
        std::cout << "(properties set / nodes deleted)\n";
    } else {
        auto render_opts = BuildRenderOptions(ctx.state);
        turbolynx::RenderResults(ctx.state.output_mode, col_names, *results, schema, render_opts);

        // If log_file is set, also render to log file
        if (!ctx.state.log_file.empty()) {
            turbolynx::RenderOptions log_opts = render_opts;
            log_opts.output_file = ctx.state.log_file;
            turbolynx::RenderResults(ctx.state.output_mode, col_names, *results, schema, log_opts);
        }
    }

    // cleanup
    for (auto& chunk : *results) chunk.reset();
    results->clear();
    for (auto* e : executors) delete e;
}

// ------------------------------------------------------------------ NL2Cypher executor
//
// Concrete CypherExecutor that drives the shell's existing Planner
// pipeline and captures the result rows into duckdb::Value vectors
// instead of rendering them. Used by ProfileCollector and (later)
// the S3 multi-candidate validator.
//
// Caveats:
//   * Re-uses ExecContext::planner. Profiling queries share the same
//     planner state as the user queries — harmless because each
//     execute() resets it.
//   * Catches all exceptions and returns {ok=false, error=...} so a
//     bad property doesn't kill a profiling sweep.
//   * Skips rendering, profiling, and the QueryProfiler hooks; this
//     is intentional — profiling queries should be invisible to the
//     normal user-facing profiler.

class ShellCypherExecutor : public turbolynx::nl2cypher::CypherExecutor {
public:
    explicit ShellCypherExecutor(ExecContext& ctx) : ctx_(ctx) {}

    turbolynx::nl2cypher::CypherResult Execute(const std::string& cypher) override {
        using turbolynx::nl2cypher::CypherResult;
        CypherResult out;

        std::string q = cypher;
        if (q.empty() || q.back() != ';') q.push_back(';');

        try {
            double compile_ms = 0;
            CompileQuery(q, ctx_, compile_ms);

            auto executors = ctx_.planner.genPipelineExecutors();
            if (executors.empty()) {
                out.ok = false; out.error = "empty plan";
                return out;
            }
            if (!executors.back() || !executors.back()->pipeline ||
                !executors.back()->pipeline->GetSink()) {
                for (auto* e : executors) delete e;
                out.ok = false; out.error = "incomplete pipeline";
                return out;
            }

            for (size_t i = 0; i < executors.size(); ++i) {
                executors[i]->ExecutePipeline();
            }

            auto& results = executors.back()->context->query_results;
            out.col_names = ctx_.planner.getQueryOutputColNames();

            // Materialise every (chunk, row) into a flat row vector.
            for (auto& chunk : *results) {
                if (!chunk) continue;
                idx_t n_rows = chunk->size();
                idx_t n_cols = chunk->ColumnCount();
                for (idx_t r = 0; r < n_rows; ++r) {
                    std::vector<duckdb::Value> row;
                    row.reserve(n_cols);
                    for (idx_t c = 0; c < n_cols; ++c) {
                        row.push_back(chunk->GetValue(c, r));
                    }
                    out.rows.push_back(std::move(row));
                }
            }

            // cleanup (mirrors RunOneIteration)
            for (auto& chunk : *results) chunk.reset();
            results->clear();
            for (auto* e : executors) delete e;

            out.ok = true;
        } catch (const std::exception& e) {
            out.ok = false; out.error = e.what();
        } catch (...) {
            out.ok = false; out.error = "unknown exception";
        }
        return out;
    }

    // S3: compile-only validation — parses + plans but never
    // executes the pipeline. Dramatically cheaper than Execute() for
    // rejecting bad candidates.
    turbolynx::nl2cypher::CompileResult Compile(const std::string& cypher) override {
        turbolynx::nl2cypher::CompileResult out;
        std::string q = cypher;
        if (q.empty() || q.back() != ';') q.push_back(';');
        try {
            double compile_ms = 0;
            CompileQuery(q, ctx_, compile_ms);
            // Ask the planner to produce pipeline executors — this
            // exercises the full physical plan build but still skips
            // execution. We delete them immediately; they own no
            // side-effecting state until ExecutePipeline() is called.
            auto executors = ctx_.planner.genPipelineExecutors();
            bool complete = !executors.empty()
                            && executors.back()
                            && executors.back()->pipeline
                            && executors.back()->pipeline->GetSink();
            for (auto* e : executors) delete e;
            if (!complete) {
                out.ok = false;
                out.error = "planner produced incomplete pipeline";
                return out;
            }
            out.ok = true;
        } catch (const std::exception& e) {
            out.ok = false; out.error = e.what();
        } catch (...) {
            out.ok = false; out.error = "unknown exception";
        }
        return out;
    }

private:
    ExecContext& ctx_;
};

// Execute `.nl profile` — sweeps the catalog and dumps a JSON profile
// next to the LLM cache. Heavy: every (label/edge × property) gets
// 4 Cypher queries plus an endpoint-inference query per edge type.
static void RunNLProfile(ExecContext& ctx) {
    using namespace turbolynx::nl2cypher;

    GraphSchema schema;
    try {
        schema = IntrospectGraphSchema(*ctx.client);
    } catch (const std::exception& e) {
        PrintError(std::string("schema introspection failed: ") + e.what());
        return;
    }

    std::cout << "[nl-profile] sweeping " << schema.labels.size()
              << " label(s), " << schema.edges.size() << " edge type(s)\n";

    ShellCypherExecutor executor(ctx);
    ProfileCollector::Config cfg;
    cfg.on_property_start = [](const std::string& tag) {
        std::cout << "  " << tag << "\n" << std::flush;
    };
    ProfileCollector collector(executor, cfg);

    GraphProfile profile;
    try {
        profile = collector.CollectAll(schema);
    } catch (const std::exception& e) {
        PrintError(std::string("profile collect threw: ") + e.what());
        return;
    }

    std::cout << "[nl-profile] done in " << profile.collect_seconds
              << "s, " << profile.n_queries << " queries ("
              << profile.n_failed << " failed)\n";

    // Persist to <workspace>/.nl2cypher/metadata.json if a workspace
    // is set; otherwise just print to stdout.
    if (!ctx.state.workspace.empty()) {
        std::string dir  = ctx.state.workspace + "/.nl2cypher";
        std::string path = dir + "/metadata.json";
        std::string mkdir_cmd = "mkdir -p '" + dir + "'";
        if (std::system(mkdir_cmd.c_str()) != 0) {
            PrintError("failed to create " + dir);
            return;
        }
        std::ofstream f(path);
        if (!f) {
            PrintError("failed to open " + path);
            return;
        }
        // `replace` substitutes U+FFFD for any invalid UTF-8 byte —
        // LDBC has binary-ish strings (locationIP, browserUsed) that
        // are not valid UTF-8 and would otherwise abort the dump.
        f << profile.ToJson().dump(
                 2, ' ', false, nlohmann::json::error_handler_t::replace)
          << '\n';
        std::cout << "[nl-profile] wrote " << path << "\n";
    } else {
        std::cout << profile.ToJson().dump(
                         2, ' ', false, nlohmann::json::error_handler_t::replace)
                  << "\n";
    }
}

// Execute `.nl summarize` — load the existing metadata.json, run the
// ProfileSummarizer to fill in short/long descriptions for every
// label/edge/property, and write the enriched JSON back. Cheap
// relative to profiling: one LLM call per label + one per edge.
static void RunNLSummarize(ExecContext& ctx) {
    using namespace turbolynx::nl2cypher;

    if (ctx.state.workspace.empty()) {
        PrintError("workspace not set — can't locate metadata.json");
        return;
    }
    std::string path = ctx.state.workspace + "/.nl2cypher/metadata.json";
    std::ifstream f(path);
    if (!f) {
        PrintError("no metadata.json at " + path
                   + " — run `.nl profile` first");
        return;
    }

    GraphProfile profile;
    try {
        nlohmann::json j;
        f >> j;
        profile = GraphProfile::FromJson(j);
    } catch (const std::exception& e) {
        PrintError(std::string("failed to parse metadata.json: ") + e.what());
        return;
    }
    std::cout << "[nl-summarize] loaded " << profile.labels.size()
              << " label(s), " << profile.edges.size() << " edge type(s)\n";

    // Spin up a dedicated LLMClient pointed at the existing llm_cache
    // so summaries are cached across re-runs (changing a property's
    // stats would change the cache key naturally).
    LLMClient::Config lcfg;
    lcfg.pool_size = 2;
    lcfg.cache_dir = ctx.state.workspace + "/.nl2cypher/llm_cache";
    LLMClient llm(lcfg);

    ProfileSummarizer::Config scfg;
    scfg.on_entity_start = [](const std::string& tag) {
        std::cout << "  " << tag << "\n" << std::flush;
    };
    ProfileSummarizer summarizer(llm, scfg);

    auto t0 = std::chrono::steady_clock::now();
    int n_ok = summarizer.SummarizeAll(profile);
    auto t1 = std::chrono::steady_clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count();
    std::cout << "[nl-summarize] done in " << sec << "s, "
              << n_ok << " entity summaries\n";

    // Write back in place.
    std::ofstream out(path);
    if (!out) { PrintError("failed to reopen " + path); return; }
    out << profile.ToJson().dump(
               2, ' ', false, nlohmann::json::error_handler_t::replace)
        << '\n';
    std::cout << "[nl-summarize] wrote " << path << "\n";

    // If the engine already exists in this shell session, tell it to
    // pick up the new file immediately — otherwise the next translate
    // would still use the stale in-memory copy.
    if (ctx.nl_engine) {
        ctx.nl_engine->ReloadProfile();
        std::cout << "[nl-summarize] engine reloaded profile\n";
    }
}

// ------------------------------------------------------------------ NL2Cypher test harness
//
// `.nl test <path.jsonl>` drives a batch of NL→Cypher evaluations:
// each line in the JSONL file is an object {name, nl, ref} where
// `nl` is the natural-language prompt and `ref` is a hand-written
// reference Cypher query. For each case we:
//   1. Translate `nl` via the NL2Cypher engine.
//   2. Execute both the translated Cypher and the reference Cypher.
//   3. Compare the result sets by canonicalising every row to a
//      pipe-joined string and sorting — this is order-insensitive,
//      which matches the "correct result set" definition most
//      NL2SQL benchmarks use.
//   4. Mark PASS / DIFF / FAIL_TRANSLATE / FAIL_EXEC accordingly.
// Results are printed per-case and summarised at the end.

namespace {

static std::string CanonicalValue(const duckdb::Value& v) {
    if (v.IsNull()) return "<NULL>";
    return v.ToString();
}

static std::vector<std::string> CanonicalRows(
    const turbolynx::nl2cypher::CypherResult& r) {
    std::vector<std::string> out;
    out.reserve(r.rows.size());
    for (const auto& row : r.rows) {
        std::string s;
        for (size_t i = 0; i < row.size(); ++i) {
            if (i) s += "|";
            s += CanonicalValue(row[i]);
        }
        out.push_back(std::move(s));
    }
    std::sort(out.begin(), out.end());
    return out;
}

// Small JSON pull — reuses nlohmann to parse each JSONL line.
struct NLTestCase {
    std::string name;
    std::string difficulty;
    std::string nl;
    std::string ref;
};

static std::vector<NLTestCase> LoadJsonl(const std::string& path) {
    std::vector<NLTestCase> out;
    std::ifstream f(path);
    if (!f) throw std::runtime_error("cannot open " + path);
    std::string line;
    while (std::getline(f, line)) {
        // Skip empty / comment lines
        size_t a = line.find_first_not_of(" \t\r\n");
        if (a == std::string::npos) continue;
        if (line[a] == '#') continue;
        auto j = nlohmann::json::parse(line);
        NLTestCase c;
        c.name       = j.value("name", "");
        c.difficulty = j.value("difficulty", "");
        c.nl         = j.at("nl").get<std::string>();
        c.ref        = j.at("ref").get<std::string>();
        out.push_back(std::move(c));
    }
    return out;
}

}  // anon

static void RunNLTest(const std::string& path, ExecContext& ctx) {
    using namespace turbolynx::nl2cypher;

    std::vector<NLTestCase> cases;
    try { cases = LoadJsonl(path); }
    catch (const std::exception& e) {
        PrintError(std::string("cannot load test set: ") + e.what());
        return;
    }
    std::cout << "[nl-test] " << cases.size() << " case(s) from " << path << "\n";

    // S3 validator: the executor used for running ref+gen queries
    // doubles as the compile-only validator that the engine uses to
    // vote between multi-candidate drafts. Kept alive for the whole
    // test run so the engine's back-reference is stable.
    static ShellCypherExecutor s_executor(ctx);

    if (!ctx.nl_engine) {
        NL2CypherEngine::Config cfg;
        cfg.workspace = ctx.state.workspace;
        cfg.llm.pool_size = 3;
        // S2 schema linker: enable when a workspace (and therefore
        // potentially a metadata.json) exists. The engine gracefully
        // falls back to the full rich prompt if the profile has no
        // summaries, so this is a no-op on S0-only setups.
        if (!cfg.workspace.empty()) {
            cfg.linker_variants = {"compact", "samples"};
        }
        // S3 multi-candidate voting.
        cfg.n_candidates = 3;
        cfg.validator    = &s_executor;
        try {
            ctx.nl_engine = std::make_unique<NL2CypherEngine>(*ctx.client, cfg);
        } catch (const std::exception& e) {
            PrintError(std::string("engine init failed: ") + e.what());
            return;
        }
    }

    // Reuse the same executor instance the engine's S3 validator
    // points at, so all query runs share one Planner state machine.
    ShellCypherExecutor& executor = s_executor;

    int n_pass = 0, n_diff = 0, n_translate_fail = 0, n_exec_fail = 0;
    struct FailInfo { std::string name; std::string reason; };
    std::vector<FailInfo> failures;

    // Per-case result log: JSON array written next to the test set.
    nlohmann::json log = nlohmann::json::array();
    auto t0 = std::chrono::steady_clock::now();

    for (size_t i = 0; i < cases.size(); ++i) {
        const auto& c = cases[i];
        std::cout << "\n[" << (i + 1) << "/" << cases.size() << "] "
                  << c.name << " (" << c.difficulty << ")\n"
                  << "  nl:  " << c.nl << "\n";

        nlohmann::json entry;
        entry["name"]       = c.name;
        entry["difficulty"] = c.difficulty;
        entry["nl"]         = c.nl;
        entry["ref"]        = c.ref;

        // 1) translate
        TranslationResult tr;
        try { tr = ctx.nl_engine->Translate(c.nl); }
        catch (const std::exception& e) {
            tr.ok = false; tr.error = e.what();
        }
        if (!tr.ok) {
            std::cout << "  status: FAIL_TRANSLATE\n  error: " << tr.error << "\n";
            n_translate_fail++;
            failures.push_back({c.name, "translate: " + tr.error});
            entry["status"] = "fail_translate";
            entry["error"]  = tr.error;
            log.push_back(std::move(entry));
            continue;
        }
        std::cout << "  gen: " << tr.cypher << "\n";
        entry["generated"] = tr.cypher;
        entry["used_cache"] = tr.used_cache;

        // 2) execute both
        auto gen_r = executor.Execute(tr.cypher);
        auto ref_r = executor.Execute(c.ref);

        if (!ref_r.ok) {
            std::cout << "  status: FAIL_REF_EXEC (bad reference query)\n"
                      << "  error: " << ref_r.error << "\n";
            n_exec_fail++;
            failures.push_back({c.name, "ref exec: " + ref_r.error});
            entry["status"]    = "fail_ref_exec";
            entry["error"]     = ref_r.error;
            log.push_back(std::move(entry));
            continue;
        }
        if (!gen_r.ok) {
            std::cout << "  status: FAIL_GEN_EXEC (generated Cypher didn't run)\n"
                      << "  error: " << gen_r.error << "\n";
            n_exec_fail++;
            failures.push_back({c.name, "gen exec: " + gen_r.error});
            entry["status"]    = "fail_gen_exec";
            entry["error"]     = gen_r.error;
            log.push_back(std::move(entry));
            continue;
        }

        // 3) compare canonicalised rows
        auto gen_canon = CanonicalRows(gen_r);
        auto ref_canon = CanonicalRows(ref_r);
        if (gen_canon == ref_canon) {
            std::cout << "  status: PASS (" << ref_canon.size() << " rows)\n";
            n_pass++;
            entry["status"] = "pass";
            entry["n_rows"] = ref_canon.size();
        } else {
            std::cout << "  status: DIFF (gen=" << gen_canon.size()
                      << " rows, ref=" << ref_canon.size() << " rows)\n";
            // Show up to 3 mismatching rows from each side.
            auto show = [](const char* label, const std::vector<std::string>& v) {
                std::cout << "  " << label << ":";
                for (size_t i = 0; i < std::min<size_t>(3, v.size()); ++i)
                    std::cout << "\n    " << v[i];
                if (v.size() > 3) std::cout << "\n    ...";
                std::cout << "\n";
            };
            show("gen_head", gen_canon);
            show("ref_head", ref_canon);
            n_diff++;
            failures.push_back({c.name, "diff"});
            entry["status"]       = "diff";
            entry["gen_n_rows"]   = gen_canon.size();
            entry["ref_n_rows"]   = ref_canon.size();
        }
        log.push_back(std::move(entry));
    }

    auto t1 = std::chrono::steady_clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count();

    std::cout << "\n======================================\n";
    std::cout << "[nl-test] " << cases.size() << " case(s) in " << sec << "s\n"
              << "  PASS:             " << n_pass << "\n"
              << "  DIFF:             " << n_diff << "\n"
              << "  FAIL_TRANSLATE:   " << n_translate_fail << "\n"
              << "  FAIL_EXEC:        " << n_exec_fail << "\n";
    if (!failures.empty()) {
        std::cout << "\nFailures:\n";
        for (const auto& f : failures)
            std::cout << "  - " << f.name << ": " << f.reason << "\n";
    }

    // Write log JSON next to the test set for later analysis.
    try {
        std::string out_path = path + ".result.json";
        std::ofstream out(out_path);
        if (out) {
            nlohmann::json root;
            root["path"]   = path;
            root["pass"]   = n_pass;
            root["diff"]   = n_diff;
            root["fail_translate"] = n_translate_fail;
            root["fail_exec"]      = n_exec_fail;
            root["total"]  = (int)cases.size();
            root["seconds"] = sec;
            root["cases"]  = std::move(log);
            out << root.dump(2, ' ', false,
                             nlohmann::json::error_handler_t::replace) << "\n";
            std::cout << "[nl-test] wrote " << out_path << "\n";
        }
    } catch (const std::exception& e) {
        spdlog::warn("[nl-test] writing result json failed: {}", e.what());
    }
}

// Detect if a query string contains mutation keywords
static bool QueryHasMutation(const std::string& q) {
    std::regex mut_re(R"(\b(?:CREATE|SET|DELETE|DETACH|MERGE|REMOVE)\b)", std::regex::icase);
    return std::regex_search(q, mut_re);
}

static void RunQuery(const std::string& raw_query, ExecContext& ctx) {
    // Normalize: strip trailing semicolons/whitespace and reject empty input.
    std::string query = raw_query;
    while (!query.empty() && (query.back() == ';' || query.back() == ' ' ||
                              query.back() == '\t' || query.back() == '\n' ||
                              query.back() == '\r'))
        query.pop_back();
    if (query.find_first_not_of(" \t\n\r") == std::string::npos) return;

    if (ctx.state.echo) std::cout << query << '\n';

    bool is_mutation = QueryHasMutation(query);

    int iters = ctx.cli.iterations + (ctx.cli.warmup ? 1 : 0);
    std::vector<double> compile_times, exec_times;

    for (int i = 0; i < iters; i++) {
        double c = 0, e = 0;
        RunOneIteration(query, ctx, c, e);
        compile_times.push_back(c);
        exec_times.push_back(e);
    }

    if (ctx.cli.warmup) {
        if (!compile_times.empty()) compile_times.erase(compile_times.begin());
        if (!exec_times.empty())    exec_times.erase(exec_times.begin());
    }

    if (ctx.state.timer_enabled && !exec_times.empty()) {
        double avg_c = std::accumulate(compile_times.begin(), compile_times.end(), 0.0) / compile_times.size();
        double avg_e = std::accumulate(exec_times.begin(),    exec_times.end(),    0.0) / exec_times.size();
        std::cout << "Time: compile " << avg_c << " ms, execute " << avg_e
                  << " ms, total " << (avg_c + avg_e) << " ms\n";
        LogQuery(query, avg_c, avg_e, ctx.state.log_file);
    }

    // Auto-checkpoint after mutations so changes are immediately visible
    if (is_mutation) {
        try {
            turbolynx_checkpoint_ctx(*ctx.client);
        } catch (...) {
            // Checkpoint failure is non-fatal
        }
    }
}

// ------------------------------------------------------------------ AIO init

static void InitializeDiskAIO(const std::string& workspace) {
    DiskAioParameters::NUM_THREADS          = 32;
    DiskAioParameters::NUM_TOTAL_CPU_CORES  = 32;
    DiskAioParameters::NUM_CPU_SOCKETS      = 2;
    DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
    DiskAioParameters::WORKSPACE            = workspace;

    int res;
    new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
    core_id::set_core_ids(DiskAioParameters::NUM_THREADS);
}

// ------------------------------------------------------------------ rc file

static void LoadRcFile(const std::string& path, ExecContext& ctx) {
    std::ifstream f(path);
    if (!f) return;

    auto executor = [&ctx](const std::string& q) {
        try { RunQuery(q, ctx); }
        catch (const std::exception& ex) { PrintError(ex.what()); }
    };

    std::string line;
    while (std::getline(f, line)) {
        // Strip comments
        size_t hash = line.find('#');
        if (hash != std::string::npos) line = line.substr(0, hash);

        size_t start = line.find_first_not_of(" \t");
        if (start == std::string::npos) continue;
        line = line.substr(start);
        if (line.empty()) continue;

        if (line[0] == '.' || line[0] == ':') {
            turbolynx::HandleDotCommand(line, ctx.state, ctx.client, executor);
        } else if (!line.empty() && line.back() == ';') {
            line.pop_back();
            try { RunQuery(line, ctx); }
            catch (const std::exception& ex) { PrintError(ex.what()); }
        }
    }
}

// ------------------------------------------------------------------ replxx input

// Read one logical "unit" from the user.
// Handles:
//   - single-line input (interactive typing)
//   - multi-line paste: replxx accumulates the full paste (including embedded
//     \n) and returns it in a single input() call when paste mode ends
//   - continuation prompting when no ';' found yet
//
// In NL mode (`nl_mode == true`) the function:
//   - shows a "(NL)" suffix in the prompt
//   - submits on ENTER (no semicolon required, no continuation)
//   - still recognises dot/colon commands so the user can `.nl off`
static std::optional<std::string> GetQueryString(replxx::Replxx& rx,
                                                  const std::string& prompt,
                                                  bool nl_mode) {
    std::string full_input;
    std::string mode_tag    = nl_mode ? " (NL)" : "";
    std::string shell_prompt = prompt + mode_tag + " >> ";
    std::string cont_prompt  = prompt + mode_tag + " -> ";

    while (true) {
        errno = 0;
        const char* raw = rx.input(full_input.empty() ? shell_prompt : cont_prompt);
        if (!raw) {
            if (errno == EAGAIN) {
                // Ctrl+C — abort current input and restart
                std::cout << "Interrupted "
                          "(Note that Cypher queries must end with a semicolon. "
                          "Type :exit to exit the shell.)\n";
                full_input.clear();
                continue;
            }
            return std::nullopt;   // Ctrl+D / EOF
        }

        // replxx may return embedded \n when pasting multi-line content.
        // Flatten newlines to spaces for the accumulated query string
        // (Cypher treats whitespace uniformly).
        std::string s;
        s.reserve(strlen(raw));
        for (const char* p = raw; *p; ++p)
            s += (*p == '\n' || *p == '\r') ? ' ' : *p;

        // Strip trailing whitespace
        while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) s.pop_back();
        if (s.empty()) continue;

        // Dot/colon commands — submit immediately without semicolon.
        // Recognised in both Cypher and NL modes so the user can always
        // type `.nl off`, `.exit`, etc.
        if (full_input.empty()) {
            size_t tstart = s.find_first_not_of(" \t");
            if (tstart != std::string::npos &&
                (s[tstart] == '.' || s[tstart] == ':')) {
                return s;
            }
        }

        // NL mode: ENTER submits the line as-is. Natural language has no
        // ';' terminator, and we don't accumulate multi-line input.
        if (nl_mode) {
            return s;
        }

        // Cypher mode: accumulate until ';'
        size_t semi = s.find(';');
        if (semi != std::string::npos) {
            full_input += s.substr(0, semi + 1);
            break;
        }
        full_input += s + ' ';
    }
    return full_input;
}

// NL prompt handler. Lazily constructs the NL2CypherEngine on the
// first call (so the LLM pool is only spawned when the user actually
// turns NL mode on), translates the question to Cypher, prints the
// generated query for transparency, and then runs it through the
// existing Cypher executor path (RunQuery).
static void RunNL(const std::string& nl_text, ExecContext& ctx) {
    using namespace turbolynx::nl2cypher;
    if (!ctx.nl_engine) {
        NL2CypherEngine::Config cfg;
        cfg.workspace = ctx.state.workspace;
        // Pool size 1 — interactive use, one prompt at a time.
        cfg.llm.pool_size = 1;
        try {
            ctx.nl_engine = std::make_unique<NL2CypherEngine>(*ctx.client, cfg);
        } catch (const std::exception& e) {
            PrintError(std::string("NL2Cypher init failed: ") + e.what());
            return;
        }
    }

    TranslationResult tr;
    try {
        tr = ctx.nl_engine->Translate(nl_text);
    } catch (const std::exception& e) {
        PrintError(std::string("NL2Cypher translate threw: ") + e.what());
        return;
    }

    if (!tr.ok) {
        PrintError("NL2Cypher: " + tr.error);
        return;
    }

    // Show the generated Cypher so the user can verify / copy / edit.
    std::cout << "-- generated Cypher"
              << (tr.used_cache ? " (cached)" : "")
              << " --\n"
              << tr.cypher << "\n"
              << "-------------------------\n";

    // Hand off to the existing executor path. We append a semicolon if
    // missing — RunQuery itself trims trailing semicolons.
    std::string to_run = tr.cypher;
    if (to_run.empty() || to_run.back() != ';') to_run.push_back(';');
    try {
        RunQuery(to_run, ctx);
    } catch (const std::exception& e) {
        PrintError(std::string("execution failed: ") + e.what());
    }
}

// ------------------------------------------------------------------ REPL

static void RunInteractive(replxx::Replxx& rx, ExecContext& ctx) {
    std::string prev;
    std::cout << "TurboLynx shell — type '.help' for commands, ':exit' to quit\n";

    while (true) {
        auto opt_input = GetQueryString(rx, ctx.state.prompt, ctx.state.nl_mode);
        if (!opt_input) break;   // Ctrl+D / EOF
        std::string input = std::move(*opt_input);
        if (input.empty()) continue;

        std::string trimmed = input;
        size_t start = trimmed.find_first_not_of(" \t\n\r");
        if (start != std::string::npos) trimmed = trimmed.substr(start);

        // Exit commands
        if (trimmed == ":exit" || trimmed == ".exit" || trimmed == ".quit") break;

        // Checkpoint / compact command
        if (trimmed == ".checkpoint" || trimmed == ":checkpoint" ||
            trimmed == ".compact"    || trimmed == ":compact") {
            if (trimmed != prev) { rx.history_add(trimmed); prev = trimmed; }
            try {
                turbolynx_checkpoint_ctx(*ctx.client);
                std::cout << "(checkpoint complete)\n";
            } catch (const std::exception& e) { PrintError(e.what()); }
            continue;
        }

        if (!trimmed.empty() && (trimmed[0] == '.' || trimmed[0] == ':')) {
            if (!trimmed.empty() && trimmed.back() == ';') trimmed.pop_back();
            // `.nl profile` / `.nl test <path>` need ExecContext
            // (planner) — handle them here before HandleDotCommand,
            // which only sees state+client.
            if (trimmed == ".nl profile" || trimmed == ":nl profile") {
                if (trimmed != prev) { rx.history_add(trimmed); prev = trimmed; }
                try { RunNLProfile(ctx); }
                catch (const std::exception& e) { PrintError(e.what()); }
                continue;
            }
            if (trimmed == ".nl summarize" || trimmed == ":nl summarize") {
                if (trimmed != prev) { rx.history_add(trimmed); prev = trimmed; }
                try { RunNLSummarize(ctx); }
                catch (const std::exception& e) { PrintError(e.what()); }
                continue;
            }
            {
                const std::string pfx_dot = ".nl test ";
                const std::string pfx_col = ":nl test ";
                std::string tpath;
                if (trimmed.rfind(pfx_dot, 0) == 0) tpath = trimmed.substr(pfx_dot.size());
                else if (trimmed.rfind(pfx_col, 0) == 0) tpath = trimmed.substr(pfx_col.size());
                if (!tpath.empty()) {
                    // Strip any surrounding whitespace/quotes.
                    while (!tpath.empty() && (tpath.front() == ' ' || tpath.front() == '"'
                                              || tpath.front() == '\''))
                        tpath.erase(tpath.begin());
                    while (!tpath.empty() && (tpath.back() == ' ' || tpath.back() == '"'
                                              || tpath.back() == '\''))
                        tpath.pop_back();
                    if (trimmed != prev) { rx.history_add(trimmed); prev = trimmed; }
                    try { RunNLTest(tpath, ctx); }
                    catch (const std::exception& e) { PrintError(e.what()); }
                    continue;
                }
            }
            auto executor = [&ctx](const std::string& q) {
                try { RunQuery(q, ctx); }
                catch (const std::exception& ex) { PrintError(ex.what()); }
            };
            if (turbolynx::HandleDotCommand(trimmed, ctx.state, ctx.client, executor)) {
                if (trimmed != prev) {
                    rx.history_add(trimmed);
                    prev = trimmed;
                }
                continue;
            }
            break;
        }

        // NL mode: bypass the Cypher pipeline entirely. Each ENTER is one
        // NL prompt, sent (eventually) through NL2Cypher. For now we hand
        // it to a stub so the mode is testable without the LLM path.
        if (ctx.state.nl_mode) {
            // Skip empty / whitespace-only prompts
            if (trimmed.empty()) continue;
            if (trimmed != prev) {
                rx.history_add(trimmed);
                prev = trimmed;
            }
            try {
                RunNL(trimmed, ctx);
            } catch (const std::exception& e) {
                PrintError(e.what());
                if (ctx.state.bail) break;
            }
            continue;
        }

        // Cypher query — strip trailing semicolon and whitespace
        while (!input.empty() && (input.back() == ';' || input.back() == ' '))
            input.pop_back();

        // Skip empty / whitespace-only queries
        if (input.find_first_not_of(" \t\n\r") == std::string::npos) continue;

        if (input != prev) {
            rx.history_add(input);
            prev = input;
        }

        // Signal-safe query execution: catch SIGSEGV/SIGABRT via longjmp
        int crash_sig = sigsetjmp(g_crash_jmpbuf, 1);
        if (crash_sig != 0) {
            // Recovered from a signal — report as error, not crash
            const char *sig_name = crash_sig == SIGSEGV ? "SIGSEGV" :
                                   crash_sig == SIGABRT ? "SIGABRT" :
                                   crash_sig == SIGFPE  ? "SIGFPE" : "SIGNAL";
            PrintError(std::string("Internal error (") + sig_name +
                       ") — query could not be executed safely");
            // Re-register signal handlers (they reset after longjmp)
            signal(SIGSEGV, crash_signal_handler);
            signal(SIGABRT, crash_signal_handler);
            signal(SIGFPE, crash_signal_handler);
            if (ctx.state.bail) break;
            continue;
        }

        g_in_query = 1;
        try {
            RunQuery(input, ctx);
        } catch (const duckdb::Exception& e) {
            PrintError(e.what());
            if (ctx.state.bail) break;
        } catch (const std::exception& e) {
            PrintError(e.what());
            if (ctx.state.bail) break;
        } catch (...) {
            // Belt-and-suspenders: anything else that escapes (e.g. a stray
            // gpos::CException) must not terminate the shell.
            PrintError("Unknown internal error — query could not be executed");
            if (ctx.state.bail) break;
        }
        g_in_query = 0;
    }
}

// ------------------------------------------------------------------ entry point

int RunShell(int argc, char** argv) {
    SetupLogger();

    // Register signal handlers for crash recovery in interactive mode
    signal(SIGSEGV, crash_signal_handler);
    signal(SIGABRT, crash_signal_handler);
    signal(SIGFPE, crash_signal_handler);

    ShellCliOptions cli;
    ParseShellOptions(argc, argv, cli);

    if (cli.workspace.empty()) {
        std::cerr << "Error: --workspace <path> is required.\n"
                  << "Run 'turbolynx shell --help' for usage.\n";
        return 1;
    }

    // Auto-initialize a fresh workspace directory if it doesn't exist yet.
    // Opening an empty directory is also fine — ChunkCacheManager creates
    // store.db on demand, and Catalog::LoadCatalog seeds a default graph
    // entry for fresh databases.
    {
        std::error_code ec;
        if (!std::filesystem::exists(cli.workspace, ec)) {
            std::filesystem::create_directories(cli.workspace, ec);
            if (ec) {
                std::cerr << "Error: cannot create workspace '"
                          << cli.workspace << "': " << ec.message() << '\n';
                return 1;
            }
        } else if (!std::filesystem::is_directory(cli.workspace, ec)) {
            std::cerr << "Error: workspace path '" << cli.workspace
                      << "' exists but is not a directory.\n";
            return 1;
        }
    }

    replxx::Replxx rx;
    rx.history_load(cli.workspace + "/.history");
    turbolynx::SetupCompletion(rx);

    // Suppress tcmalloc large-alloc messages (threshold = 10 GB)
    setenv("TCMALLOC_LARGE_ALLOC_REPORT_THRESHOLD", "10737418240", 0);

    InitializeDiskAIO(cli.workspace);

    ChunkCacheManager::ccm = new ChunkCacheManager(cli.workspace.c_str(), cli.standalone);
    auto database = std::make_unique<DuckDB>(cli.workspace.c_str());
    // WAL: replay existing log to restore DeltaStore, then open writer for new mutations
    duckdb::WALReader::Replay(cli.workspace, database->instance->delta_store);
    database->instance->wal_writer = std::make_unique<duckdb::WALWriter>(cli.workspace);
    auto client   = std::make_shared<duckdb::ClientContext>(database->instance->shared_from_this());
    duckdb::SetClientWrapper(client, std::make_shared<CatalogWrapper>(client->db->GetCatalogWrapper()));
    if (cli.enable_profile) client->EnableProfiling(); else client->DisableProfiling();

    turbolynx::Planner planner(cli.planner_config, turbolynx::MDProviderType::TBGPP, client.get());

    turbolynx::ShellState state;
    state.workspace = cli.workspace;
    if (!cli.output_mode.empty())
        state.output_mode = turbolynx::ParseOutputMode(cli.output_mode);

    ExecContext ctx{client, cli, state, planner, nullptr};

    // Populate autocomplete with vertex labels + edge types from catalog
    turbolynx::PopulateCompletions(rx, *client);

    // Load ~/.turbolynxrc if it exists
    const char* home = std::getenv("HOME");
    if (home) {
        LoadRcFile(std::string(home) + "/.turbolynxrc", ctx);
    }

    if (!cli.query.empty()) {
        try {
            RunQuery(cli.query, ctx);
        } catch (const std::exception& e) {
            PrintError(e.what());
        } catch (...) {
            PrintError("Unknown internal error — query could not be executed");
        }
    } else if (!cli.query_file.empty()) {
        std::ifstream f(cli.query_file);
        if (!f) {
            PrintError("cannot open query file: " + cli.query_file);
        } else {
            std::string buf, line;
            while (std::getline(f, line)) {
                buf += line + '\n';
                size_t pos;
                while ((pos = buf.find(';')) != std::string::npos) {
                    std::string q = buf.substr(0, pos);
                    buf = buf.substr(pos + 1);
                    size_t s = q.find_first_not_of(" \t\n\r");
                    if (s == std::string::npos) continue;
                    try { RunQuery(q.substr(s), ctx); }
                    catch (const std::exception& e) { PrintError(e.what()); }
                    catch (...) { PrintError("Unknown internal error"); }
                }
            }
            size_t s = buf.find_first_not_of(" \t\n\r");
            if (s != std::string::npos) {
                try { RunQuery(buf.substr(s), ctx); }
                catch (const std::exception& e) { PrintError(e.what()); }
                catch (...) { PrintError("Unknown internal error"); }
            }
        }
    } else {
        RunInteractive(rx, ctx);
    }

    rx.history_save(cli.workspace + "/.history");
    delete ChunkCacheManager::ccm;
    return 0;
}

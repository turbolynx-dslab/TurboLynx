#include "include/commands.hpp"

#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "common/constants.hpp"
#include "common/enums/graph_component_type.hpp"
#include "main/database.hpp"
#include "storage/statistics/histogram_generator.hpp"
#include "storage/cache/chunk_cache_manager.h"

#include <fstream>
#include <iostream>
#include <sstream>

using namespace duckdb;

namespace turbolynx {

// ------------------------------------------------------------------ helpers

static std::vector<std::string> SplitArgs(const std::string& s) {
    std::istringstream ss(s);
    std::vector<std::string> tokens;
    std::string tok;
    while (ss >> tok) tokens.push_back(tok);
    return tokens;
}

static GraphCatalogEntry* GetGraphCatalog(ClientContext& ctx) {
    auto& catalog = ctx.db->GetCatalog();
    return (GraphCatalogEntry*)catalog.GetEntry(
        ctx, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
}

static PartitionCatalogEntry* GetPartition(ClientContext& ctx, idx_t oid) {
    auto& catalog = ctx.db->GetCatalog();
    return (PartitionCatalogEntry*)catalog.GetEntry(ctx, DEFAULT_SCHEMA, oid);
}

static std::string TypeName(LogicalTypeId tid) {
    switch (tid) {
        case LogicalTypeId::BIGINT:  return "BIGINT";
        case LogicalTypeId::UBIGINT: return "UBIGINT";
        case LogicalTypeId::INTEGER: return "INTEGER";
        case LogicalTypeId::HUGEINT: return "HUGEINT";
        case LogicalTypeId::FLOAT:   return "FLOAT";
        case LogicalTypeId::DOUBLE:  return "DOUBLE";
        case LogicalTypeId::VARCHAR: return "VARCHAR";
        case LogicalTypeId::BOOLEAN: return "BOOLEAN";
        case LogicalTypeId::DATE:    return "DATE";
        case LogicalTypeId::TIMESTAMP: return "TIMESTAMP";
        default:                     return "UNKNOWN";
    }
}

// ------------------------------------------------------------------ .help

static void DoHelp() {
    std::cout <<
        "TurboLynx shell commands:\n"
        "\n"
        "  .help                   Show this help\n"
        "  .tables                 List vertex labels and edge types\n"
        "  .schema <label|type>    Show property schema for a label or edge type\n"
        "  .mode <table|csv|json|markdown>\n"
        "                          Set output format (default: table)\n"
        "  .timer <on|off>         Toggle query timing display (default: on)\n"
        "  .output [file]          Redirect results to file; no arg resets to stdout\n"
        "  .read <file>            Execute Cypher queries from a file\n"
        "  .analyze                Rebuild column statistics (histograms)\n"
        "  .exit / .quit / :exit   Exit the shell\n"
        "\n"
        "Terminate Cypher queries with ';'\n";
}

// ------------------------------------------------------------------ .tables

static void DoTables(ClientContext& ctx) {
    auto* graph = GetGraphCatalog(ctx);
    if (!graph) { std::cout << "(no graph loaded)\n"; return; }

    std::vector<std::string> labels, types;
    graph->GetVertexLabels(labels);
    graph->GetEdgeTypes(types);

    std::cout << "Vertex labels (" << labels.size() << "):\n";
    for (const auto& l : labels) std::cout << "  " << l << '\n';

    std::cout << "\nEdge types (" << types.size() << "):\n";
    for (const auto& t : types) std::cout << "  " << t << '\n';
}

// ------------------------------------------------------------------ .schema

static void DoSchema(ClientContext& ctx, const std::string& name) {
    if (name.empty()) {
        std::cout << "Usage: .schema <label|edge_type>\n";
        return;
    }

    auto* graph = GetGraphCatalog(ctx);
    if (!graph) { std::cout << "(no graph loaded)\n"; return; }

    // Try vertex label first, then edge type
    std::vector<idx_t> oids;
    bool is_edge = false;
    try {
        graph->GetVertexPartitionIndexesInLabel(ctx, name, oids);
    } catch (...) {}

    if (oids.empty()) {
        try {
            graph->GetEdgePartitionIndexesInType(ctx, name, oids);
            is_edge = true;
        } catch (...) {}
    }

    if (oids.empty()) {
        std::cout << "Unknown label/type: " << name << '\n';
        return;
    }

    // Show schema from first partition (graphlet 0 representative)
    auto* part = GetPartition(ctx, oids[0]);
    if (!part) { std::cout << "(partition not found)\n"; return; }

    auto* col_names = part->GetUniversalPropertyKeyNames();
    auto* type_ids  = part->GetUniversalPropertyTypeIds();

    std::cout << (is_edge ? "Edge type: " : "Vertex label: ") << name << '\n';
    std::cout << "  graphlets: " << oids.size() << '\n';
    std::cout << "  columns:\n";

    for (size_t i = 0; i < col_names->size(); i++) {
        std::string tname = (i < type_ids->size()) ? TypeName((*type_ids)[i]) : "?";
        std::cout << "    " << (*col_names)[i] << "  " << tname << '\n';
    }
}

// ------------------------------------------------------------------ .read

static void DoRead(const std::string& filepath,
                   const std::function<void(const std::string&)>& executor) {
    std::ifstream f(filepath);
    if (!f) { std::cerr << "Error: cannot open file: " << filepath << '\n'; return; }

    std::string query, line;
    while (std::getline(f, line)) {
        query += line + ' ';
        size_t pos = query.find(';');
        if (pos != std::string::npos) {
            std::string q = query.substr(0, pos);
            query = query.substr(pos + 1);
            size_t start = q.find_first_not_of(" \t\n\r");
            if (start != std::string::npos) {
                executor(q.substr(start));
            }
        }
    }
}

// ------------------------------------------------------------------ dispatch

bool HandleDotCommand(const std::string& cmd,
                      ShellState& state,
                      std::shared_ptr<ClientContext>& client,
                      const std::function<void(const std::string&)>& executor) {
    // Determine if this is a dot/colon command
    if (cmd.empty()) return false;
    if (cmd[0] != '.' && cmd[0] != ':') return false;

    // Strip leading dot/colon and parse
    std::string stripped = (cmd[0] == '.') ? cmd.substr(1) : cmd.substr(1);
    auto args = SplitArgs(stripped);
    if (args.empty()) return true;

    std::string command = args[0];

    if (command == "help") {
        DoHelp();
    } else if (command == "tables") {
        DoTables(*client);
    } else if (command == "schema") {
        DoSchema(*client, args.size() > 1 ? args[1] : "");
    } else if (command == "mode") {
        if (args.size() < 2) {
            std::cout << "Current mode: " << OutputModeName(state.output_mode) << '\n';
            std::cout << "Usage: .mode <table|csv|json|markdown>\n";
        } else {
            state.output_mode = ParseOutputMode(args[1]);
            std::cout << "Output mode: " << OutputModeName(state.output_mode) << '\n';
        }
    } else if (command == "timer") {
        if (args.size() < 2) {
            std::cout << "Timer is " << (state.timer_enabled ? "on" : "off") << '\n';
        } else {
            state.timer_enabled = (args[1] == "on");
            std::cout << "Timer: " << (state.timer_enabled ? "on" : "off") << '\n';
        }
    } else if (command == "output") {
        if (args.size() < 2) {
            state.output_file.clear();
            std::cout << "Output: stdout\n";
        } else {
            state.output_file = args[1];
            std::cout << "Output: " << state.output_file << '\n';
        }
    } else if (command == "read") {
        if (args.size() < 2) {
            std::cout << "Usage: .read <file>\n";
        } else {
            DoRead(args[1], executor);
        }
    } else if (command == "analyze") {
        HistogramGenerator hist_gen;
        hist_gen.CreateHistogram(client);
    } else if (command == "exit" || command == "quit" || command == "exit") {
        // Handled by the REPL loop checking ":exit" / ".exit"
        // Return false so the loop detects it as ":exit"
        return false;
    } else {
        std::cout << "Unknown command: ." << command << "  (try .help)\n";
    }

    return true;
}

} // namespace turbolynx

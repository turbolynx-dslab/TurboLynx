#include "include/commands.hpp"

#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "common/constants.hpp"
#include "common/enums/graph_component_type.hpp"
#include "main/database.hpp"
#include "storage/statistics/histogram_generator.hpp"
#include "storage/cache/chunk_cache_manager.h"

#include <cstdlib>
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

// Rejoin args[start..] into a single string
static std::string JoinArgs(const std::vector<std::string>& args, size_t start) {
    std::string result;
    for (size_t i = start; i < args.size(); i++) {
        if (i > start) result += ' ';
        result += args[i];
    }
    return result;
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
        default:                       return "UNKNOWN";
    }
}

// ------------------------------------------------------------------ .help

static void DoHelp() {
    std::cout <<
        "TurboLynx shell commands:\n"
        "\n"
        "Output control:\n"
        "  .mode <mode>            Set output format. Modes:\n"
        "                            table (default), box, column, line\n"
        "                            csv, list, tabs\n"
        "                            json, jsonlines, markdown\n"
        "                            html, latex, insert, trash\n"
        "  .headers <on|off>       Show/hide result headers (default: on)\n"
        "  .nullvalue <str>        Display string for NULL values (default: \"\")\n"
        "  .separator <col> [row]  Column separator for list/csv (default: ,)\n"
        "  .maxrows <n>            Limit rows displayed; 0 = unlimited\n"
        "  .width <n>              Minimum column display width; 0 = auto\n"
        "\n"
        "File redirection:\n"
        "  .output [file]          Redirect all results to file; no arg → stdout\n"
        "  .once <file>            Redirect next result only, then revert\n"
        "  .log [file]             Log queries to file; no arg → stop logging\n"
        "\n"
        "Schema inspection:\n"
        "  .tables                 List vertex labels and edge types\n"
        "  .schema <label|type>    Show property schema for a label or edge type\n"
        "  .indexes                Show index info (stub — not yet implemented)\n"
        "\n"
        "Execution:\n"
        "  .read <file>            Execute Cypher/dot commands from a file\n"
        "  .analyze                Rebuild column statistics (histograms)\n"
        "  .timer <on|off>         Toggle query timing display (default: on)\n"
        "  .echo <on|off>          Print each query before executing (default: off)\n"
        "  .bail <on|off>          Stop on first error in .read (default: off)\n"
        "  .profile <on|off>       Print query execution profile after each query\n"
        "\n"
        "Shell:\n"
        "  .shell <cmd>            Execute an OS command\n"
        "  .system <cmd>           Alias for .shell\n"
        "  .print <text>           Print literal text\n"
        "  .prompt <str>           Change the shell prompt\n"
        "  .show                   Show all current settings\n"
        "  .help                   Show this help\n"
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

// ------------------------------------------------------------------ .show

static void DoShow(const ShellState& state) {
    std::cout << "Current settings:\n"
              << "  mode:      " << OutputModeName(state.output_mode) << '\n'
              << "  headers:   " << (state.show_headers ? "on" : "off") << '\n'
              << "  nullvalue: \"" << state.null_value << "\"\n"
              << "  separator: \"" << state.col_sep << "\"\n"
              << "  maxrows:   " << (state.max_rows == 0 ? "unlimited" : std::to_string(state.max_rows)) << '\n'
              << "  width:     " << (state.min_col_width == 0 ? "auto" : std::to_string(state.min_col_width)) << '\n'
              << "  output:    " << (state.output_file.empty() ? "stdout" : state.output_file) << '\n'
              << "  log:       " << (state.log_file.empty() ? "off" : state.log_file) << '\n'
              << "  timer:     " << (state.timer_enabled ? "on" : "off") << '\n'
              << "  echo:      " << (state.echo ? "on" : "off") << '\n'
              << "  bail:      " << (state.bail ? "on" : "off") << '\n'
              << "  profile:   " << (state.profile ? "on" : "off") << '\n'
              << "  prompt:    \"" << state.prompt << "\"\n"
              << "  workspace: " << state.workspace << '\n';
}

// ------------------------------------------------------------------ .read

static void DoRead(const std::string& filepath,
                   const std::function<void(const std::string&)>& executor,
                   ShellState& state,
                   std::shared_ptr<ClientContext>& client) {
    std::ifstream f(filepath);
    if (!f) { std::cerr << "Error: cannot open file: " << filepath << '\n'; return; }

    std::string query, line;
    while (std::getline(f, line)) {
        // Strip inline comments from dot-command lines
        std::string trimmed = line;
        size_t tstart = trimmed.find_first_not_of(" \t");
        if (tstart != std::string::npos) trimmed = trimmed.substr(tstart);

        if (!trimmed.empty() && (trimmed[0] == '.' || trimmed[0] == ':')) {
            // Dot command: handle immediately (no semicolon needed)
            if (!query.empty()) {
                // flush any pending partial query
                size_t qs = query.find_first_not_of(" \t\n\r");
                if (qs != std::string::npos) executor(query.substr(qs));
                query.clear();
            }
            auto inner_executor = [&](const std::string& q) { executor(q); };
            HandleDotCommand(trimmed, state, client, inner_executor);
            continue;
        }

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
    if (cmd.empty()) return false;
    if (cmd[0] != '.' && cmd[0] != ':') return false;

    std::string stripped = cmd.substr(1);
    auto args = SplitArgs(stripped);
    if (args.empty()) return true;

    std::string command = args[0];

    // ---- output format ----
    if (command == "mode") {
        if (args.size() < 2) {
            std::cout << "Current mode: " << OutputModeName(state.output_mode) << '\n';
            std::cout << "Usage: .mode <table|box|column|line|csv|list|tabs|json|jsonlines|markdown|html|latex|insert|trash>\n";
        } else {
            state.output_mode = ParseOutputMode(args[1]);
            std::cout << "Output mode: " << OutputModeName(state.output_mode) << '\n';
        }
    } else if (command == "headers") {
        if (args.size() < 2) {
            std::cout << "Headers: " << (state.show_headers ? "on" : "off") << '\n';
        } else {
            state.show_headers = (args[1] == "on");
            std::cout << "Headers: " << (state.show_headers ? "on" : "off") << '\n';
        }
    } else if (command == "nullvalue") {
        if (args.size() < 2) {
            std::cout << "NULL display: \"" << state.null_value << "\"\n";
        } else {
            state.null_value = args[1];
            std::cout << "NULL display: \"" << state.null_value << "\"\n";
        }
    } else if (command == "separator") {
        if (args.size() < 2) {
            std::cout << "Separator: \"" << state.col_sep << "\"\n";
        } else {
            state.col_sep = args[1];
            std::cout << "Separator: \"" << state.col_sep << "\"\n";
        }
    } else if (command == "maxrows") {
        if (args.size() < 2) {
            std::cout << "Max rows: " << (state.max_rows == 0 ? "unlimited" : std::to_string(state.max_rows)) << '\n';
        } else {
            state.max_rows = std::stoul(args[1]);
            std::cout << "Max rows: " << (state.max_rows == 0 ? "unlimited" : std::to_string(state.max_rows)) << '\n';
        }
    } else if (command == "width") {
        if (args.size() < 2) {
            std::cout << "Min column width: " << (state.min_col_width == 0 ? "auto" : std::to_string(state.min_col_width)) << '\n';
        } else {
            state.min_col_width = std::stoul(args[1]);
            std::cout << "Min column width: " << (state.min_col_width == 0 ? "auto" : std::to_string(state.min_col_width)) << '\n';
        }

    // ---- redirection ----
    } else if (command == "output") {
        if (args.size() < 2) {
            state.output_file.clear();
            std::cout << "Output: stdout\n";
        } else {
            state.output_file = args[1];
            std::cout << "Output: " << state.output_file << '\n';
        }
    } else if (command == "once") {
        if (args.size() < 2) {
            std::cout << "Usage: .once <file>\n";
        } else {
            state.output_once = args[1];
            std::cout << "Next result → " << state.output_once << '\n';
        }
    } else if (command == "log") {
        if (args.size() < 2) {
            state.log_file.clear();
            std::cout << "Logging: off\n";
        } else {
            state.log_file = args[1];
            std::cout << "Logging: " << state.log_file << '\n';
        }

    // ---- schema inspection ----
    } else if (command == "tables") {
        DoTables(*client);
    } else if (command == "schema") {
        DoSchema(*client, args.size() > 1 ? args[1] : "");
    } else if (command == "indexes") {
        std::cout << "(index inspection not yet implemented)\n";

    // ---- execution ----
    } else if (command == "read") {
        if (args.size() < 2) {
            std::cout << "Usage: .read <file>\n";
        } else {
            DoRead(args[1], executor, state, client);
        }
    } else if (command == "analyze") {
        HistogramGenerator hist_gen;
        hist_gen.CreateHistogram(client);
    } else if (command == "timer") {
        if (args.size() < 2) state.timer_enabled = !state.timer_enabled;
        else                  state.timer_enabled = (args[1] == "on");
        std::cout << "Timer: " << (state.timer_enabled ? "on" : "off") << '\n';
    } else if (command == "echo") {
        if (args.size() < 2) state.echo = !state.echo;
        else                  state.echo = (args[1] == "on");
        std::cout << "Echo: " << (state.echo ? "on" : "off") << '\n';
    } else if (command == "bail") {
        if (args.size() < 2) state.bail = !state.bail;
        else                  state.bail = (args[1] == "on");
        std::cout << "Bail: " << (state.bail ? "on" : "off") << '\n';
    } else if (command == "profile") {
        if (args.size() < 2) state.profile = !state.profile;
        else                  state.profile = (args[1] == "on");
        if (state.profile) client->EnableProfiling();
        else               client->DisableProfiling();
        std::cout << "Profile: " << (state.profile ? "on" : "off") << '\n';

    // ---- shell / system ----
    } else if (command == "shell" || command == "system") {
        if (args.size() < 2) {
            std::cout << "Usage: .shell <command>\n";
        } else {
            std::string syscmd = JoinArgs(args, 1);
            int ret = std::system(syscmd.c_str());
            if (ret != 0) std::cerr << "Exit code: " << ret << '\n';
        }
    } else if (command == "print") {
        std::cout << JoinArgs(args, 1) << '\n';
    } else if (command == "prompt") {
        if (args.size() < 2) {
            std::cout << "Prompt: \"" << state.prompt << "\"\n";
        } else {
            state.prompt = args[1];
        }
    } else if (command == "show") {
        DoShow(state);

    // ---- help / exit ----
    } else if (command == "help") {
        DoHelp();
    } else if (command == "exit" || command == "quit") {
        return false;
    } else {
        std::cout << "Unknown command: ." << command << "  (try .help)\n";
    }

    return true;
}

} // namespace turbolynx

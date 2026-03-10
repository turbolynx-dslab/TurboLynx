#include "include/completion.hpp"

#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog.hpp"
#include "common/constants.hpp"
#include "common/enums/graph_component_type.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "linenoise.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <cstdlib>
#include <strings.h>
#include <string>
#include <vector>

using namespace duckdb;

namespace turbolynx {

// ---- static state (populated once after DB init) ----

static std::vector<std::string> g_vertex_labels;
static std::vector<std::string> g_edge_types;
static std::string              g_hint_buf;

// ---- keyword / command tables ----

static const char* DOT_COMMANDS[] = {
    ".mode", ".headers", ".nullvalue", ".separator", ".maxrows", ".width",
    ".output", ".once", ".log",
    ".tables", ".schema", ".indexes",
    ".read", ".analyze", ".timer", ".echo", ".bail",
    ".shell", ".system", ".print", ".prompt", ".show",
    ".help", ".exit", ".quit",
    nullptr
};

static const char* CYPHER_KEYWORDS[] = {
    "MATCH", "OPTIONAL MATCH",
    "WHERE", "RETURN", "CREATE", "DELETE", "DETACH DELETE",
    "SET", "REMOVE", "WITH", "UNWIND", "MERGE", "CALL", "UNION",
    "ORDER BY", "LIMIT", "SKIP",
    "AND", "OR", "NOT", "XOR",
    "IN", "IS NULL", "IS NOT NULL",
    "TRUE", "FALSE", "NULL",
    "DISTINCT", "AS",
    "COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT",
    "EXISTS", "CASE", "WHEN", "THEN", "ELSE", "END",
    "STARTS WITH", "ENDS WITH", "CONTAINS",
    nullptr
};

// Hints shown when the line exactly matches the keyword
struct KWHint { const char* keyword; const char* hint; };
static const KWHint KEYWORD_HINTS[] = {
    { "MATCH",          " (n:Label) WHERE n.prop RETURN n" },
    { "WHERE",          " n.property = value"              },
    { "RETURN",         " n.property"                      },
    { "CREATE",         " (n:Label {prop: value})"         },
    { "WITH",           " n, count(r) AS cnt"              },
    { "ORDER BY",       " n.prop ASC"                      },
    { "LIMIT",          " 10"                              },
    { "SKIP",           " 10"                              },
    { ".mode",          " table|box|column|csv|json|jsonlines|markdown|..." },
    { ".schema",        " LabelName"                       },
    { ".read",          " script.cypher"                   },
    { ".output",        " results.txt"                     },
    { ".once",          " result.txt"                      },
    { ".log",           " queries.log"                     },
    { ".maxrows",       " 100"                             },
    { ".width",         " 20"                              },
    { ".nullvalue",     " NULL"                            },
    { ".separator",     " |"                               },
    { ".prompt",        " TurboLynx"                       },
    { nullptr, nullptr }
};

// ---- helpers ----

static std::string ToUpper(const std::string& s) {
    std::string r = s;
    for (auto& c : r) c = (char)toupper((unsigned char)c);
    return r;
}

// Returns the last word (split by spaces / open parens / brackets / commas)
static std::string LastWord(const std::string& line) {
    size_t pos = line.find_last_of(" \t(,[");
    return (pos == std::string::npos) ? line : line.substr(pos + 1);
}

static std::string BeforeLastWord(const std::string& line) {
    size_t pos = line.find_last_of(" \t(,[");
    return (pos == std::string::npos) ? "" : line.substr(0, pos + 1);
}

// ---- completion callback ----

static void CompletionCallback(const char* raw, linenoiseCompletions* lc) {
    std::string buf(raw);

    // Trim leading whitespace for context detection
    size_t tstart = buf.find_first_not_of(" \t");
    std::string trimmed = (tstart == std::string::npos) ? "" : buf.substr(tstart);

    // 1. Dot command completion (line starts with '.')
    if (!trimmed.empty() && trimmed[0] == '.') {
        for (size_t i = 0; DOT_COMMANDS[i]; i++) {
            std::string cmd(DOT_COMMANDS[i]);
            if (cmd.rfind(trimmed, 0) == 0)
                linenoiseAddCompletion(lc, cmd.c_str());
        }
        return;
    }

    // 2. Label/type completion after ':' inside () or []
    //    e.g. "(n:Per" → complete vertex labels
    //         "[:KNOW"  → complete edge types
    size_t colon = buf.rfind(':');
    if (colon != std::string::npos) {
        size_t paren   = buf.rfind('(', colon);
        size_t bracket = buf.rfind('[', colon);
        if (paren != std::string::npos || bracket != std::string::npos) {
            std::string prefix = buf.substr(colon + 1);
            std::string base   = buf.substr(0, colon + 1);

            bool is_edge = (bracket != std::string::npos &&
                            (paren == std::string::npos || bracket > paren));

            auto& primary = is_edge ? g_edge_types  : g_vertex_labels;
            auto& other   = is_edge ? g_vertex_labels : g_edge_types;

            for (const auto& name : primary)
                if (name.rfind(prefix, 0) == 0)
                    linenoiseAddCompletion(lc, (base + name).c_str());
            for (const auto& name : other)
                if (name.rfind(prefix, 0) == 0)
                    linenoiseAddCompletion(lc, (base + name).c_str());
            return;
        }
    }

    // 3. Cypher keyword completion — match the last word (case-insensitive)
    std::string word   = LastWord(buf);
    std::string before = BeforeLastWord(buf);
    if (word.empty()) return;

    std::string upper_word = ToUpper(word);
    for (size_t i = 0; CYPHER_KEYWORDS[i]; i++) {
        std::string kw(CYPHER_KEYWORDS[i]);
        if (ToUpper(kw).rfind(upper_word, 0) == 0)
            linenoiseAddCompletion(lc, (before + kw).c_str());
    }
}

// ---- hints callback ----

static char* HintsCallback(const char* raw, int* color, int* bold) {
    std::string buf(raw);

    size_t tstart = buf.find_first_not_of(" \t");
    if (tstart == std::string::npos) return nullptr;
    std::string trimmed = buf.substr(tstart);
    if (trimmed.empty()) return nullptr;

    std::string upper = ToUpper(trimmed);

    for (size_t i = 0; KEYWORD_HINTS[i].keyword; i++) {
        std::string kw(KEYWORD_HINTS[i].keyword);
        std::string kw_upper = ToUpper(kw);

        // Exact match: show the argument hint
        if (upper == kw_upper) {
            g_hint_buf = KEYWORD_HINTS[i].hint;
            *color = 90; // dark gray
            *bold  = 0;
            return const_cast<char*>(g_hint_buf.c_str());
        }

        // Prefix match: show the rest of the keyword as hint
        if (kw_upper.rfind(upper, 0) == 0 && trimmed.size() < kw.size()) {
            g_hint_buf = kw.substr(trimmed.size());
            *color = 90;
            *bold  = 0;
            return const_cast<char*>(g_hint_buf.c_str());
        }
    }
    return nullptr;
}

// ---- syntax highlight callback (Neo4j-style Cypher coloring) ----
//
// Color scheme (matches Neo4j Browser palette adapted for ANSI terminals):
//   Keywords          bold blue    \033[1;34m
//   Node labels       green        \033[32m
//   Rel types         yellow       \033[33m
//   String literals   red/orange   \033[31m
//   Numeric literals  magenta      \033[35m
//   Comments (//)     dark gray    \033[90m
//   Reset                          \033[0m

// Sorted longest-first so multi-word keywords match before their prefixes.
static const char* HL_KEYWORDS[] = {
    "OPTIONAL MATCH", "DETACH DELETE", "IS NOT NULL", "STARTS WITH",
    "ENDS WITH", "ORDER BY",
    "CONTAINS", "UNION ALL",
    "MATCH", "WHERE", "RETURN", "CREATE", "DELETE", "MERGE",
    "SET", "REMOVE", "WITH", "UNWIND", "CALL", "UNION",
    "LIMIT", "SKIP", "AS", "DISTINCT",
    "AND", "OR", "NOT", "XOR", "IN", "IS NULL",
    "TRUE", "FALSE", "NULL",
    "COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT",
    "EXISTS", "CASE", "WHEN", "THEN", "ELSE", "END",
    nullptr
};

#define ANSI_RESET   "\033[0m"
#define ANSI_KEYWORD "\033[1;34m"   // bold blue  — keywords
#define ANSI_LABEL   "\033[32m"     // green      — node labels / rel types
#define ANSI_STRING  "\033[31m"     // red        — string literals
#define ANSI_NUMBER  "\033[35m"     // magenta    — numeric literals
#define ANSI_COMMENT "\033[90m"     // dark gray  — // comments

static bool isWordChar(char c) {
    return isalnum((unsigned char)c) || c == '_';
}

static char* HighlightCallback(const char* buf, size_t len) {
    std::string out;
    out.reserve(len + 256);

    size_t i = 0;
    while (i < len) {
        // --- comment: // to end of visible input ---
        if (i + 1 < len && buf[i] == '/' && buf[i+1] == '/') {
            out += ANSI_COMMENT;
            while (i < len) out += buf[i++];
            out += ANSI_RESET;
            break;
        }

        // --- string literal: single-quoted ---
        if (buf[i] == '\'') {
            out += ANSI_STRING;
            out += buf[i++];
            while (i < len && buf[i] != '\'') {
                if (buf[i] == '\\' && i + 1 < len) out += buf[i++]; // escape
                out += buf[i++];
            }
            if (i < len) out += buf[i++]; // closing quote
            out += ANSI_RESET;
            continue;
        }

        // --- string literal: double-quoted ---
        if (buf[i] == '"') {
            out += ANSI_STRING;
            out += buf[i++];
            while (i < len && buf[i] != '"') {
                if (buf[i] == '\\' && i + 1 < len) out += buf[i++];
                out += buf[i++];
            }
            if (i < len) out += buf[i++];
            out += ANSI_RESET;
            continue;
        }

        // --- node label or rel type: after ':' inside () or [] ---
        // e.g. (n:Person)  [:KNOWS]
        if (buf[i] == ':' && i + 1 < len && (isalpha((unsigned char)buf[i+1]) || buf[i+1] == '_')) {
            out += buf[i++]; // the ':'
            out += ANSI_LABEL;
            while (i < len && isWordChar(buf[i])) out += buf[i++];
            out += ANSI_RESET;
            continue;
        }

        // --- numeric literal ---
        if (isdigit((unsigned char)buf[i]) &&
            (i == 0 || !isWordChar(buf[i-1]))) {
            out += ANSI_NUMBER;
            while (i < len && (isdigit((unsigned char)buf[i]) || buf[i] == '.' ||
                                buf[i] == 'e' || buf[i] == 'E' ||
                                buf[i] == '+' || buf[i] == '-'))
                out += buf[i++];
            out += ANSI_RESET;
            continue;
        }

        // --- Cypher keyword (case-insensitive, word-boundary) ---
        bool matched = false;
        bool at_word_start = (i == 0 || !isWordChar(buf[i-1]));
        if (at_word_start && isalpha((unsigned char)buf[i])) {
            for (size_t k = 0; HL_KEYWORDS[k]; k++) {
                const char* kw = HL_KEYWORDS[k];
                size_t kwlen = strlen(kw);
                if (i + kwlen > len) continue;
                if (strncasecmp(buf + i, kw, kwlen) != 0) continue;
                // Word-boundary after keyword
                size_t after = i + kwlen;
                if (after < len && isWordChar(buf[after])) continue;
                out += ANSI_KEYWORD;
                for (size_t c = 0; c < kwlen; c++)
                    out += (char)toupper((unsigned char)buf[i + c]);
                out += ANSI_RESET;
                i += kwlen;
                matched = true;
                break;
            }
        }
        if (!matched) out += buf[i++];
    }

    return strdup(out.c_str());
}

#undef ANSI_RESET
#undef ANSI_KEYWORD
#undef ANSI_LABEL
#undef ANSI_STRING
#undef ANSI_NUMBER
#undef ANSI_COMMENT

// ---- public API ----

void SetupCompletion() {
    linenoiseSetCompletionCallback(CompletionCallback);
    linenoiseSetHintsCallback(HintsCallback);
    linenoiseSetHighlightCallback(HighlightCallback);
    // No free callback — g_hint_buf is a static std::string
}

void PopulateCompletions(duckdb::ClientContext& client) {
    g_vertex_labels.clear();
    g_edge_types.clear();
    try {
        auto& catalog = client.db->GetCatalog();
        auto* graph = (GraphCatalogEntry*)catalog.GetEntry(
            client, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
        if (graph) {
            graph->GetVertexLabels(g_vertex_labels);
            graph->GetEdgeTypes(g_edge_types);
        }
    } catch (...) {
        // Catalog may not be initialized yet — silently ignore
    }
}

} // namespace turbolynx

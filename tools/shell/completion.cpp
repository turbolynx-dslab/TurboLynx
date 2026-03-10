#include "include/completion.hpp"

#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog.hpp"
#include "common/constants.hpp"
#include "common/enums/graph_component_type.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <strings.h>
#include <string>
#include <vector>

using namespace duckdb;
using Replxx = replxx::Replxx;

namespace turbolynx {

// ---- static state ----

static std::vector<std::string> g_vertex_labels;
static std::vector<std::string> g_edge_types;

// ---- keyword tables ----

// Sorted longest-first so multi-word keywords match before their prefixes.
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
    "OPTIONAL MATCH", "DETACH DELETE", "IS NOT NULL", "STARTS WITH",
    "ENDS WITH", "ORDER BY", "UNION ALL",
    "MATCH", "WHERE", "RETURN", "CREATE", "DELETE", "MERGE",
    "SET", "REMOVE", "WITH", "UNWIND", "CALL", "UNION",
    "LIMIT", "SKIP", "AS", "DISTINCT",
    "AND", "OR", "NOT", "XOR", "IN", "IS NULL",
    "TRUE", "FALSE", "NULL",
    "COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT",
    "EXISTS", "CASE", "WHEN", "THEN", "ELSE", "END",
    nullptr
};

// Hints shown for exact keyword matches
struct KWHint { const char* keyword; const char* hint; };
static const KWHint KEYWORD_HINTS[] = {
    { "MATCH",      " (n:Label) WHERE n.prop RETURN n" },
    { "WHERE",      " n.property = value"              },
    { "RETURN",     " n.property"                      },
    { "CREATE",     " (n:Label {prop: value})"         },
    { "WITH",       " n, count(r) AS cnt"              },
    { "ORDER BY",   " n.prop ASC"                      },
    { "LIMIT",      " 10"                              },
    { "SKIP",       " 10"                              },
    { ".mode",      " table|box|column|csv|json|..."   },
    { ".schema",    " LabelName"                       },
    { ".read",      " script.cypher"                   },
    { ".output",    " results.txt"                     },
    { ".once",      " result.txt"                      },
    { ".log",       " queries.log"                     },
    { ".maxrows",   " 100"                             },
    { ".width",     " 20"                              },
    { ".nullvalue", " NULL"                            },
    { ".separator", " |"                               },
    { ".prompt",    " TurboLynx"                       },
    { nullptr, nullptr }
};

// ---- helpers ----

static std::string ToUpper(const std::string& s) {
    std::string r = s;
    for (auto& c : r) c = (char)toupper((unsigned char)c);
    return r;
}

static bool IsWordChar(char c) {
    return isalnum((unsigned char)c) || c == '_';
}

// Returns the last token (split by whitespace / parens / brackets / commas)
static std::string LastToken(const std::string& line) {
    size_t pos = line.find_last_of(" \t(,[");
    return (pos == std::string::npos) ? line : line.substr(pos + 1);
}

static std::string BeforeLastToken(const std::string& line) {
    size_t pos = line.find_last_of(" \t(,[");
    return (pos == std::string::npos) ? "" : line.substr(0, pos + 1);
}

// Advance one UTF-8 code point and return byte length (1-4)
static int Utf8CharLen(const char* s, int max) {
    if (max <= 0) return 0;
    unsigned char c = (unsigned char)s[0];
    if      (c < 0x80) return 1;
    else if (c < 0xE0) return (max >= 2) ? 2 : 1;
    else if (c < 0xF0) return (max >= 3) ? 3 : 1;
    else                return (max >= 4) ? 4 : 1;
}

// ---- completion callback ----

static Replxx::completions_t CompletionCallback(std::string const& buf, int& /*contextLen*/) {
    Replxx::completions_t completions;

    size_t tstart = buf.find_first_not_of(" \t");
    std::string trimmed = (tstart == std::string::npos) ? "" : buf.substr(tstart);

    // 1. Dot command completion
    if (!trimmed.empty() && trimmed[0] == '.') {
        for (int i = 0; DOT_COMMANDS[i]; i++) {
            std::string cmd(DOT_COMMANDS[i]);
            if (cmd.rfind(trimmed, 0) == 0)
                completions.emplace_back(cmd);
        }
        return completions;
    }

    // 2. Label / rel-type after ':'
    size_t colon = buf.rfind(':');
    if (colon != std::string::npos) {
        size_t paren   = buf.rfind('(', colon);
        size_t bracket = buf.rfind('[', colon);
        if (paren != std::string::npos || bracket != std::string::npos) {
            std::string prefix = buf.substr(colon + 1);
            std::string base   = buf.substr(0, colon + 1);
            bool is_edge = (bracket != std::string::npos &&
                            (paren == std::string::npos || bracket > paren));
            auto& primary = is_edge ? g_edge_types   : g_vertex_labels;
            auto& other   = is_edge ? g_vertex_labels : g_edge_types;
            for (const auto& name : primary)
                if (name.rfind(prefix, 0) == 0)
                    completions.emplace_back(base + name,
                        is_edge ? Replxx::Color::YELLOW : Replxx::Color::GREEN);
            for (const auto& name : other)
                if (name.rfind(prefix, 0) == 0)
                    completions.emplace_back(base + name,
                        is_edge ? Replxx::Color::GREEN : Replxx::Color::YELLOW);
            return completions;
        }
    }

    // 3. Cypher keyword completion
    std::string word   = LastToken(buf);
    std::string before = BeforeLastToken(buf);
    if (word.empty()) return completions;

    std::string upper_word = ToUpper(word);
    for (int i = 0; CYPHER_KEYWORDS[i]; i++) {
        std::string kw(CYPHER_KEYWORDS[i]);
        if (ToUpper(kw).rfind(upper_word, 0) == 0)
            completions.emplace_back(before + kw, Replxx::Color::BRIGHTBLUE);
    }
    return completions;
}

// ---- hint callback ----

static Replxx::hints_t HintCallback(std::string const& buf, int& /*contextLen*/,
                                     Replxx::Color& color) {
    Replxx::hints_t hints;
    size_t tstart = buf.find_first_not_of(" \t");
    if (tstart == std::string::npos) return hints;
    std::string trimmed = buf.substr(tstart);
    std::string upper   = ToUpper(trimmed);

    for (int i = 0; KEYWORD_HINTS[i].keyword; i++) {
        std::string kw      = KEYWORD_HINTS[i].keyword;
        std::string kw_up   = ToUpper(kw);

        // Exact match: show argument hint
        if (upper == kw_up) {
            color = Replxx::Color::GRAY;
            hints.emplace_back(KEYWORD_HINTS[i].hint);
            return hints;
        }
        // Prefix match: show rest of keyword
        if (kw_up.rfind(upper, 0) == 0 && trimmed.size() < kw.size()) {
            color = Replxx::Color::GRAY;
            hints.emplace_back(kw.substr(trimmed.size()));
            return hints;
        }
    }
    return hints;
}

// ---- highlighter callback (Neo4j-style Cypher palette) ----
//
//  BRIGHTBLUE  — keywords (MATCH, WHERE, RETURN, ...)
//  GREEN       — node labels / rel types  (after ':')
//  RED         — string literals  (' " )
//  MAGENTA     — numeric literals
//  GRAY        — // comments
//  DEFAULT     — everything else

static const char* HL_KEYWORDS[] = {
    "OPTIONAL MATCH", "DETACH DELETE", "IS NOT NULL", "STARTS WITH",
    "ENDS WITH", "ORDER BY", "UNION ALL",
    "MATCH", "WHERE", "RETURN", "CREATE", "DELETE", "MERGE",
    "SET", "REMOVE", "WITH", "UNWIND", "CALL", "UNION",
    "LIMIT", "SKIP", "AS", "DISTINCT",
    "AND", "OR", "NOT", "XOR", "IN", "IS NULL",
    "TRUE", "FALSE", "NULL",
    "COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT",
    "EXISTS", "CASE", "WHEN", "THEN", "ELSE", "END",
    nullptr
};

static void HighlightCallback(std::string const& input, Replxx::colors_t& colors) {
    int n       = (int)input.size();
    int cp_idx  = 0;   // index into colors (per code point)
    int i       = 0;   // byte index into input

    // Advance one code point and paint it
    auto paint = [&](Replxx::Color c) {
        if (i >= n || cp_idx >= (int)colors.size()) return;
        colors[cp_idx++] = c;
        i += Utf8CharLen(input.c_str() + i, n - i);
    };

    // Paint a byte range [i, end_byte) with color c
    auto paint_range = [&](int end_byte, Replxx::Color c) {
        while (i < end_byte && cp_idx < (int)colors.size()) paint(c);
    };

    while (i < n && cp_idx < (int)colors.size()) {
        // Comment: // to end of input
        if (i + 1 < n && input[i] == '/' && input[i+1] == '/') {
            paint_range(n, Replxx::Color::GRAY);
            break;
        }

        // String literal (single or double quote)
        if (input[i] == '\'' || input[i] == '"') {
            char q = input[i];
            paint(Replxx::Color::RED);
            while (i < n && input[i] != q) {
                if (input[i] == '\\' && i + 1 < n) paint(Replxx::Color::RED);
                paint(Replxx::Color::RED);
            }
            if (i < n) paint(Replxx::Color::RED);
            continue;
        }

        // Label / rel-type after ':' followed by identifier
        if (input[i] == ':' && i + 1 < n &&
            (isalpha((unsigned char)input[i+1]) || input[i+1] == '_')) {
            paint(Replxx::Color::DEFAULT);  // the ':'
            while (i < n && IsWordChar(input[i])) paint(Replxx::Color::GREEN);
            continue;
        }

        // Numeric literal (at word boundary)
        if (isdigit((unsigned char)input[i]) &&
            (i == 0 || !IsWordChar(input[i-1]))) {
            while (i < n && (isdigit((unsigned char)input[i]) || input[i] == '.'))
                paint(Replxx::Color::MAGENTA);
            continue;
        }

        // Cypher keyword (case-insensitive, word boundary)
        bool matched = false;
        if ((i == 0 || !IsWordChar(input[i-1])) && isalpha((unsigned char)input[i])) {
            for (int k = 0; HL_KEYWORDS[k]; k++) {
                const char* kw  = HL_KEYWORDS[k];
                int kwlen       = (int)strlen(kw);
                if (i + kwlen > n) continue;
                if (strncasecmp(input.c_str() + i, kw, kwlen) != 0) continue;
                int after = i + kwlen;
                if (after < n && IsWordChar(input[after])) continue;
                paint_range(i + kwlen, Replxx::Color::BRIGHTBLUE);
                matched = true;
                break;
            }
        }
        if (!matched) paint(Replxx::Color::DEFAULT);
    }
}

// ---- public API ----

void SetupCompletion(replxx::Replxx& rx) {
    rx.set_completion_callback(CompletionCallback);
    rx.set_highlighter_callback(HighlightCallback);
    rx.set_hint_callback(HintCallback);
    rx.set_word_break_characters(" \t\n\r()[]{}.,;:");
    rx.set_double_tab_completion(false);
    rx.set_max_history_size(1000);
}

void PopulateCompletions(replxx::Replxx& /*rx*/, duckdb::ClientContext& client) {
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

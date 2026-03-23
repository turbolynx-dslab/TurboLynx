#include "include/renderer.hpp"

#include "common/typedef.hpp"
#include "common/types/data_chunk.hpp"

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <unistd.h>

using namespace duckdb;

namespace turbolynx {

// UTF-8 display width: count codepoints (not bytes).
// Assumes all codepoints are single-width (no CJK fullwidth handling).
static size_t Utf8DisplayWidth(const std::string& s) {
    size_t width = 0;
    for (size_t i = 0; i < s.size(); ) {
        unsigned char c = s[i];
        if (c < 0x80)        { i += 1; }
        else if (c < 0xC0)   { i += 1; continue; } // continuation byte (skip)
        else if (c < 0xE0)   { i += 2; }
        else if (c < 0xF0)   { i += 3; }
        else                  { i += 4; }
        width++;
    }
    return width;
}

// Pad string to target display width (accounting for multi-byte UTF-8).
static std::string Utf8Pad(const std::string& s, size_t target_width) {
    size_t dw = Utf8DisplayWidth(s);
    if (dw >= target_width) return s;
    return s + std::string(target_width - dw, ' ');
}

// ---- ANSI color helpers ----

static bool UseColor(std::ostream& out) {
    return (&out == &std::cout) && isatty(STDOUT_FILENO);
}

// Write text padded to `width` chars, with optional bold+cyan coloring.
// Color codes are invisible to setw, so we pad manually.
static void WriteColored(std::ostream& out, const std::string& text,
                         size_t width, bool color) {
    if (color) {
        out << "\033[1;36m" << text << "\033[0m";
        size_t dw = Utf8DisplayWidth(text);
        if (dw < width) out << std::string(width - dw, ' ');
    } else {
        out << Utf8Pad(text, width);
    }
}

// ---- mode name mapping ----

OutputMode ParseOutputMode(const std::string& name) {
    if (name == "csv")       return OutputMode::CSV;
    if (name == "json")      return OutputMode::JSON;
    if (name == "markdown")  return OutputMode::MARKDOWN;
    if (name == "jsonlines") return OutputMode::JSONLINES;
    if (name == "box")       return OutputMode::BOX;
    if (name == "line")      return OutputMode::LINE;
    if (name == "column")    return OutputMode::COLUMN;
    if (name == "list")      return OutputMode::LIST;
    if (name == "tabs")      return OutputMode::TABS;
    if (name == "html")      return OutputMode::HTML;
    if (name == "latex")     return OutputMode::LATEX;
    if (name == "insert")    return OutputMode::INSERT;
    if (name == "trash")     return OutputMode::TRASH;
    return OutputMode::TABLE;
}

std::string OutputModeName(OutputMode mode) {
    switch (mode) {
        case OutputMode::CSV:       return "csv";
        case OutputMode::JSON:      return "json";
        case OutputMode::MARKDOWN:  return "markdown";
        case OutputMode::JSONLINES: return "jsonlines";
        case OutputMode::BOX:       return "box";
        case OutputMode::LINE:      return "line";
        case OutputMode::COLUMN:    return "column";
        case OutputMode::LIST:      return "list";
        case OutputMode::TABS:      return "tabs";
        case OutputMode::HTML:      return "html";
        case OutputMode::LATEX:     return "latex";
        case OutputMode::INSERT:    return "insert";
        case OutputMode::TRASH:     return "trash";
        default:                    return "table";
    }
}

// ---- helpers ----

static std::vector<std::vector<std::string>> CollectRows(
        std::vector<std::shared_ptr<DataChunk>>& results,
        const RenderOptions& opts) {
    std::vector<std::vector<std::string>> rows;
    bool limit_reached = false;
    for (const auto& chunk : results) {
        if (limit_reached) break;
        size_t ncols = chunk->ColumnCount();
        for (size_t r = 0; r < chunk->size(); r++) {
            if (opts.max_rows > 0 && rows.size() >= opts.max_rows) {
                limit_reached = true;
                break;
            }
            std::vector<std::string> row;
            row.reserve(ncols);
            for (size_t c = 0; c < ncols; c++) {
                auto val = chunk->GetValue(c, r);
                row.push_back(val.IsNull() ? opts.null_value : val.ToString());
            }
            rows.push_back(std::move(row));
        }
    }
    return rows;
}

static PropertyKeys ResolveColNames(const PropertyKeys& col_names, Schema& schema) {
    return col_names.empty() ? schema.getStoredColumnNames() : col_names;
}

static std::vector<size_t> ComputeWidths(const PropertyKeys& headers,
                                         const std::vector<std::vector<std::string>>& rows,
                                         size_t min_width = 0) {
    size_t ncols = headers.size();
    std::vector<size_t> widths(ncols);
    for (size_t c = 0; c < ncols; c++)
        widths[c] = std::max(Utf8DisplayWidth(headers[c]), min_width);
    for (const auto& row : rows)
        for (size_t c = 0; c < ncols && c < row.size(); c++)
            widths[c] = std::max(widths[c], Utf8DisplayWidth(row[c]));
    return widths;
}

static std::string Repeat(const std::string& s, size_t n) {
    std::string result;
    result.reserve(s.size() * n);
    for (size_t i = 0; i < n; i++) result += s;
    return result;
}

// ---- TABLE (ASCII) ----

static void RenderTable(const PropertyKeys& headers,
                        const std::vector<std::vector<std::string>>& rows,
                        const RenderOptions& opts,
                        std::ostream& out) {
    auto widths = ComputeWidths(headers, rows, opts.min_col_width);
    size_t ncols = headers.size();

    auto hline = [&]() {
        out << '+';
        for (size_t c = 0; c < ncols; c++)
            out << std::string(widths[c] + 2, '-') << '+';
        out << '\n';
    };

    bool color = UseColor(out);
    hline();
    if (opts.show_headers) {
        out << '|';
        for (size_t c = 0; c < ncols; c++) {
            out << ' ';
            WriteColored(out, headers[c], widths[c], color);
            out << " |";
        }
        out << '\n';
        hline();
    }
    for (const auto& row : rows) {
        out << '|';
        for (size_t c = 0; c < ncols; c++) {
            std::string val = (c < row.size()) ? row[c] : "";
            out << ' ' << Utf8Pad(val, widths[c]) << " |";
        }
        out << '\n';
    }
    hline();
    out << rows.size() << " row" << (rows.size() != 1 ? "s" : "") << '\n';
}

// ---- BOX (Unicode) ----

static void RenderBox(const PropertyKeys& headers,
                      const std::vector<std::vector<std::string>>& rows,
                      const RenderOptions& opts,
                      std::ostream& out) {
    auto widths = ComputeWidths(headers, rows, opts.min_col_width);
    size_t ncols = headers.size();

    auto hbar = [&](const std::string& l, const std::string& m, const std::string& r) {
        out << l;
        for (size_t c = 0; c < ncols; c++) {
            out << Repeat("─", widths[c] + 2);
            out << (c + 1 < ncols ? m : r);
        }
        out << '\n';
    };

    bool color = UseColor(out);
    hbar("┌", "┬", "┐");
    if (opts.show_headers) {
        out << "│";
        for (size_t c = 0; c < ncols; c++) {
            out << ' ';
            WriteColored(out, headers[c], widths[c], color);
            out << " │";
        }
        out << '\n';
        hbar("├", "┼", "┤");
    }
    for (const auto& row : rows) {
        out << "│";
        for (size_t c = 0; c < ncols; c++) {
            std::string val = (c < row.size()) ? row[c] : "";
            out << ' ' << Utf8Pad(val, widths[c]) << " │";
        }
        out << '\n';
    }
    hbar("└", "┴", "┘");
    out << rows.size() << " row" << (rows.size() != 1 ? "s" : "") << '\n';
}

// ---- CSV ----

static std::string CsvEscape(const std::string& s, const std::string& sep) {
    bool needs_quote = s.find_first_of(sep + "\"\n\r") != std::string::npos;
    if (!needs_quote) return s;
    std::string out = "\"";
    for (char ch : s) { if (ch == '"') out += '"'; out += ch; }
    return out + '"';
}

static void RenderCSV(const PropertyKeys& headers,
                      const std::vector<std::vector<std::string>>& rows,
                      const RenderOptions& opts,
                      std::ostream& out) {
    if (opts.show_headers) {
        for (size_t c = 0; c < headers.size(); c++)
            out << (c ? opts.col_sep : "") << CsvEscape(headers[c], opts.col_sep);
        out << '\n';
    }
    for (const auto& row : rows) {
        for (size_t c = 0; c < row.size(); c++)
            out << (c ? opts.col_sep : "") << CsvEscape(row[c], opts.col_sep);
        out << '\n';
    }
}

// ---- JSON ----

static std::string JsonEscape(const std::string& s) {
    std::string out;
    for (char ch : s) {
        if      (ch == '"')  out += "\\\"";
        else if (ch == '\\') out += "\\\\";
        else if (ch == '\n') out += "\\n";
        else if (ch == '\r') out += "\\r";
        else if (ch == '\t') out += "\\t";
        else                 out += ch;
    }
    return out;
}

static void RenderJSON(const PropertyKeys& headers,
                       const std::vector<std::vector<std::string>>& rows,
                       std::ostream& out) {
    out << "[\n";
    for (size_t r = 0; r < rows.size(); r++) {
        out << "  {";
        for (size_t c = 0; c < headers.size(); c++) {
            out << (c ? ", " : "")
                << '"' << JsonEscape(headers[c]) << "\": \""
                << JsonEscape(c < rows[r].size() ? rows[r][c] : "") << '"';
        }
        out << '}' << (r + 1 < rows.size() ? "," : "") << '\n';
    }
    out << "]\n";
}

// ---- JSONLINES (NDJSON) ----

static void RenderJsonLines(const PropertyKeys& headers,
                            const std::vector<std::vector<std::string>>& rows,
                            std::ostream& out) {
    for (const auto& row : rows) {
        out << '{';
        for (size_t c = 0; c < headers.size(); c++) {
            out << (c ? ", " : "")
                << '"' << JsonEscape(headers[c]) << "\": \""
                << JsonEscape(c < row.size() ? row[c] : "") << '"';
        }
        out << "}\n";
    }
}

// ---- MARKDOWN ----

static void RenderMarkdown(const PropertyKeys& headers,
                           const std::vector<std::vector<std::string>>& rows,
                           const RenderOptions& opts,
                           std::ostream& out) {
    auto widths = ComputeWidths(headers, rows, std::max(opts.min_col_width, size_t(3)));
    size_t ncols = headers.size();

    if (opts.show_headers) {
        out << '|';
        for (size_t c = 0; c < ncols; c++)
            out << ' ' << Utf8Pad(headers[c], widths[c]) << " |";
        out << '\n';
        out << '|';
        for (size_t c = 0; c < ncols; c++)
            out << ' ' << std::string(widths[c], '-') << " |";
        out << '\n';
    }
    for (const auto& row : rows) {
        out << '|';
        for (size_t c = 0; c < ncols; c++) {
            std::string val = (c < row.size()) ? row[c] : "";
            out << ' ' << Utf8Pad(val, widths[c]) << " |";
        }
        out << '\n';
    }
}

// ---- LINE (col = val, one per line) ----

static void RenderLine(const PropertyKeys& headers,
                       const std::vector<std::vector<std::string>>& rows,
                       std::ostream& out) {
    // Compute max header width for alignment
    size_t max_hdr = 0;
    for (const auto& h : headers) max_hdr = std::max(max_hdr, h.size());

    for (size_t r = 0; r < rows.size(); r++) {
        if (r > 0) out << '\n';
        for (size_t c = 0; c < headers.size(); c++) {
            std::string val = (c < rows[r].size()) ? rows[r][c] : "";
            out << std::right << std::setw((int)max_hdr) << headers[c] << " = " << val << '\n';
        }
    }
}

// ---- COLUMN (borderless, space-aligned) ----

static void RenderColumn(const PropertyKeys& headers,
                         const std::vector<std::vector<std::string>>& rows,
                         const RenderOptions& opts,
                         std::ostream& out) {
    auto widths = ComputeWidths(headers, rows, opts.min_col_width);
    size_t ncols = headers.size();

    bool color = UseColor(out);
    if (opts.show_headers) {
        for (size_t c = 0; c < ncols; c++) {
            WriteColored(out, headers[c], widths[c], color);
            if (c + 1 < ncols) out << "  ";
        }
        out << '\n';
        for (size_t c = 0; c < ncols; c++)
            out << std::string(widths[c], '-') << (c + 1 < ncols ? "  " : "");
        out << '\n';
    }
    for (const auto& row : rows) {
        for (size_t c = 0; c < ncols; c++) {
            std::string val = (c < row.size()) ? row[c] : "";
            out << Utf8Pad(val, widths[c]) << (c + 1 < ncols ? "  " : "");
        }
        out << '\n';
    }
}

// ---- LIST (separator-delimited, no quoting) ----

static void RenderList(const PropertyKeys& headers,
                       const std::vector<std::vector<std::string>>& rows,
                       const RenderOptions& opts,
                       std::ostream& out) {
    const std::string& sep = opts.col_sep;
    if (opts.show_headers) {
        for (size_t c = 0; c < headers.size(); c++)
            out << (c ? sep : "") << headers[c];
        out << '\n';
    }
    for (const auto& row : rows) {
        for (size_t c = 0; c < row.size(); c++)
            out << (c ? sep : "") << row[c];
        out << '\n';
    }
}

// ---- TABS (TSV) ----

static void RenderTabs(const PropertyKeys& headers,
                       const std::vector<std::vector<std::string>>& rows,
                       const RenderOptions& opts,
                       std::ostream& out) {
    if (opts.show_headers) {
        for (size_t c = 0; c < headers.size(); c++)
            out << (c ? "\t" : "") << headers[c];
        out << '\n';
    }
    for (const auto& row : rows) {
        for (size_t c = 0; c < row.size(); c++)
            out << (c ? "\t" : "") << row[c];
        out << '\n';
    }
}

// ---- HTML ----

static std::string HtmlEscape(const std::string& s) {
    std::string out;
    for (char ch : s) {
        if      (ch == '&')  out += "&amp;";
        else if (ch == '<')  out += "&lt;";
        else if (ch == '>')  out += "&gt;";
        else if (ch == '"')  out += "&quot;";
        else                 out += ch;
    }
    return out;
}

static void RenderHTML(const PropertyKeys& headers,
                       const std::vector<std::vector<std::string>>& rows,
                       const RenderOptions& opts,
                       std::ostream& out) {
    out << "<table>\n";
    if (opts.show_headers) {
        out << "<tr>";
        for (const auto& h : headers) out << "<th>" << HtmlEscape(h) << "</th>";
        out << "</tr>\n";
    }
    for (const auto& row : rows) {
        out << "<tr>";
        for (size_t c = 0; c < headers.size(); c++) {
            std::string val = (c < row.size()) ? row[c] : "";
            out << "<td>" << HtmlEscape(val) << "</td>";
        }
        out << "</tr>\n";
    }
    out << "</table>\n";
}

// ---- LATEX ----

static std::string LatexEscape(const std::string& s) {
    std::string out;
    for (char ch : s) {
        if      (ch == '&')  out += "\\&";
        else if (ch == '%')  out += "\\%";
        else if (ch == '$')  out += "\\$";
        else if (ch == '#')  out += "\\#";
        else if (ch == '_')  out += "\\_";
        else if (ch == '{')  out += "\\{";
        else if (ch == '}')  out += "\\}";
        else if (ch == '~')  out += "\\textasciitilde{}";
        else if (ch == '^')  out += "\\textasciicircum{}";
        else if (ch == '\\') out += "\\textbackslash{}";
        else                 out += ch;
    }
    return out;
}

static void RenderLatex(const PropertyKeys& headers,
                        const std::vector<std::vector<std::string>>& rows,
                        const RenderOptions& opts,
                        std::ostream& out) {
    size_t ncols = headers.size();
    out << "\\begin{tabular}{" << std::string(ncols, 'l') << "}\n";
    if (opts.show_headers) {
        for (size_t c = 0; c < ncols; c++)
            out << (c ? " & " : "") << LatexEscape(headers[c]);
        out << " \\\\\n\\hline\n";
    }
    for (const auto& row : rows) {
        for (size_t c = 0; c < ncols; c++) {
            std::string val = (c < row.size()) ? row[c] : "";
            out << (c ? " & " : "") << LatexEscape(val);
        }
        out << " \\\\\n";
    }
    out << "\\end{tabular}\n";
}

// ---- INSERT ----

static std::string SqlQuote(const std::string& s) {
    std::string out = "'";
    for (char ch : s) { if (ch == '\'') out += '\''; out += ch; }
    return out + '\'';
}

static void RenderInsert(const PropertyKeys& headers,
                         const std::vector<std::vector<std::string>>& rows,
                         const RenderOptions& opts,
                         std::ostream& out) {
    std::string cols;
    for (size_t c = 0; c < headers.size(); c++)
        cols += (c ? ", " : "") + headers[c];

    for (const auto& row : rows) {
        out << "INSERT INTO " << opts.insert_label << " (" << cols << ") VALUES (";
        for (size_t c = 0; c < headers.size(); c++) {
            std::string val = (c < row.size()) ? row[c] : "";
            out << (c ? ", " : "") << SqlQuote(val);
        }
        out << ");\n";
    }
}

// ---- entry point ----

void RenderResults(OutputMode mode,
                   const PropertyKeys& col_names,
                   std::vector<std::shared_ptr<DataChunk>>& results,
                   Schema& schema,
                   const RenderOptions& opts) {
    if (mode == OutputMode::TRASH) return;

    auto headers = ResolveColNames(col_names, schema);
    auto rows    = CollectRows(results, opts);

    std::ofstream file_stream;
    if (!opts.output_file.empty()) {
        file_stream.open(opts.output_file, std::ios::trunc);
        if (!file_stream) {
            std::cerr << "Error: cannot open output file: " << opts.output_file << '\n';
            return;
        }
    }
    std::ostream& out = opts.output_file.empty() ? std::cout : file_stream;

    switch (mode) {
        case OutputMode::CSV:       RenderCSV(headers, rows, opts, out);       break;
        case OutputMode::JSON:      RenderJSON(headers, rows, out);             break;
        case OutputMode::JSONLINES: RenderJsonLines(headers, rows, out);        break;
        case OutputMode::MARKDOWN:  RenderMarkdown(headers, rows, opts, out);   break;
        case OutputMode::BOX:       RenderBox(headers, rows, opts, out);        break;
        case OutputMode::LINE:      RenderLine(headers, rows, out);             break;
        case OutputMode::COLUMN:    RenderColumn(headers, rows, opts, out);     break;
        case OutputMode::LIST:      RenderList(headers, rows, opts, out);       break;
        case OutputMode::TABS:      RenderTabs(headers, rows, opts, out);       break;
        case OutputMode::HTML:      RenderHTML(headers, rows, opts, out);       break;
        case OutputMode::LATEX:     RenderLatex(headers, rows, opts, out);      break;
        case OutputMode::INSERT:    RenderInsert(headers, rows, opts, out);     break;
        default:                    RenderTable(headers, rows, opts, out);      break;
    }
}

} // namespace turbolynx

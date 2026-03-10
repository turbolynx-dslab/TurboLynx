#include "include/renderer.hpp"

#include "common/typedef.hpp"
#include "common/types/data_chunk.hpp"

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

using namespace duckdb;

namespace turbolynx {

OutputMode ParseOutputMode(const std::string& name) {
    if (name == "csv")      return OutputMode::CSV;
    if (name == "json")     return OutputMode::JSON;
    if (name == "markdown") return OutputMode::MARKDOWN;
    return OutputMode::TABLE;
}

std::string OutputModeName(OutputMode mode) {
    switch (mode) {
        case OutputMode::CSV:      return "csv";
        case OutputMode::JSON:     return "json";
        case OutputMode::MARKDOWN: return "markdown";
        default:                   return "table";
    }
}

static std::vector<std::vector<std::string>> CollectRows(
        std::vector<std::shared_ptr<DataChunk>>& results) {
    std::vector<std::vector<std::string>> rows;
    for (const auto& chunk : results) {
        size_t ncols = chunk->ColumnCount();
        for (size_t r = 0; r < chunk->size(); r++) {
            std::vector<std::string> row;
            row.reserve(ncols);
            for (size_t c = 0; c < ncols; c++)
                row.push_back(chunk->GetValue(c, r).ToString());
            rows.push_back(std::move(row));
        }
    }
    return rows;
}

static PropertyKeys ResolveColNames(const PropertyKeys& col_names, Schema& schema) {
    return col_names.empty() ? schema.getStoredColumnNames() : col_names;
}

// ---- TABLE ----

static void RenderTable(const PropertyKeys& headers,
                        const std::vector<std::vector<std::string>>& rows,
                        std::ostream& out) {
    size_t ncols = headers.size();
    std::vector<size_t> widths(ncols);
    for (size_t c = 0; c < ncols; c++) widths[c] = headers[c].size();
    for (const auto& row : rows)
        for (size_t c = 0; c < ncols && c < row.size(); c++)
            widths[c] = std::max(widths[c], row[c].size());

    auto hline = [&]() {
        out << '+';
        for (size_t c = 0; c < ncols; c++)
            out << std::string(widths[c] + 2, '-') << '+';
        out << '\n';
    };

    hline();
    out << '|';
    for (size_t c = 0; c < ncols; c++)
        out << ' ' << std::left << std::setw((int)widths[c]) << headers[c] << " |";
    out << '\n';
    hline();
    for (const auto& row : rows) {
        out << '|';
        for (size_t c = 0; c < ncols; c++) {
            std::string val = (c < row.size()) ? row[c] : "";
            out << ' ' << std::left << std::setw((int)widths[c]) << val << " |";
        }
        out << '\n';
    }
    hline();
    out << rows.size() << " row" << (rows.size() != 1 ? "s" : "") << '\n';
}

// ---- CSV ----

static std::string CsvEscape(const std::string& s) {
    if (s.find_first_of(",\"\n\r") == std::string::npos) return s;
    std::string out = "\"";
    for (char ch : s) { if (ch == '"') out += '"'; out += ch; }
    return out + '"';
}

static void RenderCSV(const PropertyKeys& headers,
                      const std::vector<std::vector<std::string>>& rows,
                      std::ostream& out) {
    for (size_t c = 0; c < headers.size(); c++)
        out << (c ? "," : "") << CsvEscape(headers[c]);
    out << '\n';
    for (const auto& row : rows) {
        for (size_t c = 0; c < row.size(); c++)
            out << (c ? "," : "") << CsvEscape(row[c]);
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

// ---- MARKDOWN ----

static void RenderMarkdown(const PropertyKeys& headers,
                           const std::vector<std::vector<std::string>>& rows,
                           std::ostream& out) {
    size_t ncols = headers.size();
    std::vector<size_t> widths(ncols);
    for (size_t c = 0; c < ncols; c++) widths[c] = std::max(headers[c].size(), size_t(3));
    for (const auto& row : rows)
        for (size_t c = 0; c < ncols && c < row.size(); c++)
            widths[c] = std::max(widths[c], row[c].size());

    out << '|';
    for (size_t c = 0; c < ncols; c++)
        out << ' ' << std::left << std::setw((int)widths[c]) << headers[c] << " |";
    out << '\n';
    out << '|';
    for (size_t c = 0; c < ncols; c++)
        out << ' ' << std::string(widths[c], '-') << " |";
    out << '\n';
    for (const auto& row : rows) {
        out << '|';
        for (size_t c = 0; c < ncols; c++) {
            std::string val = (c < row.size()) ? row[c] : "";
            out << ' ' << std::left << std::setw((int)widths[c]) << val << " |";
        }
        out << '\n';
    }
}

// ---- entry ----

void RenderResults(OutputMode mode,
                   const PropertyKeys& col_names,
                   std::vector<std::shared_ptr<DataChunk>>& results,
                   Schema& schema,
                   const std::string& output_file) {
    auto headers = ResolveColNames(col_names, schema);
    auto rows    = CollectRows(results);

    std::ofstream file_stream;
    if (!output_file.empty()) {
        file_stream.open(output_file, std::ios::trunc);
        if (!file_stream) {
            std::cerr << "Error: cannot open output file: " << output_file << '\n';
            return;
        }
    }
    std::ostream& out = output_file.empty() ? std::cout : file_stream;

    switch (mode) {
        case OutputMode::CSV:      RenderCSV(headers, rows, out);      break;
        case OutputMode::JSON:     RenderJSON(headers, rows, out);     break;
        case OutputMode::MARKDOWN: RenderMarkdown(headers, rows, out); break;
        default:                   RenderTable(headers, rows, out);    break;
    }
}

} // namespace turbolynx

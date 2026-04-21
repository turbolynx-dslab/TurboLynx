
#pragma once

#include "CypherLexer.h"
#include "CypherParser.h"
#ifdef INVALID_INDEX
#undef INVALID_INDEX
#endif

#include <cctype>
#include <iostream>
#include <iomanip>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <stdexcept>

#include "common/types/value.hpp"
#include "common/types/data_chunk.hpp"

namespace duckdb {

class CypherPreparedStatement {
public:
    struct BoundParameter {
        std::string literal;
        bool is_bound = false;
    };

    struct ParameterOccurrence {
        std::string name;
        size_t start = 0;
        size_t stop = 0;
    };

    std::string originalQuery;
    std::unordered_map<std::string, BoundParameter> params;
    std::vector<std::string> paramOrder;
    std::vector<ParameterOccurrence> parameterOccurrences;
    std::vector<std::shared_ptr<DataChunk>> queryResults;
    std::vector<idx_t> visibleColumnMapping;

    CypherPreparedStatement(const std::string& query) : originalQuery(query) {
        if (query.find('$') == std::string::npos) {
            return;
        }
        CollectParameterMetadata();
    }

    bool checkParamsAllSet(std::string* missing_param = nullptr) const {
        for (const auto& param_name : paramOrder) {
            auto entry = params.find(param_name);
            if (entry == params.end() || !entry->second.is_bound) {
                if (missing_param) {
                    *missing_param = param_name;
                }
                return false;
            }
        }
        return true;
    }

    size_t getNumParams() const {
        return paramOrder.size();
    }

    bool tryGetBoundQuery(std::string& out, std::string& error) const {
        std::string missing_param;
        if (!checkParamsAllSet(&missing_param)) {
            error = "Missing value for parameter " + missing_param;
            return false;
        }

        if (parameterOccurrences.empty()) {
            out = originalQuery;
            return true;
        }

        std::string result;
        result.reserve(originalQuery.size() + paramOrder.size() * 8);

        size_t cursor = 0;
        for (const auto& occurrence : parameterOccurrences) {
            auto bound_it = params.find(occurrence.name);
            if (bound_it == params.end() || !bound_it->second.is_bound) {
                error = "Missing value for parameter " + occurrence.name;
                return false;
            }
            result.append(originalQuery, cursor, occurrence.start - cursor);
            result += bound_it->second.literal;
            cursor = occurrence.stop + 1;
        }
        result.append(originalQuery, cursor, std::string::npos);
        out = std::move(result);
        return true;
    }

    std::string getBoundQuery() const {
        std::string bound_query;
        std::string error;
        if (!tryGetBoundQuery(bound_query, error)) {
            throw std::runtime_error(error);
        }
        return bound_query;
    }

    bool bindValue(int index, Value& value) {
        if (index >= 0 && index < static_cast<int>(paramOrder.size())) {
            auto type = value.type();
            auto type_id = type.id();
            auto& bound_value = params[paramOrder[index]];

            if (type_id == LogicalTypeId::BOOLEAN) {
                bound_value.literal = value.GetValue<bool>() ? "true" : "false";
            }
            else if (type_id == LogicalTypeId::VARCHAR) {
                bound_value.literal = "'" + EscapeCypherString(value.GetValue<string>()) + "'";
            }
            else if (type_id == LogicalTypeId::DATE) {
                bound_value.literal = "date('" + EscapeCypherString(value.GetValue<string>()) + "')";
            }
            else {
                bound_value.literal = value.ToString();
            }
            bound_value.is_bound = true;

            return true;
        } else {
            return false;
        }
    }

    void copyResults(vector<shared_ptr<DataChunk>>& results) {
        clearResults();
        queryResults = results;
    }

    void clearResults() {
        queryResults.clear();
    }

    size_t getNumRows() {
        size_t num_total_tuples = 0;
        for (auto &it : queryResults) num_total_tuples += it->size();
        return num_total_tuples;
    }

    ~CypherPreparedStatement() = default;

private:
    static std::string EscapeCypherString(const std::string& input) {
        std::ostringstream escaped;
        for (unsigned char ch : input) {
            switch (ch) {
            case '\\':
                escaped << "\\\\";
                break;
            case '\'':
                escaped << "\\'";
                break;
            case '\b':
                escaped << "\\b";
                break;
            case '\f':
                escaped << "\\f";
                break;
            case '\n':
                escaped << "\\n";
                break;
            case '\r':
                escaped << "\\r";
                break;
            case '\t':
                escaped << "\\t";
                break;
            default:
                if (ch < 0x20) {
                    escaped << "\\u" << std::uppercase << std::hex << std::setw(4)
                            << std::setfill('0') << static_cast<int>(ch)
                            << std::nouppercase << std::dec << std::setfill(' ');
                } else {
                    escaped << static_cast<char>(ch);
                }
                break;
            }
        }
        return escaped.str();
    }

    void CollectParameterMetadata() {
        antlr4::ANTLRInputStream input_stream(originalQuery);
        CypherLexer lexer(&input_stream);
        antlr4::CommonTokenStream tokens(&lexer);
        tokens.fill();

        CypherParser parser(&tokens);
        auto* cypher_ctx = parser.oC_Cypher();
        if (!cypher_ctx || parser.getNumberOfSyntaxErrors() > 0) {
            throw std::runtime_error("Invalid query — no parse tree produced");
        }

        CollectParameterMetadataRecursive(cypher_ctx);
    }

    void CollectParameterMetadataRecursive(antlr4::tree::ParseTree* node) {
        if (!node) {
            return;
        }

        if (auto* param_ctx = dynamic_cast<CypherParser::OC_ParameterContext*>(node)) {
            auto* start_token = param_ctx->getStart();
            auto* stop_token = param_ctx->getStop();
            if (!start_token || !stop_token) {
                throw std::runtime_error("Invalid parameter token range");
            }

            auto param_name = param_ctx->getText();
            parameterOccurrences.push_back(ParameterOccurrence{
                param_name,
                static_cast<size_t>(start_token->getStartIndex()),
                static_cast<size_t>(stop_token->getStopIndex())
            });

            if (params.find(param_name) == params.end()) {
                params.emplace(param_name, BoundParameter{});
                paramOrder.push_back(param_name);
            }
            return;
        }

        for (size_t i = 0; i < node->children.size(); ++i) {
            CollectParameterMetadataRecursive(node->children[i]);
        }
    }
};
}

#include "neo4j_runner.hpp"

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include <cstring>
#include <sstream>
#include <stdexcept>

namespace tl_fuzz_oracle {

namespace {

using json = nlohmann::json;

size_t curl_collect(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* sink = static_cast<std::string*>(userdata);
    sink->append(ptr, size * nmemb);
    return size * nmemb;
}

qtest::Value json_to_value(const json& v) {
    if (v.is_null()) {
        return qtest::Null{};
    }
    if (v.is_boolean()) {
        return qtest::Value{v.get<bool>()};
    }
    if (v.is_number_integer()) {
        return qtest::Value{static_cast<int64_t>(v.get<int64_t>())};
    }
    if (v.is_number_unsigned()) {
        return qtest::Value{static_cast<int64_t>(v.get<uint64_t>())};
    }
    if (v.is_number_float()) {
        // Match TurboLynx's snprintf("%f", ...) representation so the
        // shared canonicalizer can compare both sides byte-exact.
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%f", v.get<double>());
        return qtest::Value{std::string(buf)};
    }
    if (v.is_string()) {
        return qtest::Value{v.get<std::string>()};
    }
    // Composite types (arrays / objects) collapse to their JSON string.
    return qtest::Value{v.dump()};
}

}  // namespace

Neo4jRunner::Neo4jRunner(const std::string& uri, const std::string& database)
    : uri_(uri), database_(database) {
    static struct CurlGlobalInit {
        CurlGlobalInit()  { curl_global_init(CURL_GLOBAL_DEFAULT); }
        ~CurlGlobalInit() { curl_global_cleanup(); }
    } global_init;

    CURL* h = curl_easy_init();
    if (!h) {
        throw std::runtime_error("curl_easy_init failed");
    }
    curl_handle_ = h;
}

Neo4jRunner::~Neo4jRunner() {
    if (curl_handle_) {
        curl_easy_cleanup(static_cast<CURL*>(curl_handle_));
        curl_handle_ = nullptr;
    }
}

qtest::QueryResult Neo4jRunner::run(const std::string& cypher) {
    auto* h = static_cast<CURL*>(curl_handle_);

    json req = {
        {"statements", json::array({
            {{"statement", cypher}}
        })}
    };
    std::string req_body = req.dump();

    std::string url = uri_ + "/db/" + database_ + "/tx/commit";
    std::string response;

    curl_slist* headers = nullptr;
    headers = curl_slist_append(headers,
                                "Content-Type: application/json");
    headers = curl_slist_append(headers, "Accept: application/json");

    curl_easy_reset(h);
    curl_easy_setopt(h, CURLOPT_URL,             url.c_str());
    curl_easy_setopt(h, CURLOPT_POST,            1L);
    curl_easy_setopt(h, CURLOPT_HTTPHEADER,      headers);
    curl_easy_setopt(h, CURLOPT_POSTFIELDS,      req_body.c_str());
    curl_easy_setopt(h, CURLOPT_POSTFIELDSIZE,   (long)req_body.size());
    curl_easy_setopt(h, CURLOPT_WRITEFUNCTION,   curl_collect);
    curl_easy_setopt(h, CURLOPT_WRITEDATA,       &response);
    curl_easy_setopt(h, CURLOPT_FAILONERROR,     1L);
    curl_easy_setopt(h, CURLOPT_TIMEOUT,         30L);

    CURLcode rc = curl_easy_perform(h);
    curl_slist_free_all(headers);
    if (rc != CURLE_OK) {
        throw std::runtime_error(std::string("Neo4j HTTP error: ") +
                                 curl_easy_strerror(rc));
    }

    json doc;
    try {
        doc = json::parse(response);
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Neo4j response not JSON: ") +
                                 e.what());
    }

    if (!doc.value("errors", json::array()).empty()) {
        std::ostringstream oss;
        oss << "Neo4j Cypher error: " << doc["errors"].dump();
        throw std::runtime_error(oss.str());
    }

    qtest::QueryResult result;
    for (const auto& rs : doc.value("results", json::array())) {
        for (const auto& row_obj : rs.value("data", json::array())) {
            const auto& cells = row_obj.value("row", json::array());
            qtest::Row row;
            row.cols.reserve(cells.size());
            for (const auto& cell : cells) {
                row.cols.push_back(json_to_value(cell));
            }
            result.rows.push_back(std::move(row));
        }
    }
    return result;
}

void Neo4jRunner::wipe() {
    // Batched delete keyed off the `:Fz` namespace label so we never
    // touch unrelated host data and never hit Neo4j's per-transaction
    // memory limit on a large pre-existing DB.  Loop until empty.
    for (;;) {
        auto r = run(
            "MATCH (n:Fz) WITH n LIMIT 10000 "
            "DETACH DELETE n RETURN count(*) AS deleted");
        if (r.rows.empty()) break;
        int64_t deleted = std::get<int64_t>(r.rows[0].cols[0]);
        if (deleted == 0) break;
    }
}

}  // namespace tl_fuzz_oracle

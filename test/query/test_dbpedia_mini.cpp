// Schemaless query tests against the committed `test/data/dbpedia-mini/`
// fixture, using a live Neo4j HTTP endpoint as the oracle.
//
// Both backends are populated from the same fixture at fixture
// construction time:
//   TurboLynx: invokes `turbolynx import` against a mkdtemp workspace
//   Neo4j   : POSTs CREATE statements (namespace label `:Dbpedia_Mini`)
//             over the transactional HTTP endpoint.
//
// Each TEST_CASE runs the same Cypher on both backends and asserts
// multiset agreement.  When Neo4j is unreachable (no `NEO4J_HTTP_URI`
// env var and the default `http://localhost:7474` doesn't answer),
// every test SKIPs via `g_skip_requested` so CI on Neo4j-less hosts
// stays green.

#include "catch.hpp"

#include "helpers/query_runner.hpp"
#include "main/capi/turbolynx.h"

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

extern bool g_skip_requested;

namespace {

using json = nlohmann::json;

// ---------- Neo4j HTTP minimal client ---------------------------------------

struct CurlHandle {
    CURL* h = nullptr;
    CurlHandle()  { curl_global_init(CURL_GLOBAL_DEFAULT); h = curl_easy_init(); }
    ~CurlHandle() { if (h) curl_easy_cleanup(h); curl_global_cleanup(); }
};

size_t curl_collect(char* ptr, size_t sz, size_t nm, void* sink) {
    static_cast<std::string*>(sink)->append(ptr, sz * nm);
    return sz * nm;
}

class Neo4jHttp {
public:
    explicit Neo4jHttp(std::string uri, std::string db = "neo4j")
        : uri_(std::move(uri)), db_(std::move(db)) {}

    // Throws std::runtime_error on transport or Cypher error.
    json run(const std::string& cypher) {
        json req = { {"statements",
                       json::array({ {{"statement", cypher}} })} };
        std::string body  = req.dump();
        std::string reply;

        CURL* h = curl_.h;
        curl_easy_reset(h);
        std::string url = uri_ + "/db/" + db_ + "/tx/commit";
        curl_slist* hdr = nullptr;
        hdr = curl_slist_append(hdr, "Content-Type: application/json");
        hdr = curl_slist_append(hdr, "Accept: application/json");
        curl_easy_setopt(h, CURLOPT_URL,           url.c_str());
        curl_easy_setopt(h, CURLOPT_POST,          1L);
        curl_easy_setopt(h, CURLOPT_HTTPHEADER,    hdr);
        curl_easy_setopt(h, CURLOPT_POSTFIELDS,    body.c_str());
        curl_easy_setopt(h, CURLOPT_POSTFIELDSIZE, (long)body.size());
        curl_easy_setopt(h, CURLOPT_WRITEFUNCTION, curl_collect);
        curl_easy_setopt(h, CURLOPT_WRITEDATA,     &reply);
        curl_easy_setopt(h, CURLOPT_TIMEOUT,       60L);
        CURLcode rc = curl_easy_perform(h);
        curl_slist_free_all(hdr);
        if (rc != CURLE_OK) {
            throw std::runtime_error(std::string("Neo4j HTTP: ") +
                                     curl_easy_strerror(rc));
        }
        json doc = json::parse(reply, nullptr, false);
        if (doc.is_discarded()) {
            throw std::runtime_error("Neo4j response not JSON");
        }
        if (!doc.value("errors", json::array()).empty()) {
            throw std::runtime_error("Neo4j Cypher error: " +
                                     doc["errors"].dump());
        }
        return doc;
    }

    bool reachable() {
        try { (void)run("RETURN 1 AS x"); return true; }
        catch (...) { return false; }
    }

    static int64_t single_int64(const json& doc) {
        const auto& rs = doc["results"];
        if (rs.empty() || rs[0]["data"].empty()) return 0;
        return rs[0]["data"][0]["row"][0].get<int64_t>();
    }

    // Returns the first-column values of every row, as a sorted vector
    // of int64.  Used for multiset comparison against the TurboLynx
    // side.
    static std::vector<int64_t> int64_column(const json& doc) {
        std::vector<int64_t> out;
        if (doc["results"].empty()) return out;
        for (const auto& row : doc["results"][0]["data"]) {
            out.push_back(row["row"][0].get<int64_t>());
        }
        std::sort(out.begin(), out.end());
        return out;
    }

private:
    std::string  uri_;
    std::string  db_;
    CurlHandle   curl_;
};

// ---------- Fixture loader --------------------------------------------------

// `NODE` is a reserved word in TurboLynx Cypher grammar, but the
// committed fixture's edge CSV headers reference endpoints as
// `:START_ID(NODE)` (matching the original DBpedia loader).  We rewrite
// `(NODE)` → `(DbpediaNode)` in copies of the fixture before importing,
// and use the resulting label on both backends.
constexpr const char* kLabel           = "DbpediaNode";
constexpr const char* kRel_nationality = "NATIONALITY";
constexpr const char* kRel_country     = "COUNTRY";
constexpr const char* kRel_birthPlace  = "BIRTH_PLACE";

std::string repo_root() {
#ifdef TURBOLYNX_REPO_ROOT
    return TURBOLYNX_REPO_ROOT;
#else
    return ".";
#endif
}

std::string neo4j_uri_or_default() {
    if (const char* e = std::getenv("NEO4J_HTTP_URI")) return e;
    return "http://localhost:7474";
}

// Read every edge row from a fixture CSV.  Header is the first line
// (`:START_ID(NODE)|:END_ID(NODE)`) and is skipped.
std::vector<std::pair<int64_t, int64_t>>
read_edges(const std::string& path) {
    std::vector<std::pair<int64_t, int64_t>> out;
    std::ifstream f(path);
    if (!f) return out;
    std::string line;
    std::getline(f, line);          // header
    while (std::getline(f, line)) {
        auto bar = line.find('|');
        if (bar == std::string::npos) continue;
        int64_t a = std::stoll(line.substr(0, bar));
        int64_t b = std::stoll(line.substr(bar + 1));
        out.emplace_back(a, b);
    }
    return out;
}

// Read every node from nodes.json into a vector of {id, props_json}.
// `props_json` is the full original "properties" object so the
// schemaless property mix survives all the way into the test.
struct NodeRow { int64_t id; json props; };

std::vector<NodeRow> read_nodes(const std::string& path) {
    std::vector<NodeRow> out;
    std::ifstream f(path);
    if (!f) return out;
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        json obj = json::parse(line, nullptr, false);
        if (obj.is_discarded()) continue;
        const auto& p = obj["properties"];
        int64_t id = p.value("id", -1LL);
        if (id < 0) continue;
        out.push_back({id, p});
    }
    return out;
}

// ---------- The fixture -----------------------------------------------------

class DbpediaMiniFixture {
public:
    DbpediaMiniFixture() {
        std::string fixture_dir = repo_root() + "/test/data/dbpedia-mini";
        nodes_ = read_nodes(fixture_dir + "/nodes.json");
        nat_   = read_edges(fixture_dir + "/edges_nationality_797.csv");
        cou_   = read_edges(fixture_dir + "/edges_country_8183.csv");
        bir_   = read_edges(fixture_dir + "/edges_birthPlace_2950.csv");

        if (nodes_.empty()) return;  // fixture missing, every test will WARN

        if (!load_turbolynx(fixture_dir)) return;
        if (!load_neo4j()) {
            neo4j_ok_ = false;   // tests will SKIP individually
        }
        loaded_ = true;
    }

    ~DbpediaMiniFixture() {
        if (conn_id_ >= 0) turbolynx_disconnect(conn_id_);
        if (!workspace_.empty()) {
            std::string cmd = "rm -rf '" + workspace_ + "'";
            (void)std::system(cmd.c_str());
        }
        if (neo4j_ && neo4j_ok_) {
            try { (void)neo4j_->run(std::string("MATCH (n:") + kLabel +
                                    ") DETACH DELETE n"); }
            catch (...) {}
        }
    }

    bool loaded()    const { return loaded_; }
    bool neo4j_ok()  const { return neo4j_ok_; }

    qtest::QueryResult run_tl(const std::string& cypher) const {
        return runner_->run(cypher.c_str());
    }

    json run_neo4j(const std::string& cypher) {
        return neo4j_->run(cypher);
    }

private:
    // Copy a fixture file into `out_path`, replacing the
    // header-bound vertex label `(NODE)` with `(DbpediaNode)` so the
    // loader binds to the queryable label.  Body lines are copied
    // verbatim (they only contain numeric ids).
    static bool rewrite_label(const std::string& in_path,
                              const std::string& out_path) {
        std::ifstream fin(in_path);
        std::ofstream fout(out_path);
        if (!fin || !fout) return false;
        std::string line;
        bool first = true;
        const std::string from = "(NODE)";
        const std::string to   = std::string("(") + kLabel + ")";
        while (std::getline(fin, line)) {
            if (first) {
                size_t pos = 0;
                while ((pos = line.find(from, pos)) != std::string::npos) {
                    line.replace(pos, from.size(), to);
                    pos += to.size();
                }
                first = false;
            }
            fout << line << "\n";
        }
        return true;
    }

    bool load_turbolynx(const std::string& fixture_dir) {
        char tmpl[] = "/tmp/tl_dbpedia_mini_XXXXXX";
        char* d = mkdtemp(tmpl);
        if (!d) return false;
        workspace_ = d;

        // Stage the fixture into a scratch dir, rewriting the
        // `:END_ID(NODE)` headers so the loader uses our custom
        // (non-reserved) vertex label.
        std::string staging = workspace_ + "/__fixture";
        std::filesystem::create_directories(staging);
        for (const char* name : {"nodes.json",
                                  "edges_nationality_797.csv",
                                  "edges_country_8183.csv",
                                  "edges_birthPlace_2950.csv"}) {
            if (!rewrite_label(fixture_dir + "/" + name,
                                staging + "/" + name)) {
                std::cerr << "[dbpedia-mini] failed to stage " << name << "\n";
                return false;
            }
        }
        // Also stage the backward companions so the loader finds them.
        for (const char* name : {"edges_nationality_797.csv.backward",
                                  "edges_country_8183.csv.backward",
                                  "edges_birthPlace_2950.csv.backward"}) {
            if (!rewrite_label(fixture_dir + "/" + name,
                                staging + "/" + name)) {
                std::cerr << "[dbpedia-mini] failed to stage " << name << "\n";
                return false;
            }
        }

        std::string ws_dir = workspace_ + "/db";
        std::filesystem::create_directories(ws_dir);

        std::ostringstream cmd;
        cmd << TEST_QUERY_SHELL_BIN << " import"
            << " --workspace '" << ws_dir << "'"
            << " --skip-histogram"
            << " --nodes "         << kLabel
            << " '" << staging << "/nodes.json'"
            << " --relationships " << kRel_nationality
            << " '" << staging << "/edges_nationality_797.csv'"
            << " --relationships " << kRel_country
            << " '" << staging << "/edges_country_8183.csv'"
            << " --relationships " << kRel_birthPlace
            << " '" << staging << "/edges_birthPlace_2950.csv'"
            << " --log-level warn"
            << " > /dev/null 2>&1";
        if (std::system(cmd.str().c_str()) != 0) {
            std::cerr << "[dbpedia-mini] turbolynx import failed\n";
            return false;
        }
        runner_ = std::make_unique<qtest::QueryRunner>(ws_dir);
        return true;
    }

    bool load_neo4j() {
        neo4j_ = std::make_unique<Neo4jHttp>(neo4j_uri_or_default());
        if (!neo4j_->reachable()) return false;

        // Idempotent wipe (only nodes in our namespace label).
        try {
            (void)neo4j_->run(std::string("MATCH (n:") + kLabel +
                              ") DETACH DELETE n");
        } catch (...) { return false; }

        // Load nodes — UNWIND a parameterless inline list keeps the
        // POST self-contained.  Properties are limited to the
        // simply-typed core (id, uri) so the load Cypher stays sane;
        // the test queries focus on traversal and aggregation rather
        // than per-URI property reads.
        std::ostringstream s;
        s << "UNWIND [";
        for (size_t i = 0; i < nodes_.size(); ++i) {
            if (i) s << ",";
            int64_t nid = nodes_[i].id;
            std::string uri = nodes_[i].props.value("uri", "");
            // Escape: only need to handle \ and "
            std::string esc;
            esc.reserve(uri.size());
            for (char c : uri) {
                if (c == '\\' || c == '"') esc.push_back('\\');
                esc.push_back(c);
            }
            s << "{id:" << nid << ", uri:\"" << esc << "\"}";
        }
        s << "] AS r CREATE (n:" << kLabel
          << ") SET n.id = r.id, n.uri = r.uri";
        try { (void)neo4j_->run(s.str()); }
        catch (const std::exception& e) {
            std::cerr << "[dbpedia-mini] Neo4j node load failed: "
                      << e.what() << "\n";
            return false;
        }

        // Load edges.
        auto emit_edges = [&](const char* type, const std::vector<std::pair<int64_t,int64_t>>& es) {
            if (es.empty()) return true;
            std::ostringstream q;
            q << "UNWIND [";
            for (size_t i = 0; i < es.size(); ++i) {
                if (i) q << ",";
                q << "{s:" << es[i].first << ", t:" << es[i].second << "}";
            }
            q << "] AS r "
              << "MATCH (a:" << kLabel << " {id:r.s}), "
                    "(b:" << kLabel << " {id:r.t}) "
              << "CREATE (a)-[:" << type << "]->(b)";
            try { (void)neo4j_->run(q.str()); return true; }
            catch (const std::exception& e) {
                std::cerr << "[dbpedia-mini] Neo4j edge load failed for "
                          << type << ": " << e.what() << "\n";
                return false;
            }
        };
        if (!emit_edges(kRel_nationality, nat_)) return false;
        if (!emit_edges(kRel_country,     cou_)) return false;
        if (!emit_edges(kRel_birthPlace,  bir_)) return false;
        return true;
    }

    std::vector<NodeRow>                          nodes_;
    std::vector<std::pair<int64_t, int64_t>>      nat_, cou_, bir_;
    std::string                                   workspace_;
    int64_t                                       conn_id_ = -1;
    std::unique_ptr<qtest::QueryRunner>           runner_;
    std::unique_ptr<Neo4jHttp>                    neo4j_;
    bool                                          loaded_   = false;
    bool                                          neo4j_ok_ = true;
};

// One process-wide fixture: both backends are populated once, every
// TEST_CASE consults the same workspace + Neo4j state.
DbpediaMiniFixture& fixture() {
    static DbpediaMiniFixture f;
    return f;
}

#define SKIP_IF_NOT_READY()                                               \
    if (!fixture().loaded())   { WARN("dbpedia-mini fixture not loaded — " \
                                       "check test/data/dbpedia-mini/");    \
                                 g_skip_requested = true; return; }       \
    if (!fixture().neo4j_ok()) { WARN("Neo4j not reachable at "             \
                                       << neo4j_uri_or_default());          \
                                 g_skip_requested = true; return; }

// ---------- Result helpers --------------------------------------------------

int64_t tl_single_int64(const qtest::QueryResult& r) {
    REQUIRE(!r.rows.empty());
    return r[0].int64_at(0);
}

std::vector<int64_t> tl_int64_column(const qtest::QueryResult& r) {
    std::vector<int64_t> out;
    out.reserve(r.rows.size());
    for (const auto& row : r.rows) out.push_back(row.int64_at(0));
    std::sort(out.begin(), out.end());
    return out;
}

}  // namespace

// ===========================================================================
//  Tests
// ===========================================================================

TEST_CASE("dbpedia-mini: node count agrees with Neo4j",
          "[query][dbpedia-mini]") {
    SKIP_IF_NOT_READY();
    auto tl  = fixture().run_tl(std::string("MATCH (n:") + kLabel +
                                ") RETURN count(n)");
    auto neo = fixture().run_neo4j(std::string("MATCH (n:") + kLabel +
                                   ") RETURN count(n)");
    CHECK(tl_single_int64(tl) == Neo4jHttp::single_int64(neo));
}

TEST_CASE("dbpedia-mini: NATIONALITY edge count agrees with Neo4j",
          "[query][dbpedia-mini]") {
    SKIP_IF_NOT_READY();
    auto tl  = fixture().run_tl("MATCH ()-[r:" + std::string(kRel_nationality) +
                                "]->() RETURN count(r)");
    auto neo = fixture().run_neo4j("MATCH ()-[r:" + std::string(kRel_nationality) +
                                   "]->() RETURN count(r)");
    CHECK(tl_single_int64(tl) == Neo4jHttp::single_int64(neo));
}

TEST_CASE("dbpedia-mini: COUNTRY edge count agrees with Neo4j",
          "[query][dbpedia-mini]") {
    SKIP_IF_NOT_READY();
    auto tl  = fixture().run_tl("MATCH ()-[r:" + std::string(kRel_country) +
                                "]->() RETURN count(r)");
    auto neo = fixture().run_neo4j("MATCH ()-[r:" + std::string(kRel_country) +
                                   "]->() RETURN count(r)");
    CHECK(tl_single_int64(tl) == Neo4jHttp::single_int64(neo));
}

TEST_CASE("dbpedia-mini: BIRTH_PLACE edge count agrees with Neo4j",
          "[query][dbpedia-mini]") {
    SKIP_IF_NOT_READY();
    auto tl  = fixture().run_tl("MATCH ()-[r:" + std::string(kRel_birthPlace) +
                                "]->() RETURN count(r)");
    auto neo = fixture().run_neo4j("MATCH ()-[r:" + std::string(kRel_birthPlace) +
                                   "]->() RETURN count(r)");
    CHECK(tl_single_int64(tl) == Neo4jHttp::single_int64(neo));
}

TEST_CASE("dbpedia-mini: NATIONALITY one-hop targets agree with Neo4j",
          "[query][dbpedia-mini][traverse]") {
    SKIP_IF_NOT_READY();
    // Take a node with at least one outgoing NATIONALITY edge: the
    // very first src in the seed slice.  Anchored traversal — both
    // backends should return the identical (sorted) multiset of dst
    // ids.
    auto tl_src  = fixture().run_tl("MATCH (a:" + std::string(kLabel) +
                                    ")-[:" + kRel_nationality +
                                    "]->() RETURN a.id ORDER BY a.id LIMIT 1");
    auto neo_src = fixture().run_neo4j("MATCH (a:" + std::string(kLabel) +
                                       ")-[:" + kRel_nationality +
                                       "]->() RETURN a.id ORDER BY a.id LIMIT 1");
    int64_t src_tl  = tl_single_int64(tl_src);
    int64_t src_neo = Neo4jHttp::single_int64(neo_src);
    REQUIRE(src_tl == src_neo);

    std::string anchor = std::to_string(src_tl);
    auto tl  = fixture().run_tl("MATCH (a:" + std::string(kLabel) +
                                " {id:" + anchor + "})-[:" + kRel_nationality +
                                "]->(b) RETURN b.id ORDER BY b.id");
    auto neo = fixture().run_neo4j("MATCH (a:" + std::string(kLabel) +
                                   " {id:" + anchor + "})-[:" + kRel_nationality +
                                   "]->(b) RETURN b.id ORDER BY b.id");
    CHECK(tl_int64_column(tl) == Neo4jHttp::int64_column(neo));
}

TEST_CASE("dbpedia-mini: by-id property read agrees with Neo4j",
          "[query][dbpedia-mini][by-id]") {
    SKIP_IF_NOT_READY();
    // The committed fixture's id range is sparse (seed-derived ids
    // from the source DBpedia snapshot), so pick the smallest id
    // actually present at runtime instead of hard-coding one.
    auto tl_anchor  = fixture().run_tl(
        "MATCH (n:" + std::string(kLabel) +
        ") RETURN n.id ORDER BY n.id ASC LIMIT 1");
    auto neo_anchor = fixture().run_neo4j(
        "MATCH (n:" + std::string(kLabel) +
        ") RETURN n.id ORDER BY n.id ASC LIMIT 1");
    REQUIRE_FALSE(tl_anchor.rows.empty());
    int64_t anchor_tl  = tl_single_int64(tl_anchor);
    int64_t anchor_neo = Neo4jHttp::single_int64(neo_anchor);
    REQUIRE(anchor_tl == anchor_neo);

    std::string id_str = std::to_string(anchor_tl);
    auto tl  = fixture().run_tl("MATCH (n:" + std::string(kLabel) +
                                " {id:" + id_str + "}) RETURN n.uri");
    auto neo = fixture().run_neo4j("MATCH (n:" + std::string(kLabel) +
                                   " {id:" + id_str + "}) RETURN n.uri");
    REQUIRE_FALSE(tl.rows.empty());
    REQUIRE(!neo["results"][0]["data"].empty());
    auto tl_uri  = tl[0].str_at(0);
    auto neo_uri = neo["results"][0]["data"][0]["row"][0].get<std::string>();
    CHECK(tl_uri == neo_uri);
}

TEST_CASE("dbpedia-mini: in-degree distribution on NATIONALITY agrees",
          "[query][dbpedia-mini][aggregation]") {
    SKIP_IF_NOT_READY();
    // The size of the in-degree histogram is a single number that
    // exercises aggregation + traversal jointly.
    auto tl  = fixture().run_tl(std::string(
        "MATCH (n:") + kLabel + ")<-[r:" + kRel_nationality + "]-() "
        "WITH n, count(r) AS d "
        "RETURN sum(d) AS total");
    auto neo = fixture().run_neo4j(std::string(
        "MATCH (n:") + kLabel + ")<-[r:" + kRel_nationality + "]-() "
        "WITH n, count(r) AS d "
        "RETURN sum(d) AS total");
    CHECK(tl_single_int64(tl) == Neo4jHttp::single_int64(neo));
}

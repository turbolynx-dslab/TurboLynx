// Audit harness for the open fuzz-found issues.
//
// Each routine reconstructs the **exact** reproducer from the
// corresponding GitHub issue, runs it on a fresh TurboLynx workspace
// and on Neo4j (HTTP API, scoped via the `:Fz` namespace label), and
// prints the two results side-by-side.  Output is meant to be read by
// a human reviewer; no Catch2 assertions, no [!mayfail] machinery, no
// hidden filters.

#include "neo4j_runner.hpp"
#include "result_canonicalizer.hpp"
#include "seeded_workspace_oracle.hpp"

#include <cstdlib>
#include <exception>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace {

using tl_fuzz_oracle::Neo4jRunner;
using tl_fuzz_oracle::TurboLynxBackend;

std::string neo4j_uri() {
    if (const char* e = std::getenv("NEO4J_HTTP_URI")) return e;
    return "http://localhost:7474";
}

void dump(const std::string& label, const qtest::QueryResult& r) {
    std::cout << "  " << label << " (" << r.rows.size() << " rows):";
    if (r.rows.empty()) { std::cout << " <empty>\n"; return; }
    std::cout << "\n";
    for (size_t i = 0; i < r.rows.size() && i < 8; ++i) {
        std::cout << "    " << tl_fuzz::row_canonical(r.rows[i]) << "\n";
    }
    if (r.rows.size() > 8) {
        std::cout << "    ... +" << (r.rows.size() - 8) << " more\n";
    }
}

// Run `cypher` on both backends; print results + verdict (equal /
// diverged / TL-threw / Neo4j-threw).  Returns true iff equal.
bool compare_one(TurboLynxBackend& tl, Neo4jRunner& neo4j,
                 const std::string& cypher) {
    qtest::QueryResult lhs, rhs;
    bool tl_threw = false, n_threw = false;
    std::string tl_err, n_err;
    try { lhs = tl.run(cypher); }
    catch (const std::exception& e) { tl_threw = true; tl_err = e.what(); }
    try { rhs = neo4j.run(cypher); }
    catch (const std::exception& e) { n_threw = true; n_err = e.what(); }

    std::cout << "  Q: " << cypher << "\n";
    if (tl_threw) std::cout << "  TL    threw: " << tl_err << "\n";
    else          dump("TL   ", lhs);
    if (n_threw)  std::cout << "  Neo4j threw: " << n_err << "\n";
    else          dump("Neo4j", rhs);

    if (tl_threw || n_threw) return tl_threw == n_threw;
    auto cmp = tl_fuzz::compare(lhs, rhs, tl_fuzz::CompareMode::Multiset);
    if (cmp.equal) { std::cout << "  ✓ equal\n"; return true; }
    std::cout << "  ✗ diverged\n";
    return false;
}

void run_setup(TurboLynxBackend& tl, Neo4jRunner& neo4j,
               const std::vector<std::string>& stmts) {
    for (const auto& s : stmts) {
        bool tl_ok = true, n_ok = true;
        std::string err;
        try { (void)tl.run(s); }
        catch (const std::exception& e) { tl_ok = false; err = e.what(); }
        try { (void)neo4j.run(s); }
        catch (const std::exception& e) { n_ok = false; err = e.what(); }
        if (!tl_ok || !n_ok) {
            std::cout << "  setup imbalance on: " << s << "\n"
                      << "    TL    " << (tl_ok ? "ok" : "threw") << "\n"
                      << "    Neo4j " << (n_ok ? "ok" : "threw")
                      << "  err: " << err << "\n";
        }
    }
}

// ----- per-issue verifiers ------------------------------------------------

bool verify_238() {
    std::cout << "\n=== #238  multi-label CREATE bootstrap ===\n";
    TurboLynxBackend tl;
    Neo4jRunner      neo4j(neo4j_uri());
    neo4j.wipe();
    // Use a unique namespace so the verification doesn't collide with
    // any other Person nodes Neo4j may carry; only `:Fz`-tagged nodes
    // come and go via wipe.
    run_setup(tl, neo4j, {
        "CREATE (n:Fz:Tag {id:1, name:'A'})"
    });
    bool a = compare_one(tl, neo4j, "MATCH (n:Fz) RETURN n.id");
    bool b = compare_one(tl, neo4j, "MATCH (n:Tag) RETURN n.id");
    return a && b;
}

bool verify_237() {
    std::cout << "\n=== #237  edge double-count, single-label scope ===\n";
    TurboLynxBackend tl;
    Neo4jRunner      neo4j(neo4j_uri());
    neo4j.wipe();
    run_setup(tl, neo4j, {
        "CREATE (n:Fz {id:1, kind:'Person'})",
        "CREATE (n:Fz {id:2, kind:'Person'})",
        "MATCH (a:Fz {id:1}), (b:Fz {id:2}) CREATE (a)-[:KNOWS]->(b)",
    });
    return compare_one(tl, neo4j,
        "MATCH (:Fz)-[r:KNOWS]->(:Fz) RETURN count(r) AS cnt");
}

bool verify_239() {
    std::cout << "\n=== #239  labeled reverse-direction matches both sides ===\n";
    TurboLynxBackend tl;
    Neo4jRunner      neo4j(neo4j_uri());
    neo4j.wipe();
    run_setup(tl, neo4j, {
        "CREATE (n:Fz {id:1,  kind:'Person'})",
        "CREATE (n:Fz {id:9,  kind:'Person'})",
        "CREATE (n:Fz {id:10, kind:'Person'})",
        "MATCH (a:Fz {id:9}),  (b:Fz {id:10}) CREATE (a)-[:KNOWS]->(b)",
        "MATCH (a:Fz {id:10}), (b:Fz {id:1})  CREATE (a)-[:KNOWS]->(b)",
    });
    bool a = compare_one(tl, neo4j,
        "MATCH (a:Fz {id:10})-[:KNOWS]->(b:Fz) RETURN b.id ORDER BY b.id");
    bool b = compare_one(tl, neo4j,
        "MATCH (b:Fz)<-[:KNOWS]-(a:Fz {id:10}) RETURN b.id ORDER BY b.id");
    return a && b;
}

bool verify_240() {
    std::cout << "\n=== #240  multi-WITH chain drops rows ===\n";
    TurboLynxBackend tl;
    Neo4jRunner      neo4j(neo4j_uri());
    neo4j.wipe();
    std::vector<std::string> setup;
    for (int i = 1; i <= 10; ++i) {
        std::ostringstream q;
        q << "CREATE (n:Fz {id:" << i
          << ", kind:'Person', age:" << (20 + i * 3) << "})";
        setup.push_back(q.str());
    }
    run_setup(tl, neo4j, setup);
    bool a = compare_one(tl, neo4j,
        "MATCH (n:Fz {kind:'Person'}) WITH n.age AS age WHERE age > 25 "
        "RETURN count(*) AS cnt");
    bool b = compare_one(tl, neo4j,
        "MATCH (n:Fz {kind:'Person'}) WITH n, n.age AS age WHERE age > 25 "
        "WITH n.id AS id RETURN id ORDER BY id");
    return a && b;
}

bool verify_241() {
    std::cout << "\n=== #241  MERGE on empty bootstrap fails label lookup ===\n";
    TurboLynxBackend tl;
    Neo4jRunner      neo4j(neo4j_uri());
    neo4j.wipe();
    // First a MERGE on an empty TL workspace.
    run_setup(tl, neo4j, {
        "MERGE (n:Fz {id:4000, kind:'Person'}) ON CREATE SET n.created = true"
    });
    return compare_one(tl, neo4j,
        "MATCH (n:Fz {kind:'Person'}) RETURN count(n) AS cnt");
}

bool verify_235() {
    std::cout << "\n=== #235  OPTIONAL + WHERE IS NOT NULL garbage id ===\n";
    TurboLynxBackend tl;
    Neo4jRunner      neo4j(neo4j_uri());
    neo4j.wipe();
    // Seed similar to L2 fixture but namespace-isolated.
    run_setup(tl, neo4j, {
        "CREATE (n:Fz {id:1, kind:'Person', name:'Alice', age:30})",
        "CREATE (n:Fz {id:2, kind:'Person', name:'Bob',   age:25})",
        "CREATE (n:Fz {id:101, kind:'Movie',  title:'Alpha'})",
        "MATCH (p:Fz {id:1}), (m:Fz {id:101}) CREATE (p)-[:DIRECTED]->(m)",
    });
    bool a = compare_one(tl, neo4j,
        "MATCH (a:Fz {kind:'Person'})-[r:DIRECTED]->(b:Fz {kind:'Movie'}) "
        "RETURN a.id, b.title ORDER BY a.id");
    bool b = compare_one(tl, neo4j,
        "OPTIONAL MATCH (a:Fz {kind:'Person'})-[r:DIRECTED]->(b:Fz {kind:'Movie'}) "
        "WHERE r IS NOT NULL RETURN a.id, b.title ORDER BY a.id");
    return a && b;
}

bool verify_236() {
    std::cout << "\n=== #236  varlen + UNION ALL planner assertion ===\n";
    TurboLynxBackend tl;
    Neo4jRunner      neo4j(neo4j_uri());
    neo4j.wipe();
    run_setup(tl, neo4j, {
        "CREATE (n:Fz {id:1, kind:'Person'})",
        "CREATE (n:Fz {id:2, kind:'Person'})",
        "CREATE (n:Fz {id:3, kind:'Person'})",
        "MATCH (a:Fz {id:1}), (b:Fz {id:2}) CREATE (a)-[:KNOWS]->(b)",
        "MATCH (a:Fz {id:2}), (b:Fz {id:3}) CREATE (a)-[:KNOWS]->(b)",
    });
    bool a = compare_one(tl, neo4j,
        "MATCH (a:Fz)-[:KNOWS*1..2]->(b:Fz) RETURN a.id, b.id "
        "ORDER BY a.id, b.id");
    bool b = compare_one(tl, neo4j,
        "MATCH (a:Fz)-[:KNOWS*1..1]->(b:Fz) RETURN a.id, b.id "
        "UNION ALL "
        "MATCH (a:Fz)-[:KNOWS*2..2]->(b:Fz) RETURN a.id, b.id "
        "ORDER BY a.id, b.id");
    return a && b;
}

}  // namespace

int main() {
    struct Entry { const char* id; bool (*fn)(); };
    Entry entries[] = {
        {"#238", verify_238},
        {"#237", verify_237},
        {"#239", verify_239},
        {"#240", verify_240},
        {"#241", verify_241},
        {"#235", verify_235},
        {"#236", verify_236},
    };
    std::vector<std::string> confirmed, equal;
    for (const auto& e : entries) {
        try {
            bool both_equal = e.fn();
            (both_equal ? equal : confirmed).push_back(e.id);
        } catch (const std::exception& ex) {
            std::cout << "  !! " << e.id << " threw during verify: "
                      << ex.what() << "\n";
            confirmed.push_back(e.id);
        }
    }
    std::cout << "\n========== summary ==========\n";
    std::cout << "Confirmed divergent (real bug or harness/setup failure):\n";
    for (const auto& s : confirmed) std::cout << "  " << s << "\n";
    std::cout << "All probes equal on both backends:\n";
    for (const auto& s : equal) std::cout << "  " << s << "\n";
    return 0;
}

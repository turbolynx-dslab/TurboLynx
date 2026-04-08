// NL2CypherEngine — top-level orchestrator for translating a natural-
// language question into a Cypher query against a TurboGraph database.
//
// Layered design (this header is the public surface; internal layers
// live in the corresponding .cpp):
//
//   S0  zero-shot — full schema dump → single LLM call → extract Cypher
//   S1  profiling — offline metadata extraction (sample values, summaries)
//   S2  schema linking — multi-variant draft → field union
//   S3  multi-candidate — N candidates + compile-only validation + voting
//
// At construction time the engine loads any cached metadata from
// `<workspace>/.nl2cypher/metadata.json`. If absent, it falls back to
// the bare schema introspection (S0 mode). The richer paths (S1~S3)
// activate automatically once the metadata file is present.
//
// Thread safety: a single engine instance is intended to be owned by
// the shell (one client context = one engine). Concurrent Translate()
// calls are not supported.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "nl2cypher/llm_client.hpp"
#include "nl2cypher/schema_introspector.hpp"

namespace duckdb { class ClientContext; }

namespace turbolynx {
namespace nl2cypher {

struct TranslationResult {
    bool        ok = false;          // a candidate was selected
    std::string cypher;              // chosen Cypher query (without trailing ';')
    std::string error;               // human-readable error if !ok

    // Diagnostics for the user / for the validate-and-retry loop
    int n_candidates_generated = 0;
    int n_candidates_valid     = 0;
    bool used_cache            = false;
};

class NL2CypherEngine {
public:
    struct Config {
        // LLMClient backing config. Cache dir defaults to
        // <workspace>/.nl2cypher/llm_cache when set via Bind().
        LLMClient::Config llm;

        // Workspace path — used to resolve metadata.json + LLM cache dir.
        // Empty means "no on-disk persistence; in-memory only".
        std::string workspace;

        // S3: how many candidate Cypher queries to generate per call.
        // 1 = behave as a pure single-shot translator (S0/S1/S2).
        int n_candidates = 1;

        // S3: how many validate-and-retry rounds to attempt if every
        // candidate fails compile-only validation.
        int max_retry_rounds = 0;

        // S2: which schema-linking variants to run. Empty = no linking.
        // Each entry is a (schema-scope, profile-detail) tuple. The
        // engine maps the strings to internal enum values; unknown ones
        // are skipped with a warning. Recognised:
        //   "focused-min", "focused-max", "focused-full",
        //   "full-min",    "full-max"
        std::vector<std::string> linker_variants;
    };

    // Construct an engine bound to a specific ClientContext. The
    // context must outlive the engine; the engine reads catalog metadata
    // through it on demand.
    NL2CypherEngine(duckdb::ClientContext& client, Config cfg);
    ~NL2CypherEngine();

    NL2CypherEngine(const NL2CypherEngine&)            = delete;
    NL2CypherEngine& operator=(const NL2CypherEngine&) = delete;

    // Synchronous translate. Returns a fully-formed Cypher query the
    // shell can pass to its existing executor. Multi-candidate
    // generation, validation, and voting (S3) are applied if enabled.
    TranslationResult Translate(const std::string& nl_question);

    // Force a re-introspection of the catalog. Call after schema
    // changes (label/edge type added) so subsequent prompts see the
    // updated schema.
    void RefreshSchema();

    // Read-only access to the cached schema (for diagnostics).
    const GraphSchema& schema() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace nl2cypher
}  // namespace turbolynx

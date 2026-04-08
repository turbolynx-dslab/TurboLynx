// LLMClient — wraps the Anthropic `claude` CLI binary for synchronous,
// text-in / text-out calls used by the NL2Cypher front-end.
//
// Transport: spawns `claude --print --input-format stream-json
// --output-format stream-json --no-session-persistence --tools ""` as a
// subprocess and exchanges JSONL messages on its stdin/stdout pipes. Each
// worker is one-shot (closed after a single Call) so independent requests
// never share conversation context.
//
// Features:
//   * Pre-warmed worker pool (eliminates the ~2-5 s claude cold-start cost
//     on every call by keeping N idle workers ready).
//   * Disk cache keyed by hash(model + system + user); error responses are
//     never cached, so retries can succeed on a later run.
//   * tenacity-style exponential backoff retry on transient failures.
//   * Structured-output helper that strips ```json fences and extracts the
//     outermost balanced { ... } object before parsing.
//
// Thread safety: a single LLMClient instance is safe to share across
// threads. Internally, Call() draws one worker from the idle queue and
// spawns a replacement asynchronously.

#pragma once

#include <chrono>
#include <memory>
#include <string>

#include "nlohmann/json.hpp"

namespace turbolynx {
namespace nl2cypher {

// Models supported by the `claude` CLI's --model flag.
enum class LLMModel {
    Sonnet,
    Opus,
    Haiku,
};

const char* ToCliString(LLMModel model);

struct LLMRequest {
    std::string system;             // system prompt (may be empty)
    std::string user;               // user prompt
    LLMModel    model = LLMModel::Sonnet;

    // Per-call timeout. 0 means "use the LLMClient default".
    std::chrono::seconds timeout = std::chrono::seconds(0);
};

struct LLMResponse {
    bool        ok = false;         // true if a non-error result was received
    std::string text;               // raw assistant text on success
    std::string error;              // human-readable error message on failure
    int         attempts = 0;       // 1 = succeeded on first try
    bool        cache_hit = false;
};

struct LLMJsonResponse {
    bool           ok = false;
    nlohmann::json json;            // parsed JSON object on success
    std::string    raw;             // raw text returned by the model
    std::string    error;
    int            attempts = 0;
    bool           cache_hit = false;
};

class LLMClient {
public:
    struct Config {
        // --- Worker pool ---
        // Number of pre-warmed `claude` subprocesses kept idle. The first
        // Call() pops one (zero-latency) and the pool spawns a replacement
        // in the background so the next caller is also instant.
        // Set to 0 to disable pre-warming (each Call() spawns a fresh
        // worker on demand). 4 is a reasonable default for interactive use.
        size_t pool_size = 4;

        // Default model used for pool pre-warming. Requests for a different
        // model fall back to an on-demand fresh subprocess.
        LLMModel default_model = LLMModel::Sonnet;

        // Path to the `claude` binary. "claude" uses PATH lookup.
        std::string cli_path = "claude";

        // --- Cache ---
        // Directory in which to store one JSON file per cached response.
        // Empty string disables caching.
        std::string cache_dir;

        // --- Retry ---
        int max_attempts        = 3;
        int initial_backoff_ms  = 2000;
        int max_backoff_ms      = 30000;

        // --- Timeouts ---
        std::chrono::seconds default_timeout = std::chrono::seconds(120);
    };

    LLMClient();
    explicit LLMClient(Config config);
    ~LLMClient();

    LLMClient(const LLMClient&)            = delete;
    LLMClient& operator=(const LLMClient&) = delete;

    // Synchronous text call. Applies cache → retry → subprocess.
    LLMResponse Call(const LLMRequest& req);

    // Convenience wrapper that parses the response as JSON. Tolerant of
    // ```json ... ``` fences and surrounding prose: locates the outermost
    // balanced { ... } and parses that.
    LLMJsonResponse CallJson(const LLMRequest& req);

    const Config& config() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

// --- Free helpers (exposed for unit testing) ---

// Strip ```json ... ``` fences and locate the outermost balanced top-level
// JSON object in `text`. Returns the substring on success, empty string on
// failure. Single-quoted strings are NOT recognized; standard JSON only.
std::string ExtractJsonObject(const std::string& text);

}  // namespace nl2cypher
}  // namespace turbolynx

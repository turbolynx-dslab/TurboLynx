// =============================================================================
// [nl2cypher] LLMClient unit tests
// =============================================================================
// Tag: [nl2cypher][llm]
//
// Pure tests cover the JSON extraction helper. The end-to-end smoke test
// against the real `claude` CLI is hidden behind the `[.live]` tag — run
// it explicitly with `./test/unittest "[live][llm]"` when you want to
// verify the subprocess + pool path against the actual binary.
// =============================================================================

#include <chrono>
#include <cstdlib>
#include <string>

#include "catch.hpp"
#include "nl2cypher/llm_client.hpp"

using namespace turbolynx::nl2cypher;

TEST_CASE("ExtractJsonObject — bare object", "[nl2cypher][llm]") {
    auto out = ExtractJsonObject(R"({"a":1,"b":"x"})");
    REQUIRE(out == R"({"a":1,"b":"x"})");
}

TEST_CASE("ExtractJsonObject — object inside ```json fence",
          "[nl2cypher][llm]") {
    std::string input =
        "Here is the result:\n"
        "```json\n"
        "{\"cypher\": \"MATCH (n) RETURN n\"}\n"
        "```\n"
        "Hope this helps!";
    auto out = ExtractJsonObject(input);
    REQUIRE_FALSE(out.empty());
    REQUIRE(out.find("\"cypher\"") != std::string::npos);
    REQUIRE(out.find("MATCH (n) RETURN n") != std::string::npos);
}

TEST_CASE("ExtractJsonObject — ignores braces inside string literals",
          "[nl2cypher][llm]") {
    // The closing brace inside the value string must NOT terminate the
    // outer object. The escaped quote should be honored as well.
    std::string input = R"(Prose. {"text": "this } is fine \" and {nested}", "n": 1} trailing prose.)";
    auto out = ExtractJsonObject(input);
    REQUIRE_FALSE(out.empty());
    REQUIRE(out.front() == '{');
    REQUIRE(out.back()  == '}');
    // The substring should contain both keys.
    REQUIRE(out.find("\"text\"") != std::string::npos);
    REQUIRE(out.find("\"n\"")    != std::string::npos);
}

TEST_CASE("ExtractJsonObject — no object yields empty string",
          "[nl2cypher][llm]") {
    REQUIRE(ExtractJsonObject("nothing JSON in here").empty());
    REQUIRE(ExtractJsonObject("[1, 2, 3]").empty());  // arrays not supported
}

// ---------------------------------------------------------------------------
// Live smoke test — actually invokes the `claude` CLI. Hidden by default.
// Run with: ./test/unittest "[live][llm]"
// ---------------------------------------------------------------------------
TEST_CASE("LLMClient — live round trip against claude CLI",
          "[.live][nl2cypher][llm]") {
    LLMClient::Config cfg;
    cfg.pool_size       = 1;
    cfg.max_attempts    = 1;
    cfg.default_timeout = std::chrono::seconds(60);

    LLMClient client(cfg);

    LLMRequest req;
    req.system = "You are a calculator. Reply with only the digits, no prose.";
    req.user   = "What is 6 * 7?";

    auto resp = client.Call(req);
    INFO("error: " << resp.error << "\nattempts: " << resp.attempts);
    REQUIRE(resp.ok);
    REQUIRE_FALSE(resp.text.empty());
    REQUIRE(resp.text.find("42") != std::string::npos);
}

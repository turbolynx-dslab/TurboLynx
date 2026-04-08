// LLMClient implementation. See llm_client.hpp for the contract.
//
// Layout (in this file):
//   1. small utilities (hashing, file I/O, JSON extraction)
//   2. ClaudeWorker      — one subprocess + pipes, write-then-read-once
//   3. WorkerPool        — N pre-warmed workers, pop / spawn-replacement
//   4. DiskCache         — reads/writes one JSON file per cached response
//   5. LLMClient::Impl   — pool + cache + retry orchestration
//   6. LLMClient public  — Call / CallJson / config

#include "nl2cypher/llm_client.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "nlohmann/json.hpp"
#include "spdlog/spdlog.h"

namespace turbolynx {
namespace nl2cypher {

using json = nlohmann::json;
using clock_t_   = std::chrono::steady_clock;
using ms         = std::chrono::milliseconds;

const char* ToCliString(LLMModel model) {
    switch (model) {
        case LLMModel::Sonnet: return "sonnet";
        case LLMModel::Opus:   return "opus";
        case LLMModel::Haiku:  return "haiku";
    }
    return "sonnet";
}

// =====================================================================
// 1. Utilities
// =====================================================================

namespace {

// 128-bit fingerprint built from two FNV-1a 64-bit passes with different
// IVs. Not cryptographic, but sufficient for a local content-addressed
// response cache (the keyspace is the user's own prompts).
struct Fingerprint128 {
    uint64_t lo;
    uint64_t hi;

    std::string Hex() const {
        char buf[33];
        std::snprintf(buf, sizeof(buf), "%016lx%016lx",
                      static_cast<unsigned long>(hi),
                      static_cast<unsigned long>(lo));
        return std::string(buf);
    }
};

Fingerprint128 Fingerprint(const std::string& s) {
    constexpr uint64_t kFnvOffset1 = 0xcbf29ce484222325ULL;
    constexpr uint64_t kFnvOffset2 = 0x84222325cbf29ce4ULL;
    constexpr uint64_t kFnvPrime   = 0x100000001b3ULL;

    uint64_t a = kFnvOffset1;
    uint64_t b = kFnvOffset2;
    for (unsigned char c : s) {
        a ^= c;
        a *= kFnvPrime;
        b ^= static_cast<unsigned char>(c + 0x9e);
        b *= kFnvPrime;
    }
    // Mix the length back in so two prompts that hash to the same FNV
    // state but differ in length still produce distinct fingerprints.
    a ^= s.size();
    a *= kFnvPrime;
    b ^= s.size() << 1;
    b *= kFnvPrime;
    return {a, b};
}

// Public-facing JSON-extraction helper. Strips ```json ... ``` fences
// and finds the outermost balanced { ... } object, ignoring braces that
// appear inside JSON string literals.
std::string ExtractJsonObjectImpl(const std::string& text) {
    // Step 1: strip ```json fences if present.
    std::string body = text;
    auto fence = body.find("```");
    if (fence != std::string::npos) {
        // Drop everything up to (and including) the opening fence and its
        // optional language tag.
        size_t after = body.find('\n', fence);
        if (after != std::string::npos) {
            body = body.substr(after + 1);
        }
        auto close_fence = body.rfind("```");
        if (close_fence != std::string::npos) {
            body = body.substr(0, close_fence);
        }
    }

    // Step 2: walk the string tracking brace depth and string state.
    int depth = 0;
    bool in_string = false;
    bool escape = false;
    size_t start = std::string::npos;
    for (size_t i = 0; i < body.size(); ++i) {
        char c = body[i];
        if (in_string) {
            if (escape) {
                escape = false;
            } else if (c == '\\') {
                escape = true;
            } else if (c == '"') {
                in_string = false;
            }
            continue;
        }
        if (c == '"') {
            in_string = true;
            continue;
        }
        if (c == '{') {
            if (depth == 0) start = i;
            depth++;
        } else if (c == '}') {
            depth--;
            if (depth == 0 && start != std::string::npos) {
                return body.substr(start, i - start + 1);
            }
        }
    }
    return {};
}

bool WriteFileAtomic(const std::string& path, const std::string& content) {
    try {
        std::filesystem::path p(path);
        std::filesystem::create_directories(p.parent_path());
        std::string tmp = path + ".tmp";
        {
            std::ofstream ofs(tmp, std::ios::binary | std::ios::trunc);
            if (!ofs) return false;
            ofs.write(content.data(),
                      static_cast<std::streamsize>(content.size()));
            if (!ofs) return false;
        }
        std::filesystem::rename(tmp, path);
        return true;
    } catch (const std::exception& e) {
        spdlog::warn("[nl2cypher] cache write failed: {}", e.what());
        return false;
    }
}

bool ReadFile(const std::string& path, std::string* out) {
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) return false;
    std::ostringstream oss;
    oss << ifs.rdbuf();
    *out = oss.str();
    return static_cast<bool>(ifs);
}

// Set the close-on-exec flag so file descriptors held by the parent are
// not inherited by the child. (We dup2 the actual stdio fds we want.)
void SetCloexec(int fd) {
    int flags = fcntl(fd, F_GETFD);
    if (flags >= 0) fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
}

}  // namespace

std::string ExtractJsonObject(const std::string& text) {
    return ExtractJsonObjectImpl(text);
}

// =====================================================================
// 2. ClaudeWorker — one subprocess, one prompt, one response.
// =====================================================================
//
// Lifecycle:
//   ctor    fork+exec; both pipes open.
//   Send()  write prompt → poll/read JSONL until {"type":"result"}.
//   dtor    close pipes; SIGTERM if still alive; reap.
//
// A ClaudeWorker instance MUST NOT be reused across calls — the CLI's
// stream-json mode silently accumulates conversation context across
// messages, which would contaminate independent NL2Cypher requests and
// inflate input tokens. The pool enforces this by discarding workers
// after Send() returns.

class ClaudeWorker {
public:
    ClaudeWorker(const std::string& cli_path, LLMModel model)
        : model_(model) {
        Spawn(cli_path);
    }

    ~ClaudeWorker() { Shutdown(); }

    ClaudeWorker(const ClaudeWorker&)            = delete;
    ClaudeWorker& operator=(const ClaudeWorker&) = delete;

    bool alive() const { return pid_ > 0; }

    // Send a single user prompt and read the assistant result.
    // `combined_prompt` already contains "<system>\n\n<user>" if a system
    // prompt was supplied (stream-json has no separate system field on
    // user messages, so the caller concatenates).
    LLMResponse Send(const std::string& combined_prompt,
                     std::chrono::seconds timeout) {
        LLMResponse resp;
        if (!alive()) {
            resp.error = "worker is not alive";
            return resp;
        }

        // Step A: write the prompt as a single JSONL line.
        json msg = {
            {"type", "user"},
            {"message", {
                {"role", "user"},
                {"content", combined_prompt},
            }},
        };
        std::string line = msg.dump();
        line.push_back('\n');

        if (!WriteAll(stdin_fd_, line)) {
            resp.error = "failed to write prompt to claude stdin";
            return resp;
        }

        // Step B: read JSONL from stdout until we see a result message
        // or hit the deadline.
        auto deadline = clock_t_::now() + timeout;
        std::string buf;
        char chunk[4096];

        while (true) {
            auto now = clock_t_::now();
            if (now >= deadline) {
                resp.error = "timeout waiting for claude response";
                return resp;
            }
            int wait_ms = static_cast<int>(
                std::chrono::duration_cast<ms>(deadline - now).count());
            if (wait_ms <= 0) wait_ms = 1;

            struct pollfd pfd{};
            pfd.fd = stdout_fd_;
            pfd.events = POLLIN;
            int rc = ::poll(&pfd, 1, wait_ms);
            if (rc < 0) {
                if (errno == EINTR) continue;
                resp.error = std::string("poll failed: ") + std::strerror(errno);
                return resp;
            }
            if (rc == 0) {
                resp.error = "timeout waiting for claude response";
                return resp;
            }
            if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
                // Drain any pending bytes before giving up.
                ssize_t n = ::read(stdout_fd_, chunk, sizeof(chunk));
                if (n > 0) buf.append(chunk, static_cast<size_t>(n));
                if (TryConsume(buf, &resp)) return resp;
                resp.error = "claude pipe closed before result";
                return resp;
            }

            ssize_t n = ::read(stdout_fd_, chunk, sizeof(chunk));
            if (n < 0) {
                if (errno == EINTR) continue;
                resp.error = std::string("read failed: ") + std::strerror(errno);
                return resp;
            }
            if (n == 0) {
                if (TryConsume(buf, &resp)) return resp;
                resp.error = "claude EOF before result";
                return resp;
            }
            buf.append(chunk, static_cast<size_t>(n));

            if (TryConsume(buf, &resp)) return resp;
        }
    }

private:
    pid_t pid_       = -1;
    int   stdin_fd_  = -1;
    int   stdout_fd_ = -1;
    LLMModel model_;

    void Spawn(const std::string& cli_path) {
        int in_pipe[2];   // parent writes -> child stdin
        int out_pipe[2];  // child stdout -> parent reads
        if (::pipe(in_pipe) != 0 || ::pipe(out_pipe) != 0) {
            spdlog::error("[nl2cypher] pipe() failed: {}", std::strerror(errno));
            return;
        }

        pid_t pid = ::fork();
        if (pid < 0) {
            spdlog::error("[nl2cypher] fork() failed: {}", std::strerror(errno));
            ::close(in_pipe[0]);  ::close(in_pipe[1]);
            ::close(out_pipe[0]); ::close(out_pipe[1]);
            return;
        }

        if (pid == 0) {
            // --- child ---
            ::dup2(in_pipe[0],  STDIN_FILENO);
            ::dup2(out_pipe[1], STDOUT_FILENO);
            // Leave stderr attached to the parent's terminal so claude's
            // own diagnostics remain visible.
            ::close(in_pipe[0]);  ::close(in_pipe[1]);
            ::close(out_pipe[0]); ::close(out_pipe[1]);

            const char* model_str = ToCliString(model_);
            std::vector<const char*> argv = {
                cli_path.c_str(),
                "--print",
                "--verbose",
                "--input-format",  "stream-json",
                "--output-format", "stream-json",
                "--model",         model_str,
                // Disable all built-in tools — without this, the model
                // interprets file paths in prompts as Read-tool requests
                // and stalls on permission prompts.
                "--allowed-tools", "",
                nullptr,
            };
            ::execvp(cli_path.c_str(),
                     const_cast<char* const*>(argv.data()));
            // execvp only returns on failure.
            std::fprintf(stderr, "execvp(%s) failed: %s\n",
                         cli_path.c_str(), std::strerror(errno));
            ::_exit(127);
        }

        // --- parent ---
        ::close(in_pipe[0]);
        ::close(out_pipe[1]);
        pid_       = pid;
        stdin_fd_  = in_pipe[1];
        stdout_fd_ = out_pipe[0];
        SetCloexec(stdin_fd_);
        SetCloexec(stdout_fd_);
    }

    void Shutdown() {
        if (stdin_fd_  >= 0) { ::close(stdin_fd_);  stdin_fd_  = -1; }
        if (stdout_fd_ >= 0) { ::close(stdout_fd_); stdout_fd_ = -1; }
        if (pid_ > 0) {
            int status = 0;
            // Give the child a brief grace period to exit on its own.
            for (int i = 0; i < 20; ++i) {
                pid_t r = ::waitpid(pid_, &status, WNOHANG);
                if (r == pid_) { pid_ = -1; return; }
                if (r < 0)     { pid_ = -1; return; }
                std::this_thread::sleep_for(ms(5));
            }
            ::kill(pid_, SIGTERM);
            for (int i = 0; i < 20; ++i) {
                pid_t r = ::waitpid(pid_, &status, WNOHANG);
                if (r == pid_) { pid_ = -1; return; }
                std::this_thread::sleep_for(ms(10));
            }
            ::kill(pid_, SIGKILL);
            ::waitpid(pid_, &status, 0);
            pid_ = -1;
        }
    }

    bool WriteAll(int fd, const std::string& data) {
        size_t off = 0;
        while (off < data.size()) {
            ssize_t n = ::write(fd, data.data() + off, data.size() - off);
            if (n < 0) {
                if (errno == EINTR) continue;
                return false;
            }
            off += static_cast<size_t>(n);
        }
        return true;
    }

    // Pull complete lines off the front of `buf`. If a "result" message
    // is found, populate `out` and return true. Otherwise return false
    // and leave any incomplete trailing line in `buf`.
    bool TryConsume(std::string& buf, LLMResponse* out) {
        size_t pos = 0;
        while (true) {
            size_t nl = buf.find('\n', pos);
            if (nl == std::string::npos) break;
            std::string line = buf.substr(pos, nl - pos);
            pos = nl + 1;
            if (line.empty()) continue;

            json j;
            try {
                j = json::parse(line);
            } catch (const std::exception&) {
                // Non-JSON noise (warning lines etc.) — skip.
                continue;
            }
            std::string type = j.value("type", "");
            if (type == "result") {
                bool is_error = j.value("is_error", false);
                if (is_error) {
                    out->ok    = false;
                    out->error = j.value("result", std::string("claude error"));
                } else {
                    out->ok   = true;
                    out->text = j.value("result", std::string{});
                }
                buf.erase(0, pos);
                return true;
            }
            // Other types (system/init, assistant chunks, rate_limit_event)
            // are informational; we wait for the terminal "result".
        }
        if (pos > 0) buf.erase(0, pos);
        return false;
    }
};

// =====================================================================
// 3. WorkerPool — N pre-warmed ClaudeWorkers.
// =====================================================================

class WorkerPool {
public:
    WorkerPool(std::string cli_path, LLMModel model, size_t size)
        : cli_path_(std::move(cli_path)), model_(model), target_size_(size) {
        for (size_t i = 0; i < target_size_; ++i) {
            SpawnInline();
        }
    }

    ~WorkerPool() {
        {
            std::lock_guard<std::mutex> lock(mu_);
            shutting_down_ = true;
        }
        cv_.notify_all();
        // Wait for any in-flight async spawns to finish before tearing
        // down (their threads are detached but they capture `this`).
        while (in_flight_spawns_.load() > 0) {
            std::this_thread::sleep_for(ms(5));
        }
        std::lock_guard<std::mutex> lock(mu_);
        while (!idle_.empty()) idle_.pop();
    }

    // Pop one idle worker (block up to `wait` for warmup); spawn a
    // replacement asynchronously so the next caller is also instant.
    // Returns null if the pool is shutting down or warmup failed.
    std::unique_ptr<ClaudeWorker> Acquire(std::chrono::seconds wait) {
        std::unique_ptr<ClaudeWorker> w;
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait_for(lock, wait, [&] {
                return shutting_down_ || !idle_.empty();
            });
            if (shutting_down_) return nullptr;
            if (idle_.empty())  return nullptr;
            w = std::move(idle_.front());
            idle_.pop();
        }
        SpawnAsync();
        return w;
    }

    LLMModel model() const { return model_; }

private:
    std::string                                  cli_path_;
    LLMModel                                     model_;
    size_t                                       target_size_;
    std::mutex                                   mu_;
    std::condition_variable                      cv_;
    std::queue<std::unique_ptr<ClaudeWorker>>    idle_;
    bool                                         shutting_down_ = false;
    std::atomic<int>                             in_flight_spawns_{0};

    void SpawnInline() {
        auto w = std::make_unique<ClaudeWorker>(cli_path_, model_);
        if (!w->alive()) {
            spdlog::warn("[nl2cypher] failed to pre-warm a claude worker");
            return;
        }
        std::lock_guard<std::mutex> lock(mu_);
        idle_.push(std::move(w));
        cv_.notify_one();
    }

    void SpawnAsync() {
        in_flight_spawns_.fetch_add(1);
        std::thread([this] {
            SpawnInline();
            in_flight_spawns_.fetch_sub(1);
        }).detach();
    }
};

// =====================================================================
// 4. DiskCache
// =====================================================================

class DiskCache {
public:
    explicit DiskCache(std::string dir) : dir_(std::move(dir)) {
        if (!dir_.empty()) {
            try {
                std::filesystem::create_directories(dir_);
            } catch (const std::exception& e) {
                spdlog::warn("[nl2cypher] cache dir create failed: {}", e.what());
                dir_.clear();
            }
        }
    }

    bool enabled() const { return !dir_.empty(); }

    static std::string MakeKey(LLMModel model,
                               const std::string& system,
                               const std::string& user) {
        std::string mat;
        mat.reserve(system.size() + user.size() + 16);
        mat.append(ToCliString(model));
        mat.push_back('\n');
        mat.append(system);
        mat.push_back('\n');
        mat.append(user);
        return Fingerprint(mat).Hex();
    }

    bool Get(const std::string& key, std::string* response_text) const {
        if (!enabled()) return false;
        std::string path = PathFor(key);
        std::string body;
        if (!ReadFile(path, &body)) return false;
        try {
            json j = json::parse(body);
            if (!j.contains("response")) return false;
            *response_text = j["response"].get<std::string>();
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }

    void Put(const std::string& key, const std::string& response_text) {
        if (!enabled()) return;
        json j = {{"response", response_text}};
        WriteFileAtomic(PathFor(key), j.dump());
    }

private:
    std::string dir_;

    std::string PathFor(const std::string& key) const {
        return dir_ + "/" + key + ".json";
    }
};

// =====================================================================
// 5. LLMClient::Impl
// =====================================================================

class LLMClient::Impl {
public:
    explicit Impl(Config config)
        : config_(std::move(config)),
          cache_(config_.cache_dir),
          pool_(config_.pool_size > 0
                    ? std::make_unique<WorkerPool>(config_.cli_path,
                                                   config_.default_model,
                                                   config_.pool_size)
                    : nullptr) {}

    const Config& config() const { return config_; }

    LLMResponse Call(const LLMRequest& req) {
        LLMResponse resp;

        // --- Cache lookup (skips on cache miss; never caches errors) ---
        std::string cache_key = DiskCache::MakeKey(
            req.model, req.system, req.user);
        if (cache_.enabled()) {
            std::string cached;
            if (cache_.Get(cache_key, &cached)) {
                resp.ok        = true;
                resp.text      = std::move(cached);
                resp.attempts  = 0;
                resp.cache_hit = true;
                return resp;
            }
        }

        std::string combined =
            req.system.empty() ? req.user : (req.system + "\n\n" + req.user);

        std::chrono::seconds timeout =
            req.timeout.count() > 0 ? req.timeout : config_.default_timeout;

        int backoff_ms = config_.initial_backoff_ms;
        for (int attempt = 1; attempt <= config_.max_attempts; ++attempt) {
            resp.attempts = attempt;

            std::unique_ptr<ClaudeWorker> worker;
            if (pool_ && req.model == config_.default_model) {
                worker = pool_->Acquire(timeout);
            }
            if (!worker) {
                // No pool, mismatched model, or pool exhausted — spawn
                // a fresh worker on demand.
                worker = std::make_unique<ClaudeWorker>(
                    config_.cli_path, req.model);
            }

            if (!worker || !worker->alive()) {
                resp.ok    = false;
                resp.error = "failed to spawn claude worker";
            } else {
                resp = worker->Send(combined, timeout);
                resp.attempts = attempt;
            }

            if (resp.ok) {
                cache_.Put(cache_key, resp.text);
                return resp;
            }

            spdlog::warn("[nl2cypher] LLM call attempt {}/{} failed: {}",
                         attempt, config_.max_attempts, resp.error);

            if (attempt < config_.max_attempts) {
                std::this_thread::sleep_for(ms(backoff_ms));
                backoff_ms = std::min(backoff_ms * 2,
                                      config_.max_backoff_ms);
            }
        }
        return resp;
    }

private:
    Config                       config_;
    DiskCache                    cache_;
    std::unique_ptr<WorkerPool>  pool_;
};

// =====================================================================
// 6. LLMClient public surface
// =====================================================================

LLMClient::LLMClient() : LLMClient(Config{}) {}

LLMClient::LLMClient(Config config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

LLMClient::~LLMClient() = default;

LLMResponse LLMClient::Call(const LLMRequest& req) {
    return impl_->Call(req);
}

LLMJsonResponse LLMClient::CallJson(const LLMRequest& req) {
    LLMJsonResponse out;
    LLMResponse text = impl_->Call(req);
    out.attempts  = text.attempts;
    out.cache_hit = text.cache_hit;
    out.raw       = text.text;
    if (!text.ok) {
        out.error = text.error;
        return out;
    }
    std::string body = ExtractJsonObject(text.text);
    if (body.empty()) {
        out.error = "no JSON object found in LLM response";
        return out;
    }
    try {
        out.json = json::parse(body);
        out.ok   = true;
    } catch (const std::exception& e) {
        out.error = std::string("JSON parse failed: ") + e.what();
    }
    return out;
}

const LLMClient::Config& LLMClient::config() const {
    return impl_->config();
}

}  // namespace nl2cypher
}  // namespace turbolynx

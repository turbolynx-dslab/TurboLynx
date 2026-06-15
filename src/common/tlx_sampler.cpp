// TEMP in-process statistical profiler (env TLX_SAMPLE=1). Samples the running
// function via SIGPROF + backtrace at ~1 kHz, aggregates by symbol, prints top
// at exit. A poor-man's `perf` for environments without it.
#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <vector>
#include <execinfo.h>
#include <sys/time.h>
#include <dlfcn.h>
#include <cxxabi.h>

namespace {

constexpr int kMaxFrames = 32;
// Raw return addresses captured in the signal handler (async-signal-safe:
// backtrace() is, the storage below is a fixed ring with atomic cursor).
constexpr size_t kCap = 4000000;
void *g_pcs[kCap];
std::atomic<size_t> g_n{0};
bool g_on = false;

constexpr int kStore = 8;  // frames stored per sample
void handler(int) {
    void *bt[kMaxFrames];
    int n = backtrace(bt, kMaxFrames);
    size_t s = g_n.fetch_add(1, std::memory_order_relaxed);
    if (s >= kCap / kStore) return;
    void **slot = &g_pcs[s * kStore];
    for (int f = 0; f < kStore; f++)
        slot[f] = (f + 2 < n) ? bt[f + 2] : nullptr;  // skip handler+trampoline
}

std::string demangle(const char *sym) {
    if (!sym) return "?";
    int st = 0;
    char *d = abi::__cxa_demangle(sym, nullptr, nullptr, &st);
    std::string s = (st == 0 && d) ? d : sym;
    if (d) free(d);
    if (s.size() > 90) s = s.substr(0, 90);
    return s;
}

struct Sampler {
    Sampler() {
        if (!std::getenv("TLX_SAMPLE")) return;
        g_on = true;
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = handler;
        sigemptyset(&sa.sa_mask);
        sigaction(SIGPROF, &sa, nullptr);
        struct itimerval t;
        t.it_interval.tv_sec = 0;
        t.it_interval.tv_usec = 1000;  // 1 kHz
        t.it_value = t.it_interval;
        setitimer(ITIMER_PROF, &t, nullptr);
    }
    ~Sampler() {
        if (!g_on) return;
        struct itimerval z;
        memset(&z, 0, sizeof(z));
        setitimer(ITIMER_PROF, &z, nullptr);
        size_t n = g_n.load();
        if (n > kCap / kStore) n = kCap / kStore;
        auto resolve = [](void *pc) -> std::string {
            if (!pc) return "";
            Dl_info info;
            if (dladdr(pc, &info) && info.dli_sname) return demangle(info.dli_sname);
            return "?";
        };
        auto is_noise = [](const std::string &s) {
            return s.empty() || s == "?" || s == "syscall" || s == "malloc" ||
                   s == "free" || s == "__libc_free" || s == "cfree" ||
                   s.find("CMemoryPool") != std::string::npos ||
                   s.find("operator new") != std::string::npos ||
                   s.find("_int_") != std::string::npos ||
                   s.find("__sched_yield") != std::string::npos ||
                   s.find("malloc") != std::string::npos ||
                   s.find("std::") == 0;
        };
        std::map<std::string, uint64_t> byfn;       // leaf attribution
        std::map<std::string, uint64_t> by_app;     // first app frame (skip noise)
        for (size_t i = 0; i < n; i++) {
            void **slot = &g_pcs[i * kStore];
            byfn[resolve(slot[0])]++;
            // attribute to the topmost non-noise (app) frame
            std::string app;
            for (int f = 0; f < kStore; f++) {
                std::string s = resolve(slot[f]);
                if (!is_noise(s)) { app = s; break; }
            }
            if (!app.empty()) by_app[app]++;
        }
        {
            std::vector<std::pair<std::string, uint64_t>> v(by_app.begin(), by_app.end());
            std::sort(v.begin(), v.end(), [](auto &a, auto &b){ return a.second > b.second; });
            std::fprintf(stderr, "\n[TLX_SAMPLE] APP attribution (syscall/malloc charged to caller):\n");
            for (size_t i = 0; i < v.size() && i < 20; i++)
                std::fprintf(stderr, "  %5.1f%%  %s\n", n ? 100.0*v[i].second/n : 0.0, v[i].first.c_str());
        }
        std::vector<std::pair<std::string, uint64_t>> v(byfn.begin(), byfn.end());
        std::sort(v.begin(), v.end(),
                  [](auto &a, auto &b) { return a.second > b.second; });
        std::fprintf(stderr, "\n[TLX_SAMPLE] %zu samples, top functions by self time:\n", n);
        for (size_t i = 0; i < v.size() && i < 30; i++) {
            std::fprintf(stderr, "  %5.1f%%  %s\n",
                         n ? 100.0 * v[i].second / n : 0.0, v[i].first.c_str());
        }
    }
};
Sampler g_sampler;

}  // namespace

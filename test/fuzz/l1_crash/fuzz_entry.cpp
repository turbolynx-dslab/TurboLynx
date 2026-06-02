// L1 crash-fuzz entry point.
//
// libFuzzer drives byte sequences into `turbolynx_prepare`/`turbolynx_execute`.
// The verdict is intrinsic: any SEGV, ASAN/UBSAN hit, assertion, or hang
// counts as a finding.  Result correctness is *not* checked here — that is
// L2/L3/L4's job.
//
// State strategy: one persistent connection against an empty-bootstrap
// temp workspace is kept across all iterations.  We don't reset between
// iterations because (a) the JVM-less cost of `turbolynx_connect` is still
// non-trivial, and (b) crash-fuzz cares about *any* input that crashes,
// not about which state preceded it.  Cross-iteration state contamination
// is acceptable noise.

#include "main/capi/turbolynx.h"

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

// LeakSanitizer is off by default for L1.  TurboLynx (and ORCA in
// particular) retains memory pools until process exit on purpose, so
// every fuzz iteration would otherwise report the same harmless shutdown
// leaks.  Real heap-use-after-free / heap-buffer-overflow / stack-
// overflow / SIGSEGV findings still fire normally under
// AddressSanitizer.
extern "C" const char* __lsan_default_options() {
    return "detect_leaks=0";
}

namespace {

int64_t g_conn = -1;
char    g_workspace_dir[] = "/tmp/tl_fuzz_l1_XXXXXX";

void init_once() {
    if (g_conn >= 0) return;

    char* d = mkdtemp(g_workspace_dir);
    if (!d) {
        std::fprintf(stderr, "[fuzz_l1] mkdtemp failed: %s\n", strerror(errno));
        std::exit(2);
    }
    g_conn = turbolynx_connect(d);
    if (g_conn < 0) {
        std::fprintf(stderr, "[fuzz_l1] turbolynx_connect failed for %s\n", d);
        std::exit(2);
    }
}

void run_query(const char* query) {
    // turbolynx_prepare may return null on parse error (normal),
    // or throw internally — wrap to swallow expected exceptions.
    turbolynx_prepared_statement* prep = nullptr;
    try {
        prep = turbolynx_prepare(g_conn, const_cast<char*>(query));
    } catch (...) {
        return;
    }
    if (!prep) return;

    turbolynx_resultset_wrapper* rw = nullptr;
    try {
        turbolynx_num_rows total = turbolynx_execute(g_conn, prep, &rw);
        (void)total;
        if (rw) {
            // Drain rows so any deferred materialisation runs.
            while (turbolynx_fetch_next(rw) != TURBOLYNX_END_OF_RESULT) {
                // no-op; we just want the side effects of fetch
            }
        }
    } catch (...) {
        // Runtime errors thrown across the boundary are expected for
        // ill-typed or otherwise invalid queries.  Crash-fuzz only
        // cares about uncatchable failures (SEGV, ASAN, asserts).
    }

    if (rw) {
        try { turbolynx_close_resultset(rw); } catch (...) {}
    }
    try { turbolynx_close_prepared_statement(prep); } catch (...) {}
}

}  // namespace

extern "C" int LLVMFuzzerInitialize(int* /*argc*/, char*** /*argv*/) {
    init_once();
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    init_once();

    // Bound input size: very large inputs slow the fuzzer without
    // exercising new code paths.  16 KiB is generous for Cypher.
    if (size == 0 || size > (1u << 14)) return 0;

    std::string q(reinterpret_cast<const char*>(data), size);
    // Null bytes break the C-string API contract; replace with space so
    // the parser at least gets to look at them.
    for (char& c : q) {
        if (c == '\0') c = ' ';
    }

    run_query(q.c_str());
    return 0;
}

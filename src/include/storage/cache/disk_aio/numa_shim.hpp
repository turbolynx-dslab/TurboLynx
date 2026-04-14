// numa_shim.hpp — Drop-in replacement for <numa.h>
//
// Implements all libnuma functions used in disk_aio/ via direct Linux
// syscalls (mbind, set_mempolicy) and procfs/sysconf.
// No libnuma-dev system package required.
//
// Under TURBOLYNX_WASM: all functions return single-node defaults,
// memory allocation falls back to malloc/free.

#pragma once

#ifdef TURBOLYNX_WASM
// ======== WASM stubs: no NUMA, single node ========
#include <stdlib.h>
#include <string.h>

struct bitmask {
    unsigned long maskp[2];
};

static inline int numa_num_configured_nodes() { return 1; }
static inline int numa_num_configured_cpus() { return 1; }
static inline long numa_pagesize() { return 4096; }

static inline struct bitmask *numa_allocate_nodemask() {
    return (struct bitmask *)calloc(1, sizeof(struct bitmask));
}
static inline void numa_bitmask_setbit(struct bitmask *bmp, unsigned int n) {
    if (n < 128) bmp->maskp[n / 64] |= (1UL << (n % 64));
}
static inline void numa_free_nodemask(struct bitmask *bmp) { free(bmp); }
static inline void numa_bind(struct bitmask *) {}
static inline void numa_interleave_memory(void *, size_t, struct bitmask *) {}
static inline void numa_tonode_memory(void *, size_t, int) {}

static inline void *numa_alloc_interleaved(size_t sz) { return malloc(sz); }
static inline void *numa_alloc_onnode(size_t sz, int) { return malloc(sz); }
static inline void numa_free(void *ptr, size_t) { free(ptr); }

static inline struct bitmask *_ns_get_all_nodes_ptr() {
    static struct bitmask all = {};
    all.maskp[0] = 1UL;
    return &all;
}
#define numa_all_nodes_ptr (_ns_get_all_nodes_ptr())

#else // !TURBOLYNX_WASM — real Linux NUMA implementation

#include <sys/mman.h>     // mmap, munmap
#include <sys/syscall.h>  // SYS_mbind, SYS_set_mempolicy
#include <unistd.h>       // access, sysconf
#include <string.h>       // memset
#include <stdlib.h>       // calloc, free
#include <stdio.h>        // snprintf

// NUMA memory-policy modes (from linux/mempolicy.h)
#ifndef MPOL_BIND
#  define MPOL_BIND        2
#endif
#ifndef MPOL_INTERLEAVE
#  define MPOL_INTERLEAVE  3
#endif

// Maximum NUMA nodes we support (kernel supports up to 1024; 128 is enough)
static const int _NS_MAX_NODES  = 128;
static const int _NS_MASK_WORDS = (_NS_MAX_NODES + 63) / 64;  // = 2

// --------------------------------------------------------------------------
// struct bitmask  (replaces libnuma's struct bitmask)
// We embed the mask array directly to avoid a separate allocation.
// --------------------------------------------------------------------------
struct bitmask {
    unsigned long maskp[_NS_MASK_WORDS];
};

// --------------------------------------------------------------------------
// Topology queries
// --------------------------------------------------------------------------
static inline int numa_num_configured_nodes() {
    int n = 0;
    char path[64];
    while (n < _NS_MAX_NODES) {
        snprintf(path, sizeof(path),
                 "/sys/devices/system/node/node%d", n);
        if (access(path, F_OK) != 0) break;
        ++n;
    }
    return n > 0 ? n : 1;
}

static inline int numa_num_configured_cpus() {
    return (int)sysconf(_SC_NPROCESSORS_CONF);
}

static inline long numa_pagesize() {
    return sysconf(_SC_PAGESIZE);
}

// --------------------------------------------------------------------------
// Bitmask helpers
// --------------------------------------------------------------------------
static inline struct bitmask *numa_allocate_nodemask() {
    return (struct bitmask *)calloc(1, sizeof(struct bitmask));
}

static inline void numa_bitmask_setbit(struct bitmask *bmp, unsigned int n) {
    if (n < (unsigned)_NS_MAX_NODES)
        bmp->maskp[n / 64] |= (1UL << (n % 64));
}

static inline void numa_free_nodemask(struct bitmask *bmp) {
    free(bmp);
}

// --------------------------------------------------------------------------
// Internal: raw mbind / set_mempolicy syscall wrappers
// --------------------------------------------------------------------------
static inline int _ns_mbind(void *ptr, size_t sz, int mode,
                             const unsigned long *nodemask) {
    return (int)syscall(SYS_mbind,
                        (unsigned long)ptr, (unsigned long)sz,
                        (unsigned long)mode,
                        nodemask,
                        (unsigned long)(_NS_MAX_NODES + 1),
                        0UL);
}

// --------------------------------------------------------------------------
// Thread / process NUMA binding
// --------------------------------------------------------------------------

// numa_bind: set memory policy for this thread to MPOL_BIND on the given nodes
static inline void numa_bind(struct bitmask *bmp) {
    syscall(SYS_set_mempolicy,
            (long)MPOL_BIND,
            bmp->maskp,
            (unsigned long)(_NS_MAX_NODES + 1));
}

// --------------------------------------------------------------------------
// Memory policy on existing mappings
// --------------------------------------------------------------------------
static inline void numa_interleave_memory(void *ptr, size_t sz,
                                           struct bitmask *nodemask) {
    _ns_mbind(ptr, sz, MPOL_INTERLEAVE, nodemask->maskp);
}

static inline void numa_tonode_memory(void *ptr, size_t sz, int node) {
    unsigned long mask[_NS_MASK_WORDS] = {};
    if (node >= 0 && node < _NS_MAX_NODES)
        mask[node / 64] |= (1UL << (node % 64));
    _ns_mbind(ptr, sz, MPOL_BIND, mask);
}

// --------------------------------------------------------------------------
// NUMA-aware allocation / deallocation
// --------------------------------------------------------------------------
static inline void *numa_alloc_interleaved(size_t sz) {
    void *ptr = mmap(nullptr, sz, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) return nullptr;

    // Build all-nodes interleave mask
    int n = numa_num_configured_nodes();
    unsigned long mask[_NS_MASK_WORDS] = {};
    for (int i = 0; i < n; i++)
        mask[i / 64] |= (1UL << (i % 64));

    _ns_mbind(ptr, sz, MPOL_INTERLEAVE, mask);
    return ptr;
}

static inline void *numa_alloc_onnode(size_t sz, int node) {
    void *ptr = mmap(nullptr, sz, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) return nullptr;
    numa_tonode_memory(ptr, sz, node);
    return ptr;
}

static inline void numa_free(void *ptr, size_t sz) {
    munmap(ptr, sz);
}

// --------------------------------------------------------------------------
// numa_all_nodes_ptr — lazy-initialized Meyers singleton
// Usage is identical to the libnuma global: pass it to numa_interleave_memory
// --------------------------------------------------------------------------
static inline struct bitmask *_ns_get_all_nodes_ptr() {
    static struct bitmask all = [] {
        struct bitmask bm = {};
        int n = numa_num_configured_nodes();
        for (int i = 0; i < n; i++)
            numa_bitmask_setbit(&bm, i);
        return bm;
    }();
    return &all;
}
// Exposes as a macro so existing code using it as an rvalue compiles as-is
#define numa_all_nodes_ptr (_ns_get_all_nodes_ptr())

#endif // TURBOLYNX_WASM

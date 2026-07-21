#include "storage/cache/buffer_pool.h"

#include <algorithm>
#include <cstdlib>
#include <unistd.h>
#ifndef TURBOLYNX_WASM
#include <sys/mman.h>
#endif

#include "common/assert.hpp"

namespace duckdb {

// ---------------------------------------------------------------------------
// Constructor / Destructor
// ---------------------------------------------------------------------------

BufferPool::BufferPool(size_t memory_limit) {
    if (memory_limit == 0) {
#ifdef TURBOLYNX_WASM
        // WASM: ALLOW_MEMORY_GROWTH is enabled, sysconf reports initial
        // (tiny) heap. Use 128MB — Emscripten will grow as needed.
        memory_limit = 128ULL * 1024 * 1024;
#else
        long pages     = sysconf(_SC_PHYS_PAGES);
        long page_size = sysconf(_SC_PAGE_SIZE);
        double frac = 0.8;
        if (const char *env = getenv("TLX_BUFFER_POOL_FRAC")) {
            double v = atof(env);
            if (v > 0.0 && v <= 1.0) frac = v;
        }
        memory_limit = static_cast<size_t>(pages * page_size * frac);
        if (const char *env = getenv("TLX_BUFFER_POOL_GB")) {
            double gb = atof(env);
            if (gb > 0.0) memory_limit = static_cast<size_t>(gb * 1024.0 * 1024 * 1024);
        }
#endif
    }
    memory_limit_ = memory_limit;
}

BufferPool::~BufferPool() {
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& [cid, e] : entries_) {
        free(e.ptr);
    }
}

// ---------------------------------------------------------------------------
// Alloc
// ---------------------------------------------------------------------------

static void* PoolAlloc(size_t size) {
    void* raw = nullptr;
#if defined(MADV_HUGEPAGE)
    constexpr size_t kHugeSize = 2ULL * 1024 * 1024;
    if (size >= kHugeSize) {
        if (posix_memalign(&raw, kHugeSize, size) != 0) return nullptr;
        madvise(raw, size, MADV_HUGEPAGE);
    }
    else if (posix_memalign(&raw, 512, size) != 0) {
        return nullptr;
    }
#else
    if (posix_memalign(&raw, 512, size) != 0) return nullptr;
#endif
    // First-touch on the requesting thread so the pages land on its NUMA
    // node; otherwise the disk-aio worker's write places them remotely.
    memset(raw, 0, size);
    return raw;
}

bool BufferPool::Alloc(ChunkID cid, size_t size, uint8_t** ptr) {
    void* raw = PoolAlloc(size);
    if (raw == nullptr) return false;

    std::lock_guard<std::mutex> lk(mu_);

    if (entries_.count(cid)) {
        free(raw);
        return false;  // already cached
    }

    *ptr = static_cast<uint8_t*>(raw);
    entries_[cid] = Entry{*ptr, size, /*pin_count=*/1, /*dirty=*/false, /*clock_bit=*/true};
    clock_keys_.push_back(cid);
    used_memory_ += size;
    return true;
}

// ---------------------------------------------------------------------------
// AllocStaged / Publish — two-phase insertion
// ---------------------------------------------------------------------------
//
// A concurrent fast-path Get() must never observe a cid that is still being
// loaded (ReadData) or swizzled (CacheDataTransformer::Swizzle). The caller
// therefore loads the buffer completely BEFORE registering it in entries_.
//
// AllocStaged: posix_memalign outside entries_, charge used_memory_ so the
//              budget reflects in-flight loads. Caller must hold whatever
//              mutex serializes slow-path loaders (ChunkCacheManager::pin_mu_).
// Publish    : atomically register the fully-loaded buffer with pin_count=1.

bool BufferPool::AllocStaged(size_t size, uint8_t** ptr) {
    void* raw = PoolAlloc(size);
    if (raw == nullptr) return false;
    *ptr = static_cast<uint8_t*>(raw);

    std::lock_guard<std::mutex> lk(mu_);
    used_memory_ += size;
    return true;
}

void BufferPool::Publish(ChunkID cid, uint8_t* ptr, size_t size) {
    std::lock_guard<std::mutex> lk(mu_);
    // Caller's pin_mu_ plus the prior double-check Get() guarantee no
    // duplicate entry; assert here to catch any callsite that forgets.
    D_ASSERT(entries_.count(cid) == 0);
    entries_[cid] = Entry{ptr, size, /*pin_count=*/1, /*dirty=*/false, /*clock_bit=*/true};
    clock_keys_.push_back(cid);
}

// ---------------------------------------------------------------------------
// Get
// ---------------------------------------------------------------------------

bool BufferPool::Get(ChunkID cid, uint8_t** ptr, size_t* size) {
    std::lock_guard<std::mutex> lk(mu_);

    auto it = entries_.find(cid);
    if (it == entries_.end()) return false;

    it->second.pin_count++;
    it->second.clock_bit = true;  // recently used
    *ptr  = it->second.ptr;
    *size = it->second.size;
    return true;
}

// ---------------------------------------------------------------------------
// Release
// ---------------------------------------------------------------------------

void BufferPool::Release(ChunkID cid) {
    std::lock_guard<std::mutex> lk(mu_);

    auto it = entries_.find(cid);
    if (it == entries_.end()) return;
    if (it->second.pin_count > 0) --it->second.pin_count;
}

// ---------------------------------------------------------------------------
// Dirty-bit helpers
// ---------------------------------------------------------------------------

bool BufferPool::SetDirty(ChunkID cid) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = entries_.find(cid);
    if (it == entries_.end()) return false;
    if (it->second.dirty) return false;
    it->second.dirty = true;
    return true;
}

bool BufferPool::ClearDirty(ChunkID cid) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = entries_.find(cid);
    if (it == entries_.end()) return false;
    if (!it->second.dirty) return false;
    it->second.dirty = false;
    return true;
}

bool BufferPool::GetDirty(ChunkID cid) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = entries_.find(cid);
    return it != entries_.end() && it->second.dirty;
}

// ---------------------------------------------------------------------------
// Remove
// ---------------------------------------------------------------------------

void BufferPool::Remove(ChunkID cid) {
    std::lock_guard<std::mutex> lk(mu_);

    auto it = entries_.find(cid);
    if (it == entries_.end()) return;

    free(it->second.ptr);
    used_memory_ -= it->second.size;
    entries_.erase(it);

    // Keep clock_keys_ in sync; adjust clock_hand_ if the erased entry
    // was before the current hand position.
    auto kit = std::find(clock_keys_.begin(), clock_keys_.end(), cid);
    if (kit != clock_keys_.end()) {
        size_t idx = static_cast<size_t>(kit - clock_keys_.begin());
        clock_keys_.erase(kit);
        if (!clock_keys_.empty()) {
            if (clock_hand_ > idx) --clock_hand_;
            clock_hand_ %= clock_keys_.size();
        } else {
            clock_hand_ = 0;
        }
    }
}

// ---------------------------------------------------------------------------
// PickVictim — Second-Chance Clock
// ---------------------------------------------------------------------------

ChunkID BufferPool::PickVictim() {
    std::lock_guard<std::mutex> lk(mu_);

    if (clock_keys_.empty()) return ChunkID(-1);

    size_t n = clock_keys_.size();
    for (size_t i = 0; i < 2 * n; ++i) {
        if (clock_hand_ >= clock_keys_.size()) clock_hand_ = 0;

        ChunkID  cid = clock_keys_[clock_hand_];
        Entry&   e   = entries_[cid];

        if (e.pin_count > 0) {
            // Pinned — cannot evict.
            clock_hand_ = (clock_hand_ + 1) % clock_keys_.size();
            continue;
        }
        if (e.clock_bit) {
            // Second chance: clear bit and skip this round.
            e.clock_bit = false;
            clock_hand_ = (clock_hand_ + 1) % clock_keys_.size();
            continue;
        }
        // Victim found.  Hand stays here; caller will Remove() this entry.
        return cid;
    }
    return ChunkID(-1);  // all pages pinned
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

size_t BufferPool::FreeMemory() const {
    std::lock_guard<std::mutex> lk(mu_);
    return memory_limit_ > used_memory_ ? memory_limit_ - used_memory_ : 0;
}

size_t BufferPool::UsedMemory() const {
    std::lock_guard<std::mutex> lk(mu_);
    return used_memory_;
}

int BufferPool::RefCount(ChunkID cid) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = entries_.find(cid);
    return it != entries_.end() ? it->second.pin_count : -1;
}

} // namespace duckdb

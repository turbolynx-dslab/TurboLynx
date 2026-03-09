// =============================================================================
// BufferPool — in-process page buffer (replaces LightningClient)
// =============================================================================
// Manages a fixed memory budget of 512-byte-aligned pages identified by
// ChunkID.  Eviction uses the Second-Chance Clock algorithm.
//
// Thread-safety: all public methods are protected by an internal mutex.
// Callers must NOT hold the mutex when invoking disk I/O; the mutex is
// released between every public call so disk flushes can re-enter safely.
// =============================================================================

#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>

#include "common/constants.hpp"
#include "common/unordered_map.hpp"

namespace duckdb {

class BufferPool {
public:
    struct Entry {
        uint8_t* ptr;       // 512-byte-aligned malloc'd buffer
        size_t   size;      // allocated bytes
        int      pin_count; // number of active pinners
        bool     dirty;     // needs write-back to disk
        bool     clock_bit; // Second-Chance eviction flag
    };

    // memory_limit: maximum bytes to keep in RAM.
    // Pass 0 to use 80 % of physical RAM (sysconf).
    explicit BufferPool(size_t memory_limit = 0);
    ~BufferPool();

    // Allocate a 512-byte-aligned buffer of `size` bytes for `cid`.
    // pin_count is set to 1 on success.
    // Returns false if cid already exists or posix_memalign fails.
    bool Alloc(ChunkID cid, size_t size, uint8_t** ptr);

    // Increment pin_count and return ptr/size. Returns false on miss.
    bool Get(ChunkID cid, uint8_t** ptr, size_t* size);

    // Decrement pin_count (makes page evictable when count reaches 0).
    void Release(ChunkID cid);

    // Dirty-bit management.
    void SetDirty(ChunkID cid);
    void ClearDirty(ChunkID cid);
    bool GetDirty(ChunkID cid) const;

    // Remove a page immediately (caller must already have flushed if dirty).
    void Remove(ChunkID cid);

    // Second-Chance Clock: return one unpinned victim cid.
    // Returns ChunkID(-1) if every page is currently pinned.
    // Does NOT remove the victim; caller flushes then calls Remove().
    ChunkID PickVictim();

    // Memory statistics.
    size_t FreeMemory()  const;  // memory_limit_ - used_memory_
    size_t UsedMemory()  const;

    // Debug helper (returns -1 if cid not found).
    int RefCount(ChunkID cid) const;

private:
    duckdb::unordered_map<ChunkID, Entry> entries_;
    std::vector<ChunkID>                  clock_keys_;  // insertion-order ring
    size_t                                clock_hand_ = 0;
    size_t                                memory_limit_;
    size_t                                used_memory_ = 0;
    mutable std::mutex                    mu_;
};

} // namespace duckdb

// =============================================================================
// [storage][bufferpool] BufferPool unit tests
// =============================================================================

#include "catch.hpp"
#include "storage/cache/buffer_pool.h"

using namespace duckdb;

static constexpr size_t MB = 1024 * 1024;
static constexpr size_t LIMIT = 16 * MB;  // 16 MB for tests

// ---------------------------------------------------------------------------
// Alloc / Get basics
// ---------------------------------------------------------------------------

TEST_CASE("BufferPool: Alloc returns 512-byte-aligned pointer", "[storage][bufferpool]") {
    BufferPool pool(LIMIT);
    uint8_t* ptr = nullptr;
    REQUIRE(pool.Alloc(1, 4096, &ptr));
    REQUIRE(ptr != nullptr);
    REQUIRE(reinterpret_cast<uintptr_t>(ptr) % 512 == 0);
}

TEST_CASE("BufferPool: Alloc sets pin_count to 1", "[storage][bufferpool]") {
    BufferPool pool(LIMIT);
    uint8_t* ptr = nullptr;
    REQUIRE(pool.Alloc(42, 512, &ptr));
    REQUIRE(pool.RefCount(42) == 1);
}

TEST_CASE("BufferPool: Alloc same cid twice returns false", "[storage][bufferpool]") {
    BufferPool pool(LIMIT);
    uint8_t* ptr = nullptr;
    REQUIRE(pool.Alloc(1, 512, &ptr));
    REQUIRE_FALSE(pool.Alloc(1, 512, &ptr));  // already cached
}

TEST_CASE("BufferPool: Get hit increments pin_count and returns same ptr", "[storage][bufferpool]") {
    BufferPool pool(LIMIT);
    uint8_t* ptr = nullptr;
    REQUIRE(pool.Alloc(10, 1024, &ptr));

    uint8_t* got = nullptr;
    size_t   sz  = 0;
    REQUIRE(pool.Get(10, &got, &sz));
    REQUIRE(got == ptr);
    REQUIRE(sz  == 1024);
    REQUIRE(pool.RefCount(10) == 2);  // Alloc (1) + Get (1)
}

TEST_CASE("BufferPool: Get miss returns false", "[storage][bufferpool]") {
    BufferPool pool(LIMIT);
    uint8_t* ptr = nullptr;
    size_t   sz  = 0;
    REQUIRE_FALSE(pool.Get(99, &ptr, &sz));
}

// ---------------------------------------------------------------------------
// Release
// ---------------------------------------------------------------------------

TEST_CASE("BufferPool: Release decrements pin_count", "[storage][bufferpool]") {
    BufferPool pool(LIMIT);
    uint8_t* ptr = nullptr;
    REQUIRE(pool.Alloc(5, 512, &ptr));
    REQUIRE(pool.RefCount(5) == 1);

    pool.Release(5);
    REQUIRE(pool.RefCount(5) == 0);
}

TEST_CASE("BufferPool: Release does not go below zero", "[storage][bufferpool]") {
    BufferPool pool(LIMIT);
    uint8_t* ptr = nullptr;
    REQUIRE(pool.Alloc(5, 512, &ptr));
    pool.Release(5);
    pool.Release(5);  // extra release — must not underflow
    REQUIRE(pool.RefCount(5) == 0);
}

// ---------------------------------------------------------------------------
// Dirty bits
// ---------------------------------------------------------------------------

TEST_CASE("BufferPool: dirty bit starts false", "[storage][bufferpool]") {
    BufferPool pool(LIMIT);
    uint8_t* ptr = nullptr;
    REQUIRE(pool.Alloc(1, 512, &ptr));
    REQUIRE_FALSE(pool.GetDirty(1));
}

TEST_CASE("BufferPool: SetDirty / ClearDirty round-trip", "[storage][bufferpool]") {
    BufferPool pool(LIMIT);
    uint8_t* ptr = nullptr;
    REQUIRE(pool.Alloc(2, 512, &ptr));

    pool.SetDirty(2);
    REQUIRE(pool.GetDirty(2));

    pool.ClearDirty(2);
    REQUIRE_FALSE(pool.GetDirty(2));
}

// ---------------------------------------------------------------------------
// Remove
// ---------------------------------------------------------------------------

TEST_CASE("BufferPool: Remove frees entry; Get returns false afterwards", "[storage][bufferpool]") {
    BufferPool pool(LIMIT);
    uint8_t* ptr = nullptr;
    REQUIRE(pool.Alloc(7, 512, &ptr));

    pool.Release(7);    // unpin first
    pool.Remove(7);

    uint8_t* got = nullptr;
    size_t   sz  = 0;
    REQUIRE_FALSE(pool.Get(7, &got, &sz));
}

TEST_CASE("BufferPool: Remove reclaims memory (UsedMemory decreases)", "[storage][bufferpool]") {
    BufferPool pool(LIMIT);
    uint8_t* ptr = nullptr;
    REQUIRE(pool.Alloc(3, 4096, &ptr));
    size_t used_after_alloc = pool.UsedMemory();
    REQUIRE(used_after_alloc == 4096);

    pool.Release(3);
    pool.Remove(3);
    REQUIRE(pool.UsedMemory() == 0);
    REQUIRE(pool.FreeMemory() == LIMIT);
}

// ---------------------------------------------------------------------------
// FreeMemory
// ---------------------------------------------------------------------------

TEST_CASE("BufferPool: FreeMemory tracks allocations", "[storage][bufferpool]") {
    BufferPool pool(LIMIT);
    REQUIRE(pool.FreeMemory() == LIMIT);

    uint8_t* ptr = nullptr;
    REQUIRE(pool.Alloc(1, MB, &ptr));
    REQUIRE(pool.FreeMemory() == LIMIT - MB);

    REQUIRE(pool.Alloc(2, MB, &ptr));
    REQUIRE(pool.FreeMemory() == LIMIT - 2 * MB);
}

// ---------------------------------------------------------------------------
// PickVictim — Second-Chance Clock
// ---------------------------------------------------------------------------

TEST_CASE("BufferPool: PickVictim returns -1 when all pages pinned", "[storage][bufferpool]") {
    BufferPool pool(LIMIT);
    uint8_t* ptr = nullptr;
    REQUIRE(pool.Alloc(1, 512, &ptr));  // pin_count = 1

    REQUIRE(pool.PickVictim() == ChunkID(-1));
}

TEST_CASE("BufferPool: PickVictim skips pinned pages", "[storage][bufferpool]") {
    BufferPool pool(LIMIT);
    uint8_t* ptr = nullptr;
    REQUIRE(pool.Alloc(1, 512, &ptr));  // pin_count = 1 (pinned)
    REQUIRE(pool.Alloc(2, 512, &ptr));  // pin_count = 1 (pinned)
    pool.Release(2);                    // pin_count = 0 (evictable)

    // cid=1 is pinned, cid=2 is not — victim should be 2.
    // Second-Chance gives cid=2 one pass (clock_bit=true → clear),
    // then on the second sweep it returns cid=2.
    ChunkID v = pool.PickVictim();
    REQUIRE(v == ChunkID(2));
}

TEST_CASE("BufferPool: PickVictim gives second chance then evicts", "[storage][bufferpool]") {
    // Single unpinned page with clock_bit=true: first PickVictim clears bit
    // (returns -1 if only one candidate after clearing), second call returns it.
    BufferPool pool(LIMIT);
    uint8_t* ptr = nullptr;
    REQUIRE(pool.Alloc(1, 512, &ptr));
    pool.Release(1);  // now evictable, clock_bit = true (set by Alloc)

    // First call: clock_bit cleared (second chance), but since only one entry
    // the sweep wraps around and still returns it (2*n iterations).
    ChunkID v = pool.PickVictim();
    REQUIRE(v == ChunkID(1));  // found within 2*n sweep
}

TEST_CASE("BufferPool: Remove after PickVictim allows re-Alloc of same cid", "[storage][bufferpool]") {
    BufferPool pool(LIMIT);
    uint8_t* ptr = nullptr;
    REQUIRE(pool.Alloc(1, 512, &ptr));
    pool.Release(1);

    ChunkID v = pool.PickVictim();
    REQUIRE(v == ChunkID(1));
    pool.Remove(v);

    // cid=1 should be allocatable again
    REQUIRE(pool.Alloc(1, 512, &ptr));
    REQUIRE(pool.RefCount(1) == 1);
}

// ---------------------------------------------------------------------------
// Multiple pages — eviction sequence
// ---------------------------------------------------------------------------

TEST_CASE("BufferPool: multiple unpinned pages all become eviction candidates", "[storage][bufferpool]") {
    BufferPool pool(LIMIT);
    uint8_t* ptr = nullptr;

    for (ChunkID id = 1; id <= 5; ++id) {
        REQUIRE(pool.Alloc(id, 512, &ptr));
        pool.Release(id);  // unpin all
    }

    // All 5 pages must be evicted eventually
    size_t evicted = 0;
    for (int attempt = 0; attempt < 20; ++attempt) {
        ChunkID v = pool.PickVictim();
        if (v == ChunkID(-1)) break;
        pool.Remove(v);
        ++evicted;
    }
    REQUIRE(evicted == 5);
    REQUIRE(pool.UsedMemory() == 0);
}

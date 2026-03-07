#pragma once

#include <functional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace duckdb {

// Minimal open-addressing flat hash map (linear probing, power-of-2 capacity,
// 50 % max load factor).  Supports only the subset of the unordered_map
// interface required by bulkload_pipeline: reserve / emplace / at / size.
template <typename K, typename V,
          typename H = std::hash<K>,
          typename E = std::equal_to<K>>
class FlatHashMap {
    struct Slot {
        K key{};
        V value{};
        bool occupied = false;
    };

    std::vector<Slot> slots_;
    size_t size_ = 0;
    H hasher_;
    E equal_;

    size_t mask() const { return slots_.size() - 1; }

    // Insert key/value without load-factor check (used during rehash).
    void _raw_insert(K key, V value) {
        size_t h = hasher_(key) & mask();
        while (slots_[h].occupied && !equal_(slots_[h].key, key))
            h = (h + 1) & mask();
        if (!slots_[h].occupied) {
            slots_[h] = {std::move(key), std::move(value), true};
            ++size_;
        }
        // key already present: emplace semantics (no overwrite).
    }

    void _grow() {
        size_t new_cap = slots_.empty() ? 16u : slots_.size() * 2;
        std::vector<Slot> old = std::move(slots_);
        slots_.assign(new_cap, Slot{});
        size_ = 0;
        for (auto& s : old)
            if (s.occupied) _raw_insert(std::move(s.key), std::move(s.value));
    }

public:
    FlatHashMap() { slots_.assign(16u, Slot{}); }

    // Pre-allocate for at least n entries at ≤50 % load.
    void reserve(size_t n) {
        size_t needed = 16u;
        while (needed < n * 2) needed <<= 1;
        if (needed <= slots_.size()) return;
        std::vector<Slot> old = std::move(slots_);
        slots_.assign(needed, Slot{});
        size_ = 0;
        for (auto& s : old)
            if (s.occupied) _raw_insert(std::move(s.key), std::move(s.value));
    }

    // Insert key→value if key is not already present.
    void emplace(K key, V value) {
        if (size_ * 2 >= slots_.size()) _grow();
        _raw_insert(std::move(key), std::move(value));
    }

    // Throws std::out_of_range if key is absent.
    const V& at(const K& key) const {
        size_t h = hasher_(key) & mask();
        while (slots_[h].occupied) {
            if (equal_(slots_[h].key, key)) return slots_[h].value;
            h = (h + 1) & mask();
        }
        throw std::out_of_range("FlatHashMap::at: key not found");
    }

    // Returns pointer to value, or nullptr if key is absent.  Safe for
    // concurrent reads (no writes in flight).
    const V* get_ptr(const K& key) const {
        size_t h = hasher_(key) & mask();
        while (slots_[h].occupied) {
            if (equal_(slots_[h].key, key)) return &slots_[h].value;
            h = (h + 1) & mask();
        }
        return nullptr;
    }

    size_t size() const { return size_; }
};

} // namespace duckdb

#pragma once

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace turbolynx {

// Mixin: lifetime-management for objects produced by a class that holds
// onto them via raw observer pointers. Same idea as ORCA's GPOS_NEW(mp)
// but scoped to a normal C++ object.
//
//   class Planner : public OwnsObjects<CypherPhysicalOperator, ...> { ... };
//   auto *op = make_owned<PhysicalNodeScan>(args...);   // creates + owns
//
// make_owned<T> picks the owner vector whose element_type is a base
// of (or equal to) T. clear_owned() drops every owner vector in
// declaration order.
template <typename... OwnedTypes>
class OwnsObjects {
public:
    template <typename T, typename... Args>
    T *make_owned(Args &&...args) {
        auto p = std::make_unique<T>(std::forward<Args>(args)...);
        T *raw = p.get();
        push_into_owner<T>(std::move(p));
        return raw;
    }

    void clear_owned() {
        std::apply([](auto &...v) { (..., v.clear()); }, owned_);
    }

protected:
    ~OwnsObjects() = default;

private:
    template <typename T>
    void push_into_owner(std::unique_ptr<T> p) {
        bool placed = false;
        auto try_place = [&](auto &vec) {
            using OwnedT = typename std::decay_t<decltype(vec)>::value_type::element_type;
            if constexpr (std::is_base_of_v<OwnedT, T> || std::is_same_v<OwnedT, T>) {
                if (!placed) {
                    vec.emplace_back(std::move(p));
                    placed = true;
                }
            }
        };
        std::apply([&](auto &...vs) { (try_place(vs), ...); }, owned_);
        static_assert(sizeof...(OwnedTypes) > 0, "OwnsObjects needs at least one type");
    }

    std::tuple<std::vector<std::unique_ptr<OwnedTypes>>...> owned_;
};

} // namespace turbolynx

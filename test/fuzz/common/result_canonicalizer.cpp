#include "result_canonicalizer.hpp"

#include <algorithm>
#include <sstream>
#include <type_traits>

namespace tl_fuzz {

namespace {

std::string value_canonical(const qtest::Value& v) {
    if (qtest::is_null(v)) return "<NULL>";
    return std::visit(
        [](const auto& x) -> std::string {
            using T = std::decay_t<decltype(x)>;
            if constexpr (std::is_same_v<T, qtest::Null>) {
                return "<NULL>";
            } else if constexpr (std::is_same_v<T, int64_t>) {
                return std::to_string(x);
            } else if constexpr (std::is_same_v<T, bool>) {
                return x ? "true" : "false";
            } else if constexpr (std::is_same_v<T, std::string>) {
                return "\"" + x + "\"";
            }
        },
        v);
}

std::string dump_first_rows(const std::vector<qtest::Row>& rows,
                            size_t max_rows = 5) {
    std::ostringstream oss;
    const size_t shown = std::min(max_rows, rows.size());
    for (size_t i = 0; i < shown; ++i) {
        oss << "  " << row_canonical(rows[i]) << "\n";
    }
    if (rows.size() > shown) {
        oss << "  ... +" << (rows.size() - shown) << " more\n";
    }
    return oss.str();
}

}  // namespace

bool values_equal(const qtest::Value& a, const qtest::Value& b) {
    const bool a_null = qtest::is_null(a);
    const bool b_null = qtest::is_null(b);
    if (a_null != b_null) return false;
    if (a_null) return true;
    if (a.index() != b.index()) return false;
    // qtest::Null lacks operator==, so dispatch per alternative rather
    // than relying on std::variant's defaulted operator==.
    return std::visit(
        [&](const auto& av) -> bool {
            using T = std::decay_t<decltype(av)>;
            if constexpr (std::is_same_v<T, qtest::Null>) {
                return true;  // both nulls, handled above
            } else {
                return av == std::get<T>(b);
            }
        },
        a);
}

bool rows_equal(const qtest::Row& a, const qtest::Row& b) {
    if (a.cols.size() != b.cols.size()) return false;
    for (size_t i = 0; i < a.cols.size(); ++i) {
        if (!values_equal(a.cols[i], b.cols[i])) return false;
    }
    return true;
}

std::string row_canonical(const qtest::Row& r) {
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < r.cols.size(); ++i) {
        if (i) oss << ",";
        oss << value_canonical(r.cols[i]);
    }
    oss << "]";
    return oss.str();
}

CompareResult compare(const qtest::QueryResult& lhs,
                      const qtest::QueryResult& rhs,
                      CompareMode mode) {
    CompareResult result;

    if (lhs.size() != rhs.size()) {
        std::ostringstream oss;
        oss << "row count mismatch: lhs=" << lhs.size()
            << " rhs=" << rhs.size() << "\n"
            << "lhs first rows:\n" << dump_first_rows(lhs.rows)
            << "rhs first rows:\n" << dump_first_rows(rhs.rows);
        result.equal = false;
        result.diff = oss.str();
        return result;
    }

    if (mode == CompareMode::Ordered) {
        for (size_t i = 0; i < lhs.size(); ++i) {
            if (!rows_equal(lhs.rows[i], rhs.rows[i])) {
                std::ostringstream oss;
                oss << "row " << i << " differs:\n"
                    << "  lhs: " << row_canonical(lhs.rows[i]) << "\n"
                    << "  rhs: " << row_canonical(rhs.rows[i]) << "\n";
                result.equal = false;
                result.diff = oss.str();
                return result;
            }
        }
        return result;
    }

    // Multiset: sort row canonicalizations on both sides, pairwise compare.
    std::vector<std::string> ls;
    std::vector<std::string> rs;
    ls.reserve(lhs.size());
    rs.reserve(rhs.size());
    for (const auto& row : lhs.rows) ls.push_back(row_canonical(row));
    for (const auto& row : rhs.rows) rs.push_back(row_canonical(row));
    std::sort(ls.begin(), ls.end());
    std::sort(rs.begin(), rs.end());

    for (size_t i = 0; i < ls.size(); ++i) {
        if (ls[i] != rs[i]) {
            std::ostringstream oss;
            oss << "multiset mismatch at sorted index " << i << ":\n"
                << "  lhs: " << ls[i] << "\n"
                << "  rhs: " << rs[i] << "\n";
            result.equal = false;
            result.diff = oss.str();
            return result;
        }
    }
    return result;
}

}  // namespace tl_fuzz

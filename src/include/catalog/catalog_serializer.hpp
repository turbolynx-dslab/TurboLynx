//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_serializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
// =============================================================================
// CatalogSerializer / CatalogDeserializer
// =============================================================================
// Minimal binary I/O helpers for catalog persistence.
// Write path: catalog.bin.tmp  →  Commit()  →  catalog.bin  (atomic rename)
// =============================================================================

#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

namespace duckdb {

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------
class CatalogSerializer {
public:
    explicit CatalogSerializer(const std::string &tmp_path);

    // Primitive writes
    void Write(uint8_t  v);
    void Write(uint32_t v);
    void Write(uint64_t v);

    // String: [len: uint32][bytes]
    void WriteString(const std::string &s);

    // Vector<T> where T is a fixed-size POD: [count: uint32][elements]
    template <typename T>
    void WriteVector(const std::vector<T> &v) {
        Write(static_cast<uint32_t>(v.size()));
        if (!v.empty()) {
            out_.write(reinterpret_cast<const char *>(v.data()),
                       static_cast<std::streamsize>(v.size() * sizeof(T)));
        }
    }

    void WriteStringVector(const std::vector<std::string> &v);

    // Finalize: rename tmp → final_path  (atomic on POSIX)
    void Commit(const std::string &final_path);

    bool Good() const { return out_.good(); }

private:
    std::string   tmp_path_;
    std::ofstream out_;
};

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------
class CatalogDeserializer {
public:
    explicit CatalogDeserializer(const std::string &path);

    uint8_t     ReadU8();
    uint32_t    ReadU32();
    uint64_t    ReadU64();
    std::string ReadString();

    template <typename T>
    std::vector<T> ReadVector() {
        uint32_t n = ReadU32();
        std::vector<T> v(n);
        if (n > 0) {
            in_.read(reinterpret_cast<char *>(v.data()),
                     static_cast<std::streamsize>(n * sizeof(T)));
        }
        return v;
    }

    std::vector<std::string> ReadStringVector();

    bool Good() const { return in_.good(); }
    bool Eof()  const { return in_.eof();  }

private:
    std::ifstream in_;
};

} // namespace duckdb

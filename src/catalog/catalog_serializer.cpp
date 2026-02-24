#include "catalog/catalog_serializer.hpp"

#include <cstring>
#include <filesystem>
#include <stdexcept>

namespace duckdb {

// ---------------------------------------------------------------------------
// CatalogSerializer
// ---------------------------------------------------------------------------

CatalogSerializer::CatalogSerializer(const std::string &tmp_path)
    : tmp_path_(tmp_path) {
    out_.open(tmp_path_, std::ios::binary | std::ios::trunc);
    if (!out_.is_open()) {
        throw std::runtime_error("CatalogSerializer: cannot open " + tmp_path_);
    }
}

void CatalogSerializer::Write(uint8_t v) {
    out_.write(reinterpret_cast<const char *>(&v), 1);
}

void CatalogSerializer::Write(uint32_t v) {
    out_.write(reinterpret_cast<const char *>(&v), 4);
}

void CatalogSerializer::Write(uint64_t v) {
    out_.write(reinterpret_cast<const char *>(&v), 8);
}

void CatalogSerializer::WriteString(const std::string &s) {
    Write(static_cast<uint32_t>(s.size()));
    if (!s.empty()) {
        out_.write(s.data(), static_cast<std::streamsize>(s.size()));
    }
}

void CatalogSerializer::WriteStringVector(const std::vector<std::string> &v) {
    Write(static_cast<uint32_t>(v.size()));
    for (const auto &s : v) {
        WriteString(s);
    }
}

void CatalogSerializer::Commit(const std::string &final_path) {
    out_.flush();
    out_.close();
    std::filesystem::rename(tmp_path_, final_path);
}

// ---------------------------------------------------------------------------
// CatalogDeserializer
// ---------------------------------------------------------------------------

CatalogDeserializer::CatalogDeserializer(const std::string &path) {
    in_.open(path, std::ios::binary);
    if (!in_.is_open()) {
        throw std::runtime_error("CatalogDeserializer: cannot open " + path);
    }
}

uint8_t CatalogDeserializer::ReadU8() {
    uint8_t v = 0;
    in_.read(reinterpret_cast<char *>(&v), 1);
    return v;
}

uint32_t CatalogDeserializer::ReadU32() {
    uint32_t v = 0;
    in_.read(reinterpret_cast<char *>(&v), 4);
    return v;
}

uint64_t CatalogDeserializer::ReadU64() {
    uint64_t v = 0;
    in_.read(reinterpret_cast<char *>(&v), 8);
    return v;
}

std::string CatalogDeserializer::ReadString() {
    uint32_t len = ReadU32();
    if (len == 0) return {};
    std::string s(len, '\0');
    in_.read(s.data(), static_cast<std::streamsize>(len));
    return s;
}

std::vector<std::string> CatalogDeserializer::ReadStringVector() {
    uint32_t n = ReadU32();
    std::vector<std::string> v;
    v.reserve(n);
    for (uint32_t i = 0; i < n; i++) {
        v.push_back(ReadString());
    }
    return v;
}

} // namespace duckdb

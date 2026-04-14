#pragma once

#include <cstddef>
#include <bitset>
#include "common/vector_size.hpp"

namespace duckdb {

enum CompressionFunctionType : uint8_t {
    UNCOMPRESSED = 0,
    BITPACKING = 1,
    RLE = 2,
    DICTIONARY = 3
};

enum SwizzlingType : uint8_t {
    SWIZZLE_NONE = 0,
    SWIZZLE_VARCHAR = 1
};

struct CompressionHeader {
    CompressionHeader() {}
    // CompressionHeader(CompressionFunctionType comp_type_, size_t data_len_) : comp_type(comp_type_), data_len(data_len_), has_null(false) {
    // }
    CompressionHeader(CompressionFunctionType comp_type_, size_t data_len_) : comp_type(comp_type_), data_len(data_len_), has_null_bitmap(0) {
    }
    // CompressionHeader(CompressionFunctionType comp_type_, size_t data_len_, SwizzlingType swizzle_type_) : comp_type(comp_type_), data_len(data_len_), swizzle_type(swizzle_type_), has_null(false) {
    // }
    CompressionHeader(CompressionFunctionType comp_type_, size_t data_len_, SwizzlingType swizzle_type_) : comp_type(comp_type_), data_len(data_len_), swizzle_type(swizzle_type_), has_null_bitmap(0) {
    }

    static size_t GetSizeWoBitSet() {
        // Must match the on-disk layout: the data_len field is always 8 bytes
        // (uint64_t), even on 32-bit platforms like WASM.
        return sizeof(CompressionFunctionType) + sizeof(SwizzlingType) + 2 * sizeof(uint8_t)
            + sizeof(uint32_t) + sizeof(uint64_t);
    }
    void SetSwizzlingType(SwizzlingType swizzle_type_) {
        swizzle_type = swizzle_type_;
    }
    void SetCompFuncType(CompressionFunctionType comp_type_) {
        comp_type = comp_type_;
    }
    void SetNullMask() {
        has_null_bitmap = 1;
    }
    uint8_t HasNullMask() {
        return has_null_bitmap;
    }
    void SetNullBitmapOffset(uint32_t null_bitmap_offset_) {
        null_bitmap_offset = null_bitmap_offset_;
    }
    uint32_t GetNullBitmapOffset() {
        return null_bitmap_offset;
    }

    // 4byte
    CompressionFunctionType comp_type;
    SwizzlingType swizzle_type;
    uint8_t has_null_bitmap;
    uint8_t padding;

    // 4byte
    uint32_t null_bitmap_offset;

    // 8byte — always uint64_t for cross-platform on-disk compatibility
    uint64_t data_len;
};



}
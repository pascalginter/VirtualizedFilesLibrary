#pragma once
#include <cstdint>
#include <optional>

// -------------------------------------------------------------------------------------
namespace virtualfile {
// -------------------------------------------------------------------------------------
struct DictionaryChunkInfo {
    uint64_t unique_values_count;
    uint64_t unique_values_length;
};
// -------------------------------------------------------------------------------------
template <typename T>
struct ZoneMap {
    T min_value;
    T max_value;
};
// -------------------------------------------------------------------------------------
template <typename T>
struct ChunkInfo {
    // Necessary statistics
    uint64_t uncompressed_size;
    uint64_t tuple_count;

    // Optional statistics for better performance
    std::optional<DictionaryChunkInfo> dictionary_chunk_info;
    std::optional<ZoneMap<T>> zone_map;
};
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
} // namespace virtualfile
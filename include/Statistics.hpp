#pragma once
#include <array>
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
struct ZoneMap {
    std::array<std::byte, 8> min_value;
    std::array<std::byte, 8> max_value;
};
// -------------------------------------------------------------------------------------
struct ChunkInfo {
    // Necessary statistics
    uint64_t uncompressed_size;
    uint64_t tuple_count;

    // Optional statistics for better performance
    std::optional<DictionaryChunkInfo> dictionary_chunk_info = std::nullopt;
    std::optional<ZoneMap> zone_map = std::nullopt;
};
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
} // namespace virtualfile
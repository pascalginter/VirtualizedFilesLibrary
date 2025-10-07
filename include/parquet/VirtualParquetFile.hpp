#pragma once
// -------------------------------------------------------------------------------------
#include "Statistics.hpp"
#include "../Statistics.hpp"
// -------------------------------------------------------------------------------------
namespace virtualfile {
// -------------------------------------------------------------------------------------
enum PageType { DATA_PAGE, DICTIONARY_PAGE };
// -------------------------------------------------------------------------------------
class VirtualFile {
    static uint8_t GetVarintSize(uint64_t val) {
        uint8_t res = 0;
        do {
            val >>= 7;
            res++;
        } while (val != 0);
        return res;
    }

    static uint64_t predictPageHeaderSize(ChunkInfo<T> chunk_info) {
        4
    }
public:
    template <typename T>
    static uint64_t predictDataPageSize(ChunkInfo<T> chunk_info) {
        uint64_t data_size = chunk_info.uncompressed_size;
        uint64_t
    }
};
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
#pragma once
// -------------------------------------------------------------------------------------
#include <assert.h>
#include <memory>
#include <vector>
// -------------------------------------------------------------------------------------
#include <arrow/api.h>
// -------------------------------------------------------------------------------------
#include "ArrowReader.hpp"
#include "Statistics.hpp"
// -------------------------------------------------------------------------------------
namespace virtualfile {
// -------------------------------------------------------------------------------------
struct ByteRange {
    int64_t begin;
    int64_t end;

    uint64_t size() const { return end - begin + 1; }
};

class VirtualFile {
protected:
    std::shared_ptr<ArrowReader> reader;
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::vector<ChunkInfo>> chunkInfos;
    uint64_t size = 0; // initialized in child classes
    const size_t numColumns;
    const size_t numRowgroups;

    virtual uint64_t predictMetadataOverhead() = 0;
    virtual ChunkInfo predictChunkInfo(const ChunkInfo& info) = 0;
    virtual void registerPrecomputedSize(size_t rowgroup, size_t column, ChunkInfo predictedInfo) = 0;

    uint64_t initSize() {
        uint64_t result = 0;
        for (size_t i=0; i!=numRowgroups; i++) {
            for (size_t j=0; j!=numColumns; j++) {
                const ChunkInfo predictedChunkInfo = predictChunkInfo(chunkInfos[j][i]);
                registerPrecomputedSize(i, j, predictedChunkInfo);
                result += predictedChunkInfo.uncompressed_size;
            }
        }
        result += predictMetadataOverhead();
        return result;
    }

public:
    explicit VirtualFile(
            const std::shared_ptr<ArrowReader>& reader,
            const std::shared_ptr<arrow::Schema>& schema,
            std::vector<std::vector<ChunkInfo>>&& chunkInfos
        ) :
            reader(reader), schema(schema), chunkInfos(chunkInfos),
            numColumns(chunkInfos.size()), numRowgroups(numColumns > 0 ? chunkInfos[0].size() : 0) {
        for (size_t i=0; i!=numColumns; i++) {
            assert(chunkInfos[i].size() == numRowgroups);
        }
    }

    virtual ~VirtualFile() = default;

    virtual uint64_t predictSizeOfFile(){ return size; }
    virtual std::string getRange(ByteRange range) = 0;
};
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
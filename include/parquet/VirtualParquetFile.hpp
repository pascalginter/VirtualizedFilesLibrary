#pragma once
// -------------------------------------------------------------------------------------
#include <parquet/metadata.h>
#include <parquet/arrow/schema.h>
// -------------------------------------------------------------------------------------

#include "../VirtualFile.hpp"

// -------------------------------------------------------------------------------------
namespace virtualfile {
// -------------------------------------------------------------------------------------
enum PageType { DATA_PAGE, DICTIONARY_PAGE };
// -------------------------------------------------------------------------------------
class VirtualParquetFile final : public VirtualFile {
    static constexpr uint64_t MAGIC_NUMBER_SIZE = 4;
    static constexpr uint64_t FOOTER_LENGTH_SIZE = 4;

    std::unique_ptr<parquet::FileMetaDataBuilder> metadataBuilder;
    parquet::RowGroupMetaDataBuilder* rowgroupBuilder;
    size_t fileOffset = MAGIC_NUMBER_SIZE;
    std::string serializedMetadata;

    uint64_t predictMetadataOverhead() override {
        return 2 * MAGIC_NUMBER_SIZE + serializedMetadata.size() + FOOTER_LENGTH_SIZE;
    }

    ChunkInfo predictChunkInfo(const ChunkInfo &info) override {
        ChunkInfo predicted;
        predicted.tuple_count = info.tuple_count;
        if (info.dictionary_chunk_info) {
            // todo
            predicted.dictionary_chunk_info = info.dictionary_chunk_info;
            assert(false);
        }else{
            predicted.uncompressed_size = info.uncompressed_size;
        }
        return predicted;
    }

    void registerPrecomputedSize(const size_t rowgroup, const size_t column, const ChunkInfo predictedInfo) override {
        if (column == 0) {
            rowgroupBuilder = metadataBuilder->AppendRowGroup();
            rowgroupBuilder->set_num_rows(predictedInfo.tuple_count);
        }

        parquet::ColumnChunkMetaDataBuilder* chunkBuilder = rowgroupBuilder->NextColumnChunk();
        // TODO fix zone maps
        // TODO allow dictionary encoded data
        chunkBuilder->Finish(predictedInfo.tuple_count,
            -1, -1, fileOffset,
            predictedInfo.uncompressed_size, predictedInfo.uncompressed_size,
            false, false, {}, {});

        if (rowgroup == numRowgroups - 1 && column == numColumns - 1) {
            auto metadata = metadataBuilder->Finish();
        }
    }
public:
    explicit VirtualParquetFile(std::vector<std::vector<ChunkInfo>>&& chunkInfos) :
            VirtualFile(std::move(chunkInfos)) {
        std::shared_ptr<parquet::SchemaDescriptor> schemaDescriptor;
        const auto writerProps = parquet::WriterProperties::Builder().build();
        PARQUET_THROW_NOT_OK(parquet::arrow::ToParquetSchema(schema.get(), *writerProps,
            *parquet::default_arrow_writer_properties(), &schemaDescriptor));
        metadataBuilder = parquet::FileMetaDataBuilder::Make(schemaDescriptor.get(), writerProps);
    }

    ~VirtualParquetFile() override = default;

    std::shared_ptr<std::string> getRange(uint64_t begin, uint64_t end) override {

    }
};
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
#pragma once
// -------------------------------------------------------------------------------------
#include <parquet/metadata.h>
#include <parquet/arrow/schema.h>
// -------------------------------------------------------------------------------------
#include "../VirtualFile.hpp"
#include "ColumnChunkWriter.hpp"
#include "ParquetUtils.hpp"

// -------------------------------------------------------------------------------------
namespace virtualfile {
// -------------------------------------------------------------------------------------
enum PageType { DATA_PAGE_TYPE, DICTIONARY_PAGE_TYPE };
// -------------------------------------------------------------------------------------
class VirtualParquetFile final : public VirtualFile {
    static constexpr uint64_t MAGIC_NUMBER_SIZE = 4;
    static constexpr uint64_t FOOTER_LENGTH_SIZE = 4;

    std::shared_ptr<parquet::SchemaDescriptor> schemaDescriptor;
    std::unique_ptr<parquet::FileMetaData> metadata;
    std::unique_ptr<parquet::FileMetaDataBuilder> metadataBuilder;
    parquet::RowGroupMetaDataBuilder* rowgroupBuilder = nullptr;
    uint64_t fileOffset = MAGIC_NUMBER_SIZE;
    uint64_t metadataOffset;
    uint64_t rowGroupSize;
    std::string serializedMetadata;
    std::string buffer;
    std::vector<char> currBuffer;

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
            predicted.uncompressed_size = getSize(info);
        }
        return predicted;
    }

    void registerPrecomputedSize(const size_t rowgroup, const size_t column, const ChunkInfo predictedInfo) override {
        if (column == 0) {
            rowgroupBuilder = metadataBuilder->AppendRowGroup();
            rowgroupBuilder->set_num_rows(predictedInfo.tuple_count);
            rowGroupSize = 0;
        }

        parquet::ColumnChunkMetaDataBuilder* chunkBuilder = rowgroupBuilder->NextColumnChunk();
        // TODO fix zone maps
        // TODO allow dictionary encoded data
        chunkBuilder->Finish(predictedInfo.tuple_count,
            -1, -1, fileOffset,
            predictedInfo.uncompressed_size, predictedInfo.uncompressed_size,
            false, false, {}, {});


        fileOffset += predictedInfo.uncompressed_size;
        if (column == numColumns - 1) {
            rowgroupBuilder->Finish(predictedInfo.uncompressed_size);
        }
        if (rowgroup == numRowgroups - 1 && column == numColumns - 1) {
            metadata = metadataBuilder->Finish();
        }
    }
    // TODO deduplicate logic
    [[nodiscard]] uint64_t getDataSize(const int rowgroup, const int column) const {
        const auto& info = chunkInfos[column][rowgroup];
        uint64_t result = info.uncompressed_size;
        if (metadata->schema()->Column(column)->logical_type() == parquet::LogicalType::String()) {
            result -= 4;
        }
        return result + 5 + ParquetUtils::GetVarintSize(info.tuple_count << 1);
    }

    [[nodiscard]] static uint64_t getDictEncodedDataSize(uint64_t num_values, uint64_t num_unique_values) {
        const uint8_t byteLength = (std::bit_width(num_unique_values) + 7) / 8;
        return num_values * byteLength + 6 + ParquetUtils::GetVarintSize(num_values << 1) + ParquetUtils::GetVarintSize(((num_values + 7 )/ 8) << 1 | 1);
    }

    [[nodiscard]] static uint64_t getDataSize(const std::shared_ptr<arrow::Array>& arr, bool isDictionary) {
        uint64_t result;
        if (isDictionary || arr->type() == arrow::utf8()) {
            int64_t tuple_count = arr->data()->length;
            const auto* offsets = reinterpret_cast<const int32_t*>(arr->data()->buffers[1]->data());
            result = tuple_count * 4 + offsets[tuple_count];
        }else {
            result = arr->length() * arr->type()->byte_width();
        }
        if (isDictionary) return result;
        return result + 5 + ParquetUtils::GetVarintSize(arr->length() << 1);
    }

    uint64_t getSize(const ChunkInfo& info) const {
        uint64_t result = info.uncompressed_size;
        // TODO if ( == btrblocks::ColumnType::STRING) result -= 4;
        return result + ParquetUtils::writePageWithoutData(info.uncompressed_size, info.tuple_count).size();
    }

    [[nodiscard]] static uint64_t getDictionarySize(uint64_t num_unique_values, uint64_t total_length) {
        const uint64_t dataSize = total_length + num_unique_values * sizeof(int32_t);
        return dataSize + ParquetUtils::writePageWithoutData(dataSize, num_unique_values, true).size();
    }

    void writeChunk(
            int64_t chunkBegin, const int64_t chunkEnd, const ByteRange byteRange,
            const std::shared_ptr<arrow::Array>& arr, size_t &offset,
            bool isDictionaryPage, size_t uncompressed_size, uint64_t unique_values = 0) {
        if (!(chunkEnd < byteRange.begin || chunkBegin > byteRange.end)) {
            const int64_t begin = std::max(chunkBegin, byteRange.begin);
            const int64_t end = std::min(chunkEnd, byteRange.end);
            // should not be needed, but warns if a reader does something unexpected
            assert(begin == chunkBegin);
            assert(end == chunkEnd);

            const bool fast_path = begin == chunkBegin && end == chunkEnd;
            char* vec;
            if (fast_path) [[likely]] {
                vec = buffer.data() + offset;
            }else {
                currBuffer.resize(uncompressed_size);
                vec = currBuffer.data();
            }

            if (arrow::is_dictionary(arr->type_id())) {
                const uint8_t byteLength = (std::bit_width(unique_values) + 7) / 8;
                ColumnChunkWriter::writeDictionaryEncodedChunk(
                    ParquetUtils::writePageWithoutData(getDictEncodedDataSize(arr->length(), unique_values),
                    arr->length(), false, true, byteLength),
                    std::static_pointer_cast<arrow::DictionaryArray>(arr)->indices(), vec, byteLength, 0);
            } else {
                ColumnChunkWriter::writeColumnChunk(
                    ParquetUtils::writePageWithoutData(getDataSize(arr, isDictionaryPage),
                                                                     arr->length(), isDictionaryPage, false),
                    arr, vec);
            }


            if (!fast_path) {
                memcpy(buffer.data() + offset, currBuffer.data() + begin - chunkBegin, end - begin + 1);
            }
            offset += end - begin + 1;
        }
    }
public:
    explicit VirtualParquetFile(
        const std::shared_ptr<ArrowReader>& reader,
        const std::shared_ptr<arrow::Schema>& schema,
        std::vector<std::vector<ChunkInfo>>&& chunkInfos) :
            VirtualFile(reader, schema, std::move(chunkInfos)) {
        const auto writerProps = parquet::WriterProperties::Builder().build();
        PARQUET_THROW_NOT_OK(parquet::arrow::ToParquetSchema(schema.get(), *writerProps,
            *parquet::default_arrow_writer_properties(), &schemaDescriptor));
        metadataBuilder = parquet::FileMetaDataBuilder::Make(schemaDescriptor.get(), writerProps);
        size = initSize();
        metadataOffset = size - MAGIC_NUMBER_SIZE - FOOTER_LENGTH_SIZE;
    }

    ~VirtualParquetFile() override = default;

    std::string getRange(const ByteRange range) override {
        assert(range.end <= size);
        buffer.resize(range.size());
        size_t offset = 0;
        if (range.begin == 0) {
            buffer[0] = 'P';
            buffer[1] = 'A';
            buffer[2] = 'R';
            buffer[3] = '1';
            offset = 4;
        }


        for (uint64_t i=0; i<numRowgroups; i++) {
            for (uint64_t j=0; j!=numColumns; j++) {
                std::shared_ptr<arrow::Array> arr = reader->readChunk(i, j);
                auto columnChunkMeta = metadata->RowGroup(i)->ColumnChunk(j);
                int64_t chunkBegin = columnChunkMeta->has_dictionary_page() ?
                    columnChunkMeta->dictionary_page_offset() : columnChunkMeta->data_page_offset();

                if (columnChunkMeta->has_dictionary_page()
                    && !(columnChunkMeta->dictionary_page_offset() > range.end
                         || columnChunkMeta->data_page_offset() < range.begin)) {
                    arrow::ArrayVector arrays;
                    arrow::StringBuilder builder;

                    arrays.push_back(arr);
                    if (arrow::is_dictionary(arr->type_id())) {
                        auto dictArray = std::static_pointer_cast<arrow::DictionaryArray>(arr)->dictionary();
                        auto status = builder.AppendArraySlice(*dictArray->data(), 0, dictArray->length());
                    }


                    std::shared_ptr<arrow::Array> dictionaryArr;
                    auto status = builder.Finish(&dictionaryArr);
                    const int64_t dictionaryPageSize = columnChunkMeta->data_page_offset() - columnChunkMeta->dictionary_page_offset();
                    writeChunk(chunkBegin, chunkBegin + dictionaryPageSize - 1, range, dictionaryArr, offset, true, dictionaryPageSize);
                    chunkBegin += dictionaryPageSize;

                    int64_t chunkSize = chunkInfos[j][i].uncompressed_size;
                    int64_t chunkEnd = chunkBegin + chunkInfos[j][i].uncompressed_size - 1;
                    writeChunk(chunkBegin, chunkEnd, range, arrays[0], offset, false, chunkInfos[j][i].uncompressed_size, dictionaryArr->length());
                    // Prepare next chunk
                    chunkBegin += chunkSize;

                }else {
                    const int64_t chunkSize = chunkInfos[j][i].uncompressed_size;
                    const int64_t chunkEnd = chunkBegin + chunkSize - 1;

                    if (!(chunkEnd < range.begin || chunkBegin > range.end)) {
                        writeChunk(chunkBegin, chunkEnd, range, reader->readChunk(i, j), offset, false, chunkInfos[j][i].uncompressed_size);
                    }
                    // Prepare next chunk
                    chunkBegin += chunkSize;
                }
            }
        }

        if (range.end >= metadataOffset && range.size() != 8) {
            assert(offset + serializedMetadata.size() <= buffer.size());
            memcpy(buffer.data() + offset, serializedMetadata.data(), serializedMetadata.size());
            offset += serializedMetadata.size();
        }
        // TODO allow partial footer requests
        if (range.end == size - 1) {
            const int32_t s = serializedMetadata.size();
            std::string footer = "xxxxPAR1";
            memcpy(footer.data(), &s, 4);
            memcpy(buffer.data() + offset, footer.data(), footer.size());
            offset += footer.size();
        }
        return buffer;
    }
};
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
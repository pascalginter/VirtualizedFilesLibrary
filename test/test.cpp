// -------------------------------------------------------------------------------
#include <fcntl.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/json/api.h>
// -------------------------------------------------------------------------------
#include "../include/VirtualFile.hpp"
#include "../include/parquet/VirtualParquetFile.hpp"
// -------------------------------------------------------------------------------

std::shared_ptr<arrow::Table> readTableFromFile(const std::string& path, const std::shared_ptr<arrow::Schema>& schema) {
    auto input_res = arrow::io::ReadableFile::Open(path);
    if (!input_res.ok()) {
        std::cerr << "ReadableFile::Open failed: " << input_res.status().ToString() << "\n";
        return nullptr;
    }
    const auto input = *input_res;

    arrow::MemoryPool* pool = arrow::default_memory_pool();
    const auto read_options  = arrow::json::ReadOptions::Defaults();
    auto parse_options = arrow::json::ParseOptions::Defaults();
    parse_options.explicit_schema = schema;

    auto reader_res = arrow::json::TableReader::Make(pool, input, read_options, parse_options);
    if (!reader_res.ok()) {
        std::cerr << "TableReader::Make failed: " << reader_res.status().ToString() << "\n";
        return nullptr;
    }

    auto table_res = (*reader_res)->Read();
    if (!table_res.ok()) {
        std::cerr << "Read failed: " << table_res.status().ToString() << "\n";
        return nullptr;
    }
    return *table_res;
}


TEST(InMemoryTest, TestBasic) {
    const auto table = readTableFromFile("../test/data/simple.json", arrow::schema({
      arrow::field("a", arrow::int32()),
      arrow::field("b", arrow::int32())
    }));
    std::vector<std::vector<virtualfile::ChunkInfo>> infos{{
        {
            .uncompressed_size = 8,
            .tuple_count = 2
        }
    },
    {{
        .uncompressed_size = 8,
        .tuple_count = 2
    }}};
    std::shared_ptr<virtualfile::ArrowReader> reader =
        std::make_shared<virtualfile::InMemoryArrowReader>(table);
    const auto parquetFile = std::make_shared<virtualfile::VirtualParquetFile>(reader, table->schema(), std::move(infos));
    ASSERT_EQ(parquetFile->predictSizeOfFile(), 78);
    ASSERT_EQ(parquetFile->getRange({0, 3}), "PAR1");
    const std::string columnChunkA = parquetFile->getRange({4, 36});
    ASSERT_EQ(columnChunkA[0], 0x15);
    ASSERT_EQ(*reinterpret_cast<const int32_t*>(columnChunkA.data() + 25), 1);
    ASSERT_EQ(*reinterpret_cast<const int32_t*>(columnChunkA.data() + 29), 2);
}
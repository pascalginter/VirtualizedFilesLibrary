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

std::shared_ptr<arrow::Table> readTableFromFile(const std::string& path) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    const int fd = open(path.c_str(), O_RDONLY);
    const std::shared_ptr<arrow::io::InputStream> input = arrow::io::ReadableFile::Open(fd).ValueOrDie();

    const auto read_options = arrow::json::ReadOptions::Defaults();
    const auto parse_options = arrow::json::ParseOptions::Defaults();

    // Instantiate TableReader from input stream and options
    auto maybe_reader =
        arrow::json::TableReader::Make(pool, input, read_options, parse_options);
    if (!maybe_reader.ok()) {
        return nullptr;
    }
    const auto& reader = *maybe_reader;

    // Read table from JSON file
    auto maybe_table = reader->Read();
    if (!maybe_table.ok()) {
        // Handle JSON read error
        // (for example a JSON syntax error or failed type conversion)
    }
    return *maybe_table;
}

TEST(InMemoryTest, TestBasic) {
    const auto table = readTableFromFile("../../test/data/simple.json");
    std::cout << table->ToString() << std::endl;
}
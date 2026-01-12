#pragma once
// -------------------------------------------------------------------------------------
#include <arrow/api.h>
// -------------------------------------------------------------------------------------
namespace virtualfile {
// -------------------------------------------------------------------------------------
class ArrowReader {
protected:
    std::shared_ptr<arrow::Schema> schema;
public:
    explicit ArrowReader(const std::shared_ptr<arrow::Schema>& schema) : schema(schema){}
    virtual ~ArrowReader() = default;
    virtual std::shared_ptr<arrow::Array> readChunk(uint64_t rowgroup, uint64_t column) = 0;
};
// -------------------------------------------------------------------------------------
// debugging utility
class InMemoryArrowReader final : public ArrowReader {
    std::shared_ptr<arrow::Table> table;
public:
    explicit InMemoryArrowReader(const std::shared_ptr<arrow::Table>& table) :
            ArrowReader(table->schema()), table(table) {}

    std::shared_ptr<arrow::Array> readChunk(
            const uint64_t rowgroup, const uint64_t column) override {
        return table->column(column)->chunk(rowgroup);
    }
};
// -------------------------------------------------------------------------------------
} // namespace virtualfile
// -------------------------------------------------------------------------------------

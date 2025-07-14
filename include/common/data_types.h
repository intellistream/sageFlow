#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

namespace candy {
enum DataType {  // NOLINT
  None,
  Int8,
  Int16,
  Int32,
  Int64,
  Float32,
  Float64,
};

constexpr int DATA_TYPE_SIZE[7] = {0, 1, 2, 4, 8, 4, 8};  // Size of each data type in bytes

struct VectorData {
  int32_t dim_;                   // Dimension of the vector
  DataType type_;                 // Data type of the vector
  std::unique_ptr<char[]> data_;  // Pointer to the vector data

  // Constructor to initialize the vector data
  VectorData(int32_t dim, DataType type, char *data);

  explicit VectorData(int32_t dim, DataType type);

  VectorData(const VectorData &other);  // Copy constructor

  auto operator==(const VectorData &other) const -> bool;
  // Equality operator

  auto operator!=(const VectorData &other) const -> bool;
  // Inequality operator

  auto Serialize(std::ostream &out) const -> bool;
  auto Deserialize(std::istream &in) -> bool;
};

// Wrapper for vector data with metadata (e.g., ID, timestamp)

struct VectorRecord {
  uint64_t uid_;             // Unique identifier for the vector
  int64_t timestamp_;        // Timestamp for the record
  VectorData data_;          // Shared pointer to the vector data

  // Constructor with move semantics for efficiency
  VectorRecord(uint64_t uid, int64_t timestamp, VectorData &&data);

  // Constructor with copy semantics
  VectorRecord(uint64_t uid, int64_t timestamp, const VectorData &data);

  // Constructor with a raw data pointer
  VectorRecord(uint64_t uid, int64_t timestamp, int32_t dim, DataType type, char *data);

  // Equality operator for comparisons
  auto operator==(const VectorRecord &other) const -> bool;

  auto Serialize(std::ostream &out) const -> bool;
  auto Deserialize(std::istream &in) -> bool;
};
enum class ResponseType { None, Record, List };  // NOLINT

struct Response {
  ResponseType type_;
  std::unique_ptr<VectorRecord> record_;
  std::unique_ptr<std::vector<std::unique_ptr<VectorRecord>>> records_;

  Response() : type_(ResponseType::None), record_(nullptr) {}

  Response(const ResponseType type, std::unique_ptr<VectorRecord> record) : type_(type), record_(std::move(record)) {}

  Response(const ResponseType type, std::unique_ptr<std::vector<std::unique_ptr<VectorRecord>>> records)
      : type_(type), records_(std::move(records)) {}

  Response(const Response &other) {
    type_ = other.type_;
    if (other.record_) {
      record_ = std::make_unique<VectorRecord>(*other.record_);
    }
    if (other.records_) {
      records_ = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
      records_->reserve(other.records_->size());
      for (const auto &rec : *other.records_) {
        records_->emplace_back(std::make_unique<VectorRecord>(*rec));
      }
    }
  }

  auto operator=(const Response &other) -> Response & {
    if (this != &other) {
      type_ = other.type_;
      if (other.record_) {
        record_ = std::make_unique<VectorRecord>(*other.record_);
      }
      if (other.records_) {
        records_ = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
        records_->reserve(other.records_->size());
        for (const auto &rec : *other.records_) {
          records_->emplace_back(std::make_unique<VectorRecord>(*rec));
        }
      }
    }
    return *this;
  }
};

}  // namespace candy

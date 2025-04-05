#pragma once

#include <cstdint>
#include <cstring>
#include <memory>

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
  VectorData(int32_t dim, DataType type, char *data) : dim_(dim), type_(type), data_(data) {}

  explicit VectorData(int32_t dim, DataType type)
      : dim_(dim), type_(type), data_(new char[dim * DATA_TYPE_SIZE[type]]) {}

  VectorData(const VectorData &other) : dim_(other.dim_), type_(other.type_) {
    data_ = std::make_unique<char[]>(dim_ * DATA_TYPE_SIZE[type_]);        // Allocate memory for data
    memcpy(data_.get(), other.data_.get(), dim_ * DATA_TYPE_SIZE[type_]);  // Copy data
  }  // Copy constructor

  auto operator==(const VectorData &other) const -> bool {
    if (dim_ != other.dim_) {
      return false;  // Check dimension equality
    }
    return memcmp(data_.get(), other.data_.get(), dim_ * DATA_TYPE_SIZE[type_]) == 0;  // Compare data
  }  // Equality operator

  auto operator!=(const VectorData &other) const -> bool { return !(*this == other); }  // Inequality operator
};

// Wrapper for vector data with metadata (e.g., ID, timestamp)

struct VectorRecord {
  const uint64_t uid_;       // Unique identifier for the vector
  const int64_t timestamp_;  // Timestamp for the record
  VectorData data_;          // Shared pointer to the vector data

  // Constructor with move semantics for efficiency
  VectorRecord(const uint64_t &uid, const int64_t &timestamp, VectorData &&data)
      : uid_(uid), timestamp_(timestamp), data_(data) {}

  // Constructor with copy semantics
  VectorRecord(const uint64_t &uid, const int64_t &timestamp, const VectorData &data)
      : uid_(uid), timestamp_(timestamp), data_(data) {}

  // Constructor with raw data pointer
  VectorRecord(const uint64_t &uid, const int64_t &timestamp, int32_t dim, DataType type, char *data)
      : uid_(uid), timestamp_(timestamp), data_(dim, type, data) {}

  // Equality operator for comparisons
  auto operator==(const VectorRecord &other) const -> bool {
    return uid_ == other.uid_ && timestamp_ == other.timestamp_ && data_ == other.data_;
  }
};

}  // namespace candy

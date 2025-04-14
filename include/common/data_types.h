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
  VectorData(int32_t dim, DataType type, char *data);

  explicit VectorData(int32_t dim, DataType type);

  VectorData(const VectorData &other);  // Copy constructor

  auto operator==(const VectorData &other) const -> bool;
  // Equality operator

  auto operator!=(const VectorData &other) const -> bool;
  // Inequality operator
};

// Wrapper for vector data with metadata (e.g., ID, timestamp)

struct VectorRecord {
  const uint64_t uid_;       // Unique identifier for the vector
  const int64_t timestamp_;  // Timestamp for the record
  VectorData data_;          // Shared pointer to the vector data

  // Constructor with move semantics for efficiency
  VectorRecord(const uint64_t &uid, const int64_t &timestamp, VectorData &&data);

  // Constructor with copy semantics
  VectorRecord(const uint64_t &uid, const int64_t &timestamp, const VectorData &data);

  // Constructor with raw data pointer
  VectorRecord(const uint64_t &uid, const int64_t &timestamp, int32_t dim, DataType type, char *data);

  // Equality operator for comparisons
  auto operator==(const VectorRecord &other) const -> bool;
};

}  // namespace candy

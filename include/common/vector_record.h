#pragma once

#include <cstdint>
#include <ostream>
#include <istream>

#include "common/base_types.h"
#include "common/vector_data.h"

namespace candy {

/**
 * @brief Represents a complete vector record with metadata and data
 * 
 * VectorRecord combines a unique identifier, timestamp, and vector data
 * into a single entity for processing throughout the system.
 */
class VectorRecord {
 public:
  // Constructors with move semantics
  VectorRecord(const uint64_t &uid, const timestamp_t &timestamp, VectorData &&data)
    : uid_(uid), timestamp_(timestamp), data_(std::move(data)) {}

  // Constructor with copy semantics
  VectorRecord(const uint64_t &uid, const timestamp_t &timestamp, const VectorData &data)
    : uid_(uid), timestamp_(timestamp), data_(data) {}

  // Constructor with raw data pointer
  VectorRecord(const uint64_t &uid, const timestamp_t &timestamp, int32_t dim, DataType type, char *data)
    : uid_(uid), timestamp_(timestamp), data_(dim, type, data) {}

  // Equality operator for comparisons
  auto operator==(const VectorRecord &other) const -> bool {
    return uid_ == other.uid_ && timestamp_ == other.timestamp_ && data_ == other.data_;
  }

  // Serialization
  auto Serialize(std::ostream &out) const -> bool;
  auto Deserialize(std::istream &in) -> bool;

  // Data members (const for immutability)
  const uint64_t uid_;           // Unique identifier for the vector
  const timestamp_t timestamp_;  // Timestamp for the record
  VectorData data_;              // The actual vector data
};

}  // namespace candy
#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <variant>

namespace candy {

// Common data type for vector data
using VectorData = std::vector<float>;
using Watermark = int64_t;

// Wrapper for vector data with metadata (e.g., ID, timestamp)
struct VectorRecord {
  const std::string id_;              // Unique identifier for the vector
  std::shared_ptr<VectorData> data_;  // The vector data itself
  const int64_t timestamp_;           // Timestamp for the record

  // Constructor with move semantics for efficiency
  VectorRecord(std::string id, VectorData data, int64_t timestamp)
      : id_(std::move(id)), data_(std::make_shared<VectorData>(std::move(data))), timestamp_(timestamp) {}

  // Default constructor (optional)
  VectorRecord() : data_(std::make_shared<VectorData>()), timestamp_(0) {}

  // Equality operator for comparisons
  auto operator==(const VectorRecord &other) const -> bool {
    return id_ == other.id_ && timestamp_ == other.timestamp_ && *data_ == *other.data_;
  }
};

// Hash function for unordered containers (optional)
struct VectorRecordHash {
  auto operator()(const VectorRecord &record) const -> std::size_t {
    return std::hash<std::string>()(record.id_) ^ std::hash<int64_t>()(record.timestamp_);
  }
};

using RecordOrWatermark = std::variant<std::unique_ptr<VectorRecord>, Watermark>;

}  // namespace candy

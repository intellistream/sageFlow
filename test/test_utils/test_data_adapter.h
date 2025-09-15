// Test adapter utilities for VectorRecord creation and vector extraction,
// plus lightweight side tracking for baseline checks.
#pragma once

#include "common/data_types.h"
#include <vector>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <cstring>
#include <iostream>

namespace candy { namespace test {

enum class Side { LEFT, RIGHT, BOTH };

inline std::unique_ptr<VectorRecord> createVectorRecord(
    uint64_t uid, int64_t timestamp, const std::vector<float>& data) {
  int32_t dim = static_cast<int32_t>(data.size());
  auto raw_data = std::make_unique<char[]>(dim * sizeof(float));
  std::memcpy(raw_data.get(), data.data(), dim * sizeof(float));
  VectorData vector_data(dim, DataType::Float32, raw_data.release());
  return std::make_unique<VectorRecord>(uid, timestamp, std::move(vector_data));
}

inline std::vector<float> extractFloatVector(const VectorRecord& record) {
  const auto& vector_data = record.data_;
  int32_t dim = vector_data.dim_;
  if (dim <= 0 || dim > 1000000) {
    std::cerr << "[DIAG] extractFloatVector abnormal dim: " << dim
              << ", uid=" << record.uid_ << ", ts=" << record.timestamp_ << std::endl;
  }
  const float* float_ptr = reinterpret_cast<const float*>(vector_data.data_.get());
  std::vector<float> result(static_cast<size_t>(dim));
  std::memcpy(result.data(), float_ptr, static_cast<size_t>(dim) * sizeof(float));
  return result;
}

class TestRecordSideManager {
public:
  static TestRecordSideManager& instance() { static TestRecordSideManager inst; return inst; }
  void setSide(uint64_t uid, Side side) { std::lock_guard<std::mutex> lock(mutex_); uid_to_side_[uid] = side; }
  Side getSide(uint64_t uid) const { std::lock_guard<std::mutex> lock(mutex_); auto it = uid_to_side_.find(uid); return it != uid_to_side_.end() ? it->second : Side::LEFT; }
  void clear() { std::lock_guard<std::mutex> lock(mutex_); uid_to_side_.clear(); }
private:
  mutable std::mutex mutex_;
  std::unordered_map<uint64_t, Side> uid_to_side_;
};

}} // namespace

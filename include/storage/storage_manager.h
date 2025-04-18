#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/data_types.h"
#include "compute_engine/compute_engine.h"

namespace candy {
using idx_t = int32_t;

class StorageManager {
  std::unique_ptr<ComputeEngine> engine_ = nullptr;

 public:
  // data
  std::unordered_map<uint64_t, int32_t> map_;
  std::vector<std::unique_ptr<VectorRecord>> records_;
  // Constructor
  StorageManager() = default;

  // Destructor
  ~StorageManager() = default;

  auto insert(std::unique_ptr<VectorRecord> &record) -> idx_t;

  auto erase(uint64_t vector_id) -> bool;

  auto getVectorByUid(uint64_t vector_id) -> std::unique_ptr<VectorRecord>;

  auto getVectorsByUids(const std::vector<uint64_t> &vector_ids) -> std::vector<std::unique_ptr<VectorRecord>>;

  auto getVectorById(int32_t id) -> std::unique_ptr<VectorRecord>;

  auto getVectorsByIds(const std::vector<int32_t> &ids) -> std::vector<std::unique_ptr<VectorRecord>>;
  auto topk(const std::unique_ptr<VectorRecord> &record, int k) const -> std::vector<int32_t>;

 private:
};
}  // namespace candy
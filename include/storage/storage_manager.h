#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <shared_mutex>
#include <mutex>

#include "common/data_types.h"
#include "compute_engine/compute_engine.h"

namespace candy {
using idx_t = int32_t;

class StorageManager {
 public:
  // 写 private HNSW 访问不了
  std::shared_ptr<ComputeEngine> engine_ = nullptr;
  // data
  std::unordered_map<uint64_t, int32_t> map_;
  std::vector<std::shared_ptr<VectorRecord>> records_;
  // Constructor
  StorageManager() = default;

  // Destructor
  ~StorageManager() = default;

  auto insert(std::unique_ptr<VectorRecord> record) -> void;

  auto insert(std::shared_ptr<VectorRecord> record) -> void;

  auto erase(uint64_t vector_id) -> bool;

  auto getVectorByUid(uint64_t vector_id) -> std::shared_ptr<const VectorRecord>;

  auto getVectorsByUids(const std::vector<uint64_t> &vector_ids) -> std::vector<std::shared_ptr<const VectorRecord>>;

  auto topk(const VectorRecord &record, int k) const -> std::vector<uint64_t>;

 private:
  mutable std::shared_mutex map_mutex_;
  int begin_ = 0;
};
}  // namespace candy
#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/data_types.h"
#include "compute_engine/compute_engine.h"

namespace candy {
using idx_t = int32_t;

class StorageManager {
 public:
  explicit StorageManager(std::shared_ptr<ComputeEngine> engine);
  // Destructor
  ~StorageManager() = default;

  auto size() const -> size_t;
  auto getRecordByIndex(int32_t index) -> VectorRecord *;

  auto getEngine() -> std::shared_ptr<ComputeEngine>;
  void setEngine(std::shared_ptr<ComputeEngine> engine) { engine_ = std::move(engine); }

  auto insert(std::unique_ptr<VectorRecord> &record) -> void;

  auto erase(uint64_t vector_id) -> bool;

  auto getVectorByUid(uint64_t vector_id) -> std::unique_ptr<VectorRecord>;

  auto getVectorsByUids(const std::vector<uint64_t> &vector_ids) -> std::vector<std::unique_ptr<VectorRecord>>;

  auto topk(const std::unique_ptr<VectorRecord> &record, int k) const -> std::vector<uint64_t>;

 private:
  std::shared_ptr<ComputeEngine> engine_ = nullptr;
  std::unordered_map<uint64_t, int32_t> map_;
  std::vector<std::unique_ptr<VectorRecord>> records_;
};
}  // namespace candy
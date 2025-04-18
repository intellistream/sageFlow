#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "compute_engine/compute_engine.h"
#include "storage/storage_manager.h"

namespace candy {
enum class IndexType {  // NOLINT
  None,
  HNSW,
  BruteForce,
};

class Index {
 public:
  // data
  int index_id_ = 0;
  int dimension_ = 0;

  IndexType index_type_;
  std::shared_ptr<StorageManager> storage_manager_ = nullptr;
  std::shared_ptr<ComputeEngine> compute_engine_ = nullptr;
  // Constructor
  Index() = default;
  // Destructor
  virtual ~Index() = default;

  virtual auto insert(uint64_t id) -> bool;
  virtual auto erase(uint64_t id) -> bool;
  virtual auto query(std::unique_ptr<VectorRecord> &record, int k) -> std::vector<int32_t>;
};

class GlobalIndex : public Index {
 public:
  auto save(const std::string &path) -> bool;
  auto load(const std::string &path) -> bool;
  auto remove() -> bool;
};
}  // namespace candy
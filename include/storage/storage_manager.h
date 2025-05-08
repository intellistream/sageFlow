#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/data_types.h"
#include "compute_engine/compute_engine.h"
#include "storage/storage_types.h"

namespace candy {

/**
 * @brief Manages vector storage and retrieval operations
 */
class StorageManager {
 public:
  // Constructor
  StorageManager() = default;

  // Destructor
  ~StorageManager() = default;

  auto insert(std::unique_ptr<VectorRecord> &record) -> void;

  auto erase(uint64_t vector_id) -> bool;

  auto getVectorByUid(uint64_t vector_id) -> std::unique_ptr<VectorRecord>;

  auto getVectorsByUids(const std::vector<uint64_t> &vector_ids) -> std::vector<std::unique_ptr<VectorRecord>>;

  auto topK(const std::unique_ptr<VectorRecord> &record, int k) const -> std::vector<uint64_t>;

  // Accessor methods for index implementations
  auto isEmpty() const -> bool { return records_.empty(); }
  auto size() const -> size_t { return records_.size(); }
  auto getRecord(size_t index) const -> const VectorRecord* { 
    return index < records_.size() ? records_[index].get() : nullptr;
  }

  // Public members
  std::shared_ptr<ComputeEngine> engine_ = nullptr;
  
 private:
  // Internal storage structures
  std::unordered_map<uint64_t, int32_t> map_;  // Maps vector IDs to positions in the records vector
  std::vector<std::unique_ptr<VectorRecord>> records_;  // Storage for vector records

  // Friend classes that need direct access to private members
  friend class Ivf;
};

}  // namespace candy
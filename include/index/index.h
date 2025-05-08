#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/vector_record.h"
#include "compute_engine/compute_engine.h"
#include "index/index_types.h"
#include "storage/storage_manager.h"

namespace candy {

/**
 * @brief Base class for vector index implementations
 * 
 * The Index class defines the interface for all vector index implementations,
 * providing common functionality for insertion, deletion, and querying.
 */
class Index {
 public:
  // Default constructor
  Index() = default;
  
  // Virtual destructor for proper cleanup in derived classes
  virtual ~Index() = default;

  // Core index operations
  virtual auto insert(uint64_t id) -> bool = 0;
  virtual auto erase(uint64_t id) -> bool = 0;
  virtual auto query(std::unique_ptr<VectorRecord> &record, int k) -> std::vector<uint64_t> = 0;

  // Common data for all index types
  int index_id_ = 0;
  int dimension_ = 0;
  IndexType index_type_ = IndexType::None;
  
  // Changed from shared_ptr to reference
  // Note: Using a pointer to a reference to allow default initialization
  // This will need to be properly set before use
  StorageManager* storage_manager_ = nullptr;
};

}  // namespace candy
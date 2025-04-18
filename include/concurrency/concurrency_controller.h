#pragma once

#include <memory>
#include <vector>

#include "common/data_types.h"
#include "storage/storage_manager.h"

namespace candy {
class ConcurrencyController {
 public:
  // Constructor
  ConcurrencyController() = default;

  // Destructor
  virtual ~ConcurrencyController() = default;
  virtual auto insert( std::unique_ptr<VectorRecord> &record) -> bool;

  virtual auto erase( std::unique_ptr<VectorRecord> &record) -> bool;  // maybe local index would use this

  virtual auto query( std::unique_ptr<VectorRecord> &record,
                     int k) -> std::vector<std::unique_ptr<VectorRecord>>;

  std::shared_ptr<StorageManager> storage_manager_ = nullptr;
};
};  // namespace candy
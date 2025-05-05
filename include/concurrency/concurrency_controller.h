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
  virtual auto insert(std::unique_ptr<VectorRecord> &record) -> bool = 0;

  virtual auto erase(std::unique_ptr<VectorRecord> &record) -> bool = 0;  // maybe local index would use this

  virtual auto erase(uint64_t uid) -> bool = 0;  // maybe local index would use this

  virtual auto query(std::unique_ptr<VectorRecord> &record, int k) -> std::vector<std::unique_ptr<VectorRecord>> = 0;

  std::shared_ptr<StorageManager> storage_manager_ = nullptr;
};
};  // namespace candy
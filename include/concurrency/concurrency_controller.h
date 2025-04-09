#pragma once

#include <memory>
#include <vector>

#include "common/data_types.h"

namespace candy {
class ConcurrencyController {
 public:
  // Constructor
  ConcurrencyController() = default;

  // Destructor
  ~ConcurrencyController() = default;
  virtual auto insert(int index_id, std::unique_ptr<VectorRecord> &record) -> bool;

  virtual auto erase(int index_id, std::unique_ptr<VectorRecord> &record) -> bool;  // maybe local index would use this

  virtual auto query(int index_id, std::unique_ptr<VectorRecord> &record,
                     int k) -> std::vector<std::unique_ptr<VectorRecord>>;
};
};  // namespace candy
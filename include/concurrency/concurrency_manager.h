#pragma once
#include <memory>
#include <string>
#include <unordered_map>

#include "concurrency/concurrency_controller.h"
#include "index/index.h"

namespace candy {
struct IdWithType {
  int id_;
  IndexType index_type_;
};

class ConcurrencyManager {
 public:
  // Constructor
  ConcurrencyManager() = default;

  // Destructor
  ~ConcurrencyManager() = default;

  auto create_index(const std::string &name, const IndexType &index_type, int dimension) -> int;

  auto drop_index(const std::string &name) -> bool;

  auto insert(int index_id, std::unique_ptr<VectorRecord> &record) -> bool;

  auto erase(int index_id, std::unique_ptr<VectorRecord> &record) -> bool;  // maybe local index would use this

  auto query(int index_id, std::unique_ptr<VectorRecord> &record, int k) -> std::vector<std::unique_ptr<VectorRecord>>;

 private:
  std::unordered_map<std::string, IdWithType> index_map_;
  // controller contains index, each operation will be passed to the controller
  std::unordered_map<int, std::unique_ptr<ConcurrencyController>> controller_map_;  // controller for each index
  // controller contains storage engine, each operation will be passed to the controller
  std::unique_ptr<ConcurrencyController> storage_controller_ = nullptr;             // controller for storage engine
};

};  // namespace candy

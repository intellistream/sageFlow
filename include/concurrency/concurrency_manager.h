#pragma once
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

#include "concurrency/concurrency_controller.h"
#include "concurrency/id_with_type.h"
#include "index/index.h"

namespace candy {

/**
 * @brief Manages concurrent access to indices and storage
 * 
 * The ConcurrencyManager coordinates access to various indices and handles
 * the creation, deletion, and operations on different index types.
 */
class ConcurrencyManager {
  StorageManager& storage_;  // Changed from shared_ptr to reference

 public:
  // Constructor now accepts a reference
  explicit ConcurrencyManager(StorageManager& storage);

  // Destructor
  ~ConcurrencyManager();

  auto create_index(const std::string &name, const IndexType &index_type, int dimension) -> int;
  auto create_index(const std::string &name, int dimension) -> int;

  auto drop_index(const std::string &name) -> bool;

  auto insert(int index_id, std::unique_ptr<VectorRecord> &record) -> bool;

  auto erase(int index_id, std::unique_ptr<VectorRecord> &record) -> bool;  // maybe local index would use this

  auto query(int index_id, std::unique_ptr<VectorRecord> &record, int k) -> std::vector<std::unique_ptr<VectorRecord>>;

 private:
  std::unordered_map<std::string, IdWithType> index_map_;
  // the controller contains index, each operation will be passed to the controller
  std::unordered_map<int, std::shared_ptr<ConcurrencyController>> controller_map_;  // the controller for each index
  // controller contains storage engine, each operation will be passed to the controller
  std::shared_ptr<ConcurrencyController> storage_controller_ = nullptr;  // controller for storage engine

  std::atomic<int> index_id_counter_ = 0;  // atomic counter for index id
};

}  // namespace candy

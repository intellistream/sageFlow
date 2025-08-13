#pragma once
#include <atomic>
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
  std::shared_ptr<StorageManager> storage_;

 public:
  // Constructor
  explicit ConcurrencyManager(std::shared_ptr<StorageManager> storage);

  // Destructor
  ~ConcurrencyManager();

  auto create_index(const std::string &name, const IndexType &index_type, int dimension) -> int;
  auto create_index(const std::string &name, int dimension) -> int;

  auto drop_index(const std::string &name) -> bool;

  auto insert(int index_id, std::unique_ptr<VectorRecord> record) -> bool;

  auto erase(int index_id, std::unique_ptr<VectorRecord> record) -> bool;  // maybe local index would use this

  auto erase(int index_id, uint64_t uid) -> bool;  // maybe local index would use this

  auto query(int index_id, const VectorRecord& record, int k) -> std::vector<std::shared_ptr<const VectorRecord>>;

  // Method for join-specific queries, returning shared_ptr records
  auto query_for_join(int index_id, const VectorRecord& record,
                      double join_similarity_threshold) -> std::vector<std::shared_ptr<const VectorRecord>>;

 private:
  std::unordered_map<std::string, IdWithType> index_map_;
  // the controller contains index, each operation will be passed to the controller
  std::unordered_map<int, std::shared_ptr<ConcurrencyController>> controller_map_;  // the controller for each index
  // controller contains storage engine, each operation will be passed to the controller
  std::shared_ptr<ConcurrencyController> storage_controller_ = nullptr;  // controller for storage engine

  std::atomic<int> index_id_counter_ = 0;  // atomic counter for index id
};

};  // namespace candy

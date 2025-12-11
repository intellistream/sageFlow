#pragma once
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

#include "concurrency/concurrency_controller.h"
#include "index/index.h"
#include "index/diskann_index.h"

namespace sageFlow {
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

  /**
   * @brief 注册外部创建的索引
   * 
   * 用于注册在 ConcurrencyManager 外部创建的索引（如 PartitionedIndex）。
   * 该方法会自动配置索引的 storage_manager_，确保索引可以访问全局存储。
   * 
   * @param name 索引名称
   * @param index 外部创建的索引
   * @return 索引 ID，失败返回 -1
   */
  auto register_index(const std::string &name, std::shared_ptr<Index> index) -> int;

  auto create_index(const std::string &name, const IndexType &index_type, int dimension) -> int;
  auto create_index(const std::string &name, const IndexType &index_type, int dimension,
                    const IndexParameters& params) -> int;
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

};  // namespace sageFlow

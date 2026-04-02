#pragma once
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <vector>

#include "common/data_types.h"
#include "concurrency/concurrency_controller.h"
#include "index/index.h"

namespace sageFlow {

/// Policy for selecting the ConcurrencyController type when creating an index.
enum class ControllerPolicy {
  SHARED_LOCK,  ///< BlankController with shared_mutex (default, for shared indexes)
  DIRECT        ///< DirectController without locking (for partition-local indexes)
};

}  // namespace sageFlow

namespace sageFlow {

// Forward declaration
class PartitionedIndex;
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

  auto create_index(const std::string &name, const IndexType &index_type, int dimension,
                    ControllerPolicy policy = ControllerPolicy::SHARED_LOCK) -> int;
  auto create_index(const std::string &name, const IndexType &index_type, int dimension,
                    const IndexParameters& params,
                    ControllerPolicy policy = ControllerPolicy::SHARED_LOCK) -> int;
  auto create_index(const std::string &name, int dimension) -> int;

  auto drop_index(const std::string &name) -> bool;

  auto insert(int index_id, std::unique_ptr<VectorRecord> record) -> bool;

  auto erase(int index_id, std::unique_ptr<VectorRecord> record) -> bool;  // maybe local index would use this

  auto erase(int index_id, uint64_t uid) -> bool;  // maybe local index would use this

  auto query(int index_id, const VectorRecord& record, int k) -> std::vector<std::shared_ptr<const VectorRecord>>;

  // Method for join-specific queries, returning shared_ptr records
  auto query_for_join(int index_id, const VectorRecord& record,
                      double join_similarity_threshold,
                      double similarity_alpha) -> std::vector<std::shared_ptr<const VectorRecord>>;

  // 说明：ComputeEngine 不保存 alpha（纯计算）。alpha 由上层算子/策略绑定并显式传入 query_for_join。

  // ========== 新增：分区索引访问接口 ==========
  
  /**
   * @brief 检查索引是否为分区索引
   * @param index_id 索引 ID
   * @return true 表示是 PartitionedIndex
   */
  auto isPartitionedIndex(int index_id) const -> bool;
  
  /**
   * @brief 获取分区索引的分区数量
   * @param index_id 索引 ID
   * @return 分区数量，非分区索引返回 0
   */
  auto getPartitionCount(int index_id) const -> size_t;

  // ========== 新增：批量构建 + 原子替换（无阻塞查询） ==========

  /**
   * @brief 从批量记录构建新索引并返回新 index_id
   *
   * 说明：
   * - 会创建与 create_index 相同类型/参数的 Index，并为每条 record 写入 storage_，再将 uid 插入索引。
   * - 构建过程不影响已有索引的查询/写入。
   */
  auto build_index_from_records(const std::string& name,
                                const IndexType& index_type,
                                int dimension,
                                const IndexParameters& params,
                                const std::vector<const VectorRecord*>& records) -> int;

  /**
   * @brief 将 old_index_id 对应的 controller 原子替换为 new_index_id 的 controller
   *
   * 语义：
   * - 对外仍使用 old_index_id 进行 query/insert；替换后这些操作会落到 new_index_id 对应的 Index。
   * - 替换过程对 query 无阻塞：正在进行的 query 会继续持有旧 controller 的 shared_ptr 并完成。
   * - new_index_id 条目会从 controller_map_ 中移除，避免两个 id 指向同一 controller。
   */
  auto replace_index_by_id(int old_index_id, int new_index_id) -> bool;
  
 private:
  mutable std::shared_mutex controller_map_mutex_;

  std::unordered_map<std::string, IdWithType> index_map_;
  // the controller contains index, each operation will be passed to the controller
  std::unordered_map<int, std::shared_ptr<ConcurrencyController>> controller_map_;  // the controller for each index
  // controller contains storage engine, each operation will be passed to the controller
  std::shared_ptr<ConcurrencyController> storage_controller_ = nullptr;  // controller for storage engine

  std::atomic<int> index_id_counter_ = 0;  // atomic counter for index id
  
  /**
   * @brief 获取 PartitionedIndex 指针（内部辅助方法）
   * @param index_id 索引 ID
   * @return PartitionedIndex 指针，如果不是分区索引返回 nullptr
   */
  auto getPartitionedIndex(int index_id) -> std::shared_ptr<PartitionedIndex>;
  auto getPartitionedIndex(int index_id) const -> std::shared_ptr<const PartitionedIndex>;
};

};  // namespace sageFlow

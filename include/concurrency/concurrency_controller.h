#pragma once

#include <memory>
#include <vector>

#include "common/data_types.h"
#include "index/index.h"
#include "storage/storage_manager.h"

namespace sageFlow {
class ConcurrencyController {
 public:
  // Constructor
  ConcurrencyController() = default;

  // Destructor
  virtual ~ConcurrencyController() = default;
  virtual auto insert(std::unique_ptr<VectorRecord> record) -> bool = 0;

  virtual auto erase(std::unique_ptr<VectorRecord> record) -> bool = 0;  // maybe local index would use this

  virtual auto erase(uint64_t uid) -> bool = 0;  // maybe local index would use this

  virtual auto query(const VectorRecord& record, int k) -> std::vector<std::shared_ptr<const VectorRecord>> = 0;

  // New method for join-specific queries, returning shared_ptr records
  virtual auto query_for_join(const VectorRecord& record,
                              double join_similarity_threshold,
                              double similarity_alpha) -> std::vector<std::shared_ptr<const VectorRecord>> = 0;

  /**
   * @brief 获取底层索引（用于分区索引访问 / 原子替换）
   * @return Index 共享指针，如果不支持返回 nullptr
   */
  virtual auto getIndex() const -> std::shared_ptr<Index> { return nullptr; }

  /**
   * @brief 原子替换底层索引（默认不支持）
   *
   * 语义：替换后新的 query/insert/erase 将作用于 new_index。
   * 需要保证 query 无阻塞：正在执行的 query 若已持有旧索引的 shared_ptr，应可继续完成。
   */
  virtual auto replaceIndex(std::shared_ptr<Index> new_index) -> bool { return false; }

  /**
   * @brief 开启/关闭双写：写操作同时写入当前索引与 shadow 索引
   */
  virtual auto enableDoubleWrite(bool enable, std::shared_ptr<Index> shadow = nullptr) -> void {
    (void)enable;
    (void)shadow;
  }

  std::shared_ptr<StorageManager> storage_manager_ = nullptr;
};
};  // namespace sageFlow

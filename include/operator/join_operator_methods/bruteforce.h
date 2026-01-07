#pragma once
#include <deque>
#include <memory>
#include <vector>
#include "operator/join_operator_methods/base_method.h"
#include "operator/utils/join_strategy_config.h"
#include "function/join_function.h"
#include "concurrency/concurrency_manager.h"

namespace sageFlow {
class BruteForceJoinMethod final : public BaseMethod {
 public:
  /**
   * @brief 构造函数（兼容旧接口）
   * 
   * 注意：BruteForceJoinMethod 走索引层 query_for_join()。
   * 相似度计算由 Index 内部通过 ComputeEngine 完成，alpha 由 ConcurrencyManager 在运行时配置。
   */
  BruteForceJoinMethod(int left_index_id,
                       int right_index_id,
                       double join_similarity_threshold,
                       const std::shared_ptr<ConcurrencyManager>& concurrency_manager)
      : BaseMethod(join_similarity_threshold),
        left_index_id_(left_index_id),
        right_index_id_(right_index_id),
        concurrency_manager_(concurrency_manager) {}

  ~BruteForceJoinMethod() override = default;

  // 统一接口：所有方法均使用 Eager 模式
  std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
      const VectorRecord& query_record, int query_slot,
      size_t subtask_index = 0) override;

  // alpha/similarity_mode 不再在 method 内部保存，避免与 ComputeEngine 的运行时配置产生“双源”。

 private:
  int otherIndexId(int slot) const { return (slot == 0) ? right_index_id_ : left_index_id_; }

  int left_index_id_ = -1;
  int right_index_id_ = -1;
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};
} // namespace sageFlow

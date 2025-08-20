#pragma once
#include <list>
#include <memory>
#include <vector>
#include "operator/join_operator_methods/base_method.h"
#include "function/join_function.h"
#include "concurrency/concurrency_manager.h"

namespace candy {
class BruteForceJoinMethod final : public BaseMethod {
 public:
  BruteForceJoinMethod(int left_index_id,
             int right_index_id,
             double join_similarity_threshold,
             const std::shared_ptr<ConcurrencyManager>& concurrency_manager)
      : BaseMethod(join_similarity_threshold),
        left_index_id_(left_index_id),
        right_index_id_(right_index_id),
        concurrency_manager_(concurrency_manager) {}

  ~BruteForceJoinMethod() override = default;

  // 统一接口
  std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(const VectorRecord& query_record, int query_slot) override;
  std::vector<std::unique_ptr<VectorRecord>> ExecuteLazy(const std::list<std::unique_ptr<VectorRecord>>& query_records, int query_slot) override;

 private:
  int otherIndexId(int slot) const { return (slot == 0) ? right_index_id_ : left_index_id_; }

  int left_index_id_ = -1;
  int right_index_id_ = -1;
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};
} // namespace candy

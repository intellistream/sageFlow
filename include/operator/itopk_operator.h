#pragma once
#include <memory>
#include <unordered_set>

#include "common/data_types.h"
#include "concurrency/concurrency_manager.h"
#include "operator/operator.h"

namespace candy {
class ITopkOperator final : public Operator {
 public:
  explicit ITopkOperator(std::unique_ptr<Function> &func,
                         const std::shared_ptr<ConcurrencyManager> &concurrency_manager);

  auto process(Response &data, int slot) -> bool override;

 private:

  auto getRecord() const -> std::unique_ptr<VectorRecord>;
  std::unique_ptr<Function> itopk_func_;
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  std::unique_ptr<VectorRecord> record_;
  int index_id_ = 0;
  int k_ = 0;
  std::unordered_set<uint64_t> uids_;
};
}  // namespace candy
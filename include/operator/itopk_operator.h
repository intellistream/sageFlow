#pragma once
#include <functional>
#include <memory>

#include "common/data_types.h"
#include "concurrency/concurrency_manager.h"
#include "operator/operator.h"

namespace candy {
class ITopkOperator final : public Operator {
 public:
  explicit ITopkOperator(std::unique_ptr<Function> &itopk_func,
                         const std::shared_ptr<ConcurrencyManager> &concurrency_manager);

  auto process(Response &data, int slot) -> bool override;

 private:
  std::unique_ptr<Function> itopk_func_;
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};
}  // namespace candy
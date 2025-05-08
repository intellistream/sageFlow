#pragma once
#include <iostream>
#include <memory>

#include "common/data_types.h"
#include "concurrency/concurrency_manager.h"
#include "function/function.h"
#include "operator/operator.h"

namespace candy {
class TopkOperator final : public Operator {
 public:
  explicit TopkOperator(std::unique_ptr<Function>&& topk_func,
                        ConcurrencyManager& concurrency_manager);

  auto process(DataElement& element, int slot) -> bool override;

 private:
  std::unique_ptr<Function> topk_func_;
  ConcurrencyManager& concurrency_manager_;
};
}  // namespace candy
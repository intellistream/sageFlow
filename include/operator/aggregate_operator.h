#pragma once
#include <iostream>
#include <memory>

#include "common/data_types.h"
#include "concurrency/concurrency_manager.h"
#include "function/function.h"
#include "operator/operator.h"

namespace candy {
class AggregateOperator final : public Operator {
 public:
  explicit AggregateOperator(std::unique_ptr<Function> &aggregate_func);

  auto process(Response &data, int slot) -> bool override;

 private:
  std::unique_ptr<Function> aggregate_func_;
};
}  // namespace candy
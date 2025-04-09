#pragma once
#include <iostream>
#include <memory>

#include "common/data_types.h"
#include "function/function.h"
#include "operator/operator.h"

namespace candy {
class MapOperator final : public Operator {
 public:
  explicit MapOperator(std::unique_ptr<Function> &map_func);

  auto process(std::unique_ptr<VectorRecord> &data, int slot) -> bool override;

 private:
  std::unique_ptr<Function> map_func_;
};
}  // namespace candy
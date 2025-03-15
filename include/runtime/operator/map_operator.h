#pragma once
#include <iostream>
#include <memory>

#include "core/common/data_types.h"
#include "runtime/function/function.h"
#include "runtime/operator/operator.h"

namespace candy {
class MapOperator final : public Operator {
 public:
  explicit MapOperator(std::unique_ptr<Function> &map_func);

  auto process(std::unique_ptr<VectorRecord> &data, int slot) -> bool override;

 private:
  std::unique_ptr<Function> map_func_;
};
}  // namespace candy
#pragma once
#include <functional>
#include <memory>

#include "core/common/data_types.h"
#include "runtime/operator/operator.h"

namespace candy {
class FilterOperator final : public Operator {
 public:
  explicit FilterOperator(std::unique_ptr<Function> &filter_func);

  auto process(std::unique_ptr<VectorRecord> &data, int slot) -> bool override;

 private:
  std::unique_ptr<Function> filter_func_;
};
}  // namespace candy
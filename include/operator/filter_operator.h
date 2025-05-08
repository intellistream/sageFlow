#pragma once
#include <functional>
#include <memory>

#include "common/data_types.h"
#include "operator/operator.h"

namespace candy {
class FilterOperator final : public Operator {
 public:
  explicit FilterOperator(std::unique_ptr<Function>&& filter_func);

  // 重写父类的 processDataElement 方法来实现过滤功能
  auto processDataElement(DataElement& element, int slot) -> bool override;

 private:
  std::unique_ptr<Function> filter_func_;
};
}  // namespace candy
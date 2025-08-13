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

  auto process(Response&data, int slot) -> std::optional<Response> override;

  auto apply(Response&& record, int slot, Collector& collector) -> void override;

 private:
  std::unique_ptr<Function> map_func_;
};
}  // namespace candy
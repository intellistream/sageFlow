#pragma once
#include <functional>
#include <memory>

#include "common/data_types.h"
#include "operator/operator.h"

namespace candy {
class FilterOperator final : public Operator {
 public:
  explicit FilterOperator(std::unique_ptr<Function> &filter_func);

  auto process(Response &record, int slot) -> std::optional<Response> override;

  auto apply(Response&& record, int slot, Collector& collector) -> void override;

 private:
  std::unique_ptr<Function> filter_func_;
};
}  // namespace candy
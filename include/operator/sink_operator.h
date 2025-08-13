#pragma once
#include <functional>
#include <memory>
#include <utility>

#include "common/data_types.h"
#include "function/function.h"
#include "operator/operator.h"

namespace candy {
class SinkOperator final : public Operator {
 public:
  explicit SinkOperator(std::unique_ptr<Function> &sink_func);

  auto process(Response &data, int slot) -> std::optional<Response> override;

  auto apply(Response&& record, int slot, Collector& collector) -> void override;

 private:
  std::unique_ptr<Function> sink_func_;
};
}  // namespace candy
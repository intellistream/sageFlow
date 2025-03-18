#pragma once
#include <functional>
#include <memory>
#include <utility>

#include "core/common/data_types.h"
#include "runtime/function/function.h"
#include "runtime/operator/operator.h"

namespace candy {
class SinkOperator final : public Operator {
 public:
  explicit SinkOperator(std::unique_ptr<Function> &sink_func);

  auto process(std::unique_ptr<VectorRecord> &data, int slot) -> bool override;

 private:
  std::unique_ptr<Function> sink_func_;
};
}  // namespace candy
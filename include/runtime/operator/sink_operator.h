#pragma once
#include <functional>
#include <memory>
#include <utility>

#include "core/common/data_types.h"
#include "runtime/function/function.h"
#include "runtime/operator/base_operator.h"

namespace candy {
class SinkOperator : public Operator {
 public:
  SinkOperator(const std::string &name, std::unique_ptr<Function> &sink_func)
      : Operator(OperatorType::FILTER, name), sink_func_(std::move(sink_func)) {}

  auto process(std::unique_ptr<VectorRecord> &data) -> bool override {
    data = sink_func_->Execute(data);
    emit(0, data);
    return true;
  }

 private:
  std::unique_ptr<Function> sink_func_;
};
}  // namespace candy
#pragma once
#include <functional>
#include <memory>
#include <utility>

#include "core/common/data_types.h"
#include "runtime/operator/base_operator.h"

namespace candy {
class SinkOperator : public Operator {
 public:
  SinkOperator(const std::string &name, std::function<void(std::unique_ptr<VectorRecord> &)> &sink_func)
      : Operator(OperatorType::FILTER, name), sink_func_(std::move(sink_func)) {}

  auto process(std::unique_ptr<VectorRecord> &data) -> bool override {
    sink_func_(data);
    return true;
  }

 private:
  std::function<void(std::unique_ptr<VectorRecord> &)> sink_func_;
};
}  // namespace candy
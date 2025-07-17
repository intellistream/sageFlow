#include "operator/sink_operator.h"

candy::SinkOperator::SinkOperator(std::unique_ptr<Function>& sink_func)
    : Operator(OperatorType::FILTER), sink_func_(std::move(sink_func)) {}

auto candy::SinkOperator::process(Response& data, int slot) -> bool {
  data = sink_func_->Execute(data);
  emit(0, data);
  return true;
}
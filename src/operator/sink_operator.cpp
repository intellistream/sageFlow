#include "operator/sink_operator.h"

candy::SinkOperator::SinkOperator(std::unique_ptr<Function>& sink_func)
    : Operator(OperatorType::FILTER), sink_func_(std::move(sink_func)) {}

bool candy::SinkOperator::process(std::unique_ptr<VectorRecord>& data, int slot) {
  data = sink_func_->Execute(data);
  emit(0, data);
  return true;
}
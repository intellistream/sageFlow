#include "operator/sink_operator.h"

candy::SinkOperator::SinkOperator(std::unique_ptr<Function>&& sink_func)
    : Operator(OperatorType::FILTER), sink_func_(std::move(sink_func)) {}

bool candy::SinkOperator::process(DataElement& element, int slot) {
  element = sink_func_->Execute(element);
  emit(0, element);
  return true;
}
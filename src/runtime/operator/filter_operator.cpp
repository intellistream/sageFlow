#include "runtime/operator/filter_operator.h"

candy::FilterOperator::FilterOperator(std::unique_ptr<Function>& filter_func)
    : Operator(OperatorType::FILTER), filter_func_(std::move(filter_func)) {}

bool candy::FilterOperator::process(std::unique_ptr<VectorRecord>& data, int slot) {
  if (auto result = filter_func_->Execute(data)) {
    emit(0, result);
    return true;
  }
  return false;
}
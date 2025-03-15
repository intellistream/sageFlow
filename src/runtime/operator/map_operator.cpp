#include "runtime/operator/map_operator.h"

candy::MapOperator::MapOperator(std::unique_ptr<Function>& map_func)
    : Operator(OperatorType::FILTER), map_func_(std::move(map_func)) {}

bool candy::MapOperator::process(std::unique_ptr<VectorRecord>& data, int slot) {
  auto result = map_func_->Execute(data);
  emit(0, result);
  return true;
}
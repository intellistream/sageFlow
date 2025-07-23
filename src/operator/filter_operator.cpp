#include "operator/filter_operator.h"

candy::FilterOperator::FilterOperator(std::unique_ptr<Function>& filter_func)
    : Operator(OperatorType::FILTER), filter_func_(std::move(filter_func)) {}

auto candy::FilterOperator::process(Response& data, int slot) -> bool {
  auto resp = filter_func_->Execute(data);
  if (resp.type_ != ResponseType::None) {
    emit(0, resp);
    return true;
  }
  return false;
}
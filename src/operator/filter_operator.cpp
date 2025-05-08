#include "operator/filter_operator.h"

candy::FilterOperator::FilterOperator(std::unique_ptr<Function>&& filter_func)
    : Operator(OperatorType::FILTER), filter_func_(std::move(filter_func)) {}

bool candy::FilterOperator::processDataElement(DataElement& element, int slot) {
  // 使用函数对象执行过滤操作
  auto result = filter_func_->Execute(element);
  
  // 如果过滤结果不为空，则向下游发送
  if (!result.isEmpty()) {
    // 将元素作为通用 StreamElement 发送
    emit(0, result);
    return true;
  }
  
  // 过滤掉的元素不会传递到下游
  return false;
}
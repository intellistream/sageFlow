#include "operator/map_operator.h"

candy::MapOperator::MapOperator(std::unique_ptr<Function>&& map_func)
    : Operator(OperatorType::MAP), map_func_(std::move(map_func)) {}

bool candy::MapOperator::processDataElement(DataElement& element, int slot) {
  // 使用映射函数转换数据元素
  auto result = map_func_->Execute(element);
  
  // 将转换结果发送到下游算子
  emit(0, result);
  return true;
}
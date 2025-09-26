#include "operator/map_operator.h"

candy::MapOperator::MapOperator(std::unique_ptr<Function>& map_func)
    : Operator(OperatorType::MAP), map_func_(std::move(map_func)) {}

auto candy::MapOperator::process(Response&data, int slot) -> std::optional<Response> {
  auto result = map_func_->Execute(data);
  return result;
}

auto candy::MapOperator::apply(Response&& record, int slot, Collector& collector) -> void {
  // 使用map函数转换数据
  auto result = map_func_->Execute(record);
  // 将转换后的数据发送给下游
  collector.collect(std::make_unique<Response>(std::move(result)), slot);
}

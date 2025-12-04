#include "operator/map_operator.h"

sageFlow::MapOperator::MapOperator(std::unique_ptr<Function>& map_func)
    : Operator(OperatorType::MAP), map_func_(std::move(map_func)) {}

auto sageFlow::MapOperator::process(Response&data, int slot) -> std::optional<Response> {
  auto result = map_func_->Execute(data);
  return result;
}

auto sageFlow::MapOperator::apply(Response&& record, int slot, Collector& collector) -> void {
  // 使用map函数转换数据
  auto result = map_func_->Execute(record);
  // 将转换后的数据发送给下游
  collector.collect(std::make_unique<Response>(std::move(result)), slot);
}

auto sageFlow::MapOperator::apply(Response&& record, int slot, Collector& collector, 
                                  const RuntimeContext& context) -> void {
  // MapOperator 不需要 RuntimeContext 信息，直接委托给旧方法
  apply(std::move(record), slot, collector);
}

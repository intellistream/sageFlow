#include "operator/filter_operator.h"

candy::FilterOperator::FilterOperator(std::unique_ptr<Function>& filter_func)
    : Operator(OperatorType::FILTER), filter_func_(std::move(filter_func)) {}

auto candy::FilterOperator::process(Response& data, int slot) -> std::optional<Response> {
  auto resp = filter_func_->Execute(data);
  if (resp.type_ != ResponseType::None) {
    return resp;
  }
  return std::nullopt;
}

auto candy::FilterOperator::apply(Response&& record, int slot, Collector& collector) -> void {
  // 使用filter函数处理数据
  auto resp = filter_func_->Execute(record);
  if (resp.type_ != ResponseType::None) {
    // 只有通过过滤的数据才发送给下游
    collector.collect(std::make_unique<Response>(std::move(resp)), slot);
  }
  // 不通过过滤的数据直接丢弃，不向下游发送
}

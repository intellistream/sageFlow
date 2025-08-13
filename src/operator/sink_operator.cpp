#include "operator/sink_operator.h"

candy::SinkOperator::SinkOperator(std::unique_ptr<Function>& sink_func)
    : Operator(OperatorType::SINK), sink_func_(std::move(sink_func)) {}

auto candy::SinkOperator::process(Response&data, int slot) -> std::optional<Response> {
  auto result = sink_func_->Execute(data);
  return result;
}

auto candy::SinkOperator::apply(Response&& record, int slot, Collector& collector) -> void {
  // Sink算子通常是管道的终点，执行sink函数但不向下游发送数据
  auto result = sink_func_->Execute(record);
  // SinkOperator通常不需要向下游发送数据，因为它是数据流的终点
  // 如果sink函数有返回值需要传递，可以取消下面注释
  // if (result.type_ != ResponseType::None) {
  //   collector.collect(std::make_unique<Response>(std::move(result)), slot);
  // }
}

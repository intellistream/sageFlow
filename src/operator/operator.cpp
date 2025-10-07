#include "operator/operator.h"
sageFlow::Operator::~Operator() = default;

sageFlow::Operator::Operator(OperatorType type, size_t parallelism)
      : type_(type) {
  set_parallelism(parallelism);
  // 根据算子类型设置默认名称
  switch (type) {
    case OperatorType::OUTPUT: name = "OutputOperator"; break;
    case OperatorType::FILTER: name = "FilterOperator"; break;
    case OperatorType::MAP: name = "MapOperator"; break;
    case OperatorType::JOIN: name = "JoinOperator"; break;
    case OperatorType::SINK: name = "SinkOperator"; break;
    case OperatorType::TOPK: name = "TopKOperator"; break;
    case OperatorType::WINDOW: name = "WindowOperator"; break;
    case OperatorType::ITOPK: name = "ITopKOperator"; break;
    case OperatorType::AGGREGATE: name = "AggregateOperator"; break;
    default: name = "Operator"; break;
  }
}

auto sageFlow::Operator::getType() const -> OperatorType { return type_; }

auto sageFlow::Operator::open() -> void { is_open_ = true; }

auto sageFlow::Operator::close() -> void { is_open_ = false; }

auto sageFlow::Operator::process(Response&record, int slot) -> std::optional<Response> {
  return std::nullopt;
}

auto sageFlow::Operator::apply(Response&& record, int slot, Collector& collector) -> void {
  // 默认实现：直接将数据传递给下游
  collector.collect(std::make_unique<Response>(std::move(record)), slot);
}

void sageFlow::Operator::set_parallelism(const size_t p) {
  if (p > 0) {
    parallelism_ = p;
  }
}
auto sageFlow::Operator::get_parallelism() const -> size_t {
  return parallelism_;
}
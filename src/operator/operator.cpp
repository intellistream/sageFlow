#include "operator/operator.h"
candy::Operator::~Operator() = default;

candy::Operator::Operator(OperatorType type, size_t parallelism)
      : type_(type) {
  set_parallelism(parallelism);
  // 根据算子类型设置默认名称
  switch (type) {
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

auto candy::Operator::getType() const -> OperatorType { return type_; }

auto candy::Operator::open() -> void {
  if (is_open_) {
    return;
  }
  is_open_ = true;
  for (const auto& child : children_) {
    child->open();
  }
}

auto candy::Operator::close() -> void {}

auto candy::Operator::process(Response& record, int slot) -> bool {
  emit(0, record);
  return true;
}

void candy::Operator::emit(const int id, Response& record) const {
  if (children_.empty()) {
    return;
  }
  int slot = child2slot_[id];
  children_[id]->process(record, slot);
}

auto candy::Operator::addChild(std::shared_ptr<Operator> child, const int slot) -> int {
  children_.push_back(std::move(child));
  const int index = children_.size() - 1;
  child2slot_.push_back(slot);
  return index;
}

void candy::Operator::set_parallelism(const size_t p) {
  if (p > 0) {
    parallelism_ = p;
  }
}
auto candy::Operator::get_parallelism() const -> size_t {
  return parallelism_;
}
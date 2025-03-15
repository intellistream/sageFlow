#include "runtime/operator/operator.h"
candy::Operator::~Operator() = default;

candy::Operator::Operator(const OperatorType type) : type_(type) {}

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

auto candy::Operator::process(std::unique_ptr<VectorRecord>& record, int slot) -> bool {
  emit(0, record);
  return true;
}

void candy::Operator::emit(const int id, std::unique_ptr<VectorRecord>& record) const {
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
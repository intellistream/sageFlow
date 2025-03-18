#include "runtime/operator/join_operator.h"

candy::JoinOperator::JoinOperator(std::unique_ptr<Function>& join_func)
    : Operator(OperatorType::JOIN), join_func_(std::move(join_func)) {}

void candy::JoinOperator::open() {
  if (is_open_) {
    return;
  }
  is_open_ = true;
  mother_->open();
  for (const auto& child : children_) {
    child->open();
  }
}

bool candy::JoinOperator::process(std::unique_ptr<VectorRecord>& data, const int slot) {
  if (slot == 0) {
    left_records_.emplace_back(std::move(data));
  } else {
    right_records_.emplace_back(std::move(data));
  }
  if (left_records_.size() < 1 || right_records_.size() < 1) {
    return false;
  }
  auto left = std::move(left_records_.front());
  left_records_.pop_front();
  auto right = std::move(right_records_.front());
  right_records_.pop_front();
  auto result = join_func_->Execute(left, right);
  emit(0, result);
  return true;
}

auto candy::JoinOperator::setMother(std::shared_ptr<Operator> mother) -> void { mother_ = std::move(mother); }
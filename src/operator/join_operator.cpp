#include "operator/join_operator.h"

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

bool candy::JoinOperator::process(Response& data, const int slot) { return true; }

auto candy::JoinOperator::setMother(std::shared_ptr<Operator> mother) -> void { mother_ = std::move(mother); }
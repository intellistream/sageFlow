#pragma once

#include <functional>
#include <list>
#include <memory>

#include "core/common/data_types.h"
#include "runtime/operator/base_operator.h"

#include "iostream"

namespace candy {
class JoinOperator final : public Operator {
 public:
  explicit JoinOperator(std::unique_ptr<Function> &join_func)
      : Operator(OperatorType::JOIN), join_func_(std::move(join_func)) {}

  auto open() -> void override {
    if (is_open_) {
      return;
    }
    is_open_ = true;
    mother_->open();
    for (const auto &child : children_) {
      child->open();
    }
  }

  auto process(std::unique_ptr<VectorRecord> &data, const int slot) -> bool override {
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

  auto setMother(std::shared_ptr<Operator> mother) -> void { mother_ = std::move(mother); }

 private:
  // anns index
  std::unique_ptr<Function> join_func_;
  std::shared_ptr<Operator> mother_;
  std::list<std::unique_ptr<VectorRecord>> left_records_;
  std::list<std::unique_ptr<VectorRecord>> right_records_;
};
}  // namespace candy
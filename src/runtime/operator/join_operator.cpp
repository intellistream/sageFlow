#include "runtime/operator/join_operator.h"

#include <cassert>
#include <iostream>

#include "runtime/function/join_function.h"

candy::JoinOperator::JoinOperator(std::unique_ptr<Function>& join_func) : Operator(OperatorType::JOIN) {
  join_func_ = std::unique_ptr<JoinFunction>(dynamic_cast<JoinFunction*>(join_func.release()));
  if (join_func_ == nullptr) {
    throw std::runtime_error("JoinOperator: join_func is not a JoinFunction");
  }
}

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

bool candy :: JoinOperator :: brute_process(std :: unique_ptr < VectorRecord > &data, const int slot) {
  int now_time = data->timestamp_;
  int window = join_func_->getTimeWindow();
  
  if (slot == 0) {
    left_records_.emplace_back(std::move(data));
  } else {
    right_records_.emplace_back(std::move(data));
  }

  if (left_records_.size() < 1 || right_records_.size() < 1) {
    return false;
  }

  // 更新窗口内的数据
  // pop_front if the timestamp is older than 10 seconds
  while (!left_records_.empty() && left_records_.front()->timestamp_ < now_time - window) {
    left_records_.pop_front();
  }
  while (!right_records_.empty() && right_records_.front()->timestamp_ < now_time - window) {
    right_records_.pop_front();
  }

  for (auto& left : left_records_) {
    for (auto& right : right_records_) {
      // auto ret = join_func_->Execute(left, right);
      // auto result = std::make_unique<VectorRecord>("1", VectorData{1.0, 2.0, 3.0}, 0);
      assert(left != nullptr);
      assert(right != nullptr);
      assert(join_func_ != nullptr);
      auto result = join_func_->Execute(left, right);
      emit(0, result);
    }
  }

  left_records_.clear();
  right_records_.clear();
  return false;
}

bool candy::JoinOperator::process(std::unique_ptr<VectorRecord>& data, const int slot) {
  
  return brute_process(data, slot);
}

auto candy::JoinOperator::setMother(std::shared_ptr<Operator> mother) -> void { mother_ = std::move(mother); }
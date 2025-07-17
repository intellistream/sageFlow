/*
修改 Eager/Lazy 通过process 中的 IsEagerAlgorithm 修改
修改调用的方法， 则修改 using JoinWay 后面的等于号
*/
#include "operator/join_operator.h"

#include <cassert>



using JoinWay = candy :: BruteForceLazy;

candy::JoinOperator::JoinOperator(std::unique_ptr<Function>& join_func) : Operator(OperatorType::JOIN), join_method_(std :: make_unique<JoinWay>()) {
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

auto candy::JoinOperator::lazy_process(const int slot) -> bool {
  if (left_records_.empty() || right_records_.empty()) {
    return false;
  }

  join_method_ -> Excute(methods_return_pool, join_func_, left_records_, right_records_);
  left_records_.clear();
  right_records_.clear();
 
  return true;
}

auto candy::JoinOperator::eager_process(const int slot) -> bool {
  if (left_records_.empty() || right_records_.empty()) {
    return false;
  }

  if (slot == 0) {
    join_method_ -> Excute(methods_return_pool, join_func_, left_records_.back(), right_records_, slot);
  } else {
    join_method_ -> Excute(methods_return_pool, join_func_, right_records_.back(), left_records_, slot);
}

  return true;
}

void candy :: JoinOperator :: clear_methods_return_pool() {
  for (auto &[id, record] : methods_return_pool) {
    Response ret;
    ret.push_back(std::move(record));
    emit(id, ret);
  }
  methods_return_pool.clear();
}

auto candy::JoinOperator::process(Response& input_data, const int slot) -> bool {
  // Simplified join processing
  if (slot == 0) {
    // Left side data
    for (auto& record : input_data) {
      if (record) {
        left_records_.push_back(std::move(record));
      }
    }
  } else {
    // Right side data  
    for (auto& record : input_data) {
      if (record) {
        right_records_.push_back(std::move(record));
      }
    }
  }
  
  // If both sides have data, perform join
  if (!left_records_.empty() && !right_records_.empty()) {
    Response left_resp;
    Response right_resp;
    
    // Move records to response for join function
    for (auto& record : left_records_) {
      left_resp.push_back(std::move(record));
    }
    for (auto& record : right_records_) {
      right_resp.push_back(std::move(record));
    }
    
    // Execute join function
    auto result = join_func_->Execute(left_resp, right_resp);
    
    if (!result.empty()) {
      emit(0, result);
    }
    
    // Clear processed records
    left_records_.clear();
    right_records_.clear();
    
    return true;
  }
  
  return false;
}

auto candy::JoinOperator::setMother(std::shared_ptr<Operator> mother) -> void { mother_ = std::move(mother); }
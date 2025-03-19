/*
修改 Eager/Lazy 通过process 中的 IsEagerAlgorithm 修改
修改调用的方法， 则修改 using Derived 后面的等于号
*/
#include "runtime/operator/join_operator.h"

#include <cassert>
#include <iostream>

#include "runtime/function/join_function.h"

using Derived = candy :: BruteForceLazy;

candy::JoinOperator::JoinOperator(std::unique_ptr<Function>& join_func) : Operator(OperatorType::JOIN), join_method_(std :: make_unique<Derived>()) {
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

  if (slot == 0)
    join_method_ -> Excute(methods_return_pool, join_func_, left_records_.back(), right_records_, slot);
  else
    join_method_ -> Excute(methods_return_pool, join_func_, right_records_.back(), left_records_, slot);

  return true;
}

void candy :: JoinOperator :: clear_methods_return_pool() {
  for (auto &[id, record] : methods_return_pool) {
    emit (id, record);
  }
  methods_return_pool.clear();
}

bool candy::JoinOperator::process(std::unique_ptr<VectorRecord>& data, const int slot) {

  // 标识使用的算法 Eager/Lazy
  bool IsEagerAlgorithm = false;
  
  // input data
  if (slot == 0) {
    left_records_.emplace_back(std::move(data));
  } else {
    right_records_.emplace_back(std::move(data));
  }

  //update window
  int window = join_func_->getTimeWindow();
  auto update_window = [&](std::list<std::unique_ptr<VectorRecord>>& records) {
    // 队首队头差距 <= window
    // Lazy 算法不管这里
    // Eager 算法注意这里需要进行删除操作， 影响外部数据库的数据
    // 或者 Eager 把 Index 的删除实现丢入函数里
    while (!records.empty() && records.front()->timestamp_ < records.back()->timestamp_ - window) {
      records.pop_front();
      // TODO : 完成 Eager 算法的 index 删除
      if (IsEagerAlgorithm) {
        // do something
      }
    }
  };
  update_window(left_records_); update_window(right_records_);

  bool ReturnFlag;

  if (IsEagerAlgorithm)
    ReturnFlag = eager_process(slot);
  else
    ReturnFlag = lazy_process(slot);

  clear_methods_return_pool();
  return ReturnFlag;
}

auto candy::JoinOperator::setMother(std::shared_ptr<Operator> mother) -> void { mother_ = std::move(mother); }
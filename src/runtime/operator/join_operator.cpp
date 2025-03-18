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

// TODO :: finish lazy_process

auto candy::JoinOperator::lazy_process(const int slot) -> bool {
  if (left_records_.empty() || right_records_.empty()) {
    return false;
  }
 
  return true;
}

auto candy::JoinOperator::eager_process(const int slot) -> bool {
  if (left_records_.empty() || right_records_.empty()) {
    return false;
  }

  // 之后就新开一个文件， 在 calculate 里边调用外部实现的查询算法
  auto calculate = [&](std::unique_ptr<VectorRecord>& data,
                      std::list<std::unique_ptr<VectorRecord>>& records) -> bool{
    // TODO : 将对链表的遍历换成数据库的查询
    for (auto& record : records) {
      auto result = join_func_->Execute(data, record);
      if (result != NULL) {
        emit(0, result);
      }
    }
    return true;
  };

  if (slot == 0) 
    return calculate(left_records_.back(), right_records_);
  else 
    return calculate(right_records_.back(), left_records_);
}

bool candy::JoinOperator::process(std::unique_ptr<VectorRecord>& data, const int slot) {

  // 标识使用的算法 Eager/Lazy
  bool IsEagerAlgorithm = true;
  
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
    while (!records.empty() && records.front()->timestamp_ < records.back()->timestamp_ - window) {
      records.pop_front();
      // TODO : 完成 Eager 算法的 index 删除
      if (IsEagerAlgorithm) {
        // do something
      }
    }
  };
  update_window(left_records_); update_window(right_records_);

  if (IsEagerAlgorithm)
    return eager_process(slot);
  else
    return lazy_process(slot);
}

auto candy::JoinOperator::setMother(std::shared_ptr<Operator> mother) -> void { mother_ = std::move(mother); }
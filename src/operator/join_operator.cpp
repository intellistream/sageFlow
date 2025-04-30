/*
修改 Eager/Lazy 通过process 中的 IsEagerAlgorithm 修改
修改调用的方法， 则修改 using JoinWay 后面的等于号
*/
#include "operator/join_operator.h"

#include <cassert>
#include <iostream>



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

  if (slot == 0)
    join_method_ -> Excute(methods_return_pool, join_func_, left_records_.back(), right_records_, slot);
  else
    join_method_ -> Excute(methods_return_pool, join_func_, right_records_.back(), left_records_, slot);

  return true;
}

void candy :: JoinOperator :: clear_methods_return_pool() {
  
  for (auto &[id, record] : methods_return_pool) {
    auto ret_record = std :: move(record);
    auto ret = Response{ResponseType::Record, std::move(record)};
    emit (id, ret);
  }
  methods_return_pool.clear();
}

bool candy::JoinOperator::process(Response& input_data, const int slot) {

  auto data = std::move(input_data.record_);
  
  // 标识使用的算法 Eager/Lazy
  bool IsEagerAlgorithm = false;
  int nowTimeStamp = data -> timestamp_;

  auto update_side = [&] (std::list<std::unique_ptr<VectorRecord>>& records, auto &window) -> bool {
    records.emplace_back(std :: move(data));
    int timelimit = window.windowTimeLimit(nowTimeStamp);
    while (!records.empty() && records.front()->timestamp_ <= timelimit) {
      records.pop_front();
      // TODO : 完成 Eager 算法的 index 删除
      if (IsEagerAlgorithm) {
        // do something
      }
    }
    return window.isNeedTrigger(nowTimeStamp);
  } ;

  bool triggerflag;

  if (slot == 0) {
    triggerflag = update_side(left_records_, join_func_ -> windowL);
  } else {
    triggerflag = update_side(right_records_, join_func_ -> windowR);
  }

  // 是否触发窗口
  if (triggerflag == false) 
    return false;

  bool ReturnFlag;

  if (IsEagerAlgorithm)
    ReturnFlag = eager_process(slot);
  else
    ReturnFlag = lazy_process(slot);

  clear_methods_return_pool();
  return ReturnFlag;
}

auto candy::JoinOperator::setMother(std::shared_ptr<Operator> mother) -> void { mother_ = std::move(mother); }
/*
修改 Eager/Lazy 通过process 中的 IsEagerAlgorithm 修改
修改调用的方法， 则修改 using JoinWay 后面的等于号
*/
#include "operator/join_operator.h"
#include "stream/time/sliding_window.h"

#include <cassert>
#include <iostream>
#include <spdlog/spdlog.h>

using JoinWay = candy :: BruteForceLazy;

candy::JoinOperator::JoinOperator(std::unique_ptr<Function>&& join_func) : Operator(OperatorType::JOIN), join_method_(std :: make_unique<JoinWay>()) {
  join_func_ = std::unique_ptr<JoinFunction>(dynamic_cast<JoinFunction*>(join_func.release()));
  if (join_func_ == nullptr) {
    throw std::runtime_error("JoinOperator: join_func is not a JoinFunction");
  }
}

void candy::JoinOperator::open() {
  // Replace direct access to is_open_
  if (isOpen()) {
      spdlog::warn("JoinOperator already open");
      return;
  }
  setOpen(true);
  mother_->open();
  for (const auto& child : getChildren()) {
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
    // 创建 DataElement 而不是 Response
    auto element = DataElement(std::move(record));
    emit(id, element);
  }
  methods_return_pool.clear();
}

bool candy::JoinOperator::processDataElement(DataElement& element, const int slot) {
  // 只处理 Record 类型的数据元素
  if (!element.isRecord() || !element.getRecord()) {
    return false;
  }
  
  // 从数据元素中提取记录
  auto data = element.moveRecord();
  
  // 标识使用的算法 Eager/Lazy
  bool IsEagerAlgorithm = false;
  int nowTimeStamp = data->timestamp_;

  auto update_side = [&] (std::list<std::unique_ptr<VectorRecord>>& records, std::shared_ptr<Window>& window) -> bool {
    records.emplace_back(std::move(data));
    
    // Get the earliest time that should be kept in the window
    // For SlidingWindow, this is start time of the window
    int timeLimit = 0;
    if (auto* slidingWindow = dynamic_cast<SlidingWindow*>(window.get())) {
      timeLimit = nowTimeStamp - slidingWindow->getSlide();
    } else {
      // For other window types, use the window start
      timeLimit = window->getStart();
    }
    
    // Remove records outside the window
    while (!records.empty() && records.front()->timestamp_ <= timeLimit) {
      records.pop_front();
      // TODO : 完成 Eager 算法的 index 删除
      if (IsEagerAlgorithm) {
        // do something
      }
    }
    
    // Check if this timestamp triggers the window
    // For now, always trigger the window when a new record arrives
    // In a real implementation, we'd check if the watermark has advanced past the window end
    bool triggerWindow = true;
    
    return triggerWindow;
  };

  bool triggerflag;

  if (slot == 0) {
    triggerflag = update_side(left_records_, join_func_->windowL);
  } else {
    triggerflag = update_side(right_records_, join_func_->windowR);
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
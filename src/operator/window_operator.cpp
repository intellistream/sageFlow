//
// Created by Pygon on 25-4-30.
//
#include "operator/window_operator.h"

#include <iostream>
#include <mutex>

#include "function/window_function.h"

candy::WindowOperator::WindowOperator(std::unique_ptr<Function>& window_func) : Operator(OperatorType::WINDOW) {}

auto candy::WindowOperator::process(Response& data, int slot) -> bool {
  return Operator::process(data, slot);
}

candy::TumblingWindowOperator::TumblingWindowOperator(std::unique_ptr<Function>& window_func)
    : WindowOperator(window_func) {
  auto window_func_ = dynamic_cast<WindowFunction*>(window_func.get());
  window_size_ = window_func_->getWindowSize();
  records_ = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
  records_->reserve(window_size_);
}

auto candy::TumblingWindowOperator::process(Response& data, int slot) -> bool {
  // TODO: 多线程改造 - 滚动窗口的并发状态管理
  // 在多线程环境中，需要考虑以下改造：
  // 1. 窗口状态(records_)的并发访问保护
  // 2. 窗口触发的原子性：确保只有一个线程触发窗口
  // 3. 分区窗口：每个线程维护独立的窗口状态
  // 4. 基于时间的窗口：考虑时间戳的全局排序

  std::lock_guard<std::mutex> lock(window_mutex_);

  if (data.type_ == ResponseType::Record) {
    auto record = std::move(data.record_);
    records_->push_back(std::move(record));
  }
  if (records_->size() == window_size_) {
    auto resp = Response{ResponseType::List, std::move(records_)};
    emit(0, resp);
    records_ = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
    records_->reserve(window_size_);
    return true;
  }
  return false;
}

candy::SlidingWindowOperator::SlidingWindowOperator(std::unique_ptr<Function>& window_func)
    : WindowOperator(window_func) {
  auto window_func_ = dynamic_cast<WindowFunction*>(window_func.get());
  window_size_ = window_func_->getWindowSize();
  slide_size_ = window_func_->getSlideSize();
}

auto candy::SlidingWindowOperator::process(Response& data, int slot) -> bool {
  // TODO: 多线程改造 - 滑动窗口的并发状态管理
  // 在多线程环境中，需要考虑以下改造：
  // 1. 滑动窗口状态(records_)的并发访问保护
  // 2. 窗口滑动的原子性：确保滑动操作的一致性
  // 3. 基于时间的滑动：处理乱序数据的窗口分配
  // 4. 内存管理：避免窗口状态的内存泄漏

  std::lock_guard<std::mutex> lock(window_mutex_);

  if (data.type_ == ResponseType::Record) {
    auto record = std::move(data.record_);
    records_.push_back(std::move(record));
  }
  if (records_.size() >= window_size_) {
    auto records = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
    records->reserve(window_size_);
    for (auto& it : records_) {
      auto record = std::make_unique<VectorRecord>(*it);
      records->push_back(std::move(record));
    }
    auto resp = Response{ResponseType::List, std::move(records)};
    emit(0, resp);
    for (auto i = 0; i < slide_size_; ++i) {
      records_.pop_front();
    }
    return true;
  }
  return false;
}
//
// Created by Pygon on 25-4-30.
//
#include "operator/window_operator.h"

#include <iostream>
#include <mutex>

#include "function/window_function.h"

candy::WindowOperator::WindowOperator(std::unique_ptr<Function>& window_func) : Operator(OperatorType::WINDOW) {}

auto candy::WindowOperator::process(Response&data, int slot) -> std::optional<Response> {
  return std::nullopt;
}

candy::TumblingWindowOperator::TumblingWindowOperator(std::unique_ptr<Function>& window_func)
    : WindowOperator(window_func) {
  auto window_func_ = dynamic_cast<WindowFunction*>(window_func.get());
  window_size_ = window_func_->getWindowSize();
}

auto candy::TumblingWindowOperator::process(Response&data, int slot) -> std::optional<Response> {
  // TODO: 多线程改造 - 滚动窗口的并发状态管理
  // 在多线程环境中，需要考虑以下改造：
  // 1. 窗口状态(window_buffer_)的并发访问保护
  // 2. 窗口触发的原子性：确保只有一个线程触发窗口
  // 3. 分区窗口：每个线程维护独立的窗口状态
  // 4. 基于时间的窗口：考虑时间戳的全局排序

  std::lock_guard<std::mutex> lock(window_mutex_);

  if (data.type_ == ResponseType::Record) {
    auto record = std::make_unique<VectorRecord>(*data.record_);
    window_buffer_.push_back(std::move(record));
  }

  if (window_buffer_.size() == window_size_) {
    auto records = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
    records->reserve(window_buffer_.size());

    for (auto& record : window_buffer_) {
      records->push_back(std::move(record));
    }
    window_buffer_.clear();

    return Response{ResponseType::List, std::move(records)};
  }
  return std::nullopt;
}

candy::SlidingWindowOperator::SlidingWindowOperator(std::unique_ptr<Function>& window_func)
    : WindowOperator(window_func) {
  auto window_func_ = dynamic_cast<WindowFunction*>(window_func.get());
  window_size_ = window_func_->getWindowSize();
  slide_size_ = window_func_->getSlideSize();
}

auto candy::SlidingWindowOperator::process(Response&data, int slot) -> std::optional<Response> {
  std::lock_guard<std::mutex> lock(window_mutex_);

  if (data.type_ == ResponseType::Record) {
    auto record = std::make_unique<VectorRecord>(*data.record_);
    window_buffer_.push_back(std::move(record));
  }

  // Remove old records if window is full
  while (window_buffer_.size() > window_size_) {
    window_buffer_.pop_front();
  }

  if (window_buffer_.size() == window_size_) {
    auto records = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
    records->reserve(window_buffer_.size());

    for (const auto& record : window_buffer_) {
      records->push_back(std::make_unique<VectorRecord>(*record));
    }

    return Response{ResponseType::List, std::move(records)};
  }
  return std::nullopt;
}

auto candy::WindowOperator::apply(Response&& record, int slot, Collector& collector) -> void {
  // 基类默认实现，子类需要重写此方法
  collector.collect(std::make_unique<Response>(std::move(record)), slot);
}

auto candy::TumblingWindowOperator::apply(Response&& record, int slot, Collector& collector) -> void {
  std::lock_guard<std::mutex> lock(window_mutex_);

  if (record.type_ == ResponseType::Record && record.record_) {
    auto record_copy = std::make_unique<VectorRecord>(*record.record_);
    window_buffer_.push_back(std::move(record_copy));
  }

  if (window_buffer_.size() == window_size_) {
    auto records = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
    records->reserve(window_buffer_.size());

    for (auto& buffered_record : window_buffer_) {
      records->push_back(std::move(buffered_record));
    }
    window_buffer_.clear();

    Response window_result{ResponseType::List, std::move(records)};
    collector.collect(std::make_unique<Response>(std::move(window_result)), slot);
  }
}

auto candy::SlidingWindowOperator::apply(Response&& record, int slot, Collector& collector) -> void {
  std::lock_guard<std::mutex> lock(window_mutex_);

  if (record.type_ == ResponseType::Record && record.record_) {
    auto record_copy = std::make_unique<VectorRecord>(*record.record_);
    window_buffer_.push_back(std::move(record_copy));
  }

  // Remove old records if window is full
  while (window_buffer_.size() > window_size_) {
    window_buffer_.pop_front();
  }

  if (window_buffer_.size() == window_size_) {
    auto records = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
    records->reserve(window_buffer_.size());

    for (const auto& buffered_record : window_buffer_) {
      records->push_back(std::make_unique<VectorRecord>(*buffered_record));
    }

    Response window_result{ResponseType::List, std::move(records)};
    collector.collect(std::make_unique<Response>(std::move(window_result)), slot);
  }
}

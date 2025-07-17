//
// Created by Pygon on 25-4-30.
//
#include "operator/window_operator.h"


#include "function/window_function.h"

candy::WindowOperator::WindowOperator(std::unique_ptr<Function>& window_func) : Operator(OperatorType::WINDOW) {}

auto candy::WindowOperator::process(Response& data, const int slot) -> bool { return Operator::process(data, slot); }

candy::TumblingWindowOperator::TumblingWindowOperator(std::unique_ptr<Function>& window_func)
    : WindowOperator(window_func) {
  auto window_func_ = dynamic_cast<WindowFunction*>(window_func.get());
  window_size_ = window_func_->getWindowSize();
  records_ = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
  records_->reserve(window_size_);
}

auto candy::TumblingWindowOperator::process(Response& data, const int slot) -> bool {
  // Add all records from the input response to our window
  for (auto &record : data) {
    if (record) {
      records_->push_back(std::move(record));
    }
  }
  
  if (records_->size() >= window_size_) {
    // Create response with all records in the window
    Response resp;
    for (auto &record : *records_) {
      resp.push_back(std::move(record));
    }
    emit(0, resp);
    records_->clear();
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

auto candy::SlidingWindowOperator::process(Response& data, const int slot) -> bool {
  // Add all records from the input response to our sliding window
  for (auto &record : data) {
    if (record) {
      records_.push_back(std::move(record));
    }
  }
  
  if (records_.size() >= window_size_) {
    // Create response with a copy of records in the window
    Response resp;
    auto it = records_.begin();
    for (size_t i = 0; i < window_size_ && it != records_.end(); ++i, ++it) {
      auto record = std::make_unique<VectorRecord>(**it);
      resp.push_back(std::move(record));
    }
    emit(0, resp);
    
    // Slide the window by removing slide_size_ records from the front
    for (size_t i = 0; i < slide_size_ && !records_.empty(); ++i) {
      records_.pop_front();
    }
    return true;
  }
  return false;
}
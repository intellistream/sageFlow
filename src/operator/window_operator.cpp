//
// Created by Pygon on 25-4-30.
//
#include "operator/window_operator.h"

#include <iostream>

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
  if (data.type_ == ResponseType::Record) {
    auto record = std::move(data.record_);
    records_->push_back(std::move(record));
  }
  if (records_->size() == window_size_) {
    auto resp = Response{ResponseType::List, std::move(records_)};
    emit(0, resp);
    records_ = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
    records_->reserve(records_->capacity());
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

bool candy::SlidingWindowOperator::process(Response& data, const int slot) {
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
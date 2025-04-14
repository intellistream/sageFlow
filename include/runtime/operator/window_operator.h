#pragma once
#include <iostream>
#include <memory>
#include <vector>
#include <chrono>

#include "core/common/data_types.h"
#include "runtime/function/function.h"
#include "runtime/operator/base_operator.h"

namespace candy {

enum class WindowType {
  TUMBLING,  // Fixed-size, non-overlapping windows
  SLIDING,   // Fixed-size, overlapping windows
  SESSION    // Variable-size windows based on session activity
};

class WindowOperator : public Operator {
 public:
  WindowOperator(const std::string &name, std::unique_ptr<Function> &window_func, 
                 WindowType type = WindowType::TUMBLING, 
                 size_t window_size = 1000,
                 size_t slide_size = 1000)
      : Operator(OperatorType::FILTER, name), 
        window_func_(std::move(window_func)),
        window_type_(type),
        window_size_(window_size),
        slide_size_(slide_size) {}

  void open() override {
    window_buffer_.clear();
    last_window_time_ = std::chrono::steady_clock::now();
    Operator::open();
  }

  auto process(std::unique_ptr<VectorRecord> &data) -> bool override {
    if (!data) return false;
    
    window_buffer_.push_back(std::move(data));
    
    bool processed = false;
    
    // Check if we should process the window
    switch (window_type_) {
      case WindowType::TUMBLING:
        if (window_buffer_.size() >= window_size_) {
          processed = process_window();
        }
        break;
        
      case WindowType::SLIDING:
        if (window_buffer_.size() >= window_size_) {
          processed = process_window();
          // Keep the overlapping portion for the next window
          if (slide_size_ < window_buffer_.size()) {
            std::vector<std::unique_ptr<VectorRecord>> remaining;
            for (size_t i = slide_size_; i < window_buffer_.size(); ++i) {
              remaining.push_back(std::move(window_buffer_[i]));
            }
            window_buffer_ = std::move(remaining);
          } else {
            window_buffer_.clear();
          }
        }
        break;
        
      case WindowType::SESSION:
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_window_time_).count();
        
        // If there's inactivity for session_timeout_, process the window
        if (elapsed > session_timeout_ && !window_buffer_.empty()) {
          processed = process_window();
        }
        last_window_time_ = now;
        break;
    }
    
    return processed;
  }

 private:
  bool process_window() {
    if (window_buffer_.empty()) return false;
    
    // Process the window with the window function
    std::unique_ptr<VectorRecord> result = window_func_->Execute(window_buffer_);
    
    if (result) {
      emit(0, result);
      return true;
    }
    
    return false;
  }

  std::unique_ptr<Function> window_func_;
  WindowType window_type_;
  size_t window_size_;       // Size of the window (in number of records for count-based windows)
  size_t slide_size_;        // For sliding windows, how many records to slide
  long session_timeout_ = 5000; // For session windows, timeout in milliseconds
  
  std::vector<std::unique_ptr<VectorRecord>> window_buffer_;
  std::chrono::steady_clock::time_point last_window_time_;
};

}  // namespace candy

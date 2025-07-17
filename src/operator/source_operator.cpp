#include "operator/source_operator.h"

#include <chrono>

namespace candy {

void SourceOperator::open() {
  if (is_open_) {
    return;
  }
  
  Operator::open();
  source_func_->Init();
  is_running_ = true;
}

void SourceOperator::close() {
  if (!is_open_) {
    return;
  }
  
  stop();
  source_func_->Close();
  Operator::close();
}

auto SourceOperator::process(Response& record, int slot) -> bool {
  // Source operators don't process input records
  // They generate data through the runSource method
  return false;
}

void SourceOperator::start() {
  if (!is_running_ || worker_thread_.joinable()) {
    return;
  }
  
  worker_thread_ = std::thread([this]() {
    runSource();
  });
}

void SourceOperator::stop() {
  is_running_ = false;
  if (worker_thread_.joinable()) {
    worker_thread_.join();
  }
}

void SourceOperator::runSource() {
  while (is_running_) {
    Response dummy_input;  // Source functions don't use input
    auto response = source_func_->Execute(dummy_input);
    
    if (!response.empty()) {
      // Emit to all children
      for (size_t i = 0; i < children_.size(); ++i) {
        emit(static_cast<int>(i), response);
      }
    } else {
      // No data available, sleep briefly to avoid busy waiting
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
}

} // namespace candy

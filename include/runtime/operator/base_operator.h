
#pragma once

#include <core/common/data_types.h>  // Include VectorRecord definition

#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace candy {
enum class OperatorType { FILTER, MAP, JOIN, SINK };

// Base class for all operators
class Operator {
 protected:
  std::vector<std::unique_ptr<Operator>> next_operators_;  // List of next operators in the pipeline
  std::queue<std::unique_ptr<VectorRecord>> input_queue_;  // Queue of VectorRecords as input
  std::mutex queue_mutex_;                                 // Mutex for thread-safe queue operations
  std::condition_variable queue_cv_;                       // Condition variable for queue operations
  OperatorType type_ = OperatorType::FILTER;
  std::string name_;
  std::vector<std::thread> threads_;
  bool running_ = true;

 public:
  Operator() = default;

  Operator(OperatorType type, std::string name) : type_(type), name_(std::move(name)) {
    threads_.emplace_back([this] { process_queue(); });
  }

  virtual ~Operator() {
    running_ = false;
    queue_cv_.notify_all();
    for (auto &thread : threads_) {
      thread.join();
    }
  }

  // Open the operator (e.g., initialization logic)
  virtual void open() {
    // Default implementation: No-op
  }

  // Close the operator (e.g., cleanup logic)
  virtual void close() {
    // Default implementation: No-op
  }

  // Process a single VectorRecord
  virtual auto process(std::unique_ptr<VectorRecord> &record) -> bool;

  // Emit processed data to the next operator in the pipeline
  virtual void emit(int id, std::unique_ptr<VectorRecord> &record) {
    if (id < next_operators_.size()) {
      next_operators_[id]->enqueue(record);
    }
  }

  // Set the next operator in the pipeline
  auto set_next_operator(std::unique_ptr<Operator> &next, int &id) -> Operator * {
    next_operators_.push_back(std::move(next));
    id = next_operators_.size() - 1;
    return next_operators_[id].get();
  }

  // Add data to the input queue
  void enqueue(std::unique_ptr<VectorRecord> &data) {
    std::unique_lock lock(queue_mutex_);
    input_queue_.push(std::move(data));
    queue_cv_.notify_all();
  }

  // Pull data from the input queue and process it
  virtual void process_queue() {
    while (true) {
      std::unique_ptr<VectorRecord> data = nullptr;
      {
        std::unique_lock lock(queue_mutex_);
        if (!input_queue_.empty()) {
          data = std::move(input_queue_.front());
          input_queue_.pop();
        } else {
          if (!running_) {
            break;
          }
        }
        if (!data) {
          queue_cv_.wait(lock);
        }
      }
      if (data) {
        process(data);  // Pass unique_ptr directly
      }
    }
  }
};

}  // namespace candy


#pragma once

#include <core/common/data_types.h>  // Include VectorRecord definition

#include <memory>
#include <mutex>
#include <queue>

namespace candy {

// Base class for all operators
class Operator {
 protected:
  std::unique_ptr<Operator> next_operator_;                // Pointer to the next operator in the pipeline
  std::queue<std::unique_ptr<VectorRecord>> input_queue_;  // Queue of VectorRecords as input
  std::mutex queue_mutex_;                                 // Mutex for thread-safe queue operations

 public:
  virtual ~Operator() = default;

  // Open the operator (e.g., initialization logic)
  virtual void open() {
    // Default implementation: No-op
  }

  // Close the operator (e.g., cleanup logic)
  virtual void close() {
    // Default implementation: No-op
  }

  // Process a single VectorRecord
  virtual void process(std::unique_ptr<VectorRecord> &record);

  // Emit processed data to the next operator in the pipeline
  virtual void emit(std::unique_ptr<VectorRecord> &record) {
    if (next_operator_) {
      next_operator_->enqueue(record);
    }
  }

  // Set the next operator in the pipeline
  auto set_next_operator(std::unique_ptr<Operator> &next) -> Operator * {
    next_operator_ = std::move(next);
    return next_operator_.get();
  }

  // Add data to the input queue
  void enqueue(std::unique_ptr<VectorRecord> &data) {
    std::lock_guard lock(queue_mutex_);
    input_queue_.push(std::move(data));
  }

  // Pull data from the input queue and process it
  virtual void process_queue() {
    while (true) {
      std::unique_ptr<VectorRecord> data = nullptr;
      {
        std::lock_guard lock(queue_mutex_);
        if (!input_queue_.empty()) {
          data = std::move(input_queue_.front());
          input_queue_.pop();
        } else {
          break;  // Exit if queue is empty
        }
      }
      if (data) {
        process(data);  // Pass unique_ptr directly
      }
    }
  }
};

}  // namespace candy

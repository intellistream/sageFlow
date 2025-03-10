
#pragma once

#include <core/common/data_types.h>  // Include VectorRecord definition

#include <memory>
#include <mutex>
#include <queue>

namespace candy {
enum class OperatorType { FILTER, MAP, JOIN, SINK };

// Base class for all operators
class Operator {
 protected:
  std::unordered_map<OperatorType, std::unique_ptr<Operator>> children_;
  std::queue<std::unique_ptr<VectorRecord>> input_queue_;  // Queue of VectorRecords as input
  std::mutex queue_mutex_;                                 // Mutex for thread-safe queue operations

  OperatorType type_ = OperatorType::FILTER;
  std::string name_;

 public:
  Operator() = default;

  Operator(OperatorType type, std::string name) : type_(type), name_(std::move(name)) {}

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
  virtual auto process(std::unique_ptr<VectorRecord> &record) -> bool;

  // Emit processed data to the next operator in the pipeline
  virtual void emit(std::unique_ptr<VectorRecord> &record) {}

  // Set the next operator in the pipeline
  auto set_next_operator(std::unique_ptr<Operator> &next) -> Operator * { return nullptr; }

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

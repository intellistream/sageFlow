#pragma once

#include "core/common/data_types.h"
#include "runtime/operator/base_operator.h"
#include "streaming/data_stream/data_stream.h"

namespace candy {
class SourceOperator : public Operator {
 private:
  bool source_active_ = false;
  std::unique_ptr<DataStream> data_stream_ = nullptr;

 public:
  SourceOperator(const std::string &name = "source") : Operator(OperatorType::SOURCE, name) {}
  
  SourceOperator(const std::string &name, std::unique_ptr<DataStream> data_stream)
      : Operator(OperatorType::SOURCE, name), data_stream_(std::move(data_stream)) {}
  
  virtual ~SourceOperator() {
    // Base class destructor handles thread cleanup
  }

  void open() override {
    source_active_ = true;
    if (data_stream_) {
      data_stream_->Init();
    }
    Operator::open(); // Call base implementation
  }

  void close() override {
    source_active_ = false;
    Operator::close(); // Call base implementation
  }

  // Override process to handle source-specific logic
  auto process(std::unique_ptr<VectorRecord>& record) -> bool override {
    // In source operators, the process method might be called with a null record
    // We can generate a new record and forward it to downstream operators
    if (source_active_) {
      auto new_record = get_next_record();
      if (new_record) {
        for (size_t i = 0; i < next_operators_.size(); ++i) {
          emit(i, new_record);
        }
        return true;
      }
    }
    return false;
  }

  // Get the next record from the source
  virtual std::unique_ptr<VectorRecord> get_next_record() {
    if (data_stream_) {
      RecordOrWatermark record_or_watermark;
      if (data_stream_->Next(record_or_watermark)) {
        if (std::holds_alternative<std::unique_ptr<VectorRecord>>(record_or_watermark)) {
          return std::get<std::unique_ptr<VectorRecord>>(std::move(record_or_watermark));
        }
      }
    }
    return nullptr;
  }

  // Override process_queue to generate data proactively
  void process_queue() override {
    while (running_) {
      if (source_active_) {
        std::unique_ptr<VectorRecord> dummy = nullptr;
        process(dummy); // Call process to generate and forward data
      }
      
      // Sleep a bit to avoid busy waiting
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  // Set a new data stream
  void set_data_stream(std::unique_ptr<DataStream> data_stream) {
    data_stream_ = std::move(data_stream);
  }

  // Check if source is active
  bool is_active() const {
    return source_active_;
  }
};
}  // namespace candy
#pragma once
#include <runtime/operator/base_operator.h>

#include <memory>

#include "streaming/data_stream/data_stream.h"

namespace candy {
class Task {
 public:
  Task() = default;

  auto setDataStream(std::unique_ptr<DataStream>& data_stream) -> void {
    data_stream_ = std::move(data_stream);
    data_stream = nullptr;
  }

  void setOperator(std::unique_ptr<Operator>& op) {
    operator_ = std::move(op);
    op = nullptr;
  }

  void begin() const {
    data_stream_->Init();
    std::unique_ptr<VectorRecord> record;
    while (data_stream_->Next(record)) {
      operator_->enqueue(record);
    }
  }

 private:
  std::unique_ptr<DataStream> data_stream_;
  std::unique_ptr<Operator> operator_;
};
}  // namespace candy
#pragma once
#include "runtime/function/function.h"

namespace candy {
using SinkFunc = std::function<void(std::unique_ptr<VectorRecord> &)>;

class SinkFunction : public Function {
 public:
  explicit SinkFunction(std::string name) : Function(std::move(name), FunctionType::Sink) {}

  SinkFunction(std::string name, SinkFunc sink_func)
      : Function(std::move(name), FunctionType::Sink), sink_func_(std::move(sink_func)) {}

  auto Execute(std::unique_ptr<VectorRecord> &record) -> std::unique_ptr<VectorRecord> override {
    sink_func_(record);
    return std::move(record);
  }

  auto setSinkFunc(SinkFunc sink_func) -> void { sink_func_ = std::move(sink_func); }

 private:
  SinkFunc sink_func_;
};
};  // namespace candy
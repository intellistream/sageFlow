#pragma once
#include <functional>
#include <memory>
#include <utility>

#include "core/common/data_types.h"
#include "streaming/function/function.h"

namespace candy {
class SinkFunction : public Function {
 public:
  SinkFunction(const std::string &name, std::function<void(std::unique_ptr<VectorRecord> &)> &sink_func)
      : Function(FunctionType::FILTER, name), sink_func_(std::move(sink_func)) {}

  auto operator()(std::unique_ptr<VectorRecord> &data) -> bool override {
    sink_func_(data);
    return true;
  }

 private:
  std::function<void(std::unique_ptr<VectorRecord> &)> sink_func_;
};
}  // namespace candy
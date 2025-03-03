#pragma once
#include <functional>
#include <memory>

#include "core/common/data_types.h"
#include "streaming/function/function.h"

namespace candy {
class FilterFunction : public Function {
 public:
  FilterFunction(const std::string &name,  std::function<bool(std::unique_ptr<VectorRecord> &)> &filter_func)
      : Function(FunctionType::FILTER, name), filter_func_(std::move(filter_func)) {}

  auto operator()(std::unique_ptr<VectorRecord> &data) -> bool override { return filter_func_(data); }

 private:
  std::function<bool(std::unique_ptr<VectorRecord> &)> filter_func_;
};
}  // namespace candy
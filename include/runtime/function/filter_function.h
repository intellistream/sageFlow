#pragma once
#include "runtime/function/function.h"

namespace candy {
using FilterFunc = std::function<bool(std::unique_ptr<VectorRecord> &)>;

class FilterFunction : public Function {
 public:
  explicit FilterFunction(std::string name) : Function(std::move(name), FunctionType::Filter) {}

  FilterFunction(std::string name, FilterFunc filter_func)
      : Function(std::move(name), FunctionType::Filter), filter_func_(std::move(filter_func)) {}

  auto Execute(std::unique_ptr<VectorRecord> &record) -> std::unique_ptr<VectorRecord> override {
    if (filter_func_(record)) {
      return std::move(record);
    }
    return nullptr;
  }

  auto setFilterFunc(FilterFunc filter_func) -> void { filter_func_ = std::move(filter_func); }

 private:
  FilterFunc filter_func_;
};
};  // namespace candy
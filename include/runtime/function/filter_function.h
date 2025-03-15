#pragma once
#include "runtime/function/function.h"

namespace candy {
using FilterFunc = std::function<bool(std::unique_ptr<VectorRecord> &)>;

class FilterFunction final : public Function {
 public:
  explicit FilterFunction(std::string name);

  FilterFunction(std::string name, FilterFunc filter_func);

  auto Execute(std::unique_ptr<VectorRecord> &record) -> std::unique_ptr<VectorRecord> override;

  auto setFilterFunc(FilterFunc filter_func) -> void;

 private:
  FilterFunc filter_func_;
};
};  // namespace candy
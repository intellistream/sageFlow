#pragma once
#include <functional>
#include <memory>

#include "core/common/data_types.h"
#include "runtime/operator/base_operator.h"

namespace candy {
class FilterOperator : public Operator {
 public:
  FilterOperator(const std::string &name,  std::function<bool(std::unique_ptr<VectorRecord> &)> &filter_func)
      : Operator(OperatorType::FILTER, name), filter_func_(std::move(filter_func)) {}

  auto process(std::unique_ptr<VectorRecord> &data) -> bool override { return filter_func_(data); }

 private:
  std::function<bool(std::unique_ptr<VectorRecord> &)> filter_func_;
};
}  // namespace candy
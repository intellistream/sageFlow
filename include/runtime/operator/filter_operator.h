#pragma once
#include <functional>
#include <memory>

#include "core/common/data_types.h"
#include "runtime/function/filter_function.h"
#include "runtime/operator/base_operator.h"

namespace candy {
class FilterOperator : public Operator {
 public:
  explicit FilterOperator( std::unique_ptr<Function> &filter_func)
      : Operator(OperatorType::FILTER), filter_func_(std::move(filter_func)) {}

  auto process(std::unique_ptr<VectorRecord> &data, int slot) -> bool override {
    if (auto result = filter_func_->Execute(data)) {
      emit(0, result);
      return true;
    }
    return false;
  }

 private:
  std::unique_ptr<Function> filter_func_;
};
}  // namespace candy
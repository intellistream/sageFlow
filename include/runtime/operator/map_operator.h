#pragma once
#include <iostream>
#include <memory>

#include "core/common/data_types.h"
#include "runtime/function/function.h"
#include "runtime/operator/base_operator.h"

namespace candy {
class MapOperator : public Operator {
 public:
  MapOperator(const std::string &name, std::unique_ptr<Function> &map_func)
      : Operator(OperatorType::FILTER, name), map_func_(std::move(map_func)) {}

  auto process(std::unique_ptr<VectorRecord> &data) -> bool override {
    auto result = map_func_->Execute(data);
    emit(0, result);
    return true;
  }

 private:
  std::unique_ptr<Function> map_func_;
};
}  // namespace candy
#pragma once
#include <functional>
#include <memory>

#include "core/common/data_types.h"
#include "runtime/operator/base_operator.h"

namespace candy {
class MapOperator : public Operator {
 public:
  MapOperator(const std::string &name, std::function<void(std::unique_ptr<VectorRecord> &)> &map_func)
      : Operator(OperatorType::FILTER, name), map_func_(std::move(map_func)) {}

  auto process(std::unique_ptr<VectorRecord> &data) -> bool override {
    map_func_(data);
    return true;
  }

 private:
  std::function<void(std::unique_ptr<VectorRecord> &)> map_func_;
};
}  // namespace candy
#pragma once
#include <functional>
#include <memory>

#include "core/common/data_types.h"
#include "streaming/function/function.h"

namespace candy {
class MapFunction : public Function {
 public:
  MapFunction(const std::string &name, std::function<void(std::unique_ptr<VectorRecord> &)> &map_func)
      : Function(FunctionType::FILTER, name), map_func_(std::move(map_func)) {}

  auto operator()(std::unique_ptr<VectorRecord> &data) -> bool override {
    map_func_(data);
    return true;
  }

 private:
  std::function<void(std::unique_ptr<VectorRecord> &)> map_func_;
};
}  // namespace candy
#pragma once
#include "runtime/function/function.h"

namespace candy {
using MapFunc = std::function<void(std::unique_ptr<VectorRecord> &)>;

class MapFunction : public Function {
 public:
  explicit MapFunction(std::string name) : Function(std::move(name), FunctionType::Map) {}

  MapFunction(std::string name, MapFunc map_func)
      : Function(std::move(name), FunctionType::Map), map_func_(std::move(map_func)) {}

  auto Execute(std::unique_ptr<VectorRecord> &record) -> std::unique_ptr<VectorRecord> override {
    map_func_(record);
    return std::move(record);
  }

  auto setMapFunc(MapFunc map_func) -> void { map_func_ = std::move(map_func); }

 private:
  MapFunc map_func_;
};
};  // namespace candy
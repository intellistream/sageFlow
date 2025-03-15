#pragma once
#include "runtime/function/function.h"

namespace candy {
using MapFunc = std::function<void(std::unique_ptr<VectorRecord> &)>;

class MapFunction final : public Function {
 public:
  explicit MapFunction(std::string name);

  MapFunction(std::string name, MapFunc map_func);

  auto Execute(std::unique_ptr<VectorRecord> &record) -> std::unique_ptr<VectorRecord> override;

  auto setMapFunc(MapFunc map_func) -> void;

 private:
  MapFunc map_func_;
};
};  // namespace candy
#include "runtime/function/map_function.h"

candy::MapFunction::MapFunction(std::string name) : Function(std::move(name), FunctionType::Map) {}

candy::MapFunction::MapFunction(std::string name, MapFunc map_func)
    : Function(std::move(name), FunctionType::Map), map_func_(std::move(map_func)) {}

std::unique_ptr<candy::VectorRecord> candy::MapFunction::Execute(std::unique_ptr<VectorRecord>& record) {
  map_func_(record);
  return std::move(record);
}

auto candy::MapFunction::setMapFunc(MapFunc map_func) -> void { map_func_ = std::move(map_func); }
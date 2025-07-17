#include "function/map_function.h"

candy::MapFunction::MapFunction(std::string name) : Function(std::move(name), FunctionType::Map) {}

candy::MapFunction::MapFunction(std::string name, MapFunc map_func)
    : Function(std::move(name), FunctionType::Map), map_func_(std::move(map_func)) {}

auto candy::MapFunction::Execute(Response &resp) -> candy::Response {
  Response result;
  
  for (auto &record : resp) {
    if (record) {
      map_func_(record);
      result.push_back(std::move(record));
    }
  }
  
  return result;
}

auto candy::MapFunction::setMapFunc(MapFunc map_func) -> void { map_func_ = std::move(map_func); }
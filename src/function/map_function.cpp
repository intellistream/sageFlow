#include "function/map_function.h"

candy::MapFunction::MapFunction(std::string name) : Function(std::move(name), FunctionType::Map) {}

candy::MapFunction::MapFunction(std::string name, MapFunc map_func)
    : Function(std::move(name), FunctionType::Map), map_func_(std::move(map_func)) {}

candy::Response candy::MapFunction::Execute(Response &resp) {
  if (resp.type_ == ResponseType::Record) {
    auto record = std::move(resp.record_);
    map_func_(record);
    return Response{ResponseType::Record, std::move(record)};
  }
  if (resp.type_ == ResponseType::List) {
    auto records = std::move(resp.records_);
    auto it = records->begin();
    for (; it != records->end(); ++it) {
      map_func_(*it);
    }
    return Response{ResponseType::List, std::move(records)};
  }
  return {};
}

auto candy::MapFunction::setMapFunc(MapFunc map_func) -> void { map_func_ = std::move(map_func); }
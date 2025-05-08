#include "function/map_function.h"

candy::MapFunction::MapFunction(std::string name) : Function(std::move(name), FunctionType::Map) {}

candy::MapFunction::MapFunction(std::string name, MapFunc map_func)
    : Function(std::move(name), FunctionType::Map), map_func_(std::move(map_func)) {}

candy::DataElement candy::MapFunction::Execute(DataElement &element) {
  if (element.isRecord()) {
    auto record = element.moveRecord();
    map_func_(record);
    return DataElement(std::move(record));
  }
  if (element.isList()) {
    auto records = element.moveRecords();
    for (auto& record : *records) {
      map_func_(record);
    }
    return DataElement(std::move(records));
  }
  return DataElement(); // Return empty data element if input was invalid
}

auto candy::MapFunction::setMapFunc(MapFunc map_func) -> void { map_func_ = std::move(map_func); }
#include "function/function.h"

#include <utility>

namespace candy {
Function::Function(std::string name, FunctionType type) : name_(std::move(name)), type_(type) {}

Function::Function(FunctionType type) : type_(type) {}

Function::~Function() = default;

auto Function::getName() const -> std::string { return name_; }

auto Function::getType() const -> FunctionType { return type_; }

void Function::setName(const std::string& name) { name_ = name; }

void Function::setType(FunctionType type) { type_ = type; }

// 使用 DataElement 替换 Response
auto Function::Execute(DataElement& element) -> DataElement {
  // 创建一个带有移动内容的新 DataElement
  if (element.isRecord() && element.getRecord()) {
    return DataElement(element.moveRecord());
  } else if (element.isList() && element.getRecords()) {
    return DataElement(element.moveRecords());
  }
  return DataElement(); // 空数据元素
}

// 使用 DataElement 替换 Response
auto Function::Execute(DataElement& left, DataElement& right) -> DataElement {
  // 从 left 创建一个带有移动内容的新 DataElement
  if (left.isRecord() && left.getRecord()) {
    return DataElement(left.moveRecord());
  } else if (left.isList() && left.getRecords()) {
    return DataElement(left.moveRecords());
  }
  return DataElement(); // 空数据元素
}

};  // namespace candy

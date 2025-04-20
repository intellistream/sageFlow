#include "function/function.h"

candy::Function::Function(std::string name, FunctionType type) : name_(std::move(name)), type_(type) {}

candy::Function::~Function() = default;

auto candy::Function::getName() const -> std::string { return name_; }

auto candy::Function::getType() const -> FunctionType { return type_; }

void candy::Function::setName(const std::string& name) { name_ = name; }

void candy::Function::setType(const FunctionType type) { type_ = type; }

auto candy::Function::Execute(Response& resp) -> Response { return {}; }

auto candy::Function::Execute(Response& left, Response& right) -> Response {
  return {};
}

#include "function/function.h"

sageFlow::Function::Function(std::string name, FunctionType type) : name_(std::move(name)), type_(type) {}

sageFlow::Function::~Function() = default;

auto sageFlow::Function::getName() const -> std::string { return name_; }

auto sageFlow::Function::getType() const -> FunctionType { return type_; }

void sageFlow::Function::setName(const std::string& name) { name_ = name; }

void sageFlow::Function::setType(const FunctionType type) { type_ = type; }

auto sageFlow::Function::Execute(Response& resp) -> Response { return {}; }

auto sageFlow::Function::Execute(Response& left, Response& right) -> Response {
  return {};
}

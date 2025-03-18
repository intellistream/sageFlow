#include "runtime/function/filter_function.h"

candy::FilterFunction::FilterFunction(std::string name) : Function(std::move(name), FunctionType::Filter) {}

candy::FilterFunction::FilterFunction(std::string name, FilterFunc filter_func)
    : Function(std::move(name), FunctionType::Filter), filter_func_(std::move(filter_func)) {}

std::unique_ptr<candy::VectorRecord> candy::FilterFunction::Execute(std::unique_ptr<VectorRecord>& record) {
  if (filter_func_(record)) {
    return std::move(record);
  }
  return nullptr;
}

auto candy::FilterFunction::setFilterFunc(FilterFunc filter_func) -> void { filter_func_ = std::move(filter_func); }
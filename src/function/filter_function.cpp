#include "function/filter_function.h"

candy::FilterFunction::FilterFunction(std::string name) : Function(std::move(name), FunctionType::Filter) {}

candy::FilterFunction::FilterFunction(std::string name, FilterFunc filter_func)
    : Function(std::move(name), FunctionType::Filter), filter_func_(std::move(filter_func)) {}

auto candy::FilterFunction::Execute(Response &resp) -> candy::Response {
  Response result;
  
  for (auto &record : resp) {
    if (record && filter_func_(record)) {
      result.push_back(std::move(record));
    }
  }
  
  return result;
}

auto candy::FilterFunction::setFilterFunc(FilterFunc filter_func) -> void { filter_func_ = std::move(filter_func); }
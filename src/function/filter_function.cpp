#include "function/filter_function.h"

candy::FilterFunction::FilterFunction(std::string name) : Function(std::move(name), FunctionType::Filter) {}

candy::FilterFunction::FilterFunction(std::string name, FilterFunc filter_func)
    : Function(std::move(name), FunctionType::Filter), filter_func_(std::move(filter_func)) {}

candy::Response candy::FilterFunction::Execute(Response &resp) {
   if (resp.type_ == ResponseType::Record) {
    if (auto record = std::move(resp.record_); filter_func_(record)) {
       return Response{ResponseType::Record, std::move(record)};
     }
   }
  if (resp.type_ == ResponseType::List) {
    auto records = std::move(resp.records_);
    for (auto &record : *records) {
      if (!filter_func_(record)) {
        record.reset();
      }
    }
    return Response{ResponseType::List, std::move(records)};
  }
  return {};
}

auto candy::FilterFunction::setFilterFunc(FilterFunc filter_func) -> void { filter_func_ = std::move(filter_func); }
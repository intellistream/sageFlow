#include "function/sink_function.h"

candy::SinkFunction::SinkFunction(std::string name) : Function(std::move(name), FunctionType::Sink) {}

candy::SinkFunction::SinkFunction(std::string name, SinkFunc sink_func)
    : Function(std::move(name), FunctionType::Sink), sink_func_(std::move(sink_func)) {}

candy::Response candy::SinkFunction::Execute(Response &resp) {
  if (resp.type_ == ResponseType::Record) {
    auto record = std::move(resp.record_);
    sink_func_(record);
    return Response{ResponseType::Record, std::move(record)};
  }
  if (resp.type_ == ResponseType::List) {
    auto records = std::move(resp.records_);
    for (auto &record : *records) {
      sink_func_(record);
    }
    return Response{ResponseType::List, std::move(records)};
  }
  return {};
}

auto candy::SinkFunction::setSinkFunc(SinkFunc sink_func) -> void { sink_func_ = std::move(sink_func); }
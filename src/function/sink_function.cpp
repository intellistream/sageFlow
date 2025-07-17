#include "function/sink_function.h"

candy::SinkFunction::SinkFunction(std::string name) : Function(std::move(name), FunctionType::Sink) {}

candy::SinkFunction::SinkFunction(std::string name, SinkFunc sink_func)
    : Function(std::move(name), FunctionType::Sink), sink_func_(std::move(sink_func)) {}

auto candy::SinkFunction::Execute(Response &resp) -> candy::Response {
  Response result;
  
  for (auto &record : resp) {
    if (record) {
      sink_func_(record);
      result.push_back(std::move(record));
    }
  }
  
  return result;
}

auto candy::SinkFunction::setSinkFunc(SinkFunc sink_func) -> void { sink_func_ = std::move(sink_func); }
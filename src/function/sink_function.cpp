#include "function/sink_function.h"

candy::SinkFunction::SinkFunction(std::string name) : Function(std::move(name), FunctionType::Sink) {}

candy::SinkFunction::SinkFunction(std::string name, SinkFunc sink_func)
    : Function(std::move(name), FunctionType::Sink), sink_func_(std::move(sink_func)) {}

std::unique_ptr<candy::VectorRecord> candy::SinkFunction::Execute(std::unique_ptr<VectorRecord>& record) {
  sink_func_(record);
  return std::move(record);
}

auto candy::SinkFunction::setSinkFunc(SinkFunc sink_func) -> void { sink_func_ = std::move(sink_func); }
#include "function/sink_function.h"

sageFlow::SinkFunction::SinkFunction(std::string name) : Function(std::move(name), FunctionType::Sink) {}

sageFlow::SinkFunction::SinkFunction(std::string name, SinkFunc sink_func)
    : Function(std::move(name), FunctionType::Sink), sink_func_(std::move(sink_func)) {}

sageFlow::Response sageFlow::SinkFunction::Execute(Response &resp) {
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
  if (resp.type_ == ResponseType::RecordPair) {
    auto pair = std::move(resp.pair_);
    if (pair && pair_sink_func_) {
      pair_sink_func_(pair->left, pair->right, pair->similarity);
    }
    return Response{ResponseType::RecordPair, std::move(pair)};
  }
  return {};
}

auto sageFlow::SinkFunction::setSinkFunc(SinkFunc sink_func) -> void { sink_func_ = std::move(sink_func); }

auto sageFlow::SinkFunction::setPairSinkFunc(PairSinkFunc pair_sink_func) -> void {
  pair_sink_func_ = std::move(pair_sink_func);
}
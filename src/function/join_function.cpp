#include "function/join_function.h"

candy::JoinFunction::JoinFunction(std::string name) : Function(std::move(name), FunctionType::Join) {}

candy::JoinFunction::JoinFunction(std::string name, JoinFunc join_func)
    : Function(std::move(name), FunctionType::Join), join_func_(std::move(join_func)) {}

candy::Response candy::JoinFunction::Execute(Response& left, Response& right) {
  if (left.type_ == ResponseType::Record && right.type_ == ResponseType::Record) {
    if (auto left_record = std::move(left.record_), right_record = std::move(right.record_);
        join_func_(left_record, right_record)) {
      return Response{ResponseType::Record, std::move(left_record)};
    }
  }
  return {};
}

auto candy::JoinFunction::setJoinFunc(JoinFunc join_func) -> void { join_func_ = std::move(join_func); }

auto candy::JoinFunction::getOtherStream() -> std::shared_ptr<Stream>& { return other_stream_; }

auto candy::JoinFunction::setOtherStream(std::shared_ptr<Stream> other_plan) -> void {
  other_stream_ = std::move(other_plan);
}
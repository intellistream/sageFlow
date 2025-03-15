#include "runtime/function/join_function.h"

candy::JoinFunction::JoinFunction(std::string name) : Function(std::move(name), FunctionType::Join) {}

candy::JoinFunction::JoinFunction(std::string name, JoinFunc join_func)
    : Function(std::move(name), FunctionType::Join), join_func_(std::move(join_func)) {}

std::unique_ptr<candy::VectorRecord> candy::JoinFunction::Execute(std::unique_ptr<VectorRecord>& left,
                                                                  std::unique_ptr<VectorRecord>& right) {
  if (join_func_(left, right)) {
    return std::move(left);
  }
  return nullptr;
}

auto candy::JoinFunction::setJoinFunc(JoinFunc join_func) -> void { join_func_ = std::move(join_func); }

auto candy::JoinFunction::getOtherStream() -> std::shared_ptr<Stream>& { return other_stream_; }

auto candy::JoinFunction::setOtherStream(std::shared_ptr<Stream> other_plan) -> void {
  other_stream_ = std::move(other_plan);
}
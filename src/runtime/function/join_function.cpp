#include "runtime/function/join_function.h"

candy::JoinFunction::JoinFunction(std::string name) : Function(std::move(name), FunctionType::Join) {}

candy::JoinFunction::JoinFunction(std::string name, JoinFunc join_func, int64_t time_window)
    : Function(std::move(name), FunctionType::Join), join_func_(std::move(join_func)), time_window_(time_window) {}

std::unique_ptr<candy::VectorRecord> candy::JoinFunction::Execute(std::unique_ptr<VectorRecord>& left,
                                                                  std::unique_ptr<VectorRecord>& right) {
  return join_func_(left, right);
}

auto candy::JoinFunction::setJoinFunc(JoinFunc join_func) -> void { join_func_ = std::move(join_func); }

auto candy::JoinFunction::getOtherStream() -> std::shared_ptr<Stream>& { return other_stream_; }

auto candy::JoinFunction::setOtherStream(std::shared_ptr<Stream> other_plan) -> void {
  other_stream_ = std::move(other_plan);
}

auto candy::JoinFunction::setTimeWindow(int64_t time_window) -> void { time_window_ = time_window; }

auto candy::JoinFunction::getTimeWindow() -> int64_t { return time_window_; }
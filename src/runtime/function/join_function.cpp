#include "runtime/function/join_function.h"

candy::JoinFunction::JoinFunction(std::string name) : Function(std::move(name), FunctionType::Join) {}

// TODO : 确定这个滑动窗口的步长
// 目前是 window / 4
candy::JoinFunction::JoinFunction(std::string name, JoinFunc join_func, int64_t time_window)
    : Function(std::move(name), FunctionType::Join), 
    windowL (time_window, time_window / 4), windowR(time_window, time_window / 4), join_func_(std::move(join_func)) {}

std::unique_ptr<candy::VectorRecord> candy::JoinFunction::Execute(std::unique_ptr<VectorRecord>& left,
                                                                  std::unique_ptr<VectorRecord>& right) {
  return join_func_(left, right);
}

auto candy::JoinFunction::setJoinFunc(JoinFunc join_func) -> void { join_func_ = std::move(join_func); }

auto candy::JoinFunction::getOtherStream() -> std::shared_ptr<Stream>& { return other_stream_; }

auto candy::JoinFunction::setOtherStream(std::shared_ptr<Stream> other_plan) -> void {
  other_stream_ = std::move(other_plan);
}

auto candy::JoinFunction::setWindow(int64_t windowsize, int64_t stepsize) -> void { 
  windowL.setWindow(windowsize, stepsize);
  windowR.setWindow(windowsize, stepsize);
}
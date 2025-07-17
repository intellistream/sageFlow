#include "function/join_function.h"

candy::JoinFunction::JoinFunction(std::string name) : Function(std::move(name), FunctionType::Join) {}

candy::JoinFunction::JoinFunction(std::string name, JoinFunc join_func) :
  Function(std::move(name), FunctionType :: Join), join_func_(std::move(join_func)) {}

// TODO(xinyan): 确定这个滑动窗口的步长
// 目前是 window / 4
candy::JoinFunction::JoinFunction(std::string name, JoinFunc join_func, int64_t time_window)
    : Function(std::move(name), FunctionType::Join), 
    window_l_(time_window, time_window / 4), window_r_(time_window, time_window / 4), join_func_(std::move(join_func)) {}

auto candy::JoinFunction::Execute(Response& left, Response& right) -> candy::Response{
  Response result;
  
  // Simple nested loop join - can be optimized later
  for (auto &left_record : left) {
    for (auto &right_record : right) {
      if (left_record && right_record) {
        auto joined_record = join_func_(left_record, right_record);
        if (joined_record) {
          result.push_back(std::move(joined_record));
        }
      }
    }
  }
  
  return result;
}

auto candy::JoinFunction::setJoinFunc(JoinFunc join_func) -> void { join_func_ = std::move(join_func); }

auto candy::JoinFunction::getOtherStream() -> std::shared_ptr<Stream>& { return other_stream_; }

auto candy::JoinFunction::setOtherStream(std::shared_ptr<Stream> other_plan) -> void {
  other_stream_ = std::move(other_plan);
}

auto candy::JoinFunction::setWindow(int64_t time_window, int64_t stepsize) -> void {
  window_l_.setWindow(time_window, stepsize);
  window_r_.setWindow(time_window, stepsize);
}
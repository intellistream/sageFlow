#include "function/join_function.h"
#include "utils/logger.h"

candy::JoinFunction::JoinFunction(std::string name, int dim) : Function(std::move(name), FunctionType::Join), dim_(dim) {}

candy::JoinFunction::JoinFunction(std::string name, JoinFunc join_func, int dim) :
  Function(std::move(name), FunctionType::Join), join_func_(std::move(join_func)), dim_(dim) {}

// TODO : 确定这个滑动窗口的步长
// 目前是 window / 4
candy::JoinFunction::JoinFunction(std::string name, JoinFunc join_func, int64_t time_window, int dim)
    : Function(std::move(name), FunctionType::Join), 
    windowL (time_window, time_window / 4), windowR(time_window, time_window / 4),
    threadSafeWindowL(time_window, time_window / 4), threadSafeWindowR(time_window, time_window / 4),
    join_func_(std::move(join_func)), dim_(dim) {}

auto candy::JoinFunction::Execute(Response& left, Response& right) -> Response {
  if (left.type_ == ResponseType::Record && right.type_ == ResponseType::Record) {
    auto left_record = std::move(left.record_);
    auto right_record = std::move(right.record_);
    try {
      if (join_func_(left_record, right_record)) {
        // 注意：保持现有行为，仅返回 left_record（即使 join_func_ 产生了新结果）
        return Response{ResponseType::Record, std::move(left_record)};
      }
    } catch (const std::exception& e) {
            CANDY_LOG_ERROR("JOIN_FUNC", "left_dim={} right_dim={} left_uid={} right_uid={} what={} ",
                            (left_record ? left_record->data_.dim_ : -1),
                            (right_record ? right_record->data_.dim_ : -1),
                            (left_record ? left_record->uid_ : 0),
                            (right_record ? right_record->uid_ : 0),
                            e.what());
      throw;
    }
  }
  return {};
}

auto candy::JoinFunction::getDim() const -> int { return dim_; }

auto candy::JoinFunction::setJoinFunc(JoinFunc join_func) -> void { join_func_ = std::move(join_func); }

auto candy::JoinFunction::getOtherStream() -> std::shared_ptr<Stream>& { return other_stream_; }

auto candy::JoinFunction::setOtherStream(std::shared_ptr<Stream> other_plan) -> void {
  other_stream_ = std::move(other_plan);
}

auto candy::JoinFunction::setWindow(int64_t windowsize, int64_t stepsize) -> void { 
  windowL.setWindow(windowsize, stepsize);
  windowR.setWindow(windowsize, stepsize);
  threadSafeWindowL.setWindow(windowsize, stepsize);
  threadSafeWindowR.setWindow(windowsize, stepsize);
}
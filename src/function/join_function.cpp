#include "function/join_function.h"
#include "stream/time/sliding_window.h"

candy::JoinFunction::JoinFunction(std::string name) : Function(std::move(name), FunctionType::Join) {}

candy::JoinFunction::JoinFunction(std::string name, JoinFunc join_func) :
  Function(std::move(name), FunctionType::Join), join_func_(std::move(join_func)) {}

candy::JoinFunction::JoinFunction(std::string name, JoinFunc join_func, int64_t time_window) :
  Function(std::move(name), FunctionType::Join), 
  join_func_(std::move(join_func)) {
    // Calculate appropriate slide for the sliding window (1/4 of window size)
    timestamp_t time_slide = time_window / 4;
    // Set default windows
    windowL = std::make_shared<SlidingWindow>(0, time_window, time_window, time_slide);
    windowR = std::make_shared<SlidingWindow>(0, time_window, time_window, time_slide);
}

candy::DataElement candy::JoinFunction::Execute(DataElement& left, DataElement& right) {
  // Check that both elements contain single records
  if (left.isRecord() && right.isRecord()) {
    // Get the records
    auto left_record = left.moveRecord();
    auto right_record = right.moveRecord();
    
    // Apply join function
    if (join_func_(left_record, right_record)) {
      // Return the joined record as a new DataElement
      return DataElement(std::move(left_record));
    }
  }
  // Return empty element if join failed or inputs were invalid
  return DataElement();
}

auto candy::JoinFunction::setJoinFunc(JoinFunc join_func) -> void { join_func_ = std::move(join_func); }

auto candy::JoinFunction::getOtherStream() -> std::shared_ptr<Stream>& { return other_stream_; }

auto candy::JoinFunction::setOtherStream(std::shared_ptr<Stream> other_plan) -> void {
  other_stream_ = std::move(other_plan);
}

auto candy::JoinFunction::setWindow(int64_t window_size, int64_t slide_size) -> void {
  // Create new window objects with the updated parameters
  windowL = std::make_shared<SlidingWindow>(0, window_size, window_size, slide_size);
  windowR = std::make_shared<SlidingWindow>(0, window_size, window_size, slide_size);
}
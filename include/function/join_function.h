#pragma once
#include <functional>
#include <cassert>

#include "function/function.h"
#include "stream/stream.h"

namespace candy {
  using JoinFunc = std::function<std::unique_ptr<VectorRecord>(std::unique_ptr<VectorRecord> &, std::unique_ptr<VectorRecord> &)>;
  // TODO(xinyan): 没有删掉的草台班子滑动窗口
  class SlidingWindow {
  private :
    int64_t window_size_;
    int64_t step_size_;
    int64_t last_emitted_;
  public :
    SlidingWindow() {
      window_size_ = 10;
      step_size_ = 5;
      last_emitted_ = -1;
    }
    SlidingWindow(int64_t windowsize, int64_t stepsize) : 
      window_size_(windowsize), step_size_(stepsize), last_emitted_(-1) {}
  
    void setWindow(int64_t windowsize, int64_t stepsize) {
      window_size_ = windowsize;
      step_size_ = stepsize;
      last_emitted_ = -1;
    }
    
    auto windowTimeLimit(int64_t timestamp) -> int {
      // 返回窗口可以容忍的最靠前时间
      return timestamp - window_size_;
    }
  
    auto isNeedTrigger(int64_t timestamp) -> bool {
      // 判断是否需要触发窗口， 并移动lastEmitted一个步长的距离
      if(last_emitted_ == -1) {
        last_emitted_ = timestamp;
      }
      bool result = false;
      assert(timestamp >= last_emitted_);
      // TODO(xinyan): timestamp - lastEmitted >= windowSize
      result = timestamp - last_emitted_ >= 0;
      last_emitted_ += step_size_;
      if (last_emitted_ > timestamp) {
        last_emitted_ = timestamp;
}
      return result;
    }
  };
  
  class JoinFunction final : public Function {
   public:
    explicit JoinFunction(std::string name);
  
    explicit JoinFunction(std::string name, JoinFunc join_func);
    explicit JoinFunction(std::string name, JoinFunc join_func, int64_t time_window);
    
    auto Execute(Response &left, Response &right)
        -> Response override;
  
    auto setJoinFunc(JoinFunc join_func) -> void;
  
    auto getOtherStream() -> std::shared_ptr<Stream> &;
  
    auto setOtherStream(std::shared_ptr<Stream> other_plan) -> void;
  
    auto setWindow(int64_t time_window, int64_t stepsize) -> void;
  
    SlidingWindow window_l_, window_r_;
  
   private:
    JoinFunc join_func_;
    std::shared_ptr<Stream> other_stream_ = nullptr;
    // TODO(xinyan): 把Window逻辑扩展
    // 现在的 window 是固定长度步长的滑动窗口
  };
  };  // namespace candy
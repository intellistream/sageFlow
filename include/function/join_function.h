#pragma once
#include <algorithm>
#include <functional>
#include <assert.h>

#include "function/function.h"
#include "stream/stream.h"

namespace candy {
  using JoinFunc = std::function<std::unique_ptr<VectorRecord>(std::unique_ptr<VectorRecord> &, std::unique_ptr<VectorRecord> &)>;
  // TODO: 没有删掉的草台班子滑动窗口
  class SlidingWindow {
  private :
    int64_t windowSize;
    int64_t stepSize;
    int64_t lastEmitted;
  public :
    SlidingWindow() {
      windowSize = 10;
      stepSize = 5;
      lastEmitted = -1;
    }
    SlidingWindow(int64_t windowsize, int64_t stepsize) : 
      windowSize(windowsize), stepSize(stepsize), lastEmitted(-1) {}
  
    void setWindow(int64_t windowsize, int64_t stepsize) {
      windowSize = windowsize;
      stepSize = stepsize;
      lastEmitted = -1;
    }
    
    auto windowTimeLimit(int64_t timestamp) const -> int {
      // 返回窗口可以容忍的最靠前时间
      return timestamp - windowSize;
    }
  
    auto isNeedTrigger(int64_t timestamp) -> bool {
      // 判断是否需要触发窗口， 并移动lastEmitted一个步长的距离
      if(lastEmitted == -1) {
        lastEmitted = timestamp;
      }
      bool result = false;
      assert(timestamp >= lastEmitted);
      // TODO : timestamp - lastEmitted >= windowSize
      result = (timestamp - lastEmitted >= 0);
      // if (timestamp - lastEmitted >= 0) {
      //   result = true;
      // }
      // else {
      //   result = false;
      // }
      lastEmitted += stepSize;
      lastEmitted = std::min(lastEmitted, timestamp);
      return result;
    }
  };
  
  class JoinFunction final : public Function {
   public:
    explicit JoinFunction(std::string name, int dim);
  
    explicit JoinFunction(std::string name, JoinFunc join_func, int dim);
    explicit JoinFunction(std::string name, JoinFunc join_func, int64_t time_window, int dim);
    
    auto Execute(Response &left, Response &right)
        -> Response override;
  
    auto setJoinFunc(JoinFunc join_func) -> void;
  
    auto getOtherStream() -> std::shared_ptr<Stream> &;

    auto getDim() const -> int;
  
    auto setOtherStream(std::shared_ptr<Stream> other_plan) -> void;
  
    auto setWindow(int64_t time_window, int64_t stepsize) -> void;
  
    SlidingWindow windowL, windowR;
  
   private:
    JoinFunc join_func_;
    int dim_ = 0;
    std::shared_ptr<Stream> other_stream_ = nullptr;
    // TODO : 把Window逻辑扩展
    // 现在的 window 是固定长度步长的滑动窗口
  };
  };  // namespace candy
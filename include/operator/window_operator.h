#pragma once
#include <functional>
#include <list>
#include <memory>
#include <mutex>

#include "common/data_types.h"
#include "operator/operator.h"

namespace candy {
class WindowOperator : public Operator {
 public:
  explicit WindowOperator(std::unique_ptr<Function> &window_func);

  auto process(Response &data, int slot) -> std::optional<Response> override;

  auto apply(Response &&record, int slot, Collector &collector) -> void override;

  int window_size_;

 protected:
  // 多线程改造：添加窗口状态保护的互斥锁
  mutable std::mutex window_mutex_;

 private:
  std::unique_ptr<Function> window_func_;
};

class TumblingWindowOperator final : public WindowOperator {
 public:
  explicit TumblingWindowOperator(std::unique_ptr<Function> &window_func);

  auto process(Response &data, int slot) -> std::optional<Response> override;

  auto apply(Response &&record, int slot, Collector &collector) -> void override;

 private:
  std::list<std::unique_ptr<VectorRecord>> window_buffer_;
};

class SlidingWindowOperator final : public WindowOperator {
 public:
  explicit SlidingWindowOperator(std::unique_ptr<Function> &window_func);

  auto process(Response &data, int slot) -> std::optional<Response> override;

  auto apply(Response &&record, int slot, Collector &collector) -> void override;

 private:
  std::list<std::unique_ptr<VectorRecord>> window_buffer_;
  int slide_size_;
};
}  // namespace candy
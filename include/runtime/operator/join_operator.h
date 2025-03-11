#pragma once
#include <streaming/logical_plan.h>
#include <functional>
#include <memory>
#include <queue>
#include <mutex>
#include <condition_variable>
#include "streaming/task/task.h"
#include "runtime/function/join_function.h"

#include "core/common/data_types.h"
#include "runtime/operator/base_operator.h"
#include <iostream>

#define debug(...) fprintf(stderr, __VA_ARGS__)

namespace candy {
class JoinOperator : public Operator {
 public:
  JoinOperator(const std::string &name,
               candy :: JoinFunction &join_func,
                                  std::unique_ptr<candy::Task> &other_task)
      : Operator(OperatorType::JOIN, name), join_func_(std::move(join_func)), other_task_(std::move(other_task)) {}

  // 处理单个向量data（JOIN里 没什么用）
  auto process(std::unique_ptr<VectorRecord> &data) -> bool override { return true; }

  // 重写 process_queue 以等待左右两侧数据
  void process_queue() override {
    int id = 0;
    debug("JoinOperator::process_queue\n");
    while (running_) {
      
      debug("id = %d\n", id);
      
      
      std::unique_ptr<VectorRecord> left = nullptr;
      std::unique_ptr<VectorRecord> right = nullptr;

      {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        // 等待左侧队列数据
        queue_cv_.wait(lock, [this] { return !input_queue_.empty() || !running_; });
        if (!running_) break;
        left = std::move(input_queue_.front());
        input_queue_.pop();
      }

      debug("left one arrived!");

      {
        std::unique_lock<std::mutex> lock(right_mutex_);
        //auto right_cv_ = other_task_.get()->operator_[0].get()->GetQueueCV();
        // 等待右侧队列数据
        right_cv_.wait(lock, [this] { return !right_queue_.empty() || !running_; });
        if (!running_ && right_queue_.empty()) break;
        right = std::move(right_queue_.front());
        right_queue_.pop();
      }

      debug("right one arrived!");
      // 调用 join 函数进行关联处理
      auto result = join_func_.Execute(left, right);
      emit(0, result);
    }
  }

  //auto GetOtherPlan() -> std::unique_ptr<LogicalPlan> & { return other_plan_; }
  //auto GetOtherTask() -> std :: unique_ptr<candy::Task> & { return other_task_; }

 private:
  // anns index
  //std::function<bool(std::unique_ptr<VectorRecord> &, std::unique_ptr<VectorRecord> &)> join_func_;
  candy :: JoinFunction join_func_;
  std::unique_ptr<candy::Task> other_task_;
  std::queue<std::unique_ptr<VectorRecord>> right_queue_;
  std::mutex right_mutex_;
  //std::condition_variable right_cv_;
};
}  // namespace candy
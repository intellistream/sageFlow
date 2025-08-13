//
// Created by ZeroJustMe on 25-7-21.
//

#pragma once

#include "operator/operator.h"
#include "execution/input_gate.h"
#include "execution/result_partition.h"
#include <thread>
#include <atomic>
#include <iostream>

namespace candy {
class ExecutionVertex {
private:
  std::shared_ptr<Operator> operator_;
  std::unique_ptr<InputGate> input_gate_;
  std::unique_ptr<ResultPartition> result_partition_;
  std::unique_ptr<std::thread> thread_;
  std::atomic<bool> running_{false};
  size_t subtask_index_;
  std::string name_;

public:
  ExecutionVertex(const std::shared_ptr<Operator> &op, size_t index);

  ~ExecutionVertex();

  // 启动执行顶点
  void start();

  // 停止执行顶点
  void stop();

  // 等待执行完成
  void join()const;

  // 检查是否正在运行
  auto isRunning() const -> bool { return running_; }

  // 获取输入门和输出分区的引用，用于连接上下游
  auto getInputGate() const -> InputGate * { return input_gate_.get(); }
  auto getResultPartition() const -> ResultPartition * { return result_partition_.get(); }

  // 获取算子和子任务索引
  auto getOperator() const -> std::shared_ptr<Operator> { return operator_; }
  auto getSubtaskIndex() const -> size_t { return subtask_index_; }

private:
  void run()const;
};
} // namespace candy

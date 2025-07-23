//
// Created by ZeroJustMe on 25-7-22.
//

#include "execution/execution_vertex.h"

namespace candy {

ExecutionVertex::ExecutionVertex(const std::shared_ptr<Operator> &op, const size_t index)
  : operator_(op), subtask_index_(index) {
  input_gate_ = std::make_unique<InputGate>();
  result_partition_ = std::make_unique<ResultPartition>();
  name_ = op->name + "[" + std::to_string(index) + "]";
}

ExecutionVertex::~ExecutionVertex() {
  stop();
  if (thread_ && thread_->joinable()) {
    thread_->join();
  }
}

void ExecutionVertex::start() {
  if (running_.exchange(true)) {
    return; // 已经在运行
  }

  thread_ = std::make_unique<std::thread>(&ExecutionVertex::run, this);
}

void ExecutionVertex::stop() {
  running_ = false;
}

void ExecutionVertex::join() {
  if (thread_ && thread_->joinable()) {
    thread_->join();
  }
}

void ExecutionVertex::run() {
  std::cout << name_ << " started on thread " << std::this_thread::get_id() << std::endl;

  try {
    // 打开算子
    operator_->open();

    while (running_) {
      // 从输入门读取上游数据
      std::optional<Response> data_opt = input_gate_->read();
      if (!data_opt) {
        // 队列停止且为空，或者没有可用数据
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        continue;
      }

      // 调用算子处理逻辑
      Response data = std::move(*data_opt);
      bool processed = operator_->process(data, 0); // slot默认为0

      if (processed) {
        // 通过输出分区将结果发射给下游
        result_partition_->emit(std::move(data));
      }
    }
  } catch (const std::exception& e) {
    std::cerr << "Exception in " << name_ << ": " << e.what() << std::endl;
  }

  // 关闭算子
  operator_->close();

  std::cout << name_ << " finished" << std::endl;
}

}

//
// Created by ZeroJustMe on 25-7-22.
//

#include "execution/execution_vertex.h"
#include "execution/collector.h"
#include "operator/operator_api.h"
#include "spdlog/spdlog.h"
#include <spdlog/fmt/fmt.h>

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

void ExecutionVertex::join() const {
  if (thread_ && thread_->joinable()) {
    thread_->join();
  }
}

void ExecutionVertex::run() const {
  std::cout << name_ << " started on thread " << std::this_thread::get_id() << std::endl;

  auto source_op = dynamic_cast<OutputOperator*>(operator_.get());
  try {
    // 打开算子
    operator_->open();
    // 创建collector，将emit操作注册到collector中
    Collector collector([this](std::unique_ptr<Response> response, int slot) {
      if (response) {
        result_partition_->emit(std::move(*response), slot);
      }
    });
    collector.set_slot_size(result_partition_->get_slot_size());

    if (source_op != nullptr) [[unlikely]] {
      // 如果是OutputOperator，直接从数据源读取数据
      source_op->run(collector);
    } else {
      while (running_) {
        // 从输入门读取上游数据
        std::optional<TaggedResponse> data_opt = input_gate_->read();
        if (!data_opt) {
          // 队列停止且为空，或者没有可用数据
          std::this_thread::sleep_for(std::chrono::microseconds(100));
          continue;
        }

        // 调用算子的apply方法处理逻辑
        Response data = std::move(data_opt->response);
        operator_->apply(std::move(data), data_opt->slot, collector);
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

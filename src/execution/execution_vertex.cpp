//
// Created by ZeroJustMe on 25-7-22.
//

#include "execution/execution_vertex.h"
#include "execution/collector.h"
#include "operator/operator_api.h"
#include "spdlog/spdlog.h"
#include <spdlog/fmt/fmt.h>
#include "utils/logger.h"

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
  CANDY_LOG_INFO("VERTEX", "{} started thread={} ", name_, (size_t)std::hash<std::thread::id>{}(std::this_thread::get_id()));

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
    // 注入真实的 slot 列表，以支持广播到非零起始的 slot 键
    collector.set_slots(result_partition_->get_slots());

    if (source_op != nullptr) [[unlikely]] {
      // 如果是OutputOperator，直接从数据源读取数据
      source_op->run(collector);
    } else {
      while (running_) {
        // 从输入门读取上游数据
        std::optional<TaggedResponse> data_opt = input_gate_->read();
        if (!data_opt) {
          // 队列暂无可用数据
          std::this_thread::sleep_for(std::chrono::microseconds(100));
          continue;
        }

        // 调用算子的apply方法处理逻辑
        Response data = std::move(data_opt->response);
        try {
          operator_->apply(std::move(data), data_opt->slot, collector);
        } catch (const std::exception& e) {
          int dim = (data.record_ ? data.record_->data_.dim_ : -1);
          uint64_t uid = (data.record_ ? data.record_->uid_ : 0);
          CANDY_LOG_ERROR("APPLY", "operator={} slot={} dim={} uid={} what={} ", operator_->name, data_opt->slot, dim, uid, e.what());
          throw;
        }
      }

      // 运行标志关闭后，尝试一次性排干剩余队列，避免尚未处理的数据导致状态不一致
      while (true) {
        std::optional<TaggedResponse> data_opt = input_gate_->read();
        if (!data_opt) break; // 没有残留数据
        Response data = std::move(data_opt->response);
        try {
          operator_->apply(std::move(data), data_opt->slot, collector);
        } catch (const std::exception& e) {
          int dim = (data.record_ ? data.record_->data_.dim_ : -1);
          uint64_t uid = (data.record_ ? data.record_->uid_ : 0);
          CANDY_LOG_ERROR("DRAIN", "operator={} slot={} dim={} uid={} what={} ", operator_->name, data_opt->slot, dim, uid, e.what());
          break; // 排干阶段出现异常不再继续，防止无限重试
        }
      }
    }
  } catch (const std::exception& e) {
    CANDY_LOG_ERROR("VERTEX", "Exception name={} what={} ", name_, e.what());
  }

  // 关闭算子
  operator_->close();

  CANDY_LOG_INFO("VERTEX", "{} finished", name_);
}

}

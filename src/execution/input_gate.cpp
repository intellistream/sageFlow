//
// Created by ZeroJustMe on 25-7-22.
//

#include "execution/input_gate.h"

namespace candy {

void InputGate::setup(const std::vector<QueuePtr>& queues) {
  input_queues_ = queues;
}

void InputGate::setup(std::vector<QueuePtr>&& queues) {
  input_queues_ = std::move(queues);
}

std::optional<TaggedResponse> InputGate::read() {
  if (input_queues_.empty()) {
    return std::nullopt;
  }

  const size_t num_queues = input_queues_.size();
  for (size_t i = 0; i < num_queues; ++i) {
    // 从上一次成功的位置开始轮询
    size_t current_idx = (poll_index_ + i) % num_queues;

    auto data = input_queues_[current_idx]->pop();
    if (data) {
      // 如果成功读取，更新下一次轮询的起始位置
      poll_index_ = current_idx + 1;
      return data;
    }
  }

  return std::nullopt;
}
}
//
// Created by ZeroJustMe on 25-7-22.
//

#pragma once

#include "execution/ring_buffer_queue.h"
#include <vector>
#include <memory>
#include <optional>

namespace candy {
class InputGate {
private:
  std::vector<QueuePtr> input_queues_;
  size_t poll_index_ = 0;

public:
  // 在部署时，由调度器调用
  void setup(const std::vector<QueuePtr>& queues);
  void setup(std::vector<QueuePtr>&& queues);
  // TODO: 后续优化轮询 read 方法使其更高效
  std::optional<TaggedResponse> read();
};
}
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

  // TODO: 后续改为更高效的轮询 read 方法从上游获取数据
  std::optional<Response> read();
};
}
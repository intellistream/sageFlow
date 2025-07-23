//
// Created by ZeroJustMe on 25-7-22.
//

#include "execution/ring_buffer_queue.h"

namespace candy {
bool RingBufferQueue::push(Response&& value) {
  const auto current_tail = tail_.load(std::memory_order_relaxed);
  const auto next_tail = (current_tail + 1) % size_;

  // 检查队列是否已满 (head 追上了 tail)
  if (next_tail == head_.load(std::memory_order_acquire)) {
    return false; // 队列满
  }

  buffer_[current_tail] = std::move(value);
  tail_.store(next_tail, std::memory_order_release);
  return true;
}

std::optional<Response> RingBufferQueue::pop() {
  const auto current_head = head_.load(std::memory_order_relaxed);

  // 检查队列是否为空
  if (current_head == tail_.load(std::memory_order_acquire)) {
    return std::nullopt; // 队列空
  }

  auto value = std::move(buffer_[current_head]);
  head_.store((current_head + 1) % size_, std::memory_order_release);
  return value;
}
}
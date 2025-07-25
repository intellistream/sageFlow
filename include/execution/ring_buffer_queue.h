//
// Created by ZeroJustMe on 25-7-22.
//

#pragma once

#include <queue>
#include <condition_variable>
#include <optional>
#include "common/data_types.h"

namespace candy {
class RingBufferQueue {
public:
  explicit RingBufferQueue(size_t capacity)
        : size_(capacity + 1), buffer_(size_), head_(0), tail_(0) {}

  bool push(Response&& value);

  std::optional<Response> pop();

private:
  const size_t size_;
  std::vector<Response> buffer_;

  // head 和 tail 由不同的线程访问，放在不同的缓存行以避免伪共享
  alignas(64) std::atomic<size_t> head_;
  alignas(64) std::atomic<size_t> tail_;
};

using QueuePtr = std::shared_ptr<RingBufferQueue>;
}
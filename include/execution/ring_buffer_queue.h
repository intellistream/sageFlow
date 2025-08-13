//
// Created by ZeroJustMe on 25-7-22.
//

#pragma once

#include <queue>
#include <condition_variable>
#include <optional>
#include "common/data_types.h"
#include "execution/iqueue.h"

namespace candy {
// 适用于单生产者单消费者场景，使用环形缓冲区实现
class RingBufferQueue final : public IQueue {
public:
  explicit RingBufferQueue(const size_t capacity)
        : IQueue(capacity + 1), buffer_(size_), head_(0), tail_(0) {}

  bool push(TaggedResponse&& value) override;

  bool push(const TaggedResponse& value) override;

  std::optional<TaggedResponse> pop() override;

private:
  std::vector<TaggedResponse> buffer_;

  // head 和 tail 由不同的线程访问，放在不同的缓存行以避免伪共享
  alignas(64) std::atomic<size_t> head_;
  alignas(64) std::atomic<size_t> tail_;
};
}
//
// Created by ZeroJustMe on 25-7-30.
//

#pragma once

#include <queue>
#include <condition_variable>
#include <optional>
#include "common/data_types.h"

namespace candy {

struct TaggedResponse {
  Response response;
  int slot;
  TaggedResponse() : slot(0) {}
  TaggedResponse(Response res, int s) : response(std::move(res)), slot(s) {}
};

//TODO: 考虑使用现有的c++高性能队列替代IQueue
class IQueue {
public:
  explicit IQueue(size_t capacity) : size_(capacity + 1) {}
  virtual ~IQueue() = default;
  virtual bool push(const TaggedResponse& value) = 0;
  virtual bool push(TaggedResponse&& value) = 0;
  virtual std::optional<TaggedResponse> pop() = 0;
  // 允许停止队列以唤醒阻塞中的消费者/生产者（RingBuffer 可为 no-op）
  virtual void stop() = 0;

protected:
  const size_t size_;
};

using QueuePtr = std::shared_ptr<IQueue>;

} // namespace candy
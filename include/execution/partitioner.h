//
// Created by ZeroJustMe on 25-7-22.
//

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include "common/data_types.h"

namespace candy {
class IPartitioner {
public:
  virtual ~IPartitioner() = default;
  virtual size_t partition(const Response& data, size_t num_channels) = 0;
};

// 轮询/随机分发
class RoundRobinPartitioner : public IPartitioner {
private:
  std::atomic<size_t> counter_ = 0;
public:
  size_t partition(const Response&, size_t num_channels) override {
    return counter_++ % num_channels;
  }
};

// TODO: 按Key分区分发
// class KeyPartitioner : public IPartitioner {
//
// }

};
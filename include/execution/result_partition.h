//
// Created by ZeroJustMe on 25-7-22.
//

#pragma once

#include "execution/ring_buffer_queue.h"
#include "execution/partitioner.h"
#include "common/data_types.h"
#include <vector>
#include <memory>
#include <unordered_map>

namespace candy {
class ResultPartition {
private:
  std::unique_ptr<IPartitioner> partitioner_;
  // 存储每个 slot 对应的输出通道列表
  // 使用时需要保证InputGate(QueuePtr的内存地址)的生命周期严格长于ResultPartition
  std::unordered_map<int, const std::vector<QueuePtr>*> channel_slot_map_;
public:
  void setup(std::unique_ptr<IPartitioner> p, const std::vector<QueuePtr>& channels, int slot);

  void emit(Response&& data, int slot) const;

  auto get_slot_size() const -> size_t {
    return channel_slot_map_.size();
  }
};

}
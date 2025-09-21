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
  // 存储每个 slot 对应的输出通道列表（拷贝持有，避免悬垂）
  std::unordered_map<int, std::vector<QueuePtr>> channel_slot_map_;
public:
  void setup(std::unique_ptr<IPartitioner> p, std::vector<QueuePtr> channels, int slot);

  void emit(Response&& data, int slot) const;

  auto get_slot_size() const -> size_t { return channel_slot_map_.size(); }

  // 获取当前注册的所有 slot 键
  auto get_slots() const -> std::vector<int> {
    std::vector<int> keys;
    keys.reserve(channel_slot_map_.size());
    for (const auto &kv : channel_slot_map_) keys.push_back(kv.first);
    return keys;
  }

  // 获取指定 slot 的通道数量（用于调试日志）
  auto get_channel_count(int slot) const -> size_t {
    auto it = channel_slot_map_.find(slot);
    return it == channel_slot_map_.end() ? 0 : it->second.size();
  }
};

}
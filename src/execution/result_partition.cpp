//
// Created by ZeroJustMe on 25-7-22.
//

#include "execution/result_partition.h"

#include <chrono>
#include <thread>

namespace sageFlow {
void ResultPartition::setup(std::unique_ptr<IPartitioner> p, std::vector<QueuePtr> channels, int slot) {
  partitioner_ = std::move(p);
  channel_slot_map_[slot] = std::move(channels);
}

void ResultPartition::emit(Response&& data, int slot) const {
  if (!channel_slot_map_.contains(slot)) {
    throw std::runtime_error("ResultPartition::emit: Slot not found in channel map.");
  }
  const auto& output_channels_ = channel_slot_map_.at(slot);
  if (output_channels_.empty()) return;
  
  // 检查分区器是否已初始化
  if (!partitioner_) {
    throw std::runtime_error("ResultPartition::emit: Partitioner not initialized.");
  }
  
  // 带重试的 push 操作，避免队列满时静默丢弃数据
  auto pushWithRetry = [](const QueuePtr& queue, TaggedResponse&& tagged) {
    constexpr int kMaxRetries = 1000;        // 最大重试次数
    constexpr int kRetryDelayUs = 100;       // 每次重试等待 100 微秒
    for (int retry = 0; retry < kMaxRetries; ++retry) {
      if (queue->push(std::move(tagged))) {
        return true;
      }
      // 队列满，短暂等待后重试
      std::this_thread::sleep_for(std::chrono::microseconds(kRetryDelayUs));
    }
    // 超过最大重试次数，记录警告（实际生产环境可能需要更好的处理）
    return false;
  };

  // 检查是否为多播模式（Clustered Join 边界向量复制）
  if (partitioner_->supportsMulticast()) {
    // 获取所有目标分区
    auto targets = partitioner_->partitionMulti(data, output_channels_.size());
    
    if (targets.size() == 1) {
      // 单播：直接发送到唯一的目标分区
      size_t idx = targets[0] % output_channels_.size();
      pushWithRetry(output_channels_[idx], {std::move(data), slot});
    } else {
      // 多播：复制数据到所有目标分区
      for (size_t i = 0; i < targets.size(); ++i) {
        size_t idx = targets[i] % output_channels_.size();
        if (i == targets.size() - 1) {
          // 最后一个目标使用 move
          pushWithRetry(output_channels_[idx], {std::move(data), slot});
        } else {
          // 其他目标使用拷贝
          Response data_copy{data.type_, 
                            data.record_ ? std::make_unique<VectorRecord>(*data.record_) : nullptr};
          pushWithRetry(output_channels_[idx], {std::move(data_copy), slot});
        }
      }
    }
  } else if (partitioner_->isBroadcast()) {
    // 广播模式：将数据发送到所有通道（保持原有逻辑）
    for (size_t i = 0; i < output_channels_.size(); ++i) {
      if (i == output_channels_.size() - 1) {
        // 最后一个通道，移动数据
        pushWithRetry(output_channels_[i], {std::move(data), slot});
      } else {
        // 其他通道，复制数据
        Response data_copy{data.type_, data.record_ ? std::make_unique<VectorRecord>(*data.record_) : nullptr};
        pushWithRetry(output_channels_[i], {std::move(data_copy), slot});
      }
    }
  } else {
    // 普通分区模式：根据分区器选择一个通道
    size_t channel_index = partitioner_->partition(data, output_channels_.size());
    pushWithRetry(output_channels_[channel_index], {std::move(data), slot});
  }
}
}

//
// Created by ZeroJustMe on 25-7-22.
//

#include "execution/result_partition.h"

#include <chrono>
#include <thread>
#include <fstream>
#include <mutex>
#include <sstream>

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

  // 首先调用 partition() 来触发训练样本收集（如果是冷启动阶段）
  // 这一步是必须的，因为 addTrainingSample() 在 partition() 内部被调用
  // 即使在广播模式下也需要调用以累积训练样本并触发训练
  partitioner_->partition(data, output_channels_.size());
  
  // 检查是否为广播模式（冷启动阶段）
  // 注意：isBroadcast() 可能在 partition() 调用后改变状态（训练完成后变为 false）
  if (partitioner_->isBroadcast()) {
    // 广播模式：将数据发送到所有通道
    // Build routing_mask: all channels are targets
    uint64_t route_mask = 0;
    for (size_t i = 0; i < output_channels_.size() && i < 64; ++i) {
      route_mask |= (uint64_t{1} << i);
    }

    for (size_t i = 0; i < output_channels_.size(); ++i) {
      if (i == output_channels_.size() - 1) {
        // 最后一个通道，移动数据
        if (data.record_) data.record_->routing_mask_ = route_mask;
        pushWithRetry(output_channels_[i], {std::move(data), slot});
      } else {
        // 其他通道，复制数据
        Response data_copy{data};
        if (data_copy.record_) data_copy.record_->routing_mask_ = route_mask;
        pushWithRetry(output_channels_[i], {std::move(data_copy), slot});
      }
    }
  } else if (partitioner_->supportsMulticast()) {
    // 多播模式：获取所有目标分区并发送
    // 这用于边界向量复制（如 ClusteredJoin 的 multicast_k > 1）
    auto target_channels = partitioner_->partitionMulti(data, output_channels_.size());
    
    // Build routing_mask from multicast targets
    uint64_t route_mask = 0;
    for (size_t ch : target_channels) {
      if (ch < 64) route_mask |= (uint64_t{1} << ch);
    }

    if (target_channels.size() == 1) {
      // 只有一个目标，直接移动
      if (data.record_) data.record_->routing_mask_ = route_mask;
      pushWithRetry(output_channels_[target_channels[0]], {std::move(data), slot});
    } else {
      // 多个目标，复制数据到前 n-1 个，移动到最后一个
      for (size_t i = 0; i < target_channels.size(); ++i) {
        if (i == target_channels.size() - 1) {
          if (data.record_) data.record_->routing_mask_ = route_mask;
          pushWithRetry(output_channels_[target_channels[i]], {std::move(data), slot});
        } else {
          Response data_copy{data};
          if (data_copy.record_) data_copy.record_->routing_mask_ = route_mask;
          pushWithRetry(output_channels_[target_channels[i]], {std::move(data_copy), slot});
        }
      }
    }
  } else {
    // 单播模式：发送到单个目标分区
    size_t channel_index = partitioner_->partition(data, output_channels_.size());
    // Single target: set routing_mask to just that channel
    if (data.record_ && channel_index < 64) {
      data.record_->routing_mask_ = (uint64_t{1} << channel_index);
    }
    pushWithRetry(output_channels_[channel_index], {std::move(data), slot});
  }
}
}  // namespace sageFlow

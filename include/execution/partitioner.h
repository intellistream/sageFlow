//
// Created by ZeroJustMe on 25-7-22.
//

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include "common/data_types.h"
#include "execution/vector_space_partitioner.h"

namespace sageFlow {
class IPartitioner {
public:
  virtual ~IPartitioner() = default;
  virtual size_t partition(const Response& data, size_t num_channels) = 0;
  // 是否为广播模式（默认false），供ResultPartition检测使用
  virtual bool isBroadcast() const { return false; }
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

// 按Key分区分发 - 基于timestamp确保记录按时序顺序分配
// 对于Join算子，确保相同时间段的记录路由到同一个实例，保证插入共享索引的顺序稳定性
class KeyPartitioner : public IPartitioner {
public:
  size_t partition(const Response& data, size_t num_channels) override {
    if (!data.record_) {
      return 0;  // 默认分区
    }
    // 使用timestamp作为分区key，确保时序相近的记录到达同一实例
    // 这样可以保证共享索引中的插入顺序相对稳定，避免因调度顺序导致的竞态
    return std::hash<int64_t>{}(data.record_->timestamp_) % num_channels;
  }
};

// 基于向量内容的哈希分区 - 用于确保相似向量分配到同一实例
class VectorHashPartitioner : public IPartitioner {
public:
  size_t partition(const Response& data, size_t num_channels) override {
    if (!data.record_ || data.record_->data_.dim_ == 0) {
      return 0;
    }
    // 使用向量的前几个维度计算哈希，平衡计算开销和分区质量
    size_t hash = 0;
    const int dims_to_hash = std::min(8, data.record_->data_.dim_);
    for (int i = 0; i < dims_to_hash; ++i) {
      // 组合哈希值
      hash ^= std::hash<float>{}(data.record_->data_.data_[i]) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
    }
    return hash % num_channels;
  }
};

// 广播分区器 - 将每条记录发送到所有下游实例
// 保留此接口供未来使用：当不使用共享索引而是在分区内使用局部索引时，需要广播分发
// 注意：广播会增加网络/队列开销和内存使用，仅在必要时使用
class BroadcastPartitioner : public IPartitioner {
public:
  size_t partition(const Response&, size_t) override {
    // 广播模式下，partition方法返回值无意义
    // 实际的广播逻辑需要在ResultPartition中特殊处理
    return 0;
  }
  
  // 标记此分区器需要广播（供ResultPartition检测使用）
  bool isBroadcast() const { return true; }
};

// 基于向量空间分区器的适配器，用于将 VectorSpacePartitioner 输出对接到运行时 IPartitioner 接口
class LSHPartitionerAdapter : public IPartitioner {
public:
  explicit LSHPartitionerAdapter(std::shared_ptr<VectorSpacePartitioner> vsp)
      : vsp_(std::move(vsp)) {}

  size_t partition(const Response& data, size_t num_channels) override {
    if (!vsp_ || !data.record_) {
      return 0;
    }
    return vsp_->partition(*data.record_, num_channels);
  }

private:
  std::shared_ptr<VectorSpacePartitioner> vsp_;
};

} // namespace sageFlow
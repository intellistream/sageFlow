//
// streaming_source.cpp - 支持动态流式输入的数据源实现
//

#include "stream/data_stream_source/streaming_source.h"
#include "utils/logger.h"

namespace sageFlow {

StreamingSource::StreamingSource(std::string name, size_t capacity)
    : DataStreamSource(std::move(name), DataStreamSourceType::None),
      capacity_(capacity) {}

void StreamingSource::Init() {
  SAGEFLOW_LOG_INFO("SOURCE", "StreamingSource initialized name={} capacity={} ", 
                    name_, capacity_ == 0 ? "unlimited" : std::to_string(capacity_));
}

auto StreamingSource::Next() -> std::unique_ptr<VectorRecord> {
  std::unique_lock<std::mutex> lock(mutex_);
  
  // 等待直到有数据可用或流已结束
  not_empty_.wait(lock, [this] {
    return !queue_.empty() || finished_.load(std::memory_order_acquire);
  });
  
  // 如果队列为空且流已结束，返回 nullptr
  if (queue_.empty()) {
    return nullptr;
  }
  
  // 取出队首元素
  auto record = std::move(queue_.front());
  queue_.pop();
  
  // 通知可能在等待空间的生产者
  if (capacity_ > 0) {
    lock.unlock();
    not_full_.notify_one();
  }
  
  return record;
}

bool StreamingSource::addRecord(const VectorRecord& rec) {
  // 快速路径：检查是否已结束
  if (finished_.load(std::memory_order_acquire)) {
    SAGEFLOW_LOG_WARN("SOURCE", "StreamingSource {} is finished, addRecord ignored", name_);
    return false;
  }
  
  std::unique_lock<std::mutex> lock(mutex_);
  
  // 如果设置了容量限制，等待空间可用
  if (capacity_ > 0) {
    not_full_.wait(lock, [this] {
      return queue_.size() < capacity_ || finished_.load(std::memory_order_acquire);
    });
    
    // 再次检查是否在等待期间流已结束
    if (finished_.load(std::memory_order_acquire)) {
      return false;
    }
  }
  
  // 添加记录
  queue_.push(std::make_unique<VectorRecord>(rec));
  
  // 通知等待数据的消费者
  lock.unlock();
  not_empty_.notify_one();
  
  return true;
}

bool StreamingSource::addRecord(uint64_t uid, int64_t timestamp, VectorData&& data) {
  // 快速路径：检查是否已结束
  if (finished_.load(std::memory_order_acquire)) {
    SAGEFLOW_LOG_WARN("SOURCE", "StreamingSource {} is finished, addRecord ignored", name_);
    return false;
  }
  
  std::unique_lock<std::mutex> lock(mutex_);
  
  // 如果设置了容量限制，等待空间可用
  if (capacity_ > 0) {
    not_full_.wait(lock, [this] {
      return queue_.size() < capacity_ || finished_.load(std::memory_order_acquire);
    });
    
    if (finished_.load(std::memory_order_acquire)) {
      return false;
    }
  }
  
  queue_.push(std::make_unique<VectorRecord>(uid, timestamp, std::move(data)));
  
  lock.unlock();
  not_empty_.notify_one();
  
  return true;
}

void StreamingSource::finish() {
  finished_.store(true, std::memory_order_release);
  
  // 唤醒所有等待的线程
  not_empty_.notify_all();
  not_full_.notify_all();
  
  SAGEFLOW_LOG_INFO("SOURCE", "StreamingSource {} finished, remaining queue size={} ", 
                    name_, queue_.size());
}

size_t StreamingSource::size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return queue_.size();
}

bool StreamingSource::tryAddRecord(const VectorRecord& rec) {
  if (finished_.load(std::memory_order_acquire)) {
    return false;
  }
  
  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock.owns_lock()) {
    return false;
  }
  
  // 检查容量
  if (capacity_ > 0 && queue_.size() >= capacity_) {
    return false;
  }
  
  queue_.push(std::make_unique<VectorRecord>(rec));
  
  lock.unlock();
  not_empty_.notify_one();
  
  return true;
}

bool StreamingSource::tryAddRecord(uint64_t uid, int64_t timestamp, VectorData&& data) {
  if (finished_.load(std::memory_order_acquire)) {
    return false;
  }
  
  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock.owns_lock()) {
    return false;
  }
  
  if (capacity_ > 0 && queue_.size() >= capacity_) {
    return false;
  }
  
  queue_.push(std::make_unique<VectorRecord>(uid, timestamp, std::move(data)));
  
  lock.unlock();
  not_empty_.notify_one();
  
  return true;
}

}  // namespace sageFlow

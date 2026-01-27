#pragma once
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <utility>

#include "common/data_types.h"
#include "stream/data_stream_source/data_stream_source.h"

namespace sageFlow {

/**
 * @brief StreamingSource 支持动态流式输入的数据源
 * 
 * 与 SimpleStreamSource 不同，StreamingSource 支持：
 * 1. 先创建数据源（addStream），再动态添加记录（addRecord）
 * 2. 线程安全的并发读写：生产者可以在任意时刻添加数据，消费者会阻塞等待
 * 3. 有界/无界模式：可以设置容量限制，或者作为无界流运行
 * 4. 显式结束信号：调用 finish() 标记流结束
 * 
 * 使用场景（Python 示例）：
 * @code
 * import sage_flow as sf
 * import numpy as np
 * 
 * # 1. 创建环境和流
 * env = sf.StreamEnvironment()
 * source = sf.StreamingSource("my_stream")
 * 
 * # 2. 构建 pipeline
 * source.filter(...).writeSink(...)
 * env.addStream(source)
 * 
 * # 3. 在后台启动执行
 * env.execute()  # 非阻塞，开始消费
 * 
 * # 4. 动态添加数据（可以在不同线程/协程中）
 * for vec in streaming_vectors():
 *     source.addRecord(uid, timestamp, vec)  # 线程安全
 * 
 * # 5. 标记流结束
 * source.finish()
 * 
 * # 6. 等待处理完成
 * env.awaitTermination()
 * @endcode
 */
class StreamingSource final : public DataStreamSource {
 public:
  /**
   * @brief 构造 StreamingSource
   * @param name 数据源名称
   * @param capacity 队列容量，0 表示无限制（默认 10000）
   */
  explicit StreamingSource(std::string name, size_t capacity = 10000);

  void Init() override;

  /**
   * @brief 获取下一条记录（阻塞直到有数据或流结束）
   * @return 记录指针，若流已结束且队列为空则返回 nullptr
   */
  auto Next() -> std::unique_ptr<VectorRecord> override;

  /**
   * @brief 添加一条记录（线程安全）
   * @param rec 要添加的记录
   * @return true 如果添加成功，false 如果流已结束
   * @note 如果设置了容量限制且队列已满，此方法会阻塞等待
   */
  bool addRecord(const VectorRecord& rec);

  /**
   * @brief 添加一条记录（线程安全，移动语义）
   * @param uid 记录ID
   * @param timestamp 时间戳
   * @param data 向量数据（移动）
   * @return true 如果添加成功，false 如果流已结束
   */
  bool addRecord(uint64_t uid, int64_t timestamp, VectorData&& data);

  /**
   * @brief 标记流结束（线程安全）
   * 
   * 调用此方法后：
   * - 新的 addRecord() 调用会立即返回 false
   * - Next() 会继续消费队列中剩余的数据
   * - 当队列清空后，Next() 返回 nullptr
   */
  void finish();

  /**
   * @brief 检查流是否已结束
   */
  bool isFinished() const { return finished_.load(std::memory_order_acquire); }

  /**
   * @brief 获取当前队列中的记录数量
   */
  size_t size() const;

  /**
   * @brief 获取队列容量（0 表示无限制）
   */
  size_t capacity() const { return capacity_; }

  /**
   * @brief 设置队列容量
   * @param cap 新容量，0 表示无限制
   */
  void setCapacity(size_t cap) { capacity_ = cap; }

  /**
   * @brief 非阻塞尝试添加记录
   * @param rec 要添加的记录
   * @return true 如果添加成功，false 如果队列已满或流已结束
   */
  bool tryAddRecord(const VectorRecord& rec);

  /**
   * @brief 非阻塞尝试添加记录（移动语义）
   */
  bool tryAddRecord(uint64_t uid, int64_t timestamp, VectorData&& data);

 private:
  std::queue<std::unique_ptr<VectorRecord>> queue_;
  mutable std::mutex mutex_;
  std::condition_variable not_empty_;  // 队列非空条件
  std::condition_variable not_full_;   // 队列未满条件
  std::atomic<bool> finished_{false};
  size_t capacity_;  // 0 表示无限制
};

}  // namespace sageFlow

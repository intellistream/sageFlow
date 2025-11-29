# Task A-04: LateArrivalHandler 延迟到达处理器

**优先级**: 🟡 中  
**预估工时**: 2-3 天  
**依赖**: 无  
**输出文件**:
- `include/coordination/late_arrival_handler.h`
- `src/coordination/late_arrival_handler.cpp`
- `test/UnitTest/test_late_arrival_handler.cpp`

---

## 任务描述

实现延迟到达向量的处理机制，支持乱序数据流和 watermark 语义。

---

## 提示词

```
你是 sageFlow 项目的开发者，需要实现 LateArrivalHandler 类。

## 项目背景
sageFlow 是一个 C++20 流式向量处理引擎，遵循以下规范：
- 类名: CamelCase
- 方法名: camelBack
- 成员变量: lower_case_ 带尾部下划线
- 使用 #pragma once 作为头文件保护

## 背景
流式系统中，数据可能乱序到达。当前系统假设数据按时间戳顺序到达，
无法正确处理延迟到达的向量，可能导致 join 结果不完整。

## 任务目标
实现延迟到达处理器：
1. 维护 watermark（水位线），追踪已处理数据的时间进度
2. 识别延迟到达的记录
3. 缓冲延迟记录，定期与主窗口进行补充 join

## 文件位置
- 头文件: include/coordination/late_arrival_handler.h
- 实现文件: src/coordination/late_arrival_handler.cpp

## 接口要求

```cpp
#pragma once

#include "common/vector_record.h"
#include <deque>
#include <vector>
#include <shared_mutex>
#include <atomic>
#include <cstdint>

namespace sageFlow {

/**
 * @brief 记录到达状态
 */
enum class ArrivalStatus {
    ON_TIME,      ///< 正常到达（时间戳 >= watermark）
    LATE,         ///< 延迟但可处理（时间戳在允许延迟范围内）
    TOO_LATE      ///< 超出允许延迟，应丢弃
};

/**
 * @brief 延迟到达处理器
 * 
 * 实现 watermark 机制，处理乱序数据流。
 * 参考 Apache Flink 的 watermark 语义。
 */
class LateArrivalHandler {
public:
    /**
     * @brief 构造函数
     * @param allowed_lateness 允许的最大延迟时间（毫秒）
     * @param watermark_delay watermark 滞后于最新记录的时间（毫秒）
     */
    explicit LateArrivalHandler(int64_t allowed_lateness = 5000,
                                int64_t watermark_delay = 1000);
    
    /**
     * @brief 处理到达的记录，返回状态
     * @param record 到达的记录
     * @return 到达状态
     */
    ArrivalStatus processRecord(const VectorRecord& record);
    
    /**
     * @brief 更新 watermark
     * @param event_time 事件时间戳
     */
    void updateWatermark(int64_t event_time);
    
    /**
     * @brief 获取当前 watermark
     */
    int64_t getWatermark() const;
    
    /**
     * @brief 添加延迟记录到缓冲区
     * @param record 延迟记录
     */
    void bufferLateRecord(std::unique_ptr<VectorRecord> record);
    
    /**
     * @brief 获取并清空延迟缓冲区
     * @return 缓冲的延迟记录
     */
    std::vector<std::unique_ptr<VectorRecord>> flushLateBuffer();
    
    /**
     * @brief 获取延迟缓冲区大小
     */
    size_t getLateBufferSize() const;
    
    /**
     * @brief 统计信息
     */
    struct Stats {
        std::atomic<uint64_t> on_time_count{0};
        std::atomic<uint64_t> late_count{0};
        std::atomic<uint64_t> too_late_count{0};
    };
    
    /**
     * @brief 获取统计信息
     */
    const Stats& getStats() const { return stats_; }
    
    /**
     * @brief 重置统计信息
     */
    void resetStats();

private:
    std::atomic<int64_t> watermark_{0};
    int64_t allowed_lateness_;
    int64_t watermark_delay_;
    std::atomic<int64_t> max_seen_timestamp_{0};
    
    std::deque<std::unique_ptr<VectorRecord>> late_buffer_;
    mutable std::shared_mutex buffer_mutex_;
    
    Stats stats_;
};

} // namespace sageFlow
```

## 实现要点

1. **processRecord()**:
   ```cpp
   ArrivalStatus processRecord(const VectorRecord& record) {
       int64_t event_time = record.getTimestamp();
       
       // 更新最大观察时间戳
       int64_t expected = max_seen_timestamp_.load();
       while (event_time > expected && 
              !max_seen_timestamp_.compare_exchange_weak(expected, event_time)) {}
       
       // 更新 watermark
       updateWatermark(max_seen_timestamp_.load());
       
       int64_t current_watermark = watermark_.load();
       
       if (event_time >= current_watermark) {
           stats_.on_time_count++;
           return ArrivalStatus::ON_TIME;
       } else if (event_time >= current_watermark - allowed_lateness_) {
           stats_.late_count++;
           return ArrivalStatus::LATE;
       } else {
           stats_.too_late_count++;
           return ArrivalStatus::TOO_LATE;
       }
   }
   ```

2. **updateWatermark()**:
   ```cpp
   void updateWatermark(int64_t event_time) {
       int64_t new_watermark = event_time - watermark_delay_;
       int64_t expected = watermark_.load();
       while (new_watermark > expected && 
              !watermark_.compare_exchange_weak(expected, new_watermark)) {}
   }
   ```

3. **flushLateBuffer()**:
   - 获取 unique_lock
   - 返回所有缓冲记录，使用 std::move
   - 清空缓冲区

## 测试要求

```cpp
TEST(LateArrivalHandlerTest, OnTimeRecord) {
    LateArrivalHandler handler(5000, 1000);
    // 模拟正常到达的记录
}

TEST(LateArrivalHandlerTest, LateRecord) {
    LateArrivalHandler handler(5000, 1000);
    // 模拟延迟但在允许范围内的记录
}

TEST(LateArrivalHandlerTest, TooLateRecord) {
    LateArrivalHandler handler(5000, 1000);
    // 模拟超出允许延迟的记录
}

TEST(LateArrivalHandlerTest, WatermarkProgression) {
    // 测试 watermark 正确递增
}

TEST(LateArrivalHandlerTest, FlushLateBuffer) {
    // 测试缓冲区 flush 正确性
}
```

## 验收标准
1. 所有单元测试通过
2. watermark 语义正确
3. 线程安全
```

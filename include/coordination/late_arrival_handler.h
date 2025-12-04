#pragma once

#include "common/data_types.h"

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <shared_mutex>
#include <vector>

namespace sageFlow {

/**
 * @brief 记录到达状态
 */
enum class ArrivalStatus {
    ON_TIME,  ///< 正常到达（时间戳 >= watermark）
    LATE,     ///< 延迟但可处理（时间戳在允许延迟范围内）
    TOO_LATE  ///< 超出允许延迟，应丢弃
};

/**
 * @brief 延迟到达处理器统计信息
 */
struct LateArrivalStats {
    std::atomic<uint64_t> on_time_count{0};
    std::atomic<uint64_t> late_count{0};
    std::atomic<uint64_t> too_late_count{0};

    LateArrivalStats() = default;

    // 禁用拷贝（因为 atomic 不可拷贝）
    LateArrivalStats(const LateArrivalStats&) = delete;
    auto operator=(const LateArrivalStats&) -> LateArrivalStats& = delete;

    // 移动构造和赋值
    LateArrivalStats(LateArrivalStats&& other) noexcept
        : on_time_count(other.on_time_count.load()),
          late_count(other.late_count.load()),
          too_late_count(other.too_late_count.load()) {}

    auto operator=(LateArrivalStats&& other) noexcept -> LateArrivalStats& {
        if (this != &other) {
            on_time_count.store(other.on_time_count.load());
            late_count.store(other.late_count.load());
            too_late_count.store(other.too_late_count.load());
        }
        return *this;
    }
};

/**
 * @brief 延迟到达处理器
 *
 * 实现 watermark 机制，处理乱序数据流。
 * 参考 Apache Flink 的 watermark 语义。
 *
 * Watermark 表示"所有时间戳小于 watermark 的记录都已到达"的假设。
 * 延迟记录（late record）是指时间戳小于当前 watermark 的记录。
 *
 * 线程安全，支持高并发访问。
 */
class LateArrivalHandler {
public:
    /**
     * @brief 构造函数
     * @param allowed_lateness 允许的最大延迟时间（毫秒），默认5000ms
     * @param watermark_delay watermark 滞后于最新记录的时间（毫秒），默认1000ms
     */
    explicit LateArrivalHandler(int64_t allowed_lateness = 5000, int64_t watermark_delay = 1000);

    /**
     * @brief 析构函数
     */
    ~LateArrivalHandler() = default;

    // 禁用拷贝
    LateArrivalHandler(const LateArrivalHandler&) = delete;
    auto operator=(const LateArrivalHandler&) -> LateArrivalHandler& = delete;

    // 允许移动
    LateArrivalHandler(LateArrivalHandler&&) noexcept = default;
    auto operator=(LateArrivalHandler&&) noexcept -> LateArrivalHandler& = default;

    /**
     * @brief 处理到达的记录，返回状态
     * @param record 到达的记录
     * @return 到达状态（ON_TIME, LATE, TOO_LATE）
     */
    auto processRecord(const VectorRecord& record) -> ArrivalStatus;

    /**
     * @brief 更新 watermark
     *
     * watermark = max_seen_timestamp - watermark_delay
     * watermark 只能单调递增。
     *
     * @param event_time 事件时间戳
     */
    void updateWatermark(int64_t event_time);

    /**
     * @brief 获取当前 watermark
     * @return 当前 watermark 值
     */
    [[nodiscard]] auto getWatermark() const -> int64_t;

    /**
     * @brief 获取最大观察时间戳
     * @return 最大观察时间戳
     */
    [[nodiscard]] auto getMaxSeenTimestamp() const -> int64_t;

    /**
     * @brief 添加延迟记录到缓冲区
     * @param record 延迟记录（所有权转移）
     */
    void bufferLateRecord(std::unique_ptr<VectorRecord> record);

    /**
     * @brief 获取并清空延迟缓冲区
     * @return 缓冲的延迟记录列表
     */
    auto flushLateBuffer() -> std::vector<std::unique_ptr<VectorRecord>>;

    /**
     * @brief 获取延迟缓冲区大小
     * @return 缓冲区中的记录数量
     */
    [[nodiscard]] auto getLateBufferSize() const -> size_t;

    /**
     * @brief 获取统计信息
     * @return 统计信息引用
     */
    [[nodiscard]] auto getStats() const -> const LateArrivalStats&;

    /**
     * @brief 重置统计信息
     */
    void resetStats();

    /**
     * @brief 获取允许延迟时间配置
     * @return 允许的最大延迟时间（毫秒）
     */
    [[nodiscard]] auto getAllowedLateness() const -> int64_t;

    /**
     * @brief 获取 watermark 延迟配置
     * @return watermark 滞后时间（毫秒）
     */
    [[nodiscard]] auto getWatermarkDelay() const -> int64_t;

private:
    /**
     * @brief 原子更新最大观察时间戳
     * @param event_time 新观察到的事件时间
     */
    void updateMaxSeenTimestamp(int64_t event_time);

    /// 当前 watermark 值
    std::atomic<int64_t> watermark_{0};

    /// 允许的最大延迟时间（毫秒）
    int64_t allowed_lateness_;

    /// watermark 滞后于最新记录的时间（毫秒）
    int64_t watermark_delay_;

    /// 最大观察时间戳
    std::atomic<int64_t> max_seen_timestamp_{0};

    /// 延迟记录缓冲区
    std::deque<std::unique_ptr<VectorRecord>> late_buffer_;

    /// 缓冲区互斥锁
    mutable std::shared_mutex buffer_mutex_;

    /// 统计信息
    LateArrivalStats stats_;
};

}  // namespace sageFlow

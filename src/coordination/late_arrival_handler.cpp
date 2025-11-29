#include "coordination/late_arrival_handler.h"

#include <algorithm>
#include <mutex>
#include <utility>

namespace sageFlow {

LateArrivalHandler::LateArrivalHandler(int64_t allowed_lateness, int64_t watermark_delay)
    : allowed_lateness_(allowed_lateness), watermark_delay_(watermark_delay) {
    // 确保参数非负
    if (allowed_lateness_ < 0) {
        allowed_lateness_ = 0;
    }
    if (watermark_delay_ < 0) {
        watermark_delay_ = 0;
    }
}

auto LateArrivalHandler::processRecord(const VectorRecord& record) -> ArrivalStatus {
    int64_t event_time = record.timestamp_;

    // 原子更新最大观察时间戳
    updateMaxSeenTimestamp(event_time);

    // 更新 watermark（基于最大观察时间戳）
    updateWatermark(max_seen_timestamp_.load(std::memory_order_acquire));

    // 获取当前 watermark
    int64_t current_watermark = watermark_.load(std::memory_order_acquire);

    // 判断记录状态
    if (event_time >= current_watermark) {
        // 正常到达：事件时间 >= watermark
        stats_.on_time_count.fetch_add(1, std::memory_order_relaxed);
        return ArrivalStatus::ON_TIME;
    }

    if (event_time >= current_watermark - allowed_lateness_) {
        // 延迟但可处理：在允许延迟范围内
        stats_.late_count.fetch_add(1, std::memory_order_relaxed);
        return ArrivalStatus::LATE;
    }

    // 超出允许延迟，应丢弃
    stats_.too_late_count.fetch_add(1, std::memory_order_relaxed);
    return ArrivalStatus::TOO_LATE;
}

void LateArrivalHandler::updateMaxSeenTimestamp(int64_t event_time) {
    int64_t expected = max_seen_timestamp_.load(std::memory_order_acquire);
    while (event_time > expected) {
        if (max_seen_timestamp_.compare_exchange_weak(expected, event_time, std::memory_order_release,
                                                      std::memory_order_acquire)) {
            break;
        }
        // CAS 失败，expected 已更新为当前值，继续循环
    }
}

void LateArrivalHandler::updateWatermark(int64_t event_time) {
    // 计算新的 watermark 值
    int64_t new_watermark = event_time - watermark_delay_;

    // watermark 只能单调递增
    int64_t expected = watermark_.load(std::memory_order_acquire);
    while (new_watermark > expected) {
        if (watermark_.compare_exchange_weak(expected, new_watermark, std::memory_order_release,
                                             std::memory_order_acquire)) {
            break;
        }
        // CAS 失败，expected 已更新为当前值，继续循环
    }
}

auto LateArrivalHandler::getWatermark() const -> int64_t {
    return watermark_.load(std::memory_order_acquire);
}

auto LateArrivalHandler::getMaxSeenTimestamp() const -> int64_t {
    return max_seen_timestamp_.load(std::memory_order_acquire);
}

void LateArrivalHandler::bufferLateRecord(std::unique_ptr<VectorRecord> record) {
    if (!record) {
        return;
    }

    std::unique_lock lock(buffer_mutex_);
    late_buffer_.emplace_back(std::move(record));
}

auto LateArrivalHandler::flushLateBuffer() -> std::vector<std::unique_ptr<VectorRecord>> {
    std::unique_lock lock(buffer_mutex_);

    std::vector<std::unique_ptr<VectorRecord>> result;
    result.reserve(late_buffer_.size());

    for (auto& record : late_buffer_) {
        result.emplace_back(std::move(record));
    }
    late_buffer_.clear();

    return result;
}

auto LateArrivalHandler::getLateBufferSize() const -> size_t {
    std::shared_lock lock(buffer_mutex_);
    return late_buffer_.size();
}

auto LateArrivalHandler::getStats() const -> const LateArrivalStats& {
    return stats_;
}

void LateArrivalHandler::resetStats() {
    stats_.on_time_count.store(0, std::memory_order_relaxed);
    stats_.late_count.store(0, std::memory_order_relaxed);
    stats_.too_late_count.store(0, std::memory_order_relaxed);
}

auto LateArrivalHandler::getAllowedLateness() const -> int64_t {
    return allowed_lateness_;
}

auto LateArrivalHandler::getWatermarkDelay() const -> int64_t {
    return watermark_delay_;
}

}  // namespace sageFlow

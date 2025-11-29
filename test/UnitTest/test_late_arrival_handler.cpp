#include "coordination/late_arrival_handler.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <random>
#include <thread>
#include <vector>

namespace sageFlow {
namespace {

// 辅助函数：创建测试用的 VectorRecord
auto createTestRecord(uint64_t uid, int64_t timestamp) -> std::unique_ptr<VectorRecord> {
    VectorData data(4, DataType::Float32);
    return std::make_unique<VectorRecord>(uid, timestamp, std::move(data));
}

// ============================================================================
// 基本功能测试
// ============================================================================

TEST(LateArrivalHandlerTest, DefaultConstruction) {
    LateArrivalHandler handler;

    EXPECT_EQ(handler.getAllowedLateness(), 5000);
    EXPECT_EQ(handler.getWatermarkDelay(), 1000);
    EXPECT_EQ(handler.getWatermark(), 0);
    EXPECT_EQ(handler.getMaxSeenTimestamp(), 0);
    EXPECT_EQ(handler.getLateBufferSize(), 0);
}

TEST(LateArrivalHandlerTest, CustomConstruction) {
    LateArrivalHandler handler(10000, 2000);

    EXPECT_EQ(handler.getAllowedLateness(), 10000);
    EXPECT_EQ(handler.getWatermarkDelay(), 2000);
}

TEST(LateArrivalHandlerTest, NegativeParametersClampedToZero) {
    LateArrivalHandler handler(-100, -200);

    EXPECT_EQ(handler.getAllowedLateness(), 0);
    EXPECT_EQ(handler.getWatermarkDelay(), 0);
}

// ============================================================================
// Watermark 更新测试
// ============================================================================

TEST(LateArrivalHandlerTest, WatermarkProgression) {
    LateArrivalHandler handler(5000, 1000);

    // 初始 watermark 为 0
    EXPECT_EQ(handler.getWatermark(), 0);

    // 处理时间戳为 5000 的记录
    auto record1 = createTestRecord(1, 5000);
    handler.processRecord(*record1);

    // watermark = max_seen_timestamp - watermark_delay = 5000 - 1000 = 4000
    EXPECT_EQ(handler.getMaxSeenTimestamp(), 5000);
    EXPECT_EQ(handler.getWatermark(), 4000);

    // 处理时间戳为 8000 的记录
    auto record2 = createTestRecord(2, 8000);
    handler.processRecord(*record2);

    // watermark = 8000 - 1000 = 7000
    EXPECT_EQ(handler.getMaxSeenTimestamp(), 8000);
    EXPECT_EQ(handler.getWatermark(), 7000);
}

TEST(LateArrivalHandlerTest, WatermarkMonotonicallyIncreasing) {
    LateArrivalHandler handler(5000, 1000);

    // 先处理高时间戳记录
    auto record1 = createTestRecord(1, 10000);
    handler.processRecord(*record1);
    EXPECT_EQ(handler.getWatermark(), 9000);

    // 再处理低时间戳记录，watermark 不应回退
    auto record2 = createTestRecord(2, 5000);
    handler.processRecord(*record2);
    EXPECT_EQ(handler.getWatermark(), 9000);  // 保持不变
    EXPECT_EQ(handler.getMaxSeenTimestamp(), 10000);  // 保持不变
}

// ============================================================================
// 记录状态判定测试
// ============================================================================

TEST(LateArrivalHandlerTest, OnTimeRecord) {
    LateArrivalHandler handler(5000, 1000);

    // 处理第一条记录，建立 watermark
    auto record1 = createTestRecord(1, 10000);
    auto status1 = handler.processRecord(*record1);
    EXPECT_EQ(status1, ArrivalStatus::ON_TIME);

    // watermark = 10000 - 1000 = 9000
    // 处理时间戳 >= 9000 的记录应为 ON_TIME
    auto record2 = createTestRecord(2, 12000);
    auto status2 = handler.processRecord(*record2);
    EXPECT_EQ(status2, ArrivalStatus::ON_TIME);

    // 现在 watermark = 12000 - 1000 = 11000
    // 时间戳 11000 仍是 ON_TIME
    auto record3 = createTestRecord(3, 11000);
    auto status3 = handler.processRecord(*record3);
    EXPECT_EQ(status3, ArrivalStatus::ON_TIME);
}

TEST(LateArrivalHandlerTest, LateRecord) {
    LateArrivalHandler handler(5000, 1000);

    // 建立 watermark = 10000 - 1000 = 9000
    auto record1 = createTestRecord(1, 10000);
    handler.processRecord(*record1);

    // 时间戳在 [9000 - 5000, 9000) = [4000, 9000) 范围内为 LATE
    auto record2 = createTestRecord(2, 8000);
    auto status2 = handler.processRecord(*record2);
    EXPECT_EQ(status2, ArrivalStatus::LATE);

    auto record3 = createTestRecord(3, 4000);
    auto status3 = handler.processRecord(*record3);
    EXPECT_EQ(status3, ArrivalStatus::LATE);
}

TEST(LateArrivalHandlerTest, TooLateRecord) {
    LateArrivalHandler handler(5000, 1000);

    // 建立 watermark = 10000 - 1000 = 9000
    auto record1 = createTestRecord(1, 10000);
    handler.processRecord(*record1);

    // 时间戳 < 9000 - 5000 = 4000 为 TOO_LATE
    auto record2 = createTestRecord(2, 3999);
    auto status2 = handler.processRecord(*record2);
    EXPECT_EQ(status2, ArrivalStatus::TOO_LATE);

    auto record3 = createTestRecord(3, 0);
    auto status3 = handler.processRecord(*record3);
    EXPECT_EQ(status3, ArrivalStatus::TOO_LATE);
}

TEST(LateArrivalHandlerTest, BoundaryConditions) {
    LateArrivalHandler handler(5000, 1000);

    // 建立 watermark = 10000 - 1000 = 9000
    auto record1 = createTestRecord(1, 10000);
    handler.processRecord(*record1);

    // 边界测试：时间戳恰好等于 watermark
    auto record2 = createTestRecord(2, 9000);
    EXPECT_EQ(handler.processRecord(*record2), ArrivalStatus::ON_TIME);

    // 边界测试：时间戳恰好等于 watermark - 1
    auto record3 = createTestRecord(3, 8999);
    EXPECT_EQ(handler.processRecord(*record3), ArrivalStatus::LATE);

    // 边界测试：时间戳恰好等于 watermark - allowed_lateness
    auto record4 = createTestRecord(4, 4000);
    EXPECT_EQ(handler.processRecord(*record4), ArrivalStatus::LATE);

    // 边界测试：时间戳恰好等于 watermark - allowed_lateness - 1
    auto record5 = createTestRecord(5, 3999);
    EXPECT_EQ(handler.processRecord(*record5), ArrivalStatus::TOO_LATE);
}

// ============================================================================
// 延迟缓冲区测试
// ============================================================================

TEST(LateArrivalHandlerTest, BufferLateRecord) {
    LateArrivalHandler handler;

    EXPECT_EQ(handler.getLateBufferSize(), 0);

    auto record1 = createTestRecord(1, 1000);
    handler.bufferLateRecord(std::move(record1));
    EXPECT_EQ(handler.getLateBufferSize(), 1);

    auto record2 = createTestRecord(2, 2000);
    handler.bufferLateRecord(std::move(record2));
    EXPECT_EQ(handler.getLateBufferSize(), 2);
}

TEST(LateArrivalHandlerTest, BufferNullRecordIgnored) {
    LateArrivalHandler handler;

    handler.bufferLateRecord(nullptr);
    EXPECT_EQ(handler.getLateBufferSize(), 0);
}

TEST(LateArrivalHandlerTest, FlushLateBuffer) {
    LateArrivalHandler handler;

    // 添加多条记录
    for (uint64_t i = 1; i <= 5; ++i) {
        handler.bufferLateRecord(createTestRecord(i, static_cast<int64_t>(i * 1000)));
    }
    EXPECT_EQ(handler.getLateBufferSize(), 5);

    // Flush 缓冲区
    auto records = handler.flushLateBuffer();
    EXPECT_EQ(records.size(), 5);
    EXPECT_EQ(handler.getLateBufferSize(), 0);

    // 验证记录内容
    for (uint64_t i = 0; i < 5; ++i) {
        EXPECT_EQ(records[i]->uid_, i + 1);
        EXPECT_EQ(records[i]->timestamp_, static_cast<int64_t>((i + 1) * 1000));
    }
}

TEST(LateArrivalHandlerTest, FlushEmptyBuffer) {
    LateArrivalHandler handler;

    auto records = handler.flushLateBuffer();
    EXPECT_TRUE(records.empty());
}

TEST(LateArrivalHandlerTest, MultipleFlushes) {
    LateArrivalHandler handler;

    // 第一次添加和 flush
    handler.bufferLateRecord(createTestRecord(1, 1000));
    handler.bufferLateRecord(createTestRecord(2, 2000));
    auto batch1 = handler.flushLateBuffer();
    EXPECT_EQ(batch1.size(), 2);

    // 第二次添加和 flush
    handler.bufferLateRecord(createTestRecord(3, 3000));
    auto batch2 = handler.flushLateBuffer();
    EXPECT_EQ(batch2.size(), 1);
    EXPECT_EQ(batch2[0]->uid_, 3);
}

// ============================================================================
// 统计信息测试
// ============================================================================

TEST(LateArrivalHandlerTest, StatsAccumulation) {
    LateArrivalHandler handler(5000, 1000);

    // 建立 watermark：处理 10000 -> watermark = 9000
    handler.processRecord(*createTestRecord(1, 10000));  // ON_TIME

    // 处理不同状态的记录（注意 watermark 会随记录更新）
    // 当前 watermark = 9000, allowed_lateness = 5000
    // ON_TIME: >= 9000, LATE: [4000, 9000), TOO_LATE: < 4000

    handler.processRecord(*createTestRecord(2, 9000));   // ON_TIME (刚好等于 watermark)
    handler.processRecord(*createTestRecord(3, 8000));   // LATE (在 [4000, 9000))
    handler.processRecord(*createTestRecord(4, 4000));   // LATE (刚好等于下界)
    handler.processRecord(*createTestRecord(5, 3999));   // TOO_LATE (< 4000)
    handler.processRecord(*createTestRecord(6, 0));      // TOO_LATE

    const auto& stats = handler.getStats();
    EXPECT_EQ(stats.on_time_count.load(), 2);  // record 1, 2
    EXPECT_EQ(stats.late_count.load(), 2);     // record 3, 4
    EXPECT_EQ(stats.too_late_count.load(), 2); // record 5, 6
}

TEST(LateArrivalHandlerTest, ResetStats) {
    LateArrivalHandler handler(5000, 1000);

    handler.processRecord(*createTestRecord(1, 10000));
    handler.processRecord(*createTestRecord(2, 5000));

    EXPECT_GT(handler.getStats().on_time_count.load(), 0);

    handler.resetStats();

    const auto& stats = handler.getStats();
    EXPECT_EQ(stats.on_time_count.load(), 0);
    EXPECT_EQ(stats.late_count.load(), 0);
    EXPECT_EQ(stats.too_late_count.load(), 0);
}

// ============================================================================
// 并发测试
// ============================================================================

TEST(LateArrivalHandlerTest, ConcurrentProcessRecord) {
    LateArrivalHandler handler(5000, 1000);

    constexpr int num_threads = 8;
    constexpr int records_per_thread = 1000;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&handler, t]() {
            std::mt19937 rng(t);
            std::uniform_int_distribution<int64_t> dist(0, 20000);

            for (int i = 0; i < records_per_thread; ++i) {
                auto record = createTestRecord(static_cast<uint64_t>(t * records_per_thread + i), dist(rng));
                handler.processRecord(*record);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // 验证统计总数
    const auto& stats = handler.getStats();
    uint64_t total = stats.on_time_count.load() + stats.late_count.load() + stats.too_late_count.load();
    EXPECT_EQ(total, num_threads * records_per_thread);
}

TEST(LateArrivalHandlerTest, ConcurrentBufferOperations) {
    LateArrivalHandler handler;

    constexpr int num_producers = 4;
    constexpr int records_per_producer = 500;

    std::atomic<int> flush_count{0};
    std::atomic<size_t> total_flushed{0};

    std::vector<std::thread> producers;
    producers.reserve(num_producers);

    // 生产者线程
    for (int t = 0; t < num_producers; ++t) {
        producers.emplace_back([&handler, t]() {
            for (int i = 0; i < records_per_producer; ++i) {
                handler.bufferLateRecord(
                    createTestRecord(static_cast<uint64_t>(t * records_per_producer + i), i * 100));
            }
        });
    }

    // 消费者线程
    std::thread consumer([&handler, &flush_count, &total_flushed]() {
        while (flush_count.load() < 10) {
            auto records = handler.flushLateBuffer();
            total_flushed.fetch_add(records.size());
            flush_count.fetch_add(1);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });

    for (auto& t : producers) {
        t.join();
    }

    // 等待消费者线程完成
    flush_count.store(10);
    consumer.join();

    // 最后一次 flush 获取剩余记录
    auto remaining = handler.flushLateBuffer();
    total_flushed.fetch_add(remaining.size());

    // 验证所有记录都被处理
    EXPECT_EQ(total_flushed.load(), num_producers * records_per_producer);
}

TEST(LateArrivalHandlerTest, ConcurrentMixedOperations) {
    LateArrivalHandler handler(5000, 1000);

    constexpr int num_threads = 4;
    constexpr int iterations = 200;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&handler, t]() {
            std::mt19937 rng(t);
            std::uniform_int_distribution<int64_t> ts_dist(0, 20000);
            std::uniform_int_distribution<int> op_dist(0, 2);

            for (int i = 0; i < iterations; ++i) {
                int op = op_dist(rng);
                auto record = createTestRecord(static_cast<uint64_t>(t * iterations + i), ts_dist(rng));

                switch (op) {
                    case 0:
                        handler.processRecord(*record);
                        break;
                    case 1:
                        handler.bufferLateRecord(std::move(record));
                        break;
                    case 2:
                        handler.flushLateBuffer();
                        break;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // 无死锁和崩溃即为通过
    SUCCEED();
}

// ============================================================================
// 边缘情况测试
// ============================================================================

TEST(LateArrivalHandlerTest, ZeroWatermarkDelay) {
    LateArrivalHandler handler(5000, 0);

    auto record = createTestRecord(1, 10000);
    handler.processRecord(*record);

    // watermark = max_seen - 0 = 10000
    EXPECT_EQ(handler.getWatermark(), 10000);
}

TEST(LateArrivalHandlerTest, ZeroAllowedLateness) {
    LateArrivalHandler handler(0, 1000);

    // 建立 watermark = 10000 - 1000 = 9000
    handler.processRecord(*createTestRecord(1, 10000));

    // 任何小于 watermark 的记录都是 TOO_LATE
    auto status = handler.processRecord(*createTestRecord(2, 8999));
    EXPECT_EQ(status, ArrivalStatus::TOO_LATE);
}

TEST(LateArrivalHandlerTest, NegativeTimestamps) {
    LateArrivalHandler handler(5000, 1000);

    // 处理正时间戳建立基线
    auto record1 = createTestRecord(1, 5000);
    auto status1 = handler.processRecord(*record1);
    EXPECT_EQ(status1, ArrivalStatus::ON_TIME);

    // watermark = 5000 - 1000 = 4000
    EXPECT_EQ(handler.getWatermark(), 4000);

    // 处理负时间戳：应该是 TOO_LATE (因为 -1000 < 4000 - 5000 = -1000)
    // 边界测试：-1000 刚好等于 watermark - allowed_lateness，所以是 LATE
    auto record2 = createTestRecord(2, -1000);
    auto status2 = handler.processRecord(*record2);
    EXPECT_EQ(status2, ArrivalStatus::LATE);

    // -1001 < -1000，所以是 TOO_LATE
    auto record3 = createTestRecord(3, -1001);
    auto status3 = handler.processRecord(*record3);
    EXPECT_EQ(status3, ArrivalStatus::TOO_LATE);
}

TEST(LateArrivalHandlerTest, LargeTimestamps) {
    LateArrivalHandler handler(5000, 1000);

    int64_t large_ts = INT64_MAX - 10000;
    auto record = createTestRecord(1, large_ts);
    auto status = handler.processRecord(*record);

    EXPECT_EQ(status, ArrivalStatus::ON_TIME);
    EXPECT_EQ(handler.getMaxSeenTimestamp(), large_ts);
}

}  // namespace
}  // namespace sageFlow

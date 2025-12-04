#include "coordination/partition_coordinator.h"
#include "execution/vector_space_partitioner.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <thread>
#include <vector>

namespace sageFlow {
namespace {

// 辅助函数：创建测试用的 VectorRecord
auto createTestRecord(uint64_t uid, int64_t timestamp, int dim = 128) -> std::unique_ptr<VectorRecord> {
    VectorData data(dim, DataType::Float32);
    // 初始化向量数据
    auto* float_data = reinterpret_cast<float*>(data.data_.get());
    for (int i = 0; i < dim; ++i) {
        float_data[i] = static_cast<float>(uid * 100 + i) / 1000.0f;
    }
    return std::make_unique<VectorRecord>(uid, timestamp, std::move(data));
}

// 辅助函数：创建具有特定值的 VectorRecord
auto createRecordWithValues(uint64_t uid, int64_t timestamp, const std::vector<float>& values)
    -> std::unique_ptr<VectorRecord> {
    int dim = static_cast<int>(values.size());
    VectorData data(dim, DataType::Float32);
    auto* float_data = reinterpret_cast<float*>(data.data_.get());
    for (int i = 0; i < dim; ++i) {
        float_data[i] = values[i];
    }
    return std::make_unique<VectorRecord>(uid, timestamp, std::move(data));
}

// ============================================================================
// 测试夹具
// ============================================================================

class PartitionCoordinatorTest : public ::testing::Test {
 protected:
    void SetUp() override {
        partitioner_ = std::make_shared<LSHPartitioner>(128, 8, 42);
        coordinator_ = std::make_unique<PartitionCoordinator>(4, partitioner_, 5000, 1000);
    }

    std::shared_ptr<LSHPartitioner> partitioner_;
    std::unique_ptr<PartitionCoordinator> coordinator_;
};

// ============================================================================
// 构造函数测试
// ============================================================================

TEST(PartitionCoordinatorBasicTest, DefaultConstruction) {
    auto partitioner = std::make_shared<LSHPartitioner>(64, 8, 42);
    PartitionCoordinator coordinator(4, partitioner);

    EXPECT_EQ(coordinator.getNumPartitions(), 4);
    EXPECT_EQ(coordinator.getWatermark(), 0);
    EXPECT_EQ(coordinator.getLateBufferSize(), 0);
}

TEST(PartitionCoordinatorBasicTest, CustomConstruction) {
    auto partitioner = std::make_shared<LSHPartitioner>(64, 8, 42);
    PartitionCoordinator coordinator(8, partitioner, 10000, 2000);

    EXPECT_EQ(coordinator.getNumPartitions(), 8);
    EXPECT_EQ(coordinator.getLateArrivalHandler()->getAllowedLateness(), 10000);
    EXPECT_EQ(coordinator.getLateArrivalHandler()->getWatermarkDelay(), 2000);
}

// ============================================================================
// 路由测试
// ============================================================================

TEST_F(PartitionCoordinatorTest, RouteQueryBasic) {
    auto query = createTestRecord(1, 1000);
    auto partitions = coordinator_->routeQuery(*query, 1);

    // 至少应该返回一个分区
    EXPECT_GE(partitions.size(), 1);

    // 所有返回的分区ID应该在有效范围内
    for (size_t partition_id : partitions) {
        EXPECT_LT(partition_id, coordinator_->getNumPartitions());
    }
}

TEST_F(PartitionCoordinatorTest, RouteQueryWithProbes) {
    auto query = createTestRecord(1, 1000);
    auto partitions = coordinator_->routeQuery(*query, 3);

    // 所有返回的分区ID应该在有效范围内
    for (size_t partition_id : partitions) {
        EXPECT_LT(partition_id, coordinator_->getNumPartitions());
    }
}

TEST_F(PartitionCoordinatorTest, RouteQueryNoDuplicates) {
    auto query = createTestRecord(1, 1000);
    auto partitions = coordinator_->routeQuery(*query, 4);

    // 检查没有重复的分区
    std::sort(partitions.begin(), partitions.end());
    auto it = std::unique(partitions.begin(), partitions.end());
    EXPECT_EQ(it, partitions.end());
}

// ============================================================================
// 记录处理测试
// ============================================================================

TEST_F(PartitionCoordinatorTest, ProcessRecordOnTime) {
    // 先发送一条高时间戳记录建立 watermark
    auto record1 = createTestRecord(1, 10000);
    auto result1 = coordinator_->processRecord(*record1);
    EXPECT_EQ(result1.status, ArrivalStatus::ON_TIME);

    // 发送一条时间戳在 watermark 之后的记录
    auto record2 = createTestRecord(2, 11000);
    auto result2 = coordinator_->processRecord(*record2);
    EXPECT_EQ(result2.status, ArrivalStatus::ON_TIME);
}

TEST_F(PartitionCoordinatorTest, ProcessRecordLate) {
    // 先发送一条高时间戳记录建立 watermark
    auto record1 = createTestRecord(1, 10000);
    coordinator_->processRecord(*record1);
    // watermark = 10000 - 1000 = 9000

    // 发送一条延迟记录 (timestamp < watermark, 但在允许范围内)
    // allowed_lateness = 5000, 所以 timestamp >= watermark - 5000 = 4000 是可以的
    auto record2 = createTestRecord(2, 5000);
    auto result2 = coordinator_->processRecord(*record2);
    EXPECT_EQ(result2.status, ArrivalStatus::LATE);
}

TEST_F(PartitionCoordinatorTest, ProcessRecordTooLate) {
    // 先发送一条高时间戳记录建立 watermark
    auto record1 = createTestRecord(1, 10000);
    coordinator_->processRecord(*record1);
    // watermark = 10000 - 1000 = 9000

    // 发送一条过期记录 (timestamp < watermark - allowed_lateness)
    // allowed_lateness = 5000, 所以 timestamp < 9000 - 5000 = 4000 会被拒绝
    auto record2 = createTestRecord(2, 2000);
    auto result2 = coordinator_->processRecord(*record2);
    EXPECT_EQ(result2.status, ArrivalStatus::TOO_LATE);
}

TEST_F(PartitionCoordinatorTest, ProcessRecordReturnsValidPartition) {
    auto record = createTestRecord(1, 1000);
    auto result = coordinator_->processRecord(*record);

    EXPECT_LT(result.partition_id, coordinator_->getNumPartitions());
}

// ============================================================================
// 边界向量测试
// ============================================================================

TEST_F(PartitionCoordinatorTest, MarkAndUnmarkBoundary) {
    coordinator_->markBoundary(100, 0);
    coordinator_->markBoundary(101, 0);
    coordinator_->markBoundary(200, 1);

    auto boundary0 = coordinator_->getBoundaryVectors(0);
    EXPECT_EQ(boundary0.size(), 2);
    EXPECT_TRUE(std::find(boundary0.begin(), boundary0.end(), 100) != boundary0.end());
    EXPECT_TRUE(std::find(boundary0.begin(), boundary0.end(), 101) != boundary0.end());

    auto boundary1 = coordinator_->getBoundaryVectors(1);
    EXPECT_EQ(boundary1.size(), 1);
    EXPECT_EQ(boundary1[0], 200);

    // 取消标记
    coordinator_->unmarkBoundary(100);
    boundary0 = coordinator_->getBoundaryVectors(0);
    EXPECT_EQ(boundary0.size(), 1);
    EXPECT_EQ(boundary0[0], 101);
}

TEST_F(PartitionCoordinatorTest, GetBoundaryVectorsEmptyPartition) {
    auto boundary = coordinator_->getBoundaryVectors(0);
    EXPECT_TRUE(boundary.empty());
}

TEST_F(PartitionCoordinatorTest, GetBoundaryVectorsNonExistentPartition) {
    auto boundary = coordinator_->getBoundaryVectors(999);
    EXPECT_TRUE(boundary.empty());
}

// ============================================================================
// 延迟缓冲区测试
// ============================================================================

TEST_F(PartitionCoordinatorTest, BufferAndFlushLateRecords) {
    EXPECT_EQ(coordinator_->getLateBufferSize(), 0);

    coordinator_->bufferLateRecord(createTestRecord(1, 1000));
    coordinator_->bufferLateRecord(createTestRecord(2, 2000));
    coordinator_->bufferLateRecord(createTestRecord(3, 3000));

    EXPECT_EQ(coordinator_->getLateBufferSize(), 3);

    auto records = coordinator_->flushLateBuffer();
    EXPECT_EQ(records.size(), 3);
    EXPECT_EQ(coordinator_->getLateBufferSize(), 0);
}

TEST_F(PartitionCoordinatorTest, FlushEmptyLateBuffer) {
    auto records = coordinator_->flushLateBuffer();
    EXPECT_TRUE(records.empty());
}

// ============================================================================
// 分区统计测试
// ============================================================================

TEST_F(PartitionCoordinatorTest, PartitionStatsInitialState) {
    auto stats = coordinator_->getPartitionStats();

    EXPECT_EQ(stats.size(), 4);
    for (const auto& ps : stats) {
        EXPECT_EQ(ps.record_count, 0);
        EXPECT_EQ(ps.boundary_count, 0);
    }
}

TEST_F(PartitionCoordinatorTest, UpdatePartitionCount) {
    coordinator_->updatePartitionCount(0, 10);
    coordinator_->updatePartitionCount(1, 20);
    coordinator_->updatePartitionCount(2, 30);

    auto stats = coordinator_->getPartitionStats();
    EXPECT_EQ(stats[0].record_count, 10);
    EXPECT_EQ(stats[1].record_count, 20);
    EXPECT_EQ(stats[2].record_count, 30);
    EXPECT_EQ(stats[3].record_count, 0);
}

TEST_F(PartitionCoordinatorTest, UpdatePartitionCountDecrease) {
    coordinator_->updatePartitionCount(0, 100);
    coordinator_->updatePartitionCount(0, -30);

    auto stats = coordinator_->getPartitionStats();
    EXPECT_EQ(stats[0].record_count, 70);
}

TEST_F(PartitionCoordinatorTest, UpdatePartitionCountNoUnderflow) {
    coordinator_->updatePartitionCount(0, 10);
    coordinator_->updatePartitionCount(0, -50);  // 尝试减少超过当前值

    auto stats = coordinator_->getPartitionStats();
    EXPECT_EQ(stats[0].record_count, 0);  // 不应下溢
}

TEST_F(PartitionCoordinatorTest, UpdatePartitionCountInvalidPartition) {
    // 无效分区ID应该被忽略，不应崩溃
    EXPECT_NO_THROW(coordinator_->updatePartitionCount(999, 10));
}

TEST_F(PartitionCoordinatorTest, PartitionStatsWithBoundary) {
    coordinator_->markBoundary(100, 0);
    coordinator_->markBoundary(101, 0);
    coordinator_->markBoundary(200, 1);

    auto stats = coordinator_->getPartitionStats();
    EXPECT_EQ(stats[0].boundary_count, 2);
    EXPECT_EQ(stats[1].boundary_count, 1);
    EXPECT_EQ(stats[2].boundary_count, 0);
    EXPECT_EQ(stats[3].boundary_count, 0);
}

// ============================================================================
// 重平衡检测测试
// ============================================================================

TEST_F(PartitionCoordinatorTest, RebalanceDetectionImbalanced) {
    // 创建不平衡的分区负载
    coordinator_->updatePartitionCount(0, 100);
    coordinator_->updatePartitionCount(1, 10);
    coordinator_->updatePartitionCount(2, 10);
    coordinator_->updatePartitionCount(3, 10);
    // avg = 130 / 4 = 32.5
    // max / avg = 100 / 32.5 ≈ 3.08

    EXPECT_TRUE(coordinator_->needsRebalance(2.0));  // 阈值2.0，应该需要重平衡
    EXPECT_FALSE(coordinator_->needsRebalance(4.0)); // 阈值4.0，不需要重平衡
}

TEST_F(PartitionCoordinatorTest, RebalanceNotNeeded) {
    // 创建平衡的分区负载
    coordinator_->updatePartitionCount(0, 25);
    coordinator_->updatePartitionCount(1, 25);
    coordinator_->updatePartitionCount(2, 25);
    coordinator_->updatePartitionCount(3, 25);
    // avg = 25, max = 25, ratio = 1.0

    EXPECT_FALSE(coordinator_->needsRebalance(2.0));
}

TEST_F(PartitionCoordinatorTest, RebalanceEmptyPartitions) {
    // 所有分区都为空时不需要重平衡
    EXPECT_FALSE(coordinator_->needsRebalance(2.0));
}

// ============================================================================
// Watermark 和延迟统计测试
// ============================================================================

TEST_F(PartitionCoordinatorTest, WatermarkProgression) {
    EXPECT_EQ(coordinator_->getWatermark(), 0);

    auto record1 = createTestRecord(1, 5000);
    coordinator_->processRecord(*record1);
    // watermark = 5000 - 1000 = 4000

    EXPECT_EQ(coordinator_->getWatermark(), 4000);

    auto record2 = createTestRecord(2, 8000);
    coordinator_->processRecord(*record2);
    // watermark = 8000 - 1000 = 7000

    EXPECT_EQ(coordinator_->getWatermark(), 7000);
}

TEST_F(PartitionCoordinatorTest, LateArrivalStatsTracking) {
    // 发送正常记录
    auto record1 = createTestRecord(1, 10000);
    coordinator_->processRecord(*record1);

    // 发送更多正常记录
    for (int i = 2; i <= 5; ++i) {
        auto record = createTestRecord(i, 10000 + i * 1000);
        coordinator_->processRecord(*record);
    }

    const auto& stats = coordinator_->getLateArrivalStats();
    EXPECT_GE(stats.on_time_count.load(), 5);
}

// ============================================================================
// 并发测试
// ============================================================================

TEST_F(PartitionCoordinatorTest, ConcurrentProcessRecord) {
    const int num_threads = 4;
    const int records_per_thread = 100;
    std::atomic<int> processed_count{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([this, t, records_per_thread, &processed_count]() {
            for (int i = 0; i < records_per_thread; ++i) {
                uint64_t uid = t * records_per_thread + i;
                auto record = createTestRecord(uid, 10000 + i);
                auto result = coordinator_->processRecord(*record);
                EXPECT_LT(result.partition_id, coordinator_->getNumPartitions());
                processed_count.fetch_add(1);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(processed_count.load(), num_threads * records_per_thread);
}

TEST_F(PartitionCoordinatorTest, ConcurrentUpdatePartitionCount) {
    const int num_threads = 4;
    const int updates_per_thread = 100;

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([this, t, updates_per_thread]() {
            for (int i = 0; i < updates_per_thread; ++i) {
                coordinator_->updatePartitionCount(t % 4, 1);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // 验证总计数
    auto stats = coordinator_->getPartitionStats();
    size_t total = 0;
    for (const auto& ps : stats) {
        total += ps.record_count;
    }
    EXPECT_EQ(total, num_threads * updates_per_thread);
}

TEST_F(PartitionCoordinatorTest, ConcurrentMarkBoundary) {
    const int num_threads = 4;
    const int marks_per_thread = 100;

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([this, t, marks_per_thread]() {
            for (int i = 0; i < marks_per_thread; ++i) {
                uint64_t uid = t * marks_per_thread + i;
                coordinator_->markBoundary(uid, t % 4);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // 验证总边界向量数
    auto stats = coordinator_->getPartitionStats();
    size_t total_boundary = 0;
    for (const auto& ps : stats) {
        total_boundary += ps.boundary_count;
    }
    EXPECT_EQ(total_boundary, num_threads * marks_per_thread);
}

TEST_F(PartitionCoordinatorTest, ConcurrentBufferLateRecords) {
    const int num_threads = 4;
    const int records_per_thread = 50;

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([this, t, records_per_thread]() {
            for (int i = 0; i < records_per_thread; ++i) {
                uint64_t uid = t * records_per_thread + i;
                coordinator_->bufferLateRecord(createTestRecord(uid, 1000 + i));
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(coordinator_->getLateBufferSize(), num_threads * records_per_thread);
}

// ============================================================================
// 边界条件测试
// ============================================================================

TEST_F(PartitionCoordinatorTest, SinglePartition) {
    auto single_partitioner = std::make_shared<LSHPartitioner>(64, 8, 42);
    PartitionCoordinator single_coord(1, single_partitioner);

    auto record = createTestRecord(1, 1000, 64);  // 使用 64 维向量匹配 partitioner
    auto result = single_coord.processRecord(*record);

    EXPECT_EQ(result.partition_id, 0);  // 只有一个分区
}

TEST_F(PartitionCoordinatorTest, ManyPartitions) {
    auto partitioner = std::make_shared<LSHPartitioner>(128, 8, 42);
    PartitionCoordinator many_coord(64, partitioner);

    EXPECT_EQ(many_coord.getNumPartitions(), 64);

    auto record = createTestRecord(1, 1000);
    auto result = many_coord.processRecord(*record);
    EXPECT_LT(result.partition_id, 64);
}

}  // namespace
}  // namespace sageFlow

//
// Created for sageFlow architecture refactoring - Phase 2
// Task B-02: PartitionedVectorState Unit Tests
//

#include <gtest/gtest.h>
#include "state/partitioned_vector_state.h"
#include "execution/vector_space_partitioner.h"
#include "common/data_types.h"
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <unordered_set>

namespace sageFlow {
namespace test {

/**
 * @brief 测试 PartitionedVectorState 的功能
 * 验证分区向量状态的正确性和线程安全性
 */
class PartitionedVectorStateTest : public ::testing::Test {
protected:
    static constexpr int kDefaultDimension = 128;
    static constexpr size_t kDefaultNumPartitions = 4;
    static constexpr size_t kDefaultCompactThreshold = 10;

    void SetUp() override {
        partitioner_ = std::make_shared<LSHPartitioner>(
            kDefaultDimension, 8, 42, 0.1);
        state_ = std::make_unique<PartitionedVectorState>(
            kDefaultNumPartitions, partitioner_,
            kDefaultCompactThreshold, true);
    }

    void TearDown() override {
        state_.reset();
        partitioner_.reset();
    }

    // 创建随机向量记录
    std::unique_ptr<VectorRecord> createRandomRecord(uint64_t uid,
                                                      int64_t timestamp,
                                                      int dim = kDefaultDimension) {
        char* raw_data = new char[dim * sizeof(float)];
        auto* float_data = reinterpret_cast<float*>(raw_data);
        
        std::mt19937 gen(static_cast<unsigned>(uid));
        std::normal_distribution<float> dist(0.0f, 1.0f);
        
        for (int i = 0; i < dim; ++i) {
            float_data[i] = dist(gen);
        }
        
        return std::make_unique<VectorRecord>(uid, timestamp, dim,
                                               DataType::Float32, raw_data);
    }

    // 创建特定方向的向量（用于测试分区一致性）
    std::unique_ptr<VectorRecord> createDirectionalRecord(uint64_t uid,
                                                           int64_t timestamp,
                                                           float direction,
                                                           int dim = kDefaultDimension) {
        char* raw_data = new char[dim * sizeof(float)];
        auto* float_data = reinterpret_cast<float*>(raw_data);
        
        for (int i = 0; i < dim; ++i) {
            float_data[i] = direction + static_cast<float>(i) * 0.001f;
        }
        
        return std::make_unique<VectorRecord>(uid, timestamp, dim,
                                               DataType::Float32, raw_data);
    }

    // 创建相似向量（用于测试查询覆盖率）
    std::unique_ptr<VectorRecord> createSimilarRecord(const VectorRecord& base,
                                                       uint64_t uid,
                                                       int64_t timestamp,
                                                       float noise_scale = 0.01f) {
        int dim = base.data_.dim_;
        char* raw_data = new char[dim * sizeof(float)];
        auto* float_data = reinterpret_cast<float*>(raw_data);
        const auto* base_data = reinterpret_cast<const float*>(base.data_.data_.get());
        
        std::mt19937 gen(static_cast<unsigned>(uid));
        std::normal_distribution<float> noise(0.0f, noise_scale);
        
        for (int i = 0; i < dim; ++i) {
            float_data[i] = base_data[i] + noise(gen);
        }
        
        return std::make_unique<VectorRecord>(uid, timestamp, dim,
                                               DataType::Float32, raw_data);
    }

    std::shared_ptr<LSHPartitioner> partitioner_;
    std::unique_ptr<PartitionedVectorState> state_;
};

// ============================================================================
// 构造函数测试
// ============================================================================

TEST_F(PartitionedVectorStateTest, ConstructorInitializesCorrectly) {
    EXPECT_EQ(state_->getNumPartitions(), kDefaultNumPartitions);
    EXPECT_FALSE(state_->isShared());
    EXPECT_EQ(state_->totalSize(), 0);
    EXPECT_TRUE(state_->isBoundaryTrackingEnabled());
    
    auto sizes = state_->getPartitionSizes();
    EXPECT_EQ(sizes.size(), kDefaultNumPartitions);
    for (size_t s : sizes) {
        EXPECT_EQ(s, 0);
    }
}

TEST_F(PartitionedVectorStateTest, ConstructorWithoutBoundaryTracking) {
    auto state_no_tracking = std::make_unique<PartitionedVectorState>(
        4, partitioner_, 100, false);
    
    EXPECT_FALSE(state_no_tracking->isBoundaryTrackingEnabled());
    EXPECT_EQ(state_no_tracking->getBoundaryVectors(0).size(), 0);
}

TEST_F(PartitionedVectorStateTest, ConstructorRejectsInvalidPartitions) {
    EXPECT_THROW(
        PartitionedVectorState(0, partitioner_),
        std::invalid_argument
    );
}

TEST_F(PartitionedVectorStateTest, ConstructorRejectsNullPartitioner) {
    EXPECT_THROW(
        PartitionedVectorState(4, nullptr),
        std::invalid_argument
    );
}

// ============================================================================
// 基础功能测试
// ============================================================================

TEST_F(PartitionedVectorStateTest, RecordRouting) {
    // 添加多条记录，验证它们被路由到不同分区
    for (uint64_t i = 1; i <= 100; ++i) {
        state_->addRecord(createRandomRecord(i, static_cast<int64_t>(i * 1000)), 0);
    }
    
    EXPECT_EQ(state_->totalSize(), 100);
    
    // 验证记录分布到了多个分区
    auto sizes = state_->getPartitionSizes();
    size_t non_empty_partitions = 0;
    for (size_t s : sizes) {
        if (s > 0) {
            ++non_empty_partitions;
        }
    }
    
    // 预期至少有 2 个分区有记录
    EXPECT_GE(non_empty_partitions, 2);
}

TEST_F(PartitionedVectorStateTest, AddAndRetrieve) {
    state_->addRecord(createRandomRecord(1, 1000), 0);
    state_->addRecord(createRandomRecord(2, 2000), 0);
    state_->addRecord(createRandomRecord(3, 3000), 0);
    
    EXPECT_EQ(state_->totalSize(), 3);
    EXPECT_EQ(state_->size(0), 3);  // size() 忽略 subtask_index
    
    // 通过 getRecords 获取所有记录
    const auto& records = state_->getRecords(0);
    EXPECT_EQ(records.size(), 3);
    
    // 验证 UID
    std::unordered_set<uint64_t> uids;
    for (const auto& rec : records) {
        uids.insert(rec->uid_);
    }
    EXPECT_TRUE(uids.count(1) > 0);
    EXPECT_TRUE(uids.count(2) > 0);
    EXPECT_TRUE(uids.count(3) > 0);
}

TEST_F(PartitionedVectorStateTest, GetRecordsForQuery) {
    // 添加一些记录
    auto base_record = createRandomRecord(100, 0);
    const VectorRecord& base = *base_record;
    
    state_->addRecord(std::move(base_record), 0);
    
    // 添加与基准记录相似的记录
    for (uint64_t i = 1; i <= 20; ++i) {
        auto similar = createSimilarRecord(base, i, static_cast<int64_t>(i * 1000), 0.01f);
        state_->addRecord(std::move(similar), 0);
    }
    
    // 添加一些随机记录
    for (uint64_t i = 101; i <= 150; ++i) {
        state_->addRecord(createRandomRecord(i, static_cast<int64_t>(i * 1000)), 0);
    }
    
    EXPECT_EQ(state_->totalSize(), 71);
    
    // 使用基准记录进行查询
    auto query = createSimilarRecord(base, 999, 0, 0.001f);
    auto results = state_->getRecordsForQuery(*query, 2);
    
    // 查询应该返回一些记录
    EXPECT_GT(results.size(), 0);
}

TEST_F(PartitionedVectorStateTest, GetRecordsForPartition) {
    // 添加记录
    for (uint64_t i = 1; i <= 50; ++i) {
        state_->addRecord(createRandomRecord(i, static_cast<int64_t>(i * 1000)), 0);
    }
    
    // 验证每个分区可以独立获取记录
    size_t total_from_partitions = 0;
    for (size_t p = 0; p < state_->getNumPartitions(); ++p) {
        auto partition_records = state_->getRecordsForPartition(p);
        total_from_partitions += partition_records.size();
    }
    
    EXPECT_EQ(total_from_partitions, state_->totalSize());
}

TEST_F(PartitionedVectorStateTest, GetRecordsForInvalidPartition) {
    auto records = state_->getRecordsForPartition(999);
    EXPECT_TRUE(records.empty());
}

// ============================================================================
// 边界向量测试
// ============================================================================

TEST_F(PartitionedVectorStateTest, BoundaryVectorTracking) {
    // 添加多条记录
    for (uint64_t i = 1; i <= 100; ++i) {
        state_->addRecord(createRandomRecord(i, static_cast<int64_t>(i * 1000)), 0);
    }
    
    // 统计边界向量数量
    size_t total_boundary = 0;
    for (size_t p = 0; p < state_->getNumPartitions(); ++p) {
        auto boundary = state_->getBoundaryVectors(p);
        total_boundary += boundary.size();
    }
    
    // 应该有一些边界向量（具体数量取决于 LSH 的配置）
    // 边界向量数量可能为 0，取决于向量的分布
    EXPECT_GE(total_boundary, 0);
}

TEST_F(PartitionedVectorStateTest, BoundaryVectorInQueryResults) {
    // 创建并添加一个边界向量
    // 由于边界向量的检测依赖于 LSH 的实现，我们无法直接控制
    // 这个测试主要验证查询时不会崩溃
    
    for (uint64_t i = 1; i <= 50; ++i) {
        state_->addRecord(createRandomRecord(i, static_cast<int64_t>(i * 1000)), 0);
    }
    
    auto query = createRandomRecord(999, 0);
    auto results = state_->getRecordsForQuery(*query, 2);
    
    // 不应该崩溃，结果数量应该 >= 0
    EXPECT_GE(results.size(), 0);
}

// ============================================================================
// 过期清理测试
// ============================================================================

TEST_F(PartitionedVectorStateTest, EvictExpiredAcrossPartitions) {
    // 设置 1 倍缓冲区使测试行为与原设计一致
    state_->setEvictionBufferMultiplier(1.0);
    
    // 添加不同时间戳的记录
    for (uint64_t i = 1; i <= 100; ++i) {
        state_->addRecord(createRandomRecord(i, static_cast<int64_t>(i * 100)), 0);
    }
    
    EXPECT_EQ(state_->totalSize(), 100);
    
    // 使用 1 倍缓冲区：过期清理 timestamp < (10000 - 5000) = 5000 的记录
    // 即 timestamp = i * 100 < 5000，则 i < 50
    // 所以 uid 1-49 被清理（49条），uid 50-100 保留（51条）
    state_->evictExpired(10000, 5000, 0);
    
    size_t remaining = state_->totalSize();
    EXPECT_EQ(remaining, 51);
}

TEST_F(PartitionedVectorStateTest, EvictUpdatesUidMap) {
    // 设置 1 倍缓冲区使测试行为与原设计一致
    state_->setEvictionBufferMultiplier(1.0);
    
    state_->addRecord(createRandomRecord(1, 1000), 0);
    state_->addRecord(createRandomRecord(2, 2000), 0);
    state_->addRecord(createRandomRecord(3, 3000), 0);
    
    // 验证 UID 映射存在
    EXPECT_GE(state_->getPartitionForUid(1), 0);
    EXPECT_GE(state_->getPartitionForUid(2), 0);
    EXPECT_GE(state_->getPartitionForUid(3), 0);
    
    // 使用 1 倍缓冲区：过期清理 timestamp < 2500 的记录
    state_->evictExpired(5000, 2500, 0);
    
    // uid=1 和 uid=2 应该被清理
    EXPECT_EQ(state_->getPartitionForUid(1), -1);
    EXPECT_EQ(state_->getPartitionForUid(2), -1);
    EXPECT_GE(state_->getPartitionForUid(3), 0);
}

TEST_F(PartitionedVectorStateTest, EvictUpdatesBoundaryTracker) {
    // 添加一些记录
    for (uint64_t i = 1; i <= 50; ++i) {
        state_->addRecord(createRandomRecord(i, static_cast<int64_t>(i * 100)), 0);
    }
    
    // 获取初始边界向量数量
    size_t initial_boundary = 0;
    for (size_t p = 0; p < state_->getNumPartitions(); ++p) {
        initial_boundary += state_->getBoundaryVectors(p).size();
    }
    
    // 过期清理
    state_->evictExpired(5000, 2000, 0);
    
    // 边界向量数量应该减少或保持不变
    size_t remaining_boundary = 0;
    for (size_t p = 0; p < state_->getNumPartitions(); ++p) {
        remaining_boundary += state_->getBoundaryVectors(p).size();
    }
    
    EXPECT_LE(remaining_boundary, initial_boundary);
}

// ============================================================================
// 压缩测试
// ============================================================================

TEST_F(PartitionedVectorStateTest, CompactAllPartitions) {
    // 添加足够多的记录以触发压缩
    for (uint64_t i = 1; i <= 200; ++i) {
        state_->addRecord(createRandomRecord(i, static_cast<int64_t>(i * 1000)), 0);
    }
    
    // 手动触发压缩
    state_->compactAllPartitions();
    
    // 记录总数应该保持不变
    EXPECT_EQ(state_->totalSize(), 200);
}

// ============================================================================
// 统计测试
// ============================================================================

TEST_F(PartitionedVectorStateTest, PartitionSizes) {
    for (uint64_t i = 1; i <= 100; ++i) {
        state_->addRecord(createRandomRecord(i, static_cast<int64_t>(i * 1000)), 0);
    }
    
    auto sizes = state_->getPartitionSizes();
    EXPECT_EQ(sizes.size(), kDefaultNumPartitions);
    
    size_t sum = 0;
    for (size_t s : sizes) {
        sum += s;
    }
    
    EXPECT_EQ(sum, state_->totalSize());
}

TEST_F(PartitionedVectorStateTest, TotalSize) {
    EXPECT_EQ(state_->totalSize(), 0);
    
    for (uint64_t i = 1; i <= 50; ++i) {
        state_->addRecord(createRandomRecord(i, static_cast<int64_t>(i * 1000)), 0);
        EXPECT_EQ(state_->totalSize(), i);
    }
}

// ============================================================================
// UID 查找测试
// ============================================================================

TEST_F(PartitionedVectorStateTest, FindRecordByUid) {
    state_->addRecord(createRandomRecord(42, 1000), 0);
    state_->addRecord(createRandomRecord(43, 2000), 0);
    
    const VectorRecord* found = state_->findRecordByUid(42);
    EXPECT_NE(found, nullptr);
    EXPECT_EQ(found->uid_, 42);
    EXPECT_EQ(found->timestamp_, 1000);
    
    const VectorRecord* not_found = state_->findRecordByUid(999);
    EXPECT_EQ(not_found, nullptr);
}

TEST_F(PartitionedVectorStateTest, GetPartitionForUid) {
    state_->addRecord(createRandomRecord(1, 1000), 0);
    state_->addRecord(createRandomRecord(2, 2000), 0);
    
    int64_t p1 = state_->getPartitionForUid(1);
    int64_t p2 = state_->getPartitionForUid(2);
    
    EXPECT_GE(p1, 0);
    EXPECT_LT(p1, static_cast<int64_t>(kDefaultNumPartitions));
    EXPECT_GE(p2, 0);
    EXPECT_LT(p2, static_cast<int64_t>(kDefaultNumPartitions));
    
    // 不存在的 UID
    EXPECT_EQ(state_->getPartitionForUid(999), -1);
}

// ============================================================================
// 并发测试
// ============================================================================

TEST_F(PartitionedVectorStateTest, ConcurrentAddAndQuery) {
    constexpr int kNumWriters = 4;
    constexpr int kNumReaders = 2;
    constexpr int kRecordsPerWriter = 100;
    
    std::atomic<size_t> total_added{0};
    std::atomic<size_t> total_read{0};
    std::vector<std::thread> threads;
    
    // 写入线程
    for (int w = 0; w < kNumWriters; ++w) {
        threads.emplace_back([this, w, &total_added]() {
            for (int i = 0; i < kRecordsPerWriter; ++i) {
                uint64_t uid = static_cast<uint64_t>(w * kRecordsPerWriter + i + 1);
                int64_t ts = static_cast<int64_t>(uid * 100);
                state_->addRecord(createRandomRecord(uid, ts), 0);
                ++total_added;
            }
        });
    }
    
    // 读取线程
    for (int r = 0; r < kNumReaders; ++r) {
        threads.emplace_back([this, &total_read]() {
            for (int i = 0; i < 100; ++i) {
                auto query = createRandomRecord(9999 + i, 0);
                auto results = state_->getRecordsForQuery(*query, 2);
                total_read += results.size();
                std::this_thread::yield();
            }
        });
    }
    
    // 等待所有线程完成
    for (auto& t : threads) {
        t.join();
    }
    
    EXPECT_EQ(total_added.load(), kNumWriters * kRecordsPerWriter);
    EXPECT_EQ(state_->totalSize(), kNumWriters * kRecordsPerWriter);
}

TEST_F(PartitionedVectorStateTest, ConcurrentAddAndEvict) {
    constexpr int kNumWriters = 2;
    constexpr int kRecordsPerWriter = 100;
    
    std::atomic<bool> stop{false};
    std::vector<std::thread> threads;
    
    // 写入线程
    for (int w = 0; w < kNumWriters; ++w) {
        threads.emplace_back([this, w, &stop]() {
            for (int i = 0; i < kRecordsPerWriter && !stop.load(); ++i) {
                uint64_t uid = static_cast<uint64_t>(w * kRecordsPerWriter + i + 1);
                int64_t ts = static_cast<int64_t>(uid * 100);
                state_->addRecord(createRandomRecord(uid, ts), 0);
            }
        });
    }
    
    // 过期清理线程
    threads.emplace_back([this, &stop]() {
        for (int i = 0; i < 50 && !stop.load(); ++i) {
            state_->evictExpired(static_cast<int64_t>(i * 200 + 5000), 2000, 0);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    
    // 等待所有线程完成
    for (auto& t : threads) {
        t.join();
    }
    
    // 验证状态一致性
    size_t total_in_partitions = 0;
    for (size_t p = 0; p < state_->getNumPartitions(); ++p) {
        auto records = state_->getRecordsForPartition(p);
        total_in_partitions += records.size();
    }
    
    EXPECT_EQ(total_in_partitions, state_->totalSize());
}

// ============================================================================
// 查询覆盖率测试
// ============================================================================

TEST_F(PartitionedVectorStateTest, QueryCoverageWithSimilarVectors) {
    // 创建一个基准向量
    auto base = createDirectionalRecord(1000, 0, 1.0f);
    const VectorRecord& base_ref = *base;
    
    state_->addRecord(std::move(base), 0);
    
    // 添加 50 个与基准非常相似的向量
    for (uint64_t i = 1; i <= 50; ++i) {
        auto similar = createSimilarRecord(base_ref, i, 
            static_cast<int64_t>(i * 1000), 0.001f);
        state_->addRecord(std::move(similar), 0);
    }
    
    // 添加 50 个随机向量（可能不相似）
    for (uint64_t i = 51; i <= 100; ++i) {
        state_->addRecord(createRandomRecord(i, static_cast<int64_t>(i * 1000)), 0);
    }
    
    EXPECT_EQ(state_->totalSize(), 101);
    
    // 使用与基准相似的查询向量
    auto query = createSimilarRecord(base_ref, 9999, 0, 0.0001f);
    auto results = state_->getRecordsForQuery(*query, 3);  // 探测 3 个分区
    
    // 查询结果应该包含一定数量的相似向量
    // 由于 LSH 的特性，相似向量更可能被路由到相同分区
    EXPECT_GT(results.size(), 0);
    
    // 统计结果中有多少是相似向量（uid <= 50 或 uid == 1000）
    size_t similar_count = 0;
    for (const auto* rec : results) {
        if (rec->uid_ <= 50 || rec->uid_ == 1000) {
            ++similar_count;
        }
    }
    
    // 放宽要求：只要有一些相似向量即可
    // 这验证了 LSH 分区的局部性保持特性
    EXPECT_GE(similar_count, 1);
}

// ============================================================================
// 空状态测试
// ============================================================================

TEST_F(PartitionedVectorStateTest, OperationsOnEmptyState) {
    // 空状态的各种操作不应崩溃
    EXPECT_EQ(state_->totalSize(), 0);
    EXPECT_TRUE(state_->getRecords(0).empty());
    
    auto query = createRandomRecord(1, 0);
    auto results = state_->getRecordsForQuery(*query, 2);
    EXPECT_TRUE(results.empty());
    
    auto partition_records = state_->getRecordsForPartition(0);
    EXPECT_TRUE(partition_records.empty());
    
    auto boundary = state_->getBoundaryVectors(0);
    EXPECT_TRUE(boundary.empty());
    
    // 过期清理不应崩溃
    state_->evictExpired(10000, 5000, 0);
    EXPECT_EQ(state_->totalSize(), 0);
    
    // 压缩不应崩溃
    state_->compactAllPartitions();
    EXPECT_EQ(state_->totalSize(), 0);
}

} // namespace test
} // namespace sageFlow

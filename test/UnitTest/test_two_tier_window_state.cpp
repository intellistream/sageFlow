//
// Created for sageFlow architecture refactoring - Phase 2
// Task A-01: TwoTierWindowState Unit Tests
//

#include <gtest/gtest.h>
#include "state/two_tier_window_state.h"
#include "common/data_types.h"
#include <thread>
#include <vector>
#include <atomic>

namespace sageFlow {
namespace test {

/**
 * @brief 测试 TwoTierWindowState 的功能
 * 验证双层窗口状态的正确性和线程安全性
 */
class TwoTierWindowStateTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 默认配置：4个分区，阈值10，批量大小5
        state_ = std::make_unique<TwoTierWindowState>(4, 10, 5);
    }

    void TearDown() override {
        state_.reset();
    }

    // 创建测试用的 VectorRecord
    std::unique_ptr<VectorRecord> createTestRecord(uint64_t uid, 
                                                    int64_t timestamp, 
                                                    int dim = 64) {
        char* raw_data = new char[dim * sizeof(float)];
        float* float_data = reinterpret_cast<float*>(raw_data);
        for (int i = 0; i < dim; ++i) {
            float_data[i] = static_cast<float>(uid * 0.1 + i);
        }
        return std::make_unique<VectorRecord>(uid, timestamp, dim, 
                                               DataType::Float32, raw_data);
    }

    std::unique_ptr<TwoTierWindowState> state_;
};

// ============================================================================
// 基础功能测试
// ============================================================================

TEST_F(TwoTierWindowStateTest, ConstructorInitializesCorrectly) {
    TwoTierWindowState state(8, 100, 50);
    
    EXPECT_FALSE(state.isShared());
    for (size_t i = 0; i < 8; ++i) {
        EXPECT_EQ(state.size(i), 0);
        EXPECT_EQ(state.getWriteTierSize(i), 0);
        EXPECT_EQ(state.getCompactTierSize(i), 0);
    }
}

TEST_F(TwoTierWindowStateTest, AddRecordToWriteTier) {
    // 添加少量记录，应该只存在于写层
    state_->addRecord(createTestRecord(1, 1000), 0);
    state_->addRecord(createTestRecord(2, 2000), 0);
    state_->addRecord(createTestRecord(3, 3000), 0);
    
    EXPECT_EQ(state_->size(0), 3);
    EXPECT_EQ(state_->getWriteTierSize(0), 3);
    EXPECT_EQ(state_->getCompactTierSize(0), 0);
    
    // 其他分区应该为空
    EXPECT_EQ(state_->size(1), 0);
    EXPECT_EQ(state_->size(2), 0);
}

TEST_F(TwoTierWindowStateTest, GetRecordsReturnsAllRecords) {
    state_->addRecord(createTestRecord(1, 1000), 0);
    state_->addRecord(createTestRecord(2, 2000), 0);
    state_->addRecord(createTestRecord(3, 3000), 0);
    
    const auto& records = state_->getRecords(0);
    
    EXPECT_EQ(records.size(), 3);
    EXPECT_EQ(records[0]->uid_, 1);
    EXPECT_EQ(records[1]->uid_, 2);
    EXPECT_EQ(records[2]->uid_, 3);
}

TEST_F(TwoTierWindowStateTest, GetAllRecordsReturnsPointers) {
    state_->addRecord(createTestRecord(1, 1000), 0);
    state_->addRecord(createTestRecord(2, 2000), 0);
    
    auto all_records = state_->getAllRecords(0);
    
    EXPECT_EQ(all_records.size(), 2);
    EXPECT_EQ(all_records[0]->uid_, 1);
    EXPECT_EQ(all_records[1]->uid_, 2);
}

TEST_F(TwoTierWindowStateTest, EvictExpiredFromWriteTier) {
    // 设置 1 倍缓冲区使测试行为与原设计一致
    state_->setEvictionBufferMultiplier(1.0);
    
    state_->addRecord(createTestRecord(1, 1000), 0);
    state_->addRecord(createTestRecord(2, 2000), 0);
    state_->addRecord(createTestRecord(3, 5000), 0);
    state_->addRecord(createTestRecord(4, 8000), 0);
    
    EXPECT_EQ(state_->size(0), 4);
    
    // 清理过期记录（窗口大小为 3000，当前时间 9000）
    // 使用 1 倍缓冲区：应该保留时间戳 >= 6000 的记录
    state_->evictExpired(9000, 3000, 0);
    
    EXPECT_EQ(state_->size(0), 1);
    
    const auto& records = state_->getRecords(0);
    EXPECT_EQ(records[0]->uid_, 4);
}

TEST_F(TwoTierWindowStateTest, SizeReturnsTotal) {
    // 先添加到写层
    for (int i = 0; i < 5; ++i) {
        state_->addRecord(createTestRecord(i, 1000 + i * 100), 0);
    }
    EXPECT_EQ(state_->size(0), 5);
    
    // 手动压缩
    state_->compactTiers(0);
    
    // 总大小应该不变
    EXPECT_EQ(state_->size(0), 5);
}

// ============================================================================
// 压缩触发测试
// ============================================================================

TEST_F(TwoTierWindowStateTest, CompactTriggeredWhenThresholdReached) {
    // 阈值为10，添加10条记录应该触发压缩
    for (int i = 0; i < 10; ++i) {
        state_->addRecord(createTestRecord(i, 1000 + i * 100), 0);
    }
    
    // 压缩后，紧凑层应该有记录
    EXPECT_GT(state_->getCompactTierSize(0), 0);
    EXPECT_EQ(state_->size(0), 10);  // 总数不变
}

TEST_F(TwoTierWindowStateTest, CompactMovesOldRecordsToCompactTier) {
    // 添加6条记录（不触发自动压缩，阈值是10）
    for (int i = 0; i < 6; ++i) {
        state_->addRecord(createTestRecord(i, 1000 + i * 100), 0);
    }
    
    EXPECT_EQ(state_->getWriteTierSize(0), 6);
    EXPECT_EQ(state_->getCompactTierSize(0), 0);
    
    // 手动触发压缩（批量大小为5）
    state_->compactTiers(0);
    
    // 应该移动5条记录到紧凑层
    EXPECT_EQ(state_->getWriteTierSize(0), 1);
    EXPECT_EQ(state_->getCompactTierSize(0), 5);
    EXPECT_EQ(state_->size(0), 6);  // 总数不变
}

TEST_F(TwoTierWindowStateTest, CompactMaintainsTimestampOrder) {
    // 添加乱序时间戳的记录
    state_->addRecord(createTestRecord(1, 5000), 0);
    state_->addRecord(createTestRecord(2, 2000), 0);
    state_->addRecord(createTestRecord(3, 8000), 0);
    state_->addRecord(createTestRecord(4, 1000), 0);
    state_->addRecord(createTestRecord(5, 3000), 0);
    state_->addRecord(createTestRecord(6, 6000), 0);
    
    // 手动压缩
    state_->compactTiers(0);
    
    // 紧凑层应该按时间戳排序
    const auto& compact_records = state_->getCompactRecords(0);
    for (size_t i = 1; i < compact_records.size(); ++i) {
        EXPECT_LE(compact_records[i-1]->timestamp_, compact_records[i]->timestamp_);
    }
}

TEST_F(TwoTierWindowStateTest, CompactDoesNothingWhenInsufficientRecords) {
    // 添加少于批量大小的记录
    for (int i = 0; i < 3; ++i) {
        state_->addRecord(createTestRecord(i, 1000 + i * 100), 0);
    }
    
    // 手动压缩不应该移动任何记录
    state_->compactTiers(0);
    
    EXPECT_EQ(state_->getWriteTierSize(0), 3);
    EXPECT_EQ(state_->getCompactTierSize(0), 0);
}

// ============================================================================
// 两层清理测试
// ============================================================================

TEST_F(TwoTierWindowStateTest, EvictExpiredFromBothTiers) {
    // 设置 1 倍缓冲区使测试行为与原设计一致
    state_->setEvictionBufferMultiplier(1.0);
    
    // 添加足够多的记录触发压缩
    for (int i = 0; i < 12; ++i) {
        state_->addRecord(createTestRecord(i, 1000 + i * 100), 0);
    }
    
    // 确保两层都有记录
    EXPECT_GT(state_->getCompactTierSize(0), 0);
    EXPECT_GT(state_->getWriteTierSize(0), 0);
    
    size_t total_before = state_->size(0);
    
    // 使用 1 倍缓冲区：清理所有时间戳 < 2000 的记录
    state_->evictExpired(3000, 1000, 0);
    
    // 应该清理掉一些记录
    EXPECT_LE(state_->size(0), total_before);
    
    // 验证剩余记录都在窗口内
    auto all_records = state_->getAllRecords(0);
    for (const auto* record : all_records) {
        EXPECT_GE(record->timestamp_, 2000);
    }
}

// ============================================================================
// 并发测试
// ============================================================================

TEST_F(TwoTierWindowStateTest, ConcurrentAddRecords) {
    const int num_threads = 4;
    const int records_per_thread = 100;
    std::atomic<int> total_added{0};
    
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([this, t, records_per_thread, &total_added]() {
            for (int i = 0; i < records_per_thread; ++i) {
                state_->addRecord(
                    createTestRecord(t * 1000 + i, 1000 + i),
                    t  // 每个线程写入自己的分区
                );
                total_added.fetch_add(1);
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // 验证每个分区都有正确数量的记录
    size_t total = 0;
    for (int i = 0; i < num_threads; ++i) {
        total += state_->size(i);
    }
    EXPECT_EQ(total, num_threads * records_per_thread);
}

TEST_F(TwoTierWindowStateTest, ConcurrentReadAndWrite) {
    const int num_writers = 2;
    const int num_readers = 2;
    const int operations = 50;
    std::atomic<bool> stop{false};
    std::atomic<int> read_count{0};
    
    // 先添加一些初始数据
    for (int i = 0; i < 20; ++i) {
        state_->addRecord(createTestRecord(i, 1000 + i * 10), 0);
    }
    
    std::vector<std::thread> threads;
    
    // 写线程
    for (int w = 0; w < num_writers; ++w) {
        threads.emplace_back([this, w, operations]() {
            for (int i = 0; i < operations; ++i) {
                state_->addRecord(
                    createTestRecord(w * 1000 + i, 2000 + i * 10),
                    0
                );
                std::this_thread::yield();
            }
        });
    }
    
    // 读线程
    for (int r = 0; r < num_readers; ++r) {
        threads.emplace_back([this, &stop, &read_count]() {
            while (!stop.load()) {
                const auto& records = state_->getRecords(0);
                (void)records.size();  // 使用结果防止编译器优化
                read_count.fetch_add(1);
                std::this_thread::yield();
            }
        });
    }
    
    // 等待写线程完成
    for (int i = 0; i < num_writers; ++i) {
        threads[i].join();
    }
    
    stop.store(true);
    
    // 等待读线程完成
    for (size_t i = num_writers; i < threads.size(); ++i) {
        threads[i].join();
    }
    
    EXPECT_GT(read_count.load(), 0);
    EXPECT_EQ(state_->size(0), 20 + num_writers * operations);
}

TEST_F(TwoTierWindowStateTest, ConcurrentCompaction) {
    // 使用较小的阈值以触发更多压缩
    state_ = std::make_unique<TwoTierWindowState>(4, 5, 3);
    
    const int num_threads = 4;
    const int records_per_thread = 20;
    
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([this, t, records_per_thread]() {
            for (int i = 0; i < records_per_thread; ++i) {
                state_->addRecord(
                    createTestRecord(t * 1000 + i, 1000 + i * 10),
                    t % 2  // 两个分区
                );
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // 验证数据完整性
    size_t total = state_->size(0) + state_->size(1);
    EXPECT_EQ(total, num_threads * records_per_thread);
}

// ============================================================================
// 边界条件测试
// ============================================================================

TEST_F(TwoTierWindowStateTest, EmptyState) {
    EXPECT_EQ(state_->size(0), 0);
    EXPECT_EQ(state_->getWriteTierSize(0), 0);
    EXPECT_EQ(state_->getCompactTierSize(0), 0);
    
    const auto& records = state_->getRecords(0);
    EXPECT_TRUE(records.empty());
    
    auto all_records = state_->getAllRecords(0);
    EXPECT_TRUE(all_records.empty());
    
    const auto& compact = state_->getCompactRecords(0);
    EXPECT_TRUE(compact.empty());
}

TEST_F(TwoTierWindowStateTest, AllRecordsExpired) {
    // 设置 1 倍缓冲区使测试行为与原设计一致
    state_->setEvictionBufferMultiplier(1.0);
    
    // 添加一些记录
    for (int i = 0; i < 15; ++i) {
        state_->addRecord(createTestRecord(i, 1000 + i * 100), 0);
    }
    
    size_t before = state_->size(0);
    EXPECT_GT(before, 0);
    
    // 清理所有记录（使用很大的当前时间戳）
    state_->evictExpired(1000000, 1000, 0);
    
    EXPECT_EQ(state_->size(0), 0);
    EXPECT_EQ(state_->getWriteTierSize(0), 0);
    EXPECT_EQ(state_->getCompactTierSize(0), 0);
}

TEST_F(TwoTierWindowStateTest, SingleRecord) {
    state_->addRecord(createTestRecord(42, 5000), 0);
    
    EXPECT_EQ(state_->size(0), 1);
    
    const auto& records = state_->getRecords(0);
    EXPECT_EQ(records.size(), 1);
    EXPECT_EQ(records[0]->uid_, 42);
    EXPECT_EQ(records[0]->timestamp_, 5000);
}

TEST_F(TwoTierWindowStateTest, MultiplePartitions) {
    // 向不同分区添加不同数量的记录
    state_->addRecord(createTestRecord(1, 1000), 0);
    state_->addRecord(createTestRecord(2, 1000), 0);
    
    state_->addRecord(createTestRecord(3, 1000), 1);
    state_->addRecord(createTestRecord(4, 1000), 1);
    state_->addRecord(createTestRecord(5, 1000), 1);
    
    state_->addRecord(createTestRecord(6, 1000), 2);
    
    // 分区3为空
    
    EXPECT_EQ(state_->size(0), 2);
    EXPECT_EQ(state_->size(1), 3);
    EXPECT_EQ(state_->size(2), 1);
    EXPECT_EQ(state_->size(3), 0);
}

TEST_F(TwoTierWindowStateTest, LargeNumberOfRecords) {
    const int num_records = 1000;
    
    for (int i = 0; i < num_records; ++i) {
        state_->addRecord(createTestRecord(i, 1000 + i), 0);
    }
    
    EXPECT_EQ(state_->size(0), num_records);
    
    // 应该有多次压缩
    EXPECT_GT(state_->getCompactTierSize(0), 0);
    
    // 验证可以获取所有记录
    auto all_records = state_->getAllRecords(0);
    EXPECT_EQ(all_records.size(), num_records);
}

TEST_F(TwoTierWindowStateTest, EvictNothingWhenAllValid) {
    // 设置 1 倍缓冲区使测试行为与原设计一致
    state_->setEvictionBufferMultiplier(1.0);
    
    // 添加时间戳在窗口内的记录
    for (int i = 0; i < 5; ++i) {
        state_->addRecord(createTestRecord(i, 9000 + i * 100), 0);
    }
    
    // 清理过期记录，但所有记录都在窗口内
    state_->evictExpired(10000, 5000, 0);
    
    EXPECT_EQ(state_->size(0), 5);
}

TEST_F(TwoTierWindowStateTest, RepeatedCompaction) {
    // 使用较小的阈值
    state_ = std::make_unique<TwoTierWindowState>(1, 5, 3);
    
    // 添加足够多的记录触发多次压缩
    for (int i = 0; i < 30; ++i) {
        state_->addRecord(createTestRecord(i, 1000 + i * 10), 0);
    }
    
    EXPECT_EQ(state_->size(0), 30);
    
    // 紧凑层应该有多次压缩的结果
    EXPECT_GT(state_->getCompactTierSize(0), 10);
    
    // 验证紧凑层有序
    const auto& compact = state_->getCompactRecords(0);
    for (size_t i = 1; i < compact.size(); ++i) {
        EXPECT_LE(compact[i-1]->timestamp_, compact[i]->timestamp_);
    }
}

// ============================================================================
// isShared 测试
// ============================================================================

TEST_F(TwoTierWindowStateTest, IsNotShared) {
    EXPECT_FALSE(state_->isShared());
}

} // namespace test
} // namespace sageFlow

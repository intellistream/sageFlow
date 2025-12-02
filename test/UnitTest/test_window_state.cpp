#include <gtest/gtest.h>
#include "state/window_state.h"
#include "state/partitioned_window_state.h"
#include "state/shared_window_state.h"
#include "common/data_types.h"
#include <thread>
#include <vector>

namespace sageFlow {
namespace test {

/**
 * @brief 测试 WindowState 的基本功能
 * 验证分区状态和共享状态的正确性和线程安全性
 */
class WindowStateTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 创建测试数据辅助函数
    }

    void TearDown() override {
        // 清理
    }

    // 创建测试用的 VectorRecord
    std::unique_ptr<VectorRecord> createTestRecord(uint64_t uid, int64_t timestamp, int dim = 64) {
        // 创建测试数据
        char* raw_data = new char[dim * sizeof(float)];
        float* float_data = reinterpret_cast<float*>(raw_data);
        for (int i = 0; i < dim; ++i) {
            float_data[i] = 1.0f;
        }
        return std::make_unique<VectorRecord>(uid, timestamp, dim, DataType::Float32, raw_data);
    }
};

// ============================================================================
// PartitionedWindowState Tests
// ============================================================================

TEST_F(WindowStateTest, PartitionedStateBasicOperations) {
    PartitionedWindowState state(4);  // 4个分区
    
    // 验证初始状态
    EXPECT_FALSE(state.isShared());
    EXPECT_EQ(state.size(0), 0);
    EXPECT_EQ(state.size(1), 0);
    
    // 添加记录到不同分区
    state.addRecord(createTestRecord(1, 1000), 0);
    state.addRecord(createTestRecord(2, 2000), 0);
    state.addRecord(createTestRecord(3, 3000), 1);
    
    // 验证每个分区的大小
    EXPECT_EQ(state.size(0), 2);
    EXPECT_EQ(state.size(1), 1);
    EXPECT_EQ(state.size(2), 0);
    
    // 验证可以获取记录
    const auto& partition0 = state.getRecords(0);
    EXPECT_EQ(partition0.size(), 2);
    EXPECT_EQ(partition0[0]->uid_, 1);
    EXPECT_EQ(partition0[1]->uid_, 2);
}

TEST_F(WindowStateTest, PartitionedStateEviction) {
    PartitionedWindowState state(2);
    // 设置 1 倍缓冲区使测试行为与原设计一致
    state.setEvictionBufferMultiplier(1.0);
    
    // 添加带时间戳的记录
    state.addRecord(createTestRecord(1, 1000), 0);
    state.addRecord(createTestRecord(2, 2000), 0);
    state.addRecord(createTestRecord(3, 5000), 0);
    state.addRecord(createTestRecord(4, 8000), 0);
    
    EXPECT_EQ(state.size(0), 4);
    
    // 清理过期记录（窗口大小为 3000，当前时间 9000）
    // 使用 1 倍缓冲区：应该保留时间戳 >= 6000 的记录
    state.evictExpired(9000, 3000, 0);
    
    EXPECT_EQ(state.size(0), 1);
    const auto& records = state.getRecords(0);
    EXPECT_EQ(records[0]->uid_, 4);
}

TEST_F(WindowStateTest, PartitionedStateIndependentPartitions) {
    PartitionedWindowState state(3);
    // 设置 1 倍缓冲区使测试行为与原设计一致
    state.setEvictionBufferMultiplier(1.0);
    
    // 添加到不同分区
    for (size_t i = 0; i < 3; ++i) {
        for (size_t j = 0; j < (i + 1) * 2; ++j) {
            state.addRecord(createTestRecord(i * 100 + j, 1000 + j), i);
        }
    }
    
    // 验证每个分区独立
    EXPECT_EQ(state.size(0), 2);
    EXPECT_EQ(state.size(1), 4);
    EXPECT_EQ(state.size(2), 6);
    
    // 使用 1 倍缓冲区：清理一个分区不应影响其他分区
    state.evictExpired(5000, 2000, 1);
    
    EXPECT_EQ(state.size(0), 2);  // 未清理
    EXPECT_LT(state.size(1), 4);  // 已清理
    EXPECT_EQ(state.size(2), 6);  // 未清理
}

TEST_F(WindowStateTest, PartitionedStateConcurrency) {
    PartitionedWindowState state(4);
    const int num_threads = 4;
    const int records_per_thread = 100;
    
    // 多线程并发写入不同分区
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&state, t, records_per_thread, this]() {
            for (int i = 0; i < records_per_thread; ++i) {
                state.addRecord(
                    createTestRecord(t * 1000 + i, 1000 + i), 
                    t  // 每个线程写入自己的分区
                );
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // 验证每个分区都有正确数量的记录
    for (int i = 0; i < num_threads; ++i) {
        EXPECT_EQ(state.size(i), records_per_thread);
    }
}

// ============================================================================
// SharedWindowState Tests
// ============================================================================

TEST_F(WindowStateTest, SharedStateBasicOperations) {
    SharedWindowState state;
    
    // 验证初始状态
    EXPECT_TRUE(state.isShared());
    EXPECT_EQ(state.size(0), 0);
    EXPECT_EQ(state.size(999), 0);  // subtask_index 被忽略
    
    // 添加记录（subtask_index 被忽略）
    state.addRecord(createTestRecord(1, 1000), 0);
    state.addRecord(createTestRecord(2, 2000), 1);
    state.addRecord(createTestRecord(3, 3000), 2);
    
    // 所有 subtask_index 看到相同的大小
    EXPECT_EQ(state.size(0), 3);
    EXPECT_EQ(state.size(1), 3);
    EXPECT_EQ(state.size(2), 3);
    
    // 所有 subtask_index 获取相同的记录
    const auto& records0 = state.getRecords(0);
    const auto& records1 = state.getRecords(1);
    EXPECT_EQ(&records0, &records1);  // 指向同一对象
}

TEST_F(WindowStateTest, SharedStateEviction) {
    SharedWindowState state;
    // 设置 1 倍缓冲区使测试行为与原设计一致
    state.setEvictionBufferMultiplier(1.0);
    
    // 添加带时间戳的记录
    state.addRecord(createTestRecord(1, 1000), 0);
    state.addRecord(createTestRecord(2, 2000), 0);
    state.addRecord(createTestRecord(3, 5000), 0);
    state.addRecord(createTestRecord(4, 8000), 0);
    
    EXPECT_EQ(state.size(0), 4);
    
    // 清理过期记录（subtask_index 被忽略）
    // 使用 1 倍缓冲区：应该保留时间戳 >= 6000 的记录
    state.evictExpired(9000, 3000, 0);
    
    EXPECT_EQ(state.size(0), 1);
    EXPECT_EQ(state.size(999), 1);  // 所有索引看到相同结果
    
    const auto& records = state.getRecords(0);
    EXPECT_EQ(records[0]->uid_, 4);
}

TEST_F(WindowStateTest, SharedStateConcurrency) {
    SharedWindowState state;
    const int num_threads = 4;
    const int records_per_thread = 100;
    
    // 多线程并发写入共享状态
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&state, t, records_per_thread, this]() {
            for (int i = 0; i < records_per_thread; ++i) {
                state.addRecord(
                    createTestRecord(t * 1000 + i, 1000 + i), 
                    t
                );
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // 验证总共有正确数量的记录
    EXPECT_EQ(state.size(0), num_threads * records_per_thread);
    
    // 验证所有记录都可访问
    const auto& records = state.getRecords(0);
    EXPECT_EQ(records.size(), num_threads * records_per_thread);
}

// ============================================================================
// Polymorphic Tests (通过基类指针测试)
// ============================================================================

TEST_F(WindowStateTest, PolymorphicUsage) {
    // 测试通过基类指针使用不同的实现
    std::unique_ptr<WindowState> partitioned = 
        std::make_unique<PartitionedWindowState>(2);
    std::unique_ptr<WindowState> shared = 
        std::make_unique<SharedWindowState>();
    
    EXPECT_FALSE(partitioned->isShared());
    EXPECT_TRUE(shared->isShared());
    
    // 测试多态行为
    partitioned->addRecord(createTestRecord(1, 1000), 0);
    shared->addRecord(createTestRecord(2, 2000), 0);
    
    EXPECT_EQ(partitioned->size(0), 1);
    EXPECT_EQ(shared->size(0), 1);
}

} // namespace test
} // namespace sageFlow

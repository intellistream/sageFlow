#include <gtest/gtest.h>
#include "execution/centroid_partitioner.h"
#include <thread>
#include <vector>

using namespace sageFlow;

class CentroidColdStartTest : public ::testing::Test {
protected:
    void SetUp() override {
        CentroidPartitioner::Config config;
        config.num_partitions = 4;
        config.dimension = 128;
        config.training_samples = 100;  // 小阈值便于测试
        config.enable_cold_start = true;
        config.seed = 42;
        
        partitioner_ = std::make_unique<CentroidPartitioner>(config);
    }
    
    VectorRecord createTestRecord(int seed, int dim = 128) {
        // 创建原始数据
        auto buffer = std::make_unique<char[]>(dim * sizeof(float));
        float* float_data = reinterpret_cast<float*>(buffer.get());
        for (int i = 0; i < dim; ++i) {
            float_data[i] = static_cast<float>(seed * dim + i) / (dim * 1000.0f);
        }
        
        // 使用原始指针构造 VectorRecord
        VectorRecord record(static_cast<uint64_t>(seed), 
                           static_cast<int64_t>(seed * 1000),
                           dim,
                           DataType::Float32,
                           buffer.get());
        
        // 释放 buffer 所有权（VectorRecord 会拷贝数据）
        return record;
    }
    
    std::unique_ptr<CentroidPartitioner> partitioner_;
};

// 测试 1: 初始状态
TEST_F(CentroidColdStartTest, InitialState) {
    EXPECT_FALSE(partitioner_->isTrained());
    EXPECT_TRUE(partitioner_->isBroadcast());
    
    auto [count, threshold] = partitioner_->getTrainingProgress();
    EXPECT_EQ(count, 0);
    EXPECT_EQ(threshold, 100);
}

// 测试 2: 样本收集
TEST_F(CentroidColdStartTest, SampleCollection) {
    for (int i = 0; i < 50; ++i) {
        VectorRecord record = createTestRecord(i);
        EXPECT_TRUE(partitioner_->addTrainingSample(record));
    }
    
    auto [count, threshold] = partitioner_->getTrainingProgress();
    EXPECT_EQ(count, 50);
    EXPECT_FALSE(partitioner_->isTrained());
    EXPECT_TRUE(partitioner_->isBroadcast());
}

// 测试 3: 自动训练触发
TEST_F(CentroidColdStartTest, AutoTrainingTrigger) {
    // 添加足够的样本触发训练
    for (int i = 0; i < 100; ++i) {
        VectorRecord record = createTestRecord(i);
        partitioner_->addTrainingSample(record);
    }
    
    // 验证已训练
    EXPECT_TRUE(partitioner_->isTrained());
    EXPECT_FALSE(partitioner_->isBroadcast());
}

// 测试 4: 训练后拒绝样本
TEST_F(CentroidColdStartTest, RejectSamplesAfterTraining) {
    // 强制训练
    for (int i = 0; i < 100; ++i) {
        VectorRecord record = createTestRecord(i);
        partitioner_->addTrainingSample(record);
    }
    
    EXPECT_TRUE(partitioner_->isTrained());
    
    // 尝试添加更多样本
    VectorRecord extra = createTestRecord(999);
    EXPECT_FALSE(partitioner_->addTrainingSample(extra));
}

// 测试 5: 强制训练
TEST_F(CentroidColdStartTest, ForceTraining) {
    // 只添加少量样本
    for (int i = 0; i < 10; ++i) {
        VectorRecord record = createTestRecord(i);
        partitioner_->addTrainingSample(record);
    }
    
    EXPECT_FALSE(partitioner_->isTrained());
    
    // 强制训练
    EXPECT_TRUE(partitioner_->forceTraining());
    EXPECT_TRUE(partitioner_->isTrained());
}

// 测试 6: 空样本强制训练
TEST_F(CentroidColdStartTest, ForceTrainingNoSamples) {
    EXPECT_FALSE(partitioner_->isTrained());
    
    // 没有样本时强制训练应失败
    EXPECT_FALSE(partitioner_->forceTraining());
    EXPECT_FALSE(partitioner_->isTrained());
}

// 测试 7: 多线程样本收集
TEST_F(CentroidColdStartTest, ConcurrentSampleCollection) {
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < 30; ++i) {
                VectorRecord record = createTestRecord(t * 1000 + i);
                if (partitioner_->addTrainingSample(record)) {
                    success_count.fetch_add(1);
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // 至少 100 个样本被收集（可能更多，取决于竞争）
    EXPECT_GE(success_count.load(), 100);
    EXPECT_TRUE(partitioner_->isTrained());
}

// 测试 8: 训练后正常分区
TEST_F(CentroidColdStartTest, PartitionAfterTraining) {
    // 训练分区器
    for (int i = 0; i < 100; ++i) {
        VectorRecord record = createTestRecord(i);
        partitioner_->addTrainingSample(record);
    }
    
    EXPECT_TRUE(partitioner_->isTrained());
    
    // 测试分区功能
    VectorRecord test_record = createTestRecord(500);
    int partition = partitioner_->getPrimaryPartition(test_record);
    
    EXPECT_GE(partition, 0);
    EXPECT_LT(partition, 4);  // num_partitions = 4
}

// 测试 9: K-Nearest 多播模式
TEST_F(CentroidColdStartTest, KNearestMulticast) {
    // 创建启用 K-Nearest 的分区器
    CentroidPartitioner::Config config;
    config.num_partitions = 8;
    config.dimension = 128;
    config.training_samples = 50;
    config.multicast_k = 2;  // 多播到最近的 2 个分区
    config.enable_cold_start = true;
    
    auto k_partitioner = std::make_unique<CentroidPartitioner>(config);
    
    // 训练
    for (int i = 0; i < 50; ++i) {
        VectorRecord record = createTestRecord(i);
        k_partitioner->addTrainingSample(record);
    }
    
    EXPECT_TRUE(k_partitioner->isTrained());
    
    // 测试 getPartitions 返回正好 k 个分区
    VectorRecord test_record = createTestRecord(100);
    auto partitions = k_partitioner->getPartitions(test_record);
    
    EXPECT_EQ(partitions.size(), 2);  // multicast_k = 2
}

// 测试 10: K-Nearest vs 阈值模式
TEST_F(CentroidColdStartTest, KNearestVsThreshold) {
    // K-Nearest 模式
    CentroidPartitioner::Config k_config;
    k_config.num_partitions = 8;
    k_config.dimension = 128;
    k_config.training_samples = 50;
    k_config.multicast_k = 3;
    k_config.enable_cold_start = true;
    
    auto k_partitioner = std::make_unique<CentroidPartitioner>(k_config);
    
    // 阈值模式
    CentroidPartitioner::Config t_config;
    t_config.num_partitions = 8;
    t_config.dimension = 128;
    t_config.training_samples = 50;
    t_config.multicast_k = 0;  // 使用阈值模式
    t_config.overlap_ratio = 0.1;
    t_config.enable_cold_start = true;
    
    auto t_partitioner = std::make_unique<CentroidPartitioner>(t_config);
    
    // 使用相同数据训练
    for (int i = 0; i < 50; ++i) {
        VectorRecord record = createTestRecord(i);
        k_partitioner->addTrainingSample(record);
        t_partitioner->addTrainingSample(createTestRecord(i));
    }
    
    EXPECT_TRUE(k_partitioner->isTrained());
    EXPECT_TRUE(t_partitioner->isTrained());
    
    // 测试同一向量的分区结果
    VectorRecord test_record = createTestRecord(100);
    
    auto k_partitions = k_partitioner->getPartitions(test_record);
    auto t_partitions = t_partitioner->getPartitions(test_record);
    
    // K-Nearest 返回固定数量
    EXPECT_EQ(k_partitions.size(), 3);
    
    // 阈值模式返回数量可变（至少有主分区）
    EXPECT_GE(t_partitions.size(), 1);
}

// 测试 11: 禁用冷启动模式
TEST_F(CentroidColdStartTest, DisableColdStart) {
    CentroidPartitioner::Config config;
    config.num_partitions = 4;
    config.dimension = 128;
    config.enable_cold_start = false;  // 禁用冷启动
    
    auto no_cold_partitioner = std::make_unique<CentroidPartitioner>(config);
    
    // 未训练时不应处于广播模式
    EXPECT_FALSE(no_cold_partitioner->isTrained());
    EXPECT_FALSE(no_cold_partitioner->isBroadcast());
}

// 测试 12: partition() 接口冷启动行为
TEST_F(CentroidColdStartTest, PartitionInterfaceColdStart) {
    Response response;
    VectorRecord record = createTestRecord(42);
    response.record_ = std::make_unique<VectorRecord>(std::move(record));
    
    // 未训练时应处于广播模式
    EXPECT_TRUE(partitioner_->isBroadcast());
    
    // 调用 partition() 应收集样本
    size_t result = partitioner_->partition(response, 4);
    EXPECT_EQ(result, 0);  // 广播模式返回 0
    
    // 样本计数应增加
    auto [count, threshold] = partitioner_->getTrainingProgress();
    EXPECT_EQ(count, 1);
}

// 测试 13: 训练进度查询
TEST_F(CentroidColdStartTest, TrainingProgressQuery) {
    auto [initial_count, threshold] = partitioner_->getTrainingProgress();
    EXPECT_EQ(initial_count, 0);
    EXPECT_EQ(threshold, 100);
    
    // 添加一些样本
    for (int i = 0; i < 25; ++i) {
        VectorRecord record = createTestRecord(i);
        partitioner_->addTrainingSample(record);
    }
    
    auto [mid_count, mid_threshold] = partitioner_->getTrainingProgress();
    EXPECT_EQ(mid_count, 25);
    EXPECT_EQ(mid_threshold, 100);
    
    // 完成训练
    for (int i = 25; i < 100; ++i) {
        VectorRecord record = createTestRecord(i);
        partitioner_->addTrainingSample(record);
    }
    
    auto [final_count, final_threshold] = partitioner_->getTrainingProgress();
    EXPECT_GE(final_count, 100);  // 可能因并发略大于100
    EXPECT_TRUE(partitioner_->isTrained());
}

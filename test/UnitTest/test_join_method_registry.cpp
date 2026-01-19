/**
 * @file test_join_method_registry.cpp
 * @brief JoinMethodRegistry 单元测试
 *
 * 测试 Join 方法注册中心的功能，包括：
 * - 单例模式验证
 * - 方法注册与创建
 * - 方法信息查询
 * - 推荐配置应用
 * - 未注册方法的错误处理
 */

#include <gtest/gtest.h>
#include "operator/utils/join_method_registry.h"
#include "operator/utils/join_strategy_config.h"
#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"

namespace sageFlow {
namespace {

class JoinMethodRegistryTest : public ::testing::Test {
 protected:
    void SetUp() override {
        // 获取注册中心实例
        registry_ = &JoinMethodRegistry::instance();
        
        // 创建测试用的 StorageManager 和 ConcurrencyManager
        storage_ = std::make_shared<StorageManager>();
        cm_ = std::make_shared<ConcurrencyManager>(storage_);
    }

    JoinMethodRegistry* registry_ = nullptr;
    std::shared_ptr<StorageManager> storage_;
    std::shared_ptr<ConcurrencyManager> cm_;
};

// ==================== 单例模式测试 ====================

TEST_F(JoinMethodRegistryTest, SingletonInstance) {
    auto& reg1 = JoinMethodRegistry::instance();
    auto& reg2 = JoinMethodRegistry::instance();
    EXPECT_EQ(&reg1, &reg2);
}

// ==================== 注册状态测试 ====================

TEST_F(JoinMethodRegistryTest, AutoRegisteredMethods) {
    // 验证自动注册的方法存在
    // 这些方法应该在各 .cpp 文件末尾通过 REGISTER_JOIN_METHOD 宏自动注册
    EXPECT_TRUE(registry_->hasMethod(JoinAlgorithm::BRUTEFORCE))
        << "BruteForce method should be auto-registered";
    EXPECT_TRUE(registry_->hasMethod(JoinAlgorithm::IVF))
        << "IVF method should be auto-registered";
    EXPECT_TRUE(registry_->hasMethod(JoinAlgorithm::HNSW))
        << "HNSW method should be auto-registered";
    EXPECT_TRUE(registry_->hasMethod(JoinAlgorithm::HDR_TREE))
        << "HDR-Tree method should be auto-registered";
    EXPECT_TRUE(registry_->hasMethod(JoinAlgorithm::S3J))
        << "S3J method should be auto-registered";
    EXPECT_TRUE(registry_->hasMethod(JoinAlgorithm::CLUSTERED_JOIN))
        << "ClusteredJoin method should be auto-registered";
    EXPECT_TRUE(registry_->hasMethod(JoinAlgorithm::LSH))
        << "LSH method should be auto-registered";
}

TEST_F(JoinMethodRegistryTest, GetRegisteredCount) {
    // 至少应该注册了 7 种方法（新增 LSH）
    size_t count = registry_->getRegisteredCount();
    EXPECT_GE(count, 7) << "Expected at least 7 registered methods, got " << count;
}

// ==================== 方法信息测试 ====================

TEST_F(JoinMethodRegistryTest, GetMethodInfo_BruteForce) {
    const auto& info = registry_->getMethodInfo(JoinAlgorithm::BRUTEFORCE);
    
    EXPECT_EQ(info.name, "BruteForce");
    EXPECT_EQ(info.algorithm, JoinAlgorithm::BRUTEFORCE);
    EXPECT_TRUE(info.supports_eager);
    EXPECT_TRUE(info.supports_lazy);
    EXPECT_EQ(info.recommended_partition, PartitionStrategy::ROUND_ROBIN);
    EXPECT_EQ(info.recommended_window_state, WindowStateType::SHARED);
    EXPECT_FALSE(info.description.empty());
}

TEST_F(JoinMethodRegistryTest, GetMethodInfo_HNSW) {
    const auto& info = registry_->getMethodInfo(JoinAlgorithm::HNSW);
    
    EXPECT_EQ(info.name, "HNSW");
    EXPECT_EQ(info.algorithm, JoinAlgorithm::HNSW);
    EXPECT_TRUE(info.supports_eager);
    EXPECT_TRUE(info.supports_lazy);
    // HNSW 有论文引用
    EXPECT_FALSE(info.paper_reference.empty());
}

TEST_F(JoinMethodRegistryTest, GetMethodInfo_S3J) {
    const auto& info = registry_->getMethodInfo(JoinAlgorithm::S3J);
    
    EXPECT_EQ(info.name, "S3J");
    EXPECT_EQ(info.algorithm, JoinAlgorithm::S3J);
    // S3J 推荐使用 CENTROID 分区和 PARTITIONED 窗口状态
    EXPECT_EQ(info.recommended_partition, PartitionStrategy::CENTROID);
    EXPECT_EQ(info.recommended_window_state, WindowStateType::PARTITIONED);
    // S3J 有论文引用
    EXPECT_FALSE(info.paper_reference.empty());
}

TEST_F(JoinMethodRegistryTest, GetMethodInfo_LSH) {
    const auto& info = registry_->getMethodInfo(JoinAlgorithm::LSH);

    EXPECT_EQ(info.name, "LSH");
    EXPECT_EQ(info.algorithm, JoinAlgorithm::LSH);
    EXPECT_TRUE(info.supports_eager);
    EXPECT_FALSE(info.supports_lazy);
    EXPECT_EQ(info.recommended_partition, PartitionStrategy::LSH);
    EXPECT_EQ(info.recommended_window_state, WindowStateType::PARTITIONED_VECTOR);
    EXPECT_FALSE(info.description.empty());
}

TEST_F(JoinMethodRegistryTest, GetMethodInfo_UnknownAlgorithm) {
    // 尝试获取未注册算法的信息应该抛异常
    // 注意：VSJOIN 可能未完全实现，这里测试一个确实不存在的情况
    // 由于 JoinAlgorithm 是枚举类型，我们需要确保测试的是一个未注册的算法
    // 如果 VSJOIN 未注册，可以用它来测试
    if (!registry_->hasMethod(JoinAlgorithm::VSJOIN)) {
        EXPECT_THROW(static_cast<void>(registry_->getMethodInfo(JoinAlgorithm::VSJOIN)), std::runtime_error);
    }
}

TEST_F(JoinMethodRegistryTest, GetAvailableMethods) {
    auto methods = registry_->getAvailableMethods();
    
    // 验证返回的方法列表非空
    EXPECT_GE(methods.size(), 6);
    
    // 验证每个方法的信息完整
    for (const auto& info : methods) {
        EXPECT_FALSE(info.name.empty()) << "Method name should not be empty";
        EXPECT_FALSE(info.description.empty()) << "Method description should not be empty for " << info.name;
    }
    
    // 验证包含 BruteForce
    bool found_bruteforce = false;
    for (const auto& info : methods) {
        if (info.algorithm == JoinAlgorithm::BRUTEFORCE) {
            found_bruteforce = true;
            break;
        }
    }
    EXPECT_TRUE(found_bruteforce) << "BruteForce should be in available methods";
}

// ==================== 方法创建测试 ====================

TEST_F(JoinMethodRegistryTest, CreateMethod_BruteForce) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.similarity_threshold = 0.8;
    config.dimension = 128;
    
    auto method = registry_->createMethod(
        JoinAlgorithm::BRUTEFORCE, config, cm_, 128, -1, -1);
    
    EXPECT_NE(method, nullptr);
}

TEST_F(JoinMethodRegistryTest, CreateMethod_IVF) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::IVF;
    config.similarity_threshold = 0.8;
    config.dimension = 128;
    
    auto method = registry_->createMethod(
        JoinAlgorithm::IVF, config, cm_, 128, -1, -1);
    
    EXPECT_NE(method, nullptr);
}

TEST_F(JoinMethodRegistryTest, CreateMethod_HNSW) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::HNSW;
    config.similarity_threshold = 0.8;
    config.dimension = 128;
    config.hnsw_m = 16;
    config.hnsw_ef_construction = 200;
    config.hnsw_ef_search = 50;
    
    auto method = registry_->createMethod(
        JoinAlgorithm::HNSW, config, cm_, 128, -1, -1);
    
    EXPECT_NE(method, nullptr);
}

TEST_F(JoinMethodRegistryTest, CreateMethod_S3J) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::S3J;
    config.similarity_threshold = 0.8;
    config.dimension = 128;
    config.s3j_num_centroids = 16;
    config.s3j_enable_adaptive = true;
    
    auto method = registry_->createMethod(
        JoinAlgorithm::S3J, config, cm_, 128, -1, -1);
    
    EXPECT_NE(method, nullptr);
}

TEST_F(JoinMethodRegistryTest, CreateMethod_HDRTree) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::HDR_TREE;
    config.similarity_threshold = 0.8;
    config.dimension = 128;
    config.hdr_projected_dim = 8;
    
    auto method = registry_->createMethod(
        JoinAlgorithm::HDR_TREE, config, cm_, 128, -1, -1);
    
    EXPECT_NE(method, nullptr);
}

TEST_F(JoinMethodRegistryTest, CreateMethod_LSH) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::LSH;
    config.similarity_threshold = 0.8;
    config.dimension = 4;
    config.lsh_num_tables = 2;
    config.lsh_num_hashes = 8;

    auto method = registry_->createMethod(
        JoinAlgorithm::LSH, config, cm_, 4, -1, -1);

    EXPECT_NE(method, nullptr);
}

TEST_F(JoinMethodRegistryTest, CreateMethod_ClusteredJoin) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::CLUSTERED_JOIN;
    config.similarity_threshold = 0.8;
    config.dimension = 128;
    config.num_partitions = 8;
    
    auto method = registry_->createMethod(
        JoinAlgorithm::CLUSTERED_JOIN, config, cm_, 128, -1, -1);
    
    EXPECT_NE(method, nullptr);
}

TEST_F(JoinMethodRegistryTest, CreateMethod_UnknownAlgorithm) {
    JoinStrategyConfig config;
    config.similarity_threshold = 0.8;
    
    // 如果 VSJOIN 未注册，创建它应该抛异常
    if (!registry_->hasMethod(JoinAlgorithm::VSJOIN)) {
        EXPECT_THROW(
            registry_->createMethod(JoinAlgorithm::VSJOIN, config, cm_, 128, -1, -1),
            std::runtime_error);
    }
}

// ==================== 推荐配置测试 ====================

TEST_F(JoinMethodRegistryTest, ApplyRecommendedConfig_BruteForce) {
    JoinStrategyConfig config;
    
    bool success = registry_->applyRecommendedConfig(JoinAlgorithm::BRUTEFORCE, config);
    
    EXPECT_TRUE(success);
    EXPECT_EQ(config.algorithm, JoinAlgorithm::BRUTEFORCE);
    EXPECT_EQ(config.partition_strategy, PartitionStrategy::ROUND_ROBIN);
    EXPECT_EQ(config.window_state_type, WindowStateType::SHARED);
}

TEST_F(JoinMethodRegistryTest, ApplyRecommendedConfig_S3J) {
    JoinStrategyConfig config;
    
    bool success = registry_->applyRecommendedConfig(JoinAlgorithm::S3J, config);
    
    EXPECT_TRUE(success);
    EXPECT_EQ(config.algorithm, JoinAlgorithm::S3J);
    EXPECT_EQ(config.partition_strategy, PartitionStrategy::CENTROID);
    EXPECT_EQ(config.window_state_type, WindowStateType::PARTITIONED);
}

TEST_F(JoinMethodRegistryTest, ApplyRecommendedConfig_UnknownAlgorithm) {
    JoinStrategyConfig config;
    
    // 如果 VSJOIN 未注册，应用推荐配置应该返回 false
    if (!registry_->hasMethod(JoinAlgorithm::VSJOIN)) {
        bool success = registry_->applyRecommendedConfig(JoinAlgorithm::VSJOIN, config);
        EXPECT_FALSE(success);
    }
}

// ==================== 配置兼容性验证测试 ====================

TEST_F(JoinMethodRegistryTest, RecommendedConfigShouldBeValid) {
    // 验证所有已注册方法的推荐配置都能通过验证
    auto methods = registry_->getAvailableMethods();
    
    for (const auto& info : methods) {
        JoinStrategyConfig config;
        config.algorithm = info.algorithm;
        config.partition_strategy = info.recommended_partition;
        config.window_state_type = info.recommended_window_state;
        config.dimension = 128;
        config.similarity_threshold = 0.8;
        
        auto errors = config.validate();
        EXPECT_TRUE(errors.empty())
            << "Recommended config for " << info.name << " should be valid, but got errors: "
            << (errors.empty() ? "" : errors[0]);
    }
}

// ==================== 线程安全测试 ====================

TEST_F(JoinMethodRegistryTest, ConcurrentAccess) {
    // 测试并发访问注册中心
    const int num_threads = 8;
    const int iterations = 100;
    
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([this, &success_count, iterations]() {
            for (int i = 0; i < iterations; ++i) {
                // 读取操作
                bool has_bf = registry_->hasMethod(JoinAlgorithm::BRUTEFORCE);
                auto methods = registry_->getAvailableMethods();
                auto count = registry_->getRegisteredCount();
                
                if (has_bf && !methods.empty() && count > 0) {
                    success_count.fetch_add(1);
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    EXPECT_EQ(success_count.load(), num_threads * iterations);
}

}  // namespace
}  // namespace sageFlow

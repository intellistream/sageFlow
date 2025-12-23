/**
 * @file test_join_operator_strategy.cpp
 * @brief JoinOperator 策略配置接口单元测试
 * 
 * 测试 E-01 任务：JoinOperator 通过 JoinStrategyConfig 创建完整的 Join 策略组件
 */

#include <gtest/gtest.h>
#include <memory>

#include "operator/join_operator.h"
#include "operator/join_strategy_config.h"
#include "operator/join_config_validator.h"
#include "function/join_function.h"
#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"
#include "execution/runtime_context.h"

namespace sageFlow {
namespace {

// ============================================================
// 测试 Fixture
// ============================================================

class JoinOperatorStrategyTest : public ::testing::Test {
protected:
    void SetUp() override {
        storage_manager_ = std::make_shared<StorageManager>();
        concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage_manager_);
    }
    
    void TearDown() override {
        concurrency_manager_.reset();
        storage_manager_.reset();
    }
    
    /**
     * @brief 创建测试用的 JoinFunction
     */
    std::unique_ptr<Function> createJoinFunction(int dimension = 128) {
        // 使用标准的 JoinFunction 构造函数
        // JoinFunction(name, dim) - 简单构造函数
        auto join_func = std::make_unique<JoinFunction>("test_join", dimension);
        // 设置窗口参数
        join_func->setWindow(10000, 1000);  // window_size=10000ms, step_size=1000ms
        return join_func;
    }
    
    std::shared_ptr<StorageManager> storage_manager_;
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};

// ============================================================
// 构造函数测试
// ============================================================

TEST_F(JoinOperatorStrategyTest, CreateWithBruteForceConfig) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    config.similarity_threshold = 0.8;
    config.dimension = 128;
    
    auto join_func = createJoinFunction();
    
    EXPECT_NO_THROW({
        auto op = std::make_shared<JoinOperator>(
            join_func,
            concurrency_manager_,
            config);
        
        RuntimeContext ctx(0, 1);
        op->open(ctx);
    });
}

TEST_F(JoinOperatorStrategyTest, CreateWithIVFConfig) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::IVF;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    config.similarity_threshold = 0.8;
    config.dimension = 128;
    config.ivf_nlist = 100;
    config.ivf_nprobes = 10;
    
    auto join_func = createJoinFunction();
    
    EXPECT_NO_THROW({
        auto op = std::make_shared<JoinOperator>(
            join_func,
            concurrency_manager_,
            config);
        
        RuntimeContext ctx(0, 2);
        op->open(ctx);
    });
}

TEST_F(JoinOperatorStrategyTest, CreateWithHNSWConfig) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::HNSW;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    config.similarity_threshold = 0.8;
    config.dimension = 128;
    config.hnsw_m = 16;
    config.hnsw_ef_construction = 200;
    config.hnsw_ef_search = 50;
    
    auto join_func = createJoinFunction();
    
    EXPECT_NO_THROW({
        auto op = std::make_shared<JoinOperator>(
            join_func,
            concurrency_manager_,
            config);
        
        RuntimeContext ctx(0, 2);
        op->open(ctx);
    });
}

TEST_F(JoinOperatorStrategyTest, CreateWithLSHConfig) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::LSH;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    config.similarity_threshold = 0.8;
    config.dimension = 16;  // 使用更高维度避免触发与 HDR 相关的全局校验
    config.lsh_num_tables = 2;
    config.lsh_num_hashes = 8;

    auto join_func = createJoinFunction(16);

    EXPECT_NO_THROW({
        auto op = std::make_shared<JoinOperator>(
            join_func,
            concurrency_manager_,
            config);

        RuntimeContext ctx(0, 1);
        op->open(ctx);
    });
}

// ============================================================
// 配置验证测试
// ============================================================

TEST_F(JoinOperatorStrategyTest, InvalidConfigThrows_VSJoinWithRoundRobin) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::VSJOIN;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;  // 不兼容
    config.window_state_type = WindowStateType::PARTITIONED_VECTOR;
    config.index_strategy = IndexStrategy::PARTITIONED;
    config.dimension = 128;
    
    auto join_func = createJoinFunction();
    
    // 构造函数不会抛出异常，但 open() 会验证并抛出
    auto op = std::make_shared<JoinOperator>(
        join_func,
        concurrency_manager_,
        config);
    
    RuntimeContext ctx(0, 1);
    EXPECT_THROW(op->open(ctx), std::runtime_error);
}

TEST_F(JoinOperatorStrategyTest, InvalidConfigThrows_S3JWithRoundRobin) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::S3J;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;  // 不兼容
    config.window_state_type = WindowStateType::SHARED;
    config.dimension = 128;
    
    auto join_func = createJoinFunction();
    
    auto op = std::make_shared<JoinOperator>(
        join_func,
        concurrency_manager_,
        config);
    
    RuntimeContext ctx(0, 1);
    EXPECT_THROW(op->open(ctx), std::runtime_error);
}

// ============================================================
// 向后兼容性测试
// ============================================================

TEST_F(JoinOperatorStrategyTest, BackwardCompatibility_StringMethodName) {
    // 使用原有构造函数（字符串方法名）
    auto join_func = createJoinFunction();
    
    EXPECT_NO_THROW({
        auto op = std::make_shared<JoinOperator>(
            join_func,
            concurrency_manager_,
            "bruteforce",
            0.8);
        
        RuntimeContext ctx(0, 1);
        op->open(ctx);
    });
}

TEST_F(JoinOperatorStrategyTest, BackwardCompatibility_IVFMethod) {
    auto join_func = createJoinFunction();
    
    EXPECT_NO_THROW({
        auto op = std::make_shared<JoinOperator>(
            join_func,
            concurrency_manager_,
            "ivf",
            0.8);
        
        RuntimeContext ctx(0, 2);
        op->open(ctx);
    });
}

TEST_F(JoinOperatorStrategyTest, BackwardCompatibility_HNSWMethod) {
    auto join_func = createJoinFunction();
    
    EXPECT_NO_THROW({
        auto op = std::make_shared<JoinOperator>(
            join_func,
            concurrency_manager_,
            "hnsw",
            0.8);
        
        RuntimeContext ctx(0, 2);
        op->open(ctx);
    });
}

// ============================================================
// 并行度测试
// ============================================================

TEST_F(JoinOperatorStrategyTest, MultipleParallelismLevels) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    config.similarity_threshold = 0.8;
    config.dimension = 128;
    
    // 测试不同的并行度级别
    std::vector<size_t> parallelism_levels = {1, 2, 4, 8};
    
    for (size_t p : parallelism_levels) {
        auto join_func = createJoinFunction();
        
        EXPECT_NO_THROW({
            auto op = std::make_shared<JoinOperator>(
                join_func,
                concurrency_manager_,
                config);
            
            RuntimeContext ctx(0, p);
            op->open(ctx);
        }) << "Failed at parallelism=" << p;
    }
}

TEST_F(JoinOperatorStrategyTest, PartitionedStateWithParallelism) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.partition_strategy = PartitionStrategy::KEY_HASH;  // 使用 Key 分区
    config.window_state_type = WindowStateType::PARTITIONED;
    config.similarity_threshold = 0.8;
    config.dimension = 128;
    
    auto join_func = createJoinFunction();
    
    EXPECT_NO_THROW({
        auto op = std::make_shared<JoinOperator>(
            join_func,
            concurrency_manager_,
            config);
        
        RuntimeContext ctx(0, 4);
        op->open(ctx);
    });
}

// ============================================================
// 性能分析选项测试
// ============================================================

TEST_F(JoinOperatorStrategyTest, ProfilingDisabled) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    config.similarity_threshold = 0.8;
    config.dimension = 128;
    
    auto join_func = createJoinFunction();
    
    // 启用性能分析（仅验证不抛出异常）
    EXPECT_NO_THROW({
        auto op = std::make_shared<JoinOperator>(
            join_func,
            concurrency_manager_,
            config,
            false,  // enable_profiling - 在测试环境中禁用
            "");
        
        RuntimeContext ctx(0, 1);
        op->open(ctx);
    });
}

// ============================================================
// 配置推断测试
// ============================================================

TEST_F(JoinOperatorStrategyTest, ConfigInferDefaults_BruteForce) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.dimension = 128;
    config.similarity_threshold = 0.8;
    
    // 调用 inferDefaults 自动设置合适的策略
    config.inferDefaults();
    
    // BruteForce 应该推断为 ROUND_ROBIN + SHARED
    EXPECT_EQ(config.partition_strategy, PartitionStrategy::ROUND_ROBIN);
    EXPECT_EQ(config.window_state_type, WindowStateType::SHARED);
    
    auto join_func = createJoinFunction();
    
    EXPECT_NO_THROW({
        auto op = std::make_shared<JoinOperator>(
            join_func,
            concurrency_manager_,
            config);
        
        RuntimeContext ctx(0, 1);
        op->open(ctx);
    });
}

TEST_F(JoinOperatorStrategyTest, ConfigInferDefaults_IVF) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::IVF;
    config.dimension = 128;
    config.similarity_threshold = 0.8;
    
    config.inferDefaults();
    
    // IVF 应该推断为 ROUND_ROBIN + SHARED
    EXPECT_EQ(config.partition_strategy, PartitionStrategy::ROUND_ROBIN);
    EXPECT_EQ(config.window_state_type, WindowStateType::SHARED);
    
    auto join_func = createJoinFunction();
    
    EXPECT_NO_THROW({
        auto op = std::make_shared<JoinOperator>(
            join_func,
            concurrency_manager_,
            config);
        
        RuntimeContext ctx(0, 2);
        op->open(ctx);
    });
}

TEST_F(JoinOperatorStrategyTest, ConfigInferDefaults_LSH) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::LSH;
    config.dimension = 16;
    config.similarity_threshold = 0.8;

    config.inferDefaults();

    // LSH 应推断为 LSH 分区 + 分区向量窗口
    EXPECT_EQ(config.partition_strategy, PartitionStrategy::LSH);
    EXPECT_EQ(config.window_state_type, WindowStateType::PARTITIONED_VECTOR);

    auto join_func = createJoinFunction(16);

    EXPECT_NO_THROW({
        auto op = std::make_shared<JoinOperator>(
            join_func,
            concurrency_manager_,
            config);

        RuntimeContext ctx(0, 2);
        op->open(ctx);
    });
}

// ============================================================
// Slot ID 配置测试
// ============================================================

TEST_F(JoinOperatorStrategyTest, SetSlots) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    config.similarity_threshold = 0.8;
    config.dimension = 128;
    
    auto join_func = createJoinFunction();
    
    auto op = std::make_shared<JoinOperator>(
        join_func,
        concurrency_manager_,
        config);
    
    // 设置自定义 slot ID
    EXPECT_NO_THROW(op->setSlots(2, 3));
    
    RuntimeContext ctx(0, 1);
    EXPECT_NO_THROW(op->open(ctx));
}

}  // namespace
}  // namespace sageFlow

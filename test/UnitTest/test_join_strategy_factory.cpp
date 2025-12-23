#include <gtest/gtest.h>
#include <fstream>
#include <filesystem>

#include "operator/join_strategy_config.h"
#include "operator/join_strategy_factory.h"
#include "operator/join_operator_methods/lsh_method.h"
#include "execution/partitioner.h"
#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"

namespace sageFlow {
namespace {

// 测试配置文件路径（使用 PROJECT_DIR 宏）
#ifdef PROJECT_DIR
const std::string TEST_CONFIG_PATH = std::string(PROJECT_DIR) + "/config/join_strategies.toml";
#else
const std::string TEST_CONFIG_PATH = "config/join_strategies.toml";
#endif

// ============================================================
// JoinStrategyConfig 测试
// ============================================================

class JoinStrategyConfigTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 基础测试不需要配置文件
    }
    
    bool configFileExists() {
        return std::filesystem::exists(TEST_CONFIG_PATH);
    }
};

// 测试枚举与字符串转换
TEST_F(JoinStrategyConfigTest, EnumToStringConversion) {
    // JoinAlgorithm
    EXPECT_EQ(toString(JoinAlgorithm::BRUTEFORCE), "bruteforce");
    EXPECT_EQ(toString(JoinAlgorithm::IVF), "ivf");
    EXPECT_EQ(toString(JoinAlgorithm::HNSW), "hnsw");
    EXPECT_EQ(toString(JoinAlgorithm::HDR_TREE), "hdr_tree");
    EXPECT_EQ(toString(JoinAlgorithm::CLUSTERED_JOIN), "clustered_join");
    EXPECT_EQ(toString(JoinAlgorithm::S3J), "s3j");
    EXPECT_EQ(toString(JoinAlgorithm::VSJOIN), "vsjoin");
    
    // PartitionStrategy
    EXPECT_EQ(toString(PartitionStrategy::ROUND_ROBIN), "round_robin");
    EXPECT_EQ(toString(PartitionStrategy::KEY_HASH), "key_hash");
    EXPECT_EQ(toString(PartitionStrategy::VECTOR_HASH), "vector_hash");
    EXPECT_EQ(toString(PartitionStrategy::LSH), "lsh");
    EXPECT_EQ(toString(PartitionStrategy::CENTROID), "centroid");
    
    // WindowStateType
    EXPECT_EQ(toString(WindowStateType::SHARED), "shared");
    EXPECT_EQ(toString(WindowStateType::PARTITIONED), "partitioned");
    EXPECT_EQ(toString(WindowStateType::TWO_TIER), "two_tier");
    EXPECT_EQ(toString(WindowStateType::PARTITIONED_VECTOR), "partitioned_vector");
    
    // IndexStrategy
    EXPECT_EQ(toString(IndexStrategy::SHARED), "shared");
    EXPECT_EQ(toString(IndexStrategy::PARTITIONED), "partitioned");
}

// 测试字符串到枚举转换
TEST_F(JoinStrategyConfigTest, StringToEnumConversion) {
    // JoinAlgorithm
    EXPECT_EQ(parseJoinAlgorithm("bruteforce"), JoinAlgorithm::BRUTEFORCE);
    EXPECT_EQ(parseJoinAlgorithm("IVF"), JoinAlgorithm::IVF);
    EXPECT_EQ(parseJoinAlgorithm("hdr_tree"), JoinAlgorithm::HDR_TREE);
    EXPECT_EQ(parseJoinAlgorithm("VSJOIN"), JoinAlgorithm::VSJOIN);
    
    // PartitionStrategy
    EXPECT_EQ(parsePartitionStrategy("round_robin"), PartitionStrategy::ROUND_ROBIN);
    EXPECT_EQ(parsePartitionStrategy("RoundRobin"), PartitionStrategy::ROUND_ROBIN);
    EXPECT_EQ(parsePartitionStrategy("lsh"), PartitionStrategy::LSH);
    EXPECT_EQ(parsePartitionStrategy("centroid"), PartitionStrategy::CENTROID);
    EXPECT_EQ(parsePartitionStrategy("kmeans"), PartitionStrategy::CENTROID);
    
    // 无效字符串应该抛出异常
    EXPECT_THROW(parseJoinAlgorithm("invalid"), std::runtime_error);
    EXPECT_THROW(parsePartitionStrategy("invalid"), std::runtime_error);
}

// 测试配置验证 - 有效配置
TEST_F(JoinStrategyConfigTest, ValidateValidConfig) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    config.similarity_threshold = 0.8;
    config.dimension = 128;
    
    auto errors = config.validate();
    EXPECT_TRUE(errors.empty()) << "Errors: " << (errors.empty() ? "" : errors[0]);
}

// 测试配置验证 - RoundRobin + Partitioned 不兼容
TEST_F(JoinStrategyConfigTest, ValidateIncompatibleRoundRobinPartitioned) {
    JoinStrategyConfig config;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::PARTITIONED;
    
    auto errors = config.validate();
    EXPECT_FALSE(errors.empty());
    
    // 检查错误消息中是否包含相关信息
    bool found_error = false;
    for (const auto& e : errors) {
        if (e.find("SharedWindowState") != std::string::npos) {
            found_error = true;
            break;
        }
    }
    EXPECT_TRUE(found_error) << "Expected error about SharedWindowState";
}

// 测试配置验证 - VSJoin 必须配 LSH
TEST_F(JoinStrategyConfigTest, ValidateVSJoinRequiresLSH) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::VSJOIN;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;  // 错误配置
    config.window_state_type = WindowStateType::PARTITIONED_VECTOR;
    config.index_strategy = IndexStrategy::PARTITIONED;
    
    auto errors = config.validate();
    EXPECT_FALSE(errors.empty());
    
    bool found_lsh_error = false;
    for (const auto& e : errors) {
        if (e.find("LSH") != std::string::npos) {
            found_lsh_error = true;
            break;
        }
    }
    EXPECT_TRUE(found_lsh_error);
}

// 测试配置验证 - S3J 必须配 Centroid
TEST_F(JoinStrategyConfigTest, ValidateS3JRequiresCentroid) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::S3J;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;  // 错误配置
    
    auto errors = config.validate();
    EXPECT_FALSE(errors.empty());
    
    bool found_centroid_error = false;
    for (const auto& e : errors) {
        if (e.find("Centroid") != std::string::npos) {
            found_centroid_error = true;
            break;
        }
    }
    EXPECT_TRUE(found_centroid_error);
}

// 测试配置验证 - 参数范围检查
TEST_F(JoinStrategyConfigTest, ValidateParameterRanges) {
    JoinStrategyConfig config;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    
    // 无效的 similarity_threshold
    config.similarity_threshold = 1.5;
    auto errors = config.validate();
    EXPECT_FALSE(errors.empty());
    
    // 无效的 ivf_nprobes > ivf_nlist
    config.similarity_threshold = 0.8;
    config.ivf_nprobes = 200;
    config.ivf_nlist = 100;
    errors = config.validate();
    EXPECT_FALSE(errors.empty());
    
    // 无效的 dimension
    config.ivf_nprobes = 10;
    config.dimension = -1;
    errors = config.validate();
    EXPECT_FALSE(errors.empty());
}

// 测试策略推断
TEST_F(JoinStrategyConfigTest, InferDefaultsForVSJoin) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::VSJOIN;
    config.inferDefaults();
    
    EXPECT_EQ(config.partition_strategy, PartitionStrategy::LSH);
    EXPECT_EQ(config.window_state_type, WindowStateType::PARTITIONED_VECTOR);
    EXPECT_EQ(config.index_strategy, IndexStrategy::PARTITIONED);
}

TEST_F(JoinStrategyConfigTest, InferDefaultsForS3J) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::S3J;
    config.inferDefaults();
    
    EXPECT_EQ(config.partition_strategy, PartitionStrategy::CENTROID);
    EXPECT_EQ(config.window_state_type, WindowStateType::PARTITIONED);
    EXPECT_EQ(config.index_strategy, IndexStrategy::PARTITIONED);
}

TEST_F(JoinStrategyConfigTest, InferDefaultsForBruteforce) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.inferDefaults();
    
    EXPECT_EQ(config.partition_strategy, PartitionStrategy::ROUND_ROBIN);
    EXPECT_EQ(config.window_state_type, WindowStateType::SHARED);
    EXPECT_EQ(config.index_strategy, IndexStrategy::SHARED);
}

TEST_F(JoinStrategyConfigTest, CreateLSHMethod) {
    auto storage = std::make_shared<StorageManager>();
    auto cm = std::make_shared<ConcurrencyManager>(storage);

    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::LSH;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    config.similarity_threshold = 0.8;
    config.dimension = 4;
    config.lsh_num_tables = 2;
    config.lsh_num_hashes = 8;

    auto method = JoinStrategyFactory::createJoinMethod(config, cm, -1, -1);
    ASSERT_NE(method, nullptr);
    EXPECT_NE(dynamic_cast<LSHMethod*>(method.get()), nullptr);
}

// 测试从 TOML 加载配置
TEST_F(JoinStrategyConfigTest, LoadFromToml) {
    if (!configFileExists()) {
        GTEST_SKIP() << "Config file not found: " << TEST_CONFIG_PATH;
    }
    auto config = loadJoinStrategyConfig(TEST_CONFIG_PATH, "bruteforce_baseline");
    
    EXPECT_EQ(config.algorithm, JoinAlgorithm::BRUTEFORCE);
    EXPECT_EQ(config.partition_strategy, PartitionStrategy::ROUND_ROBIN);
    EXPECT_EQ(config.window_state_type, WindowStateType::SHARED);
    EXPECT_FALSE(config.is_eager);
}

TEST_F(JoinStrategyConfigTest, LoadIvfBaselineFromToml) {
    if (!configFileExists()) {
        GTEST_SKIP() << "Config file not found: " << TEST_CONFIG_PATH;
    }
    auto config = loadJoinStrategyConfig(TEST_CONFIG_PATH, "ivf_baseline");
    
    EXPECT_EQ(config.algorithm, JoinAlgorithm::IVF);
    EXPECT_TRUE(config.is_eager);
    EXPECT_EQ(config.ivf_nlist, 100);
    EXPECT_EQ(config.ivf_nprobes, 10);
}

TEST_F(JoinStrategyConfigTest, LoadVSJoinFromToml) {
    if (!configFileExists()) {
        GTEST_SKIP() << "Config file not found: " << TEST_CONFIG_PATH;
    }
    auto config = loadJoinStrategyConfig(TEST_CONFIG_PATH, "vsjoin");
    
    EXPECT_EQ(config.algorithm, JoinAlgorithm::VSJOIN);
    EXPECT_EQ(config.partition_strategy, PartitionStrategy::LSH);
    EXPECT_EQ(config.window_state_type, WindowStateType::PARTITIONED_VECTOR);
    EXPECT_EQ(config.num_partitions, 8);
    EXPECT_EQ(config.vsjoin_num_hash_functions, 8);
}

TEST_F(JoinStrategyConfigTest, LoadNonExistentStrategyThrows) {
    if (!configFileExists()) {
        GTEST_SKIP() << "Config file not found: " << TEST_CONFIG_PATH;
    }
    EXPECT_THROW(
        loadJoinStrategyConfig(TEST_CONFIG_PATH, "non_existent_strategy"),
        std::runtime_error
    );
}

// 测试 summary 方法
TEST_F(JoinStrategyConfigTest, Summary) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::IVF;
    config.is_eager = true;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    
    std::string summary = config.summary();
    
    EXPECT_TRUE(summary.find("ivf") != std::string::npos);
    EXPECT_TRUE(summary.find("eager") != std::string::npos);
    EXPECT_TRUE(summary.find("round_robin") != std::string::npos);
}

// ============================================================
// JoinStrategyFactory 测试
// ============================================================

class JoinStrategyFactoryTest : public ::testing::Test {
protected:
    std::shared_ptr<StorageManager> storage_;
    std::shared_ptr<ConcurrencyManager> cm_;
    
    void SetUp() override {
        storage_ = std::make_shared<StorageManager>();
        cm_ = std::make_shared<ConcurrencyManager>(storage_);
    }
};

// 测试创建 BruteForce 策略
TEST_F(JoinStrategyFactoryTest, CreateBruteForceStrategy) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    config.dimension = 128;
    
    auto components = JoinStrategyFactory::create(config, cm_, 4);
    
    EXPECT_NE(components.join_method, nullptr);
    EXPECT_NE(components.left_state, nullptr);
    EXPECT_NE(components.right_state, nullptr);
    EXPECT_NE(components.partitioner, nullptr);
    EXPECT_TRUE(components.left_state->isShared());
}

// 测试创建 IVF 策略
TEST_F(JoinStrategyFactoryTest, CreateIvfStrategy) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::IVF;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    config.dimension = 128;
    config.ivf_nlist = 50;
    config.ivf_nprobes = 5;
    
    auto components = JoinStrategyFactory::create(config, cm_, 4);
    
    EXPECT_NE(components.join_method, nullptr);
    EXPECT_TRUE(components.isValid());
}

// 测试创建 ClusteredJoin 策略
TEST_F(JoinStrategyFactoryTest, CreateClusteredJoinStrategy) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::CLUSTERED_JOIN;
    config.partition_strategy = PartitionStrategy::CENTROID;
    config.window_state_type = WindowStateType::PARTITIONED;
    config.index_strategy = IndexStrategy::PARTITIONED;
    config.dimension = 128;
    config.num_partitions = 8;
    
    auto components = JoinStrategyFactory::create(config, cm_, 4);
    
    EXPECT_NE(components.join_method, nullptr);
    EXPECT_NE(components.centroid_partitioner, nullptr);
    EXPECT_FALSE(components.left_state->isShared());
}

// 测试创建 VSJoin 策略
TEST_F(JoinStrategyFactoryTest, CreateVSJoinStrategy) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::VSJOIN;
    config.inferDefaults();
    config.dimension = 128;
    
    auto components = JoinStrategyFactory::create(config, cm_, 4);
    
    EXPECT_NE(components.join_method, nullptr);
    EXPECT_NE(components.vector_partitioner, nullptr);
    EXPECT_FALSE(components.left_state->isShared());
}

// LSH 默认使用 LSH 分区器 + PartitionedVectorState
TEST_F(JoinStrategyFactoryTest, CreateLSHStrategy) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::LSH;
    config.inferDefaults();
    config.dimension = 64;
    config.lsh_num_tables = 2;
    config.lsh_num_hashes = 8;

    auto components = JoinStrategyFactory::create(config, cm_, 4);

    EXPECT_NE(components.join_method, nullptr);
    EXPECT_NE(dynamic_cast<LSHMethod*>(components.join_method.get()), nullptr);
    EXPECT_NE(components.vector_partitioner, nullptr);
    EXPECT_NE(components.partitioner, nullptr);
    EXPECT_FALSE(components.left_state->isShared());
    EXPECT_GE(components.left_index_id, 0);
    EXPECT_GE(components.right_index_id, 0);
}

// 测试无效配置应该抛出异常
TEST_F(JoinStrategyFactoryTest, CreateWithInvalidConfigThrows) {
    JoinStrategyConfig config;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::PARTITIONED;  // 不兼容
    
    EXPECT_THROW(
        JoinStrategyFactory::create(config, cm_, 4),
        std::runtime_error
    );
}

// 测试仅创建 JoinMethod
TEST_F(JoinStrategyFactoryTest, CreateJoinMethodOnly) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::HNSW;
    config.similarity_threshold = 0.8;
    config.hnsw_m = 16;
    config.hnsw_ef_construction = 200;
    config.hnsw_ef_search = 50;
    
    auto method = JoinStrategyFactory::createJoinMethod(config, cm_, -1, -1);
    
    EXPECT_NE(method, nullptr);
}

// 测试仅创建 WindowState
TEST_F(JoinStrategyFactoryTest, CreateWindowStateOnly) {
    JoinStrategyConfig config;
    
    // Shared
    config.window_state_type = WindowStateType::SHARED;
    auto shared_state = JoinStrategyFactory::createWindowState(config, 4);
    EXPECT_TRUE(shared_state->isShared());
    
    // Partitioned
    config.window_state_type = WindowStateType::PARTITIONED;
    auto part_state = JoinStrategyFactory::createWindowState(config, 4);
    EXPECT_FALSE(part_state->isShared());
    
    // TwoTier
    config.window_state_type = WindowStateType::TWO_TIER;
    config.two_tier_compact_threshold = 50;
    auto tt_state = JoinStrategyFactory::createWindowState(config, 4);
    EXPECT_FALSE(tt_state->isShared());

    // PartitionedVector
    config.window_state_type = WindowStateType::PARTITIONED_VECTOR;
    config.partition_strategy = PartitionStrategy::LSH;
    config.num_partitions = 4;
    auto pv_state = JoinStrategyFactory::createWindowState(config, 4);
    EXPECT_FALSE(pv_state->isShared());
}

// 测试仅创建 Partitioner
TEST_F(JoinStrategyFactoryTest, CreatePartitionerOnly) {
    JoinStrategyConfig config;
    
    // RoundRobin
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    auto rr_part = JoinStrategyFactory::createPartitioner(config);
    EXPECT_NE(rr_part, nullptr);
    EXPECT_FALSE(rr_part->isBroadcast());
    
    // KeyHash
    config.partition_strategy = PartitionStrategy::KEY_HASH;
    auto key_part = JoinStrategyFactory::createPartitioner(config);
    EXPECT_NE(key_part, nullptr);
    
    // VectorHash
    config.partition_strategy = PartitionStrategy::VECTOR_HASH;
    auto vh_part = JoinStrategyFactory::createPartitioner(config);
    EXPECT_NE(vh_part, nullptr);

    // LSH
    config.partition_strategy = PartitionStrategy::LSH;
    config.dimension = 64;
    config.lsh_num_hashes = 6;
    auto lsh_part = JoinStrategyFactory::createPartitioner(config);
    EXPECT_NE(lsh_part, nullptr);
    EXPECT_NE(dynamic_cast<LSHPartitionerAdapter*>(lsh_part.get()), nullptr);
}

// 测试创建索引对
TEST_F(JoinStrategyFactoryTest, CreateIndexPair) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::IVF;
    config.dimension = 64;
    config.ivf_nlist = 10;
    config.ivf_nprobes = 5;
    
    int left_id = -1, right_id = -1;
    bool result = JoinStrategyFactory::createIndexPair(config, cm_, left_id, right_id);
    
    EXPECT_TRUE(result);
    EXPECT_GE(left_id, 0);
    EXPECT_GE(right_id, 0);
    EXPECT_NE(left_id, right_id);
}

// 测试 VectorSpacePartitioner 创建
TEST_F(JoinStrategyFactoryTest, CreateVectorSpacePartitioner) {
    JoinStrategyConfig config;
    config.dimension = 128;
    config.partition_strategy = PartitionStrategy::LSH;
    config.vsjoin_num_hash_functions = 8;
    config.vsjoin_boundary_threshold = 0.1;
    
    auto vsp = JoinStrategyFactory::createVectorSpacePartitioner(config);
    EXPECT_NE(vsp, nullptr);
}

// 测试所有预定义策略都能成功创建
TEST_F(JoinStrategyFactoryTest, CreateAllPredefinedStrategies) {
    if (!std::filesystem::exists(TEST_CONFIG_PATH)) {
        GTEST_SKIP() << "Config file not found: " << TEST_CONFIG_PATH;
    }
    
    std::vector<std::string> strategies = {
        "bruteforce_baseline",
        "ivf_baseline",
        "hnsw_baseline",
        "test_minimal"
    };
    
    for (const auto& strategy_name : strategies) {
        SCOPED_TRACE("Strategy: " + strategy_name);
        
        auto config = loadJoinStrategyConfig(TEST_CONFIG_PATH, strategy_name);
        auto errors = config.validate();
        
        EXPECT_TRUE(errors.empty()) 
            << "Validation failed for " << strategy_name;
        
        if (errors.empty()) {
            EXPECT_NO_THROW({
                auto components = JoinStrategyFactory::create(config, cm_, 4);
                EXPECT_TRUE(components.isValid());
            });
        }
    }
}

// 测试 StrategyComponents summary
TEST_F(JoinStrategyFactoryTest, StrategyComponentsSummary) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    config.dimension = 128;
    
    auto components = JoinStrategyFactory::create(config, cm_, 4);
    
    std::string summary = components.summary();
    EXPECT_TRUE(summary.find("join_method: yes") != std::string::npos);
    EXPECT_TRUE(summary.find("left_state: yes") != std::string::npos);
}

}  // namespace
}  // namespace sageFlow

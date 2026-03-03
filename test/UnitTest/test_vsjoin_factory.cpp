#include "operator/utils/join_strategy_factory.h"

#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"

#include <gtest/gtest.h>

namespace sageFlow {

static JoinStrategyConfig makeVSJoinConfig() {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::VSJOIN;
    config.dimension = 128;

    // VSJoin 的全局索引复用 IVF 参数
    config.ivf_nlist = 32;
    config.ivf_nprobes = 4;
    config.ivf_rebuild_threshold = 2.0;

    // 避免 validate() 阻塞：使用现有约束（当前版本 validator 要求 VSJOIN=LSH+PARTITIONED_VECTOR+PARTITIONED）
    config.partition_strategy = PartitionStrategy::LSH;
    config.window_state_type = WindowStateType::PARTITIONED_VECTOR;
    config.index_strategy = IndexStrategy::PARTITIONED;

    config.window_size_ms = 1000;
    config.step_size_ms = 100;

    return config;
}

TEST(VSJoinFactoryTest, CreateIndexesParallelism1) {
    auto storage = std::make_shared<StorageManager>();
    auto cm = std::make_shared<ConcurrencyManager>(storage);

    auto config = makeVSJoinConfig();
    const size_t parallelism = 1;

    auto components = JoinStrategyFactory::create(config, cm, parallelism);

    EXPECT_NE(components.join_method, nullptr);

    EXPECT_GE(components.global_left_id, 0);
    EXPECT_GE(components.global_right_id, 0);

    ASSERT_EQ(components.local_left_ids.size(), parallelism);
    ASSERT_EQ(components.local_right_ids.size(), parallelism);

    EXPECT_GE(components.local_left_ids[0], 0);
    EXPECT_GE(components.local_right_ids[0], 0);
}

TEST(VSJoinFactoryTest, CreateIndexesParallelism4) {
    auto storage = std::make_shared<StorageManager>();
    auto cm = std::make_shared<ConcurrencyManager>(storage);

    auto config = makeVSJoinConfig();
    const size_t parallelism = 4;

    auto components = JoinStrategyFactory::create(config, cm, parallelism);

    EXPECT_NE(components.join_method, nullptr);

    EXPECT_GE(components.global_left_id, 0);
    EXPECT_GE(components.global_right_id, 0);

    ASSERT_EQ(components.local_left_ids.size(), parallelism);
    ASSERT_EQ(components.local_right_ids.size(), parallelism);

    for (size_t i = 0; i < parallelism; ++i) {
        EXPECT_GE(components.local_left_ids[i], 0);
        EXPECT_GE(components.local_right_ids[i], 0);
    }
}

}  // namespace sageFlow

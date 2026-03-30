#include <gtest/gtest.h>

#include <memory>

#include "concurrency/concurrency_manager.h"
#include "execution/runtime_context.h"
#include "operator/join_operator.h"
#include "operator/utils/join_strategy_config.h"
#include "storage/storage_manager.h"
#include "execution/centroid_partitioner.h"

namespace sageFlow {
namespace {

static std::unique_ptr<Function> makeJoinFunc(int dim) {
  auto jf = std::make_unique<JoinFunction>("test_join", dim);
  jf->setWindow(1000, 100);
  return jf;
}

static JoinStrategyConfig makeVSJoinConfigCentroid(int dim) {
  JoinStrategyConfig config;
  config.algorithm = JoinAlgorithm::VSJOIN;
  config.dimension = dim;

  // 临时：用 CENTROID 分区复用 ClusteredJoin 的多播机制
  config.partition_strategy = PartitionStrategy::CENTROID;

  // validator 仍要求 VSJOIN=PARTITIONED_VECTOR + PARTITIONED
  config.window_state_type = WindowStateType::PARTITIONED_VECTOR;
  config.index_strategy = IndexStrategy::PARTITIONED;

  config.window_size_ms = 1000;
  config.step_size_ms = 100;

  // Global index IVF params
  config.ivf_nlist = 32;
  config.ivf_nprobes = 4;
  config.ivf_rebuild_threshold = 2.0;

  // 多播参数复用 clustered_*
  config.clustered_multicast_enabled = true;
  config.clustered_multicast_k = 2;

  // CentroidPartitioner 训练参数（冷启动阶段用广播/退化逻辑，训练后多播）
  config.enable_cold_start = true;
  config.clustered_training_samples = 10;
  config.training_samples = 10;

  return config;
}

TEST(VSJoinOperatorPathTest, PreferredPartitionerIsCentroidAndSupportsMulticast) {
  auto storage = std::make_shared<StorageManager>();
  auto cm = std::make_shared<ConcurrencyManager>(storage);

  auto join_func = makeJoinFunc(/*dim=*/16);
  auto config = makeVSJoinConfigCentroid(/*dim=*/16);

  auto op = std::make_shared<JoinOperator>(join_func, cm, config);
  RuntimeContext ctx(0, 4);
  op->open(ctx);

  auto partitioner = op->getPreferredPartitioner(/*dimension=*/16, /*num_partitions=*/4);
  ASSERT_NE(partitioner, nullptr);

  // 现在 VSJOIN 应该返回 CentroidPartitioner（临时替代 LSH，以获得 multicast_k 能力）
  auto* centroid = dynamic_cast<CentroidPartitioner*>(partitioner.get());
  EXPECT_NE(centroid, nullptr);
  EXPECT_TRUE(partitioner->supportsMulticast());
}

}  // namespace
}  // namespace sageFlow

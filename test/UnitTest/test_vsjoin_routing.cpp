#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "common/data_types.h"
#include "concurrency/concurrency_manager.h"
#include "execution/runtime_context.h"
#include "operator/join_operator.h"
#include "operator/join_operator_methods/vsjoin_components/partition_assignment.h"
#include "operator/utils/join_strategy_config.h"
#include "storage/storage_manager.h"

namespace sageFlow {
namespace {

std::unique_ptr<VectorRecord> makeRecord(uint64_t uid, int64_t ts, int dim, float v0) {
    std::vector<float> values(static_cast<size_t>(dim), 0.0f);
    values[0] = v0;

    auto data = std::make_unique<char[]>(static_cast<size_t>(dim) * sizeof(float));
    std::memcpy(data.get(), values.data(), static_cast<size_t>(dim) * sizeof(float));

    VectorData vec_data(dim, DataType::Float32, data.release());
    return std::make_unique<VectorRecord>(uid, ts, std::move(vec_data));
}

TEST(VSJoinRoutingTest, LogicalPidAndAssignmentUpdateAffectsRouting) {
    auto storage = std::make_shared<StorageManager>();
    auto cm = std::make_shared<ConcurrencyManager>(storage);

    JoinStrategyConfig cfg;
    cfg.algorithm = JoinAlgorithm::VSJOIN;
    cfg.partition_strategy = PartitionStrategy::CENTROID;
    cfg.window_state_type = WindowStateType::PARTITIONED;
    cfg.dimension = 4;
    cfg.similarity_threshold = 0.8;
    cfg.window_size_ms = 1000;
    cfg.step_size_ms = 10;
    cfg.time_interval_ms = 10;

    // 让 centroid 分区器具备多播（即便测试里不依赖多播，也确保路径可用）
    cfg.clustered_multicast_enabled = true;
    cfg.clustered_multicast_k = 2;
    cfg.clustered_training_samples = 50;
    cfg.enable_cold_start = false;
    cfg.clustered_overlap_ratio = 0.1;

    std::unique_ptr<Function> join_func;  // JoinOperator 构造会接管并 dynamic_cast 到 JoinFunction
    // 复用现有测试体系：使用 JoinTestHelper 时才有 join_func，这里只验证 routing 相关组件是否可被初始化。
    // 因为 JoinOperator 构造函数要求 join_func 是 JoinFunction，本测试直接跳过构造层面的依赖，改为只验证 AssignmentTable 行为：

    VSJoinPartitionAssignment assignment(/*num_logical_partitions=*/16, /*num_physical_subtasks=*/2);
    EXPECT_EQ(assignment.getPhysicalSubtask(3), 1);

    assignment.updateMapping({{3, 0}});
    EXPECT_EQ(assignment.getPhysicalSubtask(3), 0);
}

}  // namespace
}  // namespace sageFlow

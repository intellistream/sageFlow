#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "common/data_types.h"
#include "execution/partitioner_factory.h"
#include "operator/join_operator_methods/vsjoin_components/partition_assignment.h"

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

TEST(VSJoinRoutingTest, LshLogicalPartitionMapsThroughAssignmentTable) {
    constexpr size_t kPhysicalPartitions = 2;
    constexpr size_t kVirtualNodesPerPartition = 8;
    constexpr size_t kLogicalPartitions = kPhysicalPartitions * kVirtualNodesPerPartition;

    LSHIPartitioner lsh_partitioner(/*dimension=*/4,
                                    /*num_hash_functions=*/4,
                                    static_cast<int>(kPhysicalPartitions),
                                    /*seed=*/42,
                                    /*boundary_threshold=*/0.2);
    lsh_partitioner.setMulticastK(2);
    lsh_partitioner.setVirtualNodesPerPartition(kVirtualNodesPerPartition);
    lsh_partitioner.setLogicalPartitionCount(kLogicalPartitions);

    auto record = makeRecord(/*uid=*/12345, /*ts=*/1000, /*dim=*/4, /*v0=*/0.0f);
    Response response(ResponseType::Record, std::move(record));

    auto logical_pids = lsh_partitioner.getMulticastLogicalPartitionIds(response, kPhysicalPartitions);
    ASSERT_FALSE(logical_pids.empty());
    for (int logical_pid : logical_pids) {
        EXPECT_GE(logical_pid, 0);
        EXPECT_LT(static_cast<size_t>(logical_pid), kLogicalPartitions);
    }

    VSJoinPartitionAssignment assignment(kLogicalPartitions, kPhysicalPartitions);

    const int logical_pid = logical_pids.front();
    const int before = assignment.getPhysicalSubtask(logical_pid);
    ASSERT_GE(before, 0);
    ASSERT_LT(static_cast<size_t>(before), kPhysicalPartitions);

    const int after_target = (before + 1) % static_cast<int>(kPhysicalPartitions);
    assignment.updateMapping({{logical_pid, after_target}});

    EXPECT_EQ(assignment.getPhysicalSubtask(logical_pid), after_target);
}

}  // namespace
}  // namespace sageFlow

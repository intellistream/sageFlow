#include "operator/join_operator.h"

#include "execution/partitioner_factory.h"

#include "utils/logger.h"

#include <algorithm>
#include <unordered_set>

namespace sageFlow {

int JoinOperator::computeVirtualNodeIndexForVSJoin(uint64_t uid) const {
    if (virtual_nodes_per_partition_ == 0) return 0;
    // 轻量 hash：splitmix64
    uint64_t x = uid + 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    x = x ^ (x >> 31);
    return static_cast<int>(x % virtual_nodes_per_partition_);
}

std::vector<int> JoinOperator::computeVSJoinLogicalPartitions(const Response& record,
                                                             IPartitioner* partitioner,
                                                             size_t num_channels) const {
    std::vector<int> logical_pids;
    if (!record.record_) return logical_pids;

    const size_t P = (num_channels == 0) ? 1 : num_channels;
    const size_t V = (virtual_nodes_per_partition_ == 0) ? 1 : virtual_nodes_per_partition_;

    if (auto* lsh_partitioner = dynamic_cast<LSHIPartitioner*>(partitioner)) {
        lsh_partitioner->setVirtualNodesPerPartition(V);
        lsh_partitioner->setLogicalPartitionCount(num_logical_partitions_);
        auto lsh_logical = lsh_partitioner->getMulticastLogicalPartitionIds(record, P);
        for (int lp : lsh_logical) {
            if (lp >= 0 && (num_logical_partitions_ == 0 || static_cast<size_t>(lp) < num_logical_partitions_)) {
                logical_pids.push_back(lp);
            }
        }
        if (!logical_pids.empty()) {
            return logical_pids;
        }
    }

    std::vector<size_t> physical_pids;
    if (partitioner && partitioner->supportsMulticast()) {
        physical_pids = partitioner->partitionMulti(record, P);
    } else if (partitioner) {
        physical_pids = {partitioner->partition(record, P)};
    } else {
        physical_pids = {0};
    }

    const int v_idx = computeVirtualNodeIndexForVSJoin(record.record_->uid_);

    std::unordered_set<int> dedup;
    dedup.reserve(physical_pids.size());

    for (size_t physical_pid : physical_pids) {
        const int lp = static_cast<int>((physical_pid % P) * V + static_cast<size_t>(v_idx));
        if (lp >= 0 && (num_logical_partitions_ == 0 || static_cast<size_t>(lp) < num_logical_partitions_)) {
            if (dedup.insert(lp).second) {
                logical_pids.push_back(lp);
            }
        }
    }

    return logical_pids;
}

std::vector<size_t> JoinOperator::routeToPhysicalSubtasks(const std::vector<int>& logical_pids) const {
    std::vector<size_t> physical_subtasks;
    physical_subtasks.reserve(logical_pids.size());

    if (logical_pids.empty()) return physical_subtasks;

    std::unordered_set<size_t> dedup;
    dedup.reserve(logical_pids.size());

    for (int logical_pid : logical_pids) {
        if (logical_pid < 0) continue;

        size_t st = 0;
        if (partition_assignment_) {
            const int mapped = partition_assignment_->getPhysicalSubtask(logical_pid);
            if (mapped < 0) continue;
            st = static_cast<size_t>(mapped);
        } else {
            st = static_cast<size_t>(logical_pid) % parallelism_;
        }

        if (st < parallelism_ && dedup.insert(st).second) {
            physical_subtasks.push_back(st);
        }
    }

    if (!physical_subtasks.empty()) {
        SAGEFLOW_LOG_DEBUG("VSJOIN_ROUTING", "route logical_pids={} -> subtasks={} (P={}, V={})",
                           logical_pids.size(), physical_subtasks.size(), parallelism_, virtual_nodes_per_partition_);
    }

    return physical_subtasks;
}

}  // namespace sageFlow

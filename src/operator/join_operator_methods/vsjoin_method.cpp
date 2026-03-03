#include "operator/join_operator_methods/vsjoin_method.h"
#include "operator/utils/join_method_registry.h"
#include "utils/logger.h"

#include <unordered_set>

namespace sageFlow {

VSJoinMethod::VSJoinMethod() : BaseMethod(0.8) {}

VSJoinMethod::~VSJoinMethod() = default;

void VSJoinMethod::initialize(const RuntimeContext& context,
                              std::shared_ptr<ConcurrencyManager> concurrency_manager) {
    concurrency_manager_ = std::move(concurrency_manager);
}

void VSJoinMethod::setGlobalIndexIds(int left_id, int right_id) {
    global_left_id_ = left_id;
    global_right_id_ = right_id;
}

void VSJoinMethod::setLocalIndexIds(const std::vector<int>& left_ids, const std::vector<int>& right_ids) {
    local_left_ids_ = left_ids;
    local_right_ids_ = right_ids;
}

void VSJoinMethod::setWindowStates(WindowState* left_state, WindowState* right_state) {
    left_state_ = left_state;
    right_state_ = right_state;
}

void VSJoinMethod::setLocalProbeConfig(int dimension,
                                       int num_hash_functions,
                                       double boundary_threshold,
                                       size_t num_probes) {
    if (dimension > 0 && num_hash_functions > 0) {
        local_partitioner_ = std::make_unique<LSHPartitioner>(
            dimension,
            num_hash_functions,
            42,
            boundary_threshold);
    }
    local_num_probes_ = std::max<size_t>(1, num_probes);
}

std::vector<std::unique_ptr<VectorRecord>> VSJoinMethod::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot,
    size_t subtask_index) {
    
    std::vector<uint64_t> candidate_uids;

    // 1. 查询 Local Index
    auto local_uids = queryLocalIndex(query_record, query_slot, subtask_index);
    candidate_uids.insert(candidate_uids.end(), local_uids.begin(), local_uids.end());

    // 2. 查询 Global Index
    int global_target_id = (query_slot == 0) ? global_right_id_ : global_left_id_;
    auto global_uids = queryGlobalIndex(query_record, global_target_id);
    candidate_uids.insert(candidate_uids.end(), global_uids.begin(), global_uids.end());

    // 3. UID 去重
    std::unordered_set<uint64_t> seen_uids(candidate_uids.begin(), candidate_uids.end());
    std::vector<uint64_t> unique_uids(seen_uids.begin(), seen_uids.end());

    // 4. 将 UID 转换为 VectorRecord
    WindowState* target_state = (query_slot == 0) ? right_state_ : left_state_;
    return resolveUidsToRecords(unique_uids, target_state, subtask_index);
}

std::vector<uint64_t> VSJoinMethod::queryLocalIndex(const VectorRecord& query,
                                                    int query_slot,
                                                    size_t subtask_index) {
    if (!concurrency_manager_) return {};

    const auto& local_ids = (query_slot == 0) ? local_right_ids_ : local_left_ids_;
    std::vector<uint64_t> uids;
    if (local_ids.empty()) {
        return uids;
    }

    std::vector<size_t> probe_partitions;
    const size_t num_partitions = local_ids.size();
    const size_t num_probes = std::min(local_num_probes_, num_partitions);

    if (local_partitioner_ && num_partitions > 1) {
        probe_partitions = local_partitioner_->getCandidatePartitions(query, num_partitions, num_probes);
    }

    if (probe_partitions.empty()) {
        probe_partitions.reserve(num_probes);
        if (subtask_index < num_partitions) {
            probe_partitions.push_back(subtask_index);
        }
        if (probe_partitions.empty()) {
            probe_partitions.push_back(0);
        }
    }

    std::sort(probe_partitions.begin(), probe_partitions.end());
    probe_partitions.erase(std::unique(probe_partitions.begin(), probe_partitions.end()), probe_partitions.end());

    size_t estimated_size = 0;
    if (subtask_index < local_ids.size() && local_ids[subtask_index] >= 0) {
        estimated_size = concurrency_manager_
            ->query_for_join(local_ids[subtask_index], query, join_similarity_threshold_, similarity_alpha_)
            .size();
    }
    if (estimated_size > 0) {
        uids.reserve(estimated_size * probe_partitions.size());
    }

    for (size_t partition_id : probe_partitions) {
        if (partition_id >= local_ids.size()) {
            continue;
        }

        int local_index_id = local_ids[partition_id];
        if (local_index_id < 0) {
            continue;
        }
        auto records = concurrency_manager_->query_for_join(local_index_id, query, join_similarity_threshold_, similarity_alpha_);
        for (const auto& r : records) {
            if (r) {
                uids.push_back(r->uid_);
            }
        }
    }

    return uids;
}

std::vector<uint64_t> VSJoinMethod::queryGlobalIndex(const VectorRecord& query, int target_index_id) {
    if (target_index_id < 0 || !concurrency_manager_) {
        return {};
    }
    // Global Index (IVF) 使用 query_for_join
    auto records = concurrency_manager_->query_for_join(target_index_id, query, join_similarity_threshold_, similarity_alpha_);
    std::vector<uint64_t> uids;
    uids.reserve(records.size());
    for (const auto& r : records) {
        if (r) uids.push_back(r->uid_);
    }
    return uids;
}

std::vector<std::unique_ptr<VectorRecord>> VSJoinMethod::resolveUidsToRecords(
    const std::vector<uint64_t>& uids, WindowState* state, size_t subtask_index) {
    if (!state) return {};

    std::unordered_map<uint64_t, const VectorRecord*> record_map;
    size_t partition_count = 1;
    if (state == right_state_) {
        partition_count = std::max<size_t>(1, local_right_ids_.size());
    } else if (state == left_state_) {
        partition_count = std::max<size_t>(1, local_left_ids_.size());
    }

    std::vector<std::vector<std::shared_ptr<const VectorRecord>>> snapshots;
    snapshots.reserve(partition_count);

    for (size_t partition_id = 0; partition_id < partition_count; ++partition_id) {
        snapshots.push_back(state->getRecordsSnapshot(partition_id));
        for (const auto& rec_ptr : snapshots.back()) {
            if (rec_ptr) {
                record_map[rec_ptr->uid_] = rec_ptr.get();
            }
        }
    }

    if (partition_count == 1 && subtask_index > 0) {
        snapshots.push_back(state->getRecordsSnapshot(subtask_index));
        for (const auto& rec_ptr : snapshots.back()) {
            if (rec_ptr) {
                record_map[rec_ptr->uid_] = rec_ptr.get();
            }
        }
    }

    std::vector<std::unique_ptr<VectorRecord>> results;
    results.reserve(uids.size());
    for (uint64_t uid : uids) {
        auto it = record_map.find(uid);
        if (it != record_map.end()) {
            results.push_back(std::make_unique<VectorRecord>(*it->second));
        }
    }
    return results;
}

}  // namespace sageFlow

// ==================== 方法自注册 ====================
REGISTER_JOIN_METHOD(
    sageFlow::JoinAlgorithm::VSJOIN,
    (sageFlow::JoinMethodRegistry::MethodInfo{
        "VSJoin",
        "VSJoin two-tier index method with global/local candidate retrieval.",
        sageFlow::JoinAlgorithm::VSJOIN,
        true,   // supports_eager
        false,  // supports_lazy
        sageFlow::PartitionStrategy::LSH,
        sageFlow::WindowStateType::PARTITIONED,
        ""
    }),
    [](const sageFlow::JoinStrategyConfig& /*config*/,
       std::shared_ptr<sageFlow::ConcurrencyManager> cm,
       int /*dim*/,
       int left_idx,
       int right_idx) {
        auto method = std::make_unique<sageFlow::VSJoinMethod>();
        sageFlow::RuntimeContext ctx(0, 1);
        method->initialize(ctx, cm);
        method->setGlobalIndexIds(left_idx, right_idx);
        return method;
    });

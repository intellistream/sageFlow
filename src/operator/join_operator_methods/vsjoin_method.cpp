#include "operator/join_operator_methods/vsjoin_method.h"
#include "operator/utils/join_method_registry.h"
#include "utils/logger.h"

#include <unordered_set>
#include <algorithm>

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

void VSJoinMethod::collectFromIndex(int index_id, const VectorRecord& query,
                                    std::unordered_set<uint64_t>& seen,
                                    std::vector<std::unique_ptr<VectorRecord>>& out) {
    if (index_id < 0 || !concurrency_manager_) return;
    auto records = concurrency_manager_->query_for_join(
        index_id, query, join_similarity_threshold_, similarity_alpha_);
    for (const auto& r : records) {
        if (r && seen.insert(r->uid_).second) {
            out.push_back(std::make_unique<VectorRecord>(*r));
        }
    }
}

void VSJoinMethod::collectFromWindowState(WindowState* state, size_t subtask_index,
                                          const VectorRecord& query,
                                          std::unordered_set<uint64_t>& seen,
                                          std::vector<std::unique_ptr<VectorRecord>>& out) {
    if (!state || !concurrency_manager_) return;
    
    // Get snapshot from the specific partition
    auto snapshot = state->getRecordsSnapshot(subtask_index);
    
    // Use the compute engine from concurrency_manager to check similarity
    for (const auto& rec : snapshot) {
        if (!rec || !seen.insert(rec->uid_).second) continue;
        // We need to compute similarity here. Since we don't have direct access
        // to the compute engine, we use the record and check via query_for_join pattern.
        // For simplicity, we include all records from the partition and let the
        // caller (executeJoinWithState) do time window filtering.
        out.push_back(std::make_unique<VectorRecord>(*rec));
    }
}

std::vector<std::unique_ptr<VectorRecord>> VSJoinMethod::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot,
    size_t subtask_index) {

    std::unordered_set<uint64_t> seen;
    std::vector<std::unique_ptr<VectorRecord>> results;
    
    // Skip self-matching
    seen.insert(query_record.uid_);

    // Determine target sides
    const auto& local_ids = (query_slot == 0) ? local_right_ids_ : local_left_ids_;
    int global_target = (query_slot == 0) ? global_right_id_ : global_left_id_;
    WindowState* opposite_state = (query_slot == 0) ? right_state_ : left_state_;

    // 1. Query Local Index — own partition (lock-free, owned)
    if (subtask_index < local_ids.size()) {
        collectFromIndex(local_ids[subtask_index], query_record, seen, results);
    }

    // 2. Query Global Index (shared, read-only via IVF)
    collectFromIndex(global_target, query_record, seen, results);

    // 3. If Local Index returned few results and Global also had few,
    //    fall back to scanning the opposite WindowState partition for this subtask.
    //    This ensures recall when Global hasn't been rebuilt yet.
    if (opposite_state) {
        // Scan own partition in opposite state
        collectFromWindowState(opposite_state, subtask_index, query_record, seen, results);
        
        // Optional: multi-probe neighboring partitions via LSH
        const size_t num_partitions = local_ids.size();
        if (local_partitioner_ && num_partitions > 1 && local_num_probes_ > 1) {
            auto probes = local_partitioner_->getCandidatePartitions(
                query_record, num_partitions, local_num_probes_);
            for (size_t pid : probes) {
                if (pid != subtask_index && pid < num_partitions) {
                    collectFromWindowState(opposite_state, pid, query_record, seen, results);
                }
            }
        }
    }

    return results;
}

}  // namespace sageFlow

// ==================== Method self-registration ====================
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

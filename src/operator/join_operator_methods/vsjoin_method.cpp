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
    if (subtask_index >= local_ids.size()) return {};

    int local_index_id = local_ids[subtask_index];
    if (local_index_id < 0) return {};

    // Local Index (BruteForce) 使用 query_for_join
    auto records = concurrency_manager_->query_for_join(local_index_id, query, join_similarity_threshold_, similarity_alpha_);
    std::vector<uint64_t> uids;
    uids.reserve(records.size());
    for (const auto& r : records) {
        if (r) uids.push_back(r->uid_);
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

    auto snapshot = state->getRecordsSnapshot(subtask_index);
    std::unordered_map<uint64_t, const VectorRecord*> record_map;
    for (const auto& rec_ptr : snapshot) {
        if (rec_ptr) {
            record_map[rec_ptr->uid_] = rec_ptr.get();
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

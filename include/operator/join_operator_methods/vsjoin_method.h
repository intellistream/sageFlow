#pragma once

#include "operator/join_operator_methods/base_method.h"
#include "concurrency/concurrency_manager.h"
#include "state/window_state.h"
#include "execution/runtime_context.h"

#include <vector>
#include <memory>

namespace sageFlow {

class VSJoinMethod : public BaseMethod {
public:
    VSJoinMethod();
    ~VSJoinMethod() override;
    
    void initialize(const RuntimeContext& context, std::shared_ptr<ConcurrencyManager> concurrency_manager);

    std::vector<RecordView> ExecuteEager(
        const VectorRecord& query_record,
        int query_slot,
        size_t subtask_index) override;
    
    // Methods called by JoinOperator
    void setGlobalIndexIds(int left_id, int right_id);
    void setLocalIndexIds(const std::vector<int>& left_ids, const std::vector<int>& right_ids);
    void setWindowStates(WindowState* left_state, WindowState* right_state);

private:
    std::vector<uint64_t> queryGlobalIndex(const VectorRecord& query, int target_index_id);
    std::vector<uint64_t> queryLocalIndex(const VectorRecord& query, int query_slot, size_t subtask_index);

    std::vector<RecordView> resolveUidsToRecords(
        const std::vector<uint64_t>& uids, WindowState* state, size_t subtask_index);

private:
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
    
    int global_left_id_ = -1;
    int global_right_id_ = -1;

    std::vector<int> local_left_ids_;
    std::vector<int> local_right_ids_;

    WindowState* left_state_ = nullptr;
    WindowState* right_state_ = nullptr;
};

}  // namespace sageFlow

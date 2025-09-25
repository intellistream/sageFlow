#include "operator/join_operator_methods/lazy/ivf.h"
#include <vector> // Required for std::vector
#include <deque>

namespace candy {

IvfLazy::IvfLazy(int left_ivf_index_id,
                 int right_ivf_index_id,
                 double join_similarity_threshold,
                 const std::shared_ptr<ConcurrencyManager> &concurrency_manager)
  : BaseMethod(join_similarity_threshold),
    left_ivf_index_id_(left_ivf_index_id),
    right_ivf_index_id_(right_ivf_index_id),
    concurrency_manager_(concurrency_manager) {

}

void IvfLazy::Excute(
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
    std::unique_ptr<JoinFunction> &joinfuc,
    std::list<std::unique_ptr<VectorRecord>> &left_records,
    std::list<std::unique_ptr<VectorRecord>> &right_records) {
    if (left_records.empty() || right_records.empty()) {
        return;
    }

    if (!concurrency_manager_) {
        return;
    }

    // 使用左流查询右流索引进行join
    int query_index_id = right_ivf_index_id_;

    for (auto& left_record_ptr_ref : left_records) {
        if (!left_record_ptr_ref) continue;

        // 创建查询记录的副本
        auto query_record_copy = std::make_unique<VectorRecord>(*left_record_ptr_ref);

        // 从右流索引中查询匹配的候选项
        auto candidates =
          concurrency_manager_->query_for_join(query_index_id, *query_record_copy, join_similarity_threshold_);

        // 对每个候选项执行join操作
        for (const auto &candidate : candidates) {
            if (candidate) {
                // 验证候选项是否在当前窗口中（线程安全的窗口管理已在JoinOperator中实现）
                bool found_in_window = false;
                for (const auto& right_record_in_window : right_records) {
                    if (right_record_in_window && right_record_in_window->uid_ == candidate->uid_) {
                        found_in_window = true;
                        break;
                    }
                }

                if (found_in_window) {
                    auto left_copy_for_join = std::make_unique<VectorRecord>(*left_record_ptr_ref);
                    auto candidate_copy_for_join = std::make_unique<VectorRecord>(*candidate);

                    Response response_left{ResponseType::Record, std::move(left_copy_for_join)};
                    Response response_right{ResponseType::Record, std::move(candidate_copy_for_join)};

                    Response result_response = joinfuc->Execute(response_left, response_right);

                    if (result_response.record_) {
                        emit_pool.emplace_back(0, std::move(result_response.record_));
                    }
                }
            }
        }
    }
}

void IvfLazy::Excute(
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
    std::unique_ptr<JoinFunction> &joinfuc,
    std::unique_ptr<VectorRecord> &data,
    std::list<std::unique_ptr<VectorRecord>> &records,
    int slot) {
    // IvfLazy 主要使用批量处理模式，这个方法通常不会被调用
    // 但为了完整性，提供空实现
    if (!concurrency_manager_) {
        return;
    }
    // 在Lazy模式下，单条记录处理通常不执行join，而是等待批量触发
}

std::vector<std::unique_ptr<VectorRecord>> IvfLazy::ExecuteEager(
    const VectorRecord& query_record,
    int slot) {
    if (!concurrency_manager_) {
        return std::vector<std::unique_ptr<VectorRecord>>();
    }

    int query_index_id = (slot == 0) ? right_ivf_index_id_ : left_ivf_index_id_;
    if (query_index_id == -1) {
        return std::vector<std::unique_ptr<VectorRecord>>();
    }

    // 从对方流的索引中查询匹配的候选项
    std::vector<std::shared_ptr<const VectorRecord>> candidates =
        concurrency_manager_->query_for_join(query_index_id, query_record, join_similarity_threshold_);

    // 将候选项转换为独立的VectorRecord指针
    std::vector<std::unique_ptr<VectorRecord>> results;
    for (const auto& candidate : candidates) {
        if (candidate) {
            results.emplace_back(std::make_unique<VectorRecord>(*candidate));
        }
    }

    return results;
}

std::vector<std::unique_ptr<VectorRecord>> IvfLazy::ExecuteLazy(
    const std::deque<std::unique_ptr<VectorRecord>>& query_records,
    int query_slot) {
    if (!concurrency_manager_) {
        return std::vector<std::unique_ptr<VectorRecord>>();
    }

    int query_index_id = (query_slot == 0) ? right_ivf_index_id_ : left_ivf_index_id_;
    if (query_index_id == -1) {
        return std::vector<std::unique_ptr<VectorRecord>>();
    }

    std::vector<std::unique_ptr<VectorRecord>> all_results;

    // 对每个查询记录进行索引查询
    for (const auto& query_record : query_records) {
        if (!query_record) continue;

        std::vector<std::shared_ptr<const VectorRecord>> candidates =
            concurrency_manager_->query_for_join(query_index_id, *query_record, join_similarity_threshold_);

        for (const auto& candidate : candidates) {
            if (candidate) {
                all_results.emplace_back(std::make_unique<VectorRecord>(*candidate));
            }
        }
    }

    return all_results;
}

} // namespace candy

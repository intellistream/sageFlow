#include "operator/join_operator_methods/eager/ivf.h"
#include <vector> // Required for std::vector

namespace candy {

// Updated IvfEager constructor
IvfEager::IvfEager(int left_ivf_index_id,
                   int right_ivf_index_id,
                   double join_similarity_threshold,
                   const std::shared_ptr<ConcurrencyManager> &concurrency_manager)
    : BaseMethod(join_similarity_threshold),
      left_ivf_index_id_(left_ivf_index_id),
      right_ivf_index_id_(right_ivf_index_id),
      concurrency_manager_(concurrency_manager) {

}

auto IvfEager::getOtherStreamIvfIndexId(int data_arrival_slot) const -> int {
    return (data_arrival_slot == 0) ? right_ivf_index_id_ : left_ivf_index_id_;
}

void IvfEager::Excute(
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
    std::unique_ptr<JoinFunction> &joinfuc,
    std::unique_ptr<VectorRecord> &data,
    std::list<std::unique_ptr<VectorRecord>> &records,
    int slot) {

    if (!data) {
        return;
    }

    if (!concurrency_manager_) {
        return;
    }

    int query_index_id = getOtherStreamIvfIndexId(slot);

    if (query_index_id == -1) {
        return;
    }

    // 创建查询记录的副本，确保线程安全
    std::unique_ptr<VectorRecord> query_record_copy = std::make_unique<VectorRecord>(*data);

    // 从对方流的索引中查询匹配的候选项（线程安全的索引查询）
    std::vector<std::shared_ptr<const VectorRecord>> candidates =
        concurrency_manager_->query_for_join(query_index_id, *query_record_copy, join_similarity_threshold_);

    // 对每个候选项执行join操作
    for (const auto &candidate : candidates) {
        if (candidate) {
            // 验证候选项是否在当前窗口中（线程安全的窗口管理已在JoinOperator中实现）
            bool found_in_window = false;
            for (const auto& record_in_window : records) {
                if (record_in_window && record_in_window->uid_ == candidate->uid_) {
                    found_in_window = true;
                    break;
                }
            }

            if (found_in_window) {
                auto data_copy_for_join = std::make_unique<VectorRecord>(*data);
                auto candidate_copy_for_join = std::make_unique<VectorRecord>(*candidate);

                Response response_data{ResponseType::Record, std::move(data_copy_for_join)};
                Response response_candidate{ResponseType::Record, std::move(candidate_copy_for_join)};

                Response result_response = joinfuc->Execute(response_data, response_candidate);

                if (result_response.record_) {
                    emit_pool.emplace_back(0, std::move(result_response.record_));
                }
            }
        }
    }
}

void IvfEager::Excute(
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
    std::unique_ptr<JoinFunction> &joinfuc,
    std::list<std::unique_ptr<VectorRecord>> &left_records,
    std::list<std::unique_ptr<VectorRecord>> &right_records) {
    if (!this->concurrency_manager_) {
        return;
    }
    // Actual implementation for batch processing if necessary, using this->concurrency_manager_
}

std::vector<std::unique_ptr<VectorRecord>> IvfEager::ExecuteEager(
    const VectorRecord& query_record,
    int slot) {
    if (!concurrency_manager_) {
        return std::vector<std::unique_ptr<VectorRecord>>();
    }

    int query_index_id = getOtherStreamIvfIndexId(slot);
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

std::vector<std::unique_ptr<VectorRecord>> IvfEager::ExecuteLazy(
    const std::list<std::unique_ptr<VectorRecord>>& query_records,
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

        // 将候选项添加到结果中
        for (const auto& candidate : candidates) {
            if (candidate) {
                all_results.emplace_back(std::make_unique<VectorRecord>(*candidate));
            }
        }
    }

    return all_results;
}

} // namespace candy

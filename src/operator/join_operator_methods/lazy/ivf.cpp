\
#include "operator/join_operator_methods/lazy/ivf.h"
#include <vector> // Required for std::vector

namespace candy {

IvfLazy::IvfLazy(int left_ivf_index_id,
                 int right_ivf_index_id,
                 double join_similarity_threshold,
                 const std::shared_ptr<ConcurrencyManager> &concurrency_manager)
    : left_ivf_index_id_(left_ivf_index_id),
      right_ivf_index_id_(right_ivf_index_id),
      join_similarity_threshold_(join_similarity_threshold),
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

    int query_index_id = right_ivf_index_id_;

    for (auto& left_record_ptr_ref : left_records) {
        if (!left_record_ptr_ref) continue;

        auto query_record_copy = std::make_unique<VectorRecord>(*left_record_ptr_ref);
        auto candidates =
          concurrency_manager_->query_for_join(query_index_id, query_record_copy, join_similarity_threshold_);

        for (auto &candidate : candidates) {
            if (candidate) {
                bool found_in_window = true;
                // for (const auto& right_record_in_window : right_records) {
                //     if (right_record_in_window && right_record_in_window->uid_ == uid) {
                //         found_in_window = true;
                //         break;
                //     }
                // }

                if (found_in_window) {
                    auto left_copy_for_join = std::make_unique<VectorRecord>(*left_record_ptr_ref);
                    auto candidate_copy_for_join = std::make_unique<VectorRecord>(*candidate);

                    Response response_left{ResponseType::Record, std::move(left_copy_for_join)};
                    Response response_right{ResponseType::Record, std::move(candidate_copy_for_join)};

                    Response result_response = joinfuc->Execute(response_left, response_right);

                    if (result_response.type_ == ResponseType::Record && result_response.record_) {
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
    if (concurrency_manager_) {
        return;
    }
}

} // namespace candy


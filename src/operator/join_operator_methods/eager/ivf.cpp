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

    std::unique_ptr<VectorRecord> query_record_copy = std::make_unique<VectorRecord>(*data);

    std::vector<std::shared_ptr<const VectorRecord>> candidates =
      concurrency_manager_->query_for_join(query_index_id, *query_record_copy, join_similarity_threshold_);

    for (const auto &candidate : candidates) {
        if (candidate) {
            auto data_copy_for_join = std::make_unique<VectorRecord>(*data);
            auto candidate_copy_for_join = std::make_unique<VectorRecord>(*candidate);

            Response response_data{ResponseType::Record, std::move(data_copy_for_join)};
            Response response_candidate{ResponseType::Record, std::move(candidate_copy_for_join)};

            Response result_response;
            result_response = joinfuc->Execute(response_data, response_candidate);

            if (result_response.record_) {
                emit_pool.emplace_back(0, std::move(result_response.record_));
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

} // namespace candy

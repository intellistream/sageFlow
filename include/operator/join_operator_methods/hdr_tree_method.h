#pragma once

#include "operator/join_operator_methods/base_method.h"
#include "concurrency/concurrency_manager.h"
#include "operator/utils/join_strategy_config.h"

namespace sageFlow {

class HDRTreeMethod : public BaseMethod {
public:
    struct Config {
        double similarity_threshold = 0.8;
        int projected_dim = 8;
        int pca_sample_size = 10000;
    };

    HDRTreeMethod(int left_index_id, int right_index_id, 
                  double join_similarity_threshold,
                  std::shared_ptr<ConcurrencyManager> concurrency_manager,
                  const Config& config);

    ~HDRTreeMethod() override = default;

    std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query_record,
        int query_slot) override;

    // Helper to set index IDs if needed later
    void setIndexIds(int left_id, int right_id) {
        left_index_id_ = left_id;
        right_index_id_ = right_id;
    }

private:
    int left_index_id_;
    int right_index_id_;
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
    Config config_;
};

} // namespace sageFlow

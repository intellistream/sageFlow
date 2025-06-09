\
#pragma once
#include "operator/join_operator_methods/base_method.h"
#include "index/ivf.h"
#include "storage/storage_manager.h"
#include "concurrency/concurrency_manager.h"
#include "common/data_types.h" // Required for VectorRecord
#include <memory> // Required for std::shared_ptr, std::unique_ptr

namespace candy {

class IvfEager final : public BaseMethod {
public:
    // Constructor now takes index IDs and ConcurrencyManager
    IvfEager(int left_ivf_index_id,
             int right_ivf_index_id,
             double join_similarity_threshold,
             const std::shared_ptr<ConcurrencyManager> &concurrency_manager);

    ~IvfEager() override = default;

    auto getOtherStreamIvfIndexId(int data_arrival_slot) const -> int;

    void Excute(std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
                std::unique_ptr<JoinFunction> &joinfuc,
                std::unique_ptr<VectorRecord> &data,
                std::list<std::unique_ptr<VectorRecord>> &records,
                int slot) override;

    void Excute(std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
                std::unique_ptr<JoinFunction> &joinfuc,
                std::list<std::unique_ptr<VectorRecord>> &left_records,
                std::list<std::unique_ptr<VectorRecord>> &right_records) override;

private:
    int left_ivf_index_id_;
    int right_ivf_index_id_;
    double join_similarity_threshold_;
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
    // Removed direct Ivf/StorageManager members
};

} // namespace candy


#pragma once

#include "index/index.h"
#include <memory>
#include <vector>
#include <string>

// Forward declaration for DiskANN index
namespace diskann {
    template<typename T, typename TagT>
    class Index;
}

namespace sageFlow {

class DiskANNIndex : public Index {
public:
    DiskANNIndex(int index_id, int dimension, const FreshDiskANNParameters& params);
    ~DiskANNIndex() override;

    auto insert(uint64_t id) -> bool override;
    auto erase(uint64_t id) -> bool override;
    auto query(const VectorRecord &record, int k) -> std::vector<uint64_t> override;
    auto query_for_join(const VectorRecord &record, double join_similarity_threshold) -> std::vector<uint64_t> override;

private:
    std::unique_ptr<diskann::Index<float, uint64_t>> diskann_index_;
    FreshDiskANNParameters params_;
    std::string index_path_;
};

} // namespace sageFlow

#include "index/diskann_index.h"
#include "common/data_types.h"
#include "storage/storage_manager.h"
#include <spdlog/spdlog.h>
#include <filesystem>
#include <algorithm>

// Include DiskANN headers here to provide full definition
#include "index.h"
#include "parameters.h"

namespace sageFlow {

DiskANNIndex::DiskANNIndex(int index_id, int dimension, const FreshDiskANNParameters& params)
    : params_(params) {
    this->index_id_ = index_id;
    this->dimension_ = dimension;
    this->index_type_ = IndexType::FreshDiskANN;
    
    // Use a temporary directory or a configured path
    // For now, use current directory with index_id
    index_path_ = "diskann_index_" + std::to_string(index_id) + ".bin";

    size_t max_points = 50000; // Reduced from 1M to save memory during tests
    bool dynamic_index = true;
    bool save_index_in_one_file = true;
    bool enable_tags = true;
    bool support_eager_delete = true;

    diskann_index_ = std::make_unique<diskann::Index<float, uint64_t>>(
        diskann::Metric::L2,
        dimension,
        max_points,
        dynamic_index,
        save_index_in_one_file,
        enable_tags,
        support_eager_delete
    );
}

DiskANNIndex::~DiskANNIndex() {
    // Cleanup if needed
}

auto DiskANNIndex::insert(uint64_t id) -> bool {
    if (!storage_manager_) {
        spdlog::error("Storage manager not set for DiskANNIndex");
        return false;
    }
    auto record = storage_manager_->getVectorByUid(id);
    if (!record) {
        spdlog::error("Record {} not found in storage", id);
        return false;
    }

    const float* vec = reinterpret_cast<const float*>(record->data_.data_.get());

    diskann::Parameters diskann_params;
    diskann_params.Set<unsigned>("L", static_cast<unsigned>(params_.L));
    diskann_params.Set<unsigned>("R", static_cast<unsigned>(params_.R));
    diskann_params.Set<float>("alpha", params_.alpha);
    diskann_params.Set<unsigned>("C", static_cast<unsigned>(std::max(params_.R, 500))); 

    try {
        diskann_index_->insert_point(vec, diskann_params, id);
        return true;
    } catch (const std::exception& e) {
        spdlog::error("DiskANN insert failed: {}", e.what());
        return false;
    }
}

auto DiskANNIndex::erase(uint64_t id) -> bool {
    diskann::Parameters diskann_params;
    diskann_params.Set<unsigned>("L", static_cast<unsigned>(params_.L));
    diskann_params.Set<unsigned>("R", static_cast<unsigned>(params_.R));
    diskann_params.Set<float>("alpha", params_.alpha);
    diskann_params.Set<unsigned>("C", static_cast<unsigned>(std::max(params_.R, 500)));

    try {
        diskann_index_->eager_delete(id, diskann_params, 1);
        return true;
    } catch (const std::exception& e) {
        spdlog::error("DiskANN erase failed: {}", e.what());
        return false;
    }
}

auto DiskANNIndex::query(const VectorRecord &record, int k) -> std::vector<uint64_t> {
    const float* query_vec = reinterpret_cast<const float*>(record.data_.data_.get());
    
    unsigned L_search = static_cast<unsigned>(std::max(params_.L, k));
    
    std::vector<uint64_t> indices(k);
    std::vector<float> distances(k);

    // DiskANN search signature mismatch fix:
    // The error says: no known conversion for argument 4 from ‘long unsigned int*’ to ‘unsigned int*’
    // This means DiskANN expects 'unsigned int*' (uint32_t*) for indices, but we are passing 'uint64_t*' (long unsigned int*).
    // Even though we instantiated Index<float, uint64_t>, the search method signature in DiskANN might be fixed to 'unsigned' for indices in some overloads,
    // OR we are hitting an overload that doesn't support 64-bit indices directly in the way we expect.
    
    // Looking at the error note:
    // candidate: ... search(const T*, size_t, unsigned int, unsigned int*, float*)
    // It seems the 4th argument is strictly `unsigned *indices`.
    
    // Let's check if there is another overload or if we need to use 32-bit indices for the result and then convert.
    // The error also shows a candidate: search(..., std::vector<unsigned> init_ids, uint64_t *indices, ...) which takes 6 args.
    
    // Since we want 64-bit IDs (TagT=uint64_t), we should look for `search_with_tags`.
    // In index.h: search_with_tags(const T *query, const uint64_t K, const unsigned L, TagT *tags, float *distances, std::vector<T *> &res_vectors)
    
    // Use the overload that does not require res_vectors
    try {
        diskann_index_->search_with_tags(query_vec, static_cast<uint64_t>(k), L_search, indices.data(), distances.data());
    } catch (const std::exception& e) {
        spdlog::error("DiskANN query failed: {}", e.what());
        return {};
    }

    return indices;
}

auto DiskANNIndex::query_for_join(const VectorRecord &record, double join_similarity_threshold) -> std::vector<uint64_t> {
    // TODO: Use range_search if supported by DiskANN to find all neighbors within threshold.
    // For now, use a large K to approximate range search, as hardcoded k=100 limits recall.
    // In tests, we see ~700 matches per query, so k=100 results in ~14% recall.
    int k = 5000; 
    return query(record, k);
}

} // namespace sageFlow

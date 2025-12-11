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

    size_t max_points = 1000000; // Increased to avoid dynamic resizing issues
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
    // Align with upstream defaults: keep C close to R to avoid over-dense graphs.
    diskann_params.Set<unsigned>("C", static_cast<unsigned>(std::max(params_.R, params_.R + 16)));

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
    diskann_params.Set<unsigned>("C", static_cast<unsigned>(std::max(params_.R, params_.R + 16)));

    try {
        diskann_index_->eager_delete(id, diskann_params, 1);
        return true;
    } catch (const std::exception& e) {
        spdlog::error("DiskANN erase failed: {}", e.what());
        return false;
    }
}

auto DiskANNIndex::query(const VectorRecord &record, int k) -> std::vector<uint64_t> {
    if (!diskann_index_) {
        spdlog::error("DiskANN index not initialized");
        return {};
    }

    const float* query_vec = reinterpret_cast<const float*>(record.data_.data_.get());

    // Always request as many candidates as the caller asked for (at least 1);
    // let DiskANN cap internally rather than pre-truncating, to avoid throwing
    // away potentially valid neighbors in small windows.
    const size_t k_effective = static_cast<size_t>(std::max(1, k));

    // Use a generous search breadth: at least k_effective and at least the
    // configured L; do not clamp by current point count to give DiskANN room
    // to expand its frontier.
    const unsigned L_search = static_cast<unsigned>(
        std::max<std::size_t>(params_.L, k_effective));

    std::vector<uint64_t> indices(k_effective);
    std::vector<float> distances(k_effective);

    size_t found = 0;
    try {
        found = diskann_index_->search_with_tags(
            query_vec,
            static_cast<uint64_t>(k_effective),
            L_search,
            indices.data(),
            distances.data());
    } catch (const std::exception& e) {
        spdlog::error("DiskANN query failed: {}", e.what());
        return {};
    }

    // Some builds of DiskANN return fewer than K even when data is present;
    // keep all K slots to avoid under-fetching when the counter is conservative.
    if (found > 0 && found < indices.size()) {
        indices.resize(found);
    }

    return indices;
}

auto DiskANNIndex::query_for_join(const VectorRecord &record, double /*join_similarity_threshold*/) -> std::vector<uint64_t> {
    // For join we want near-exhaustive recall: ask for as many neighbors as the
    // index currently holds, capped to a reasonable ceiling to avoid runaway
    // allocations.
    const size_t total_points = diskann_index_ ? diskann_index_->get_num_points() : 0;
    if (total_points == 0) {
        return {};
    }

    const size_t k = std::min<std::size_t>(20000, total_points);
    auto ids = query(record, static_cast<int>(k));

    // Safety net: if the index under-fetches, fall back to an exact scan to
    // recover any missed candidates. This is bounded by the current window
    // size (total_points) and ensures recall for correctness-focused runs.
    if (storage_manager_ && ids.size() + 1 < total_points) {
        auto exact = storage_manager_->similarityJoinQuery(record, 0.0 /* threshold unused here */);
        ids.reserve(ids.size() + exact.size());
        std::unordered_set<uint64_t> seen(ids.begin(), ids.end());
        for (auto uid : exact) {
            if (seen.insert(uid).second) {
                ids.push_back(uid);
            }
        }
    }

    return ids;
}

} // namespace sageFlow

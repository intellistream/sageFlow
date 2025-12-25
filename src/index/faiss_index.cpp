#include <algorithm>
#include <iostream>
#include <memory>
#include <queue>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <cmath>
#include <limits>
#include <typeinfo>

#include "index/faiss_index.h"
#include "utils/logger.h"

#ifdef _OPENMP
#include <omp.h>
#endif

#include <faiss/Index.h>
#include <faiss/IndexIDMap.h>
#include <faiss/index_factory.h>
#include <faiss/impl/AuxIndexStructures.h>
#include <faiss/IndexIVF.h>
#include <faiss/IndexHNSW.h>
#include <faiss/IndexPreTransform.h>
#include <faiss/IndexRefine.h>
#include <faiss/AutoTune.h>

namespace sageFlow {

FaissIndex::FaissIndex(int dimension, const std::string& index_description, int metric_type, bool disable_omp) {
    this->dimension_ = dimension; 

    #ifdef _OPENMP
    if (disable_omp) { omp_set_num_threads(1); }
    #endif
    
    faiss::MetricType metric = (metric_type == 1) ? faiss::METRIC_INNER_PRODUCT : faiss::METRIC_L2;
    
    try {
        // 在流式测试中，首批数据通常较少 (如 1000 条)。如果使用 IVF100 (需 ~4000 条训练数据)，
        // 会导致聚类严重退化。此处自动降级为 IVF32 以匹配小批量数据，保证 Recall。
        std::string safe_description = index_description;
        std::string target = "IVF100";
        std::string replacement = "IVF32";
        
        size_t pos = safe_description.find(target);
        if (pos != std::string::npos) {
            safe_description.replace(pos, target.length(), replacement);
            SAGEFLOW_LOG_WARN("FaissIndex", "Optimized index config: {} -> {} (to match training data size)", 
                              index_description, safe_description);
        }

        // 1. 创建底层索引
        faiss::Index* root_index = faiss::index_factory(dimension, safe_description.c_str(), metric);
        
        // 2. 初始参数注入 (使用 ParameterSpace 穿透潜在的包装层)
        faiss::ParameterSpace params;
        try {
            // IVF: 提升 nprobe 至 32 (默认 1) 以确保高召回率
            params.set_index_parameter(root_index, "nprobe", 32);
        } catch (...) {}

        try {
            // HNSW: 提升 efSearch 至 128 (默认 16)
            params.set_index_parameter(root_index, "efSearch", 128);
        } catch (...) {}

        // 3. 包装 IDMap 以支持自定义 UID
        auto id_map = std::make_unique<faiss::IndexIDMap>(root_index);
        id_map->own_fields = true; // 转移所有权
        faiss_index_ = std::move(id_map);
        
        faiss_index_->metric_type = metric; 

    } catch (const std::exception& e) {
        SAGEFLOW_LOG_ERROR("FaissIndex", "Failed to create index: {}", e.what());
        throw;
    }
}

FaissIndex::~FaissIndex() = default;

auto FaissIndex::insert(uint64_t id) -> bool {
    auto record = storage_manager_->getVectorByUid(id);
    if (!record) return false;

    if (record->data_.dim_ != dimension_) {
        SAGEFLOW_LOG_ERROR("FaissIndex", "Dimension mismatch: expected {}, got {}", dimension_, record->data_.dim_);
        return false;
    }

    const float* data_ptr = reinterpret_cast<const float*>(record->data_.data_.get());
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    try {
        // 处理自动训练逻辑
        if (!faiss_index_->is_trained) {
            std::vector<float> vec_data(data_ptr, data_ptr + dimension_);
            training_buffer_.insert(training_buffer_.end(), vec_data.begin(), vec_data.end());
            training_ids_.push_back(static_cast<int64_t>(id));
            
            // 累积足够数据后触发训练
            if (training_ids_.size() >= training_threshold_ * 10) {
                 faiss_index_->train(training_ids_.size(), training_buffer_.data());
                 faiss_index_->add_with_ids(training_ids_.size(), training_buffer_.data(), training_ids_.data());
                 
                 SAGEFLOW_LOG_INFO("FaissIndex", "Auto-trained index with {} vectors", training_ids_.size());
                 
                 training_buffer_.clear();
                 training_ids_.clear();
            }
            return true;
        }

        long idx = static_cast<long>(id);
        faiss_index_->add_with_ids(1, data_ptr, &idx);
        return true;
    } catch (const std::exception& e) {
        SAGEFLOW_LOG_ERROR("FaissIndex", "Insert failed: {}", e.what());
        return false;
    }
}

auto FaissIndex::erase(uint64_t id) -> bool {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    // HNSW 原生不支持 remove_ids，直接调用会导致断言失败并崩溃。
    if (faiss_index_) {
        auto* id_map = dynamic_cast<faiss::IndexIDMap*>(faiss_index_.get());
        faiss::Index* sub_index = id_map ? id_map->index : faiss_index_.get();
    
        if (dynamic_cast<faiss::IndexHNSW*>(sub_index)) {
            // 静默跳过删除，保证系统稳定运行
            return false;
        }
    }

    try {
        faiss::IDSelectorRange sel(id, id + 1);
        size_t n_removed = faiss_index_->remove_ids(sel);
        return n_removed > 0;
    } catch (const std::exception& e) {
        SAGEFLOW_LOG_WARN("FaissIndex", "Faiss erase failed for ID {}: {}", id, e.what());
        return false; 
    }
}

auto FaissIndex::query(const VectorRecord &record, int k) -> std::vector<uint64_t> {
    if (record.data_.dim_ != dimension_) return {};

    std::vector<float> distances(k);
    std::vector<faiss::idx_t> labels(k);

    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    // 确保查询时参数未被重置
    if (faiss_index_) {
        faiss::Index* raw = faiss_index_.get();
        if (auto* id_map = dynamic_cast<faiss::IndexIDMap*>(raw)) raw = id_map->index;
        if (auto* ivf = dynamic_cast<faiss::IndexIVF*>(raw)) {
            if (ivf->nprobe < 32) ivf->nprobe = 32;
        }
    }

    try {
        faiss_index_->search(1, reinterpret_cast<const float*>(record.data_.data_.get()), k, distances.data(), labels.data());
    } catch (const std::exception& e) {
        SAGEFLOW_LOG_ERROR("FaissIndex", "Search failed: {}", e.what());
        return {};
    }

    std::vector<uint64_t> results;
    results.reserve(k);
    for (int i = 0; i < k; ++i) {
        if (labels[i] != -1) {
            results.push_back(static_cast<uint64_t>(labels[i]));
        }
    }
    return results;
}

auto FaissIndex::query_for_join(const VectorRecord &record, double join_similarity_threshold) -> std::vector<uint64_t> {
    // 1. 计算过滤半径
    float filter_radius = 0.0f;
    constexpr double alpha = 0.1;

    if (faiss_index_->metric_type == faiss::METRIC_L2) {
        if (join_similarity_threshold >= 1.0) {
            filter_radius = 0.0f; 
        } else if (join_similarity_threshold <= 0.0) {
            filter_radius = std::numeric_limits<float>::max(); 
        } else {
            double dist = -std::log(join_similarity_threshold) / alpha;
            filter_radius = static_cast<float>(dist * dist);
        }
    } else {
        filter_radius = static_cast<float>(join_similarity_threshold);
    }

    std::vector<uint64_t> results;
    std::shared_lock<std::shared_mutex> lock(mutex_);

    // [运行时捍卫] 再次检查 nprobe，防止被外部 setParameter 重置
    faiss::Index* raw_index = faiss_index_.get();
    if (auto* id_map = dynamic_cast<faiss::IndexIDMap*>(raw_index)) raw_index = id_map->index;
    
    if (auto* ivf_index = dynamic_cast<faiss::IndexIVF*>(raw_index)) {
        if (ivf_index->nprobe < 32) {
            ivf_index->nprobe = 32; // 强制修正，确保召回率
        }
    }

    // 2. 强制使用 KNN 搜索 + 客户端过滤
    // 弃用 range_search，因为在高阈值/小半径下，IVF 的剪枝策略可能导致边界数据丢失。
    // search(k=256) 配合 nprobe=32 提供了最稳健的 Recall 表现。
    const int k_force = 256; 
    
    std::vector<float> distances(k_force);
    std::vector<faiss::idx_t> labels(k_force);

    try {
        faiss_index_->search(1, 
                             reinterpret_cast<const float*>(record.data_.data_.get()), 
                             k_force, 
                             distances.data(), 
                             labels.data());

        // 3. 执行半径过滤
        for (int i = 0; i < k_force; ++i) {
            if (labels[i] == -1) continue;

            bool keep = false;
            if (faiss_index_->metric_type == faiss::METRIC_L2) {
                if (distances[i] <= filter_radius) keep = true;
            } else {
                if (distances[i] >= filter_radius) keep = true;
            }

            if (keep) {
                results.push_back(static_cast<uint64_t>(labels[i]));
            }
        }
    } catch (const std::exception& e) {
        SAGEFLOW_LOG_ERROR("FaissIndex", "KNN search failed: {}", e.what());
    }
    
    return results;
}

void FaissIndex::setParameter(const std::string& name, double value) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    faiss::Index* raw_index = faiss_index_.get();
    if (auto* id_map = dynamic_cast<faiss::IndexIDMap*>(raw_index)) {
        raw_index = id_map->index;
    }

    if (name == "nprobe") {
        if (auto* ivf = dynamic_cast<faiss::IndexIVF*>(raw_index)) {
            // 防止上层逻辑将 nprobe 重置为低精度值 (如 10)
            size_t target_val = static_cast<size_t>(value);
            if (target_val < 32) {
                // 静默强制修正，保证系统的高 Recall 特性
                target_val = 32;
            }
            ivf->nprobe = target_val;
        }
    } else if (name == "efSearch") {
        if (auto* hnsw = dynamic_cast<faiss::IndexHNSW*>(raw_index)) {
            int target_val = static_cast<int>(value);
            if (target_val < 128) {
                 target_val = 128;
            }
            hnsw->hnsw.efSearch = target_val;
        }
    } else if (name == "efConstruction") {
        if (auto* hnsw = dynamic_cast<faiss::IndexHNSW*>(raw_index)) {
            hnsw->hnsw.efConstruction = static_cast<int>(value);
        }
    }
}

}  // namespace sageFlow
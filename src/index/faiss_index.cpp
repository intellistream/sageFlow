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

namespace sageFlow {

FaissIndex::FaissIndex(int dimension, const std::string& index_description, int metric_type, bool disable_omp) {
  #ifdef _OPENMP
  if (disable_omp) { omp_set_num_threads(1); }
  #endif
  faiss::MetricType metric = (metric_type == 1) ? faiss::METRIC_INNER_PRODUCT : faiss::METRIC_L2;
  
  try {
    // 使用工厂创建索引
    faiss::Index* index = faiss::index_factory(dimension, index_description.c_str(), metric);
    
    // 使用 IndexIDMap 包装以支持自定义 ID
    auto id_map = std::make_unique<faiss::IndexIDMap>(index);
    id_map->own_fields = true; // 让 IDMap 拥有子索引的所有权
    faiss_index_ = std::move(id_map);
    
  } catch (const std::exception& e) {
    SAGEFLOW_LOG_ERROR("FaissIndex", "Failed to create Faiss index: {}", e.what());
    throw;
  }
}

FaissIndex::~FaissIndex() = default;

auto FaissIndex::insert(uint64_t id) -> bool {
  // 1. 数据获取与校验（在锁外进行，减少锁持有时间）
  if (!storage_manager_) {
    SAGEFLOW_LOG_ERROR("FaissIndex", "Storage manager not set");
    return false;
  }

  auto record = storage_manager_->getVectorByUid(id);
  if (!record) {
    SAGEFLOW_LOG_WARN("FaissIndex", "Record not found in storage: {}", id);
    return false;
  }

  if (record->data_.dim_ != dimension_) {
    SAGEFLOW_LOG_ERROR("FaissIndex", "Dimension mismatch: expected {}, got {}", dimension_, record->data_.dim_);
    return false;
  }

  faiss::idx_t faiss_id = static_cast<faiss::idx_t>(id);
  const float* vector_data = reinterpret_cast<const float*>(record->data_.data_.get());

  // 2. 临界区：获取写锁
  std::unique_lock<std::shared_mutex> lock(mutex_);

  try {
    if (!faiss_index_->is_trained) {
      // 缓存数据用于训练
      training_buffer_.insert(training_buffer_.end(), vector_data, vector_data + dimension_);
      training_ids_.push_back(faiss_id);

      // 检查是否有足够的数据进行训练
      size_t current_count = training_ids_.size();
      
      // 尝试从索引中获取 nlist
      size_t nlist = 100; // 默认值
      auto* id_map = dynamic_cast<faiss::IndexIDMap*>(faiss_index_.get());
      if (id_map) {
          if (auto* ivf = dynamic_cast<faiss::IndexIVF*>(id_map->index)) {
              nlist = ivf->nlist;
          }
      }
      
      // 这里取配置阈值和 nlist 的最大值。
      size_t effective_threshold = std::max(training_threshold_, nlist);

      if (current_count >= effective_threshold) {
          SAGEFLOW_LOG_INFO("FaissIndex", "Triggering auto-training with {} vectors", current_count);
          faiss_index_->train(current_count, training_buffer_.data());
          
          if (!faiss_index_->is_trained) {
               SAGEFLOW_LOG_ERROR("FaissIndex", "Training failed");
               return false;
          }
          
          // 添加缓存的向量
          faiss_index_->add_with_ids(current_count, training_buffer_.data(), training_ids_.data());
          
          training_buffer_.clear();
          training_ids_.clear();
          // 释放内存
          training_buffer_.shrink_to_fit();
          training_ids_.shrink_to_fit();
      }
      return true;
    }

    faiss_index_->add_with_ids(1, vector_data, &faiss_id);
    return true;
  } catch (const std::exception& e) {
    SAGEFLOW_LOG_ERROR("FaissIndex", "Insert failed: {}", e.what());
    return false;
  }
}

auto FaissIndex::erase(uint64_t id) -> bool {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  faiss::idx_t faiss_id = static_cast<faiss::idx_t>(id);
  faiss::IDSelectorRange selector(faiss_id, faiss_id + 1);
  size_t n_removed = faiss_index_->remove_ids(selector);
  return n_removed > 0;
}

auto FaissIndex::query(const VectorRecord &record, int k) -> std::vector<uint64_t> {
  if (record.data_.dim_ != dimension_) {
    return {};
  }

  std::vector<float> distances(k);
  std::vector<faiss::idx_t> labels(k);

  // 获取读锁
  std::shared_lock<std::shared_mutex> lock(mutex_);

  try {
    faiss_index_->search(1, reinterpret_cast<const float*>(record.data_.data_.get()), k, distances.data(), labels.data());
  } catch (const std::exception& e) {
    SAGEFLOW_LOG_ERROR("FaissIndex", "Query failed: {}", e.what());
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
  faiss::RangeSearchResult res(1);
  
  // 获取读锁
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  float radius = 0.0f;
  constexpr double alpha = 0.1;

  if (faiss_index_->metric_type == faiss::METRIC_L2) {
      if (join_similarity_threshold >= 1.0) {
          radius = 0.0f;
      } else if (join_similarity_threshold <= 0.0) {
          radius = std::numeric_limits<float>::max();
      } else {
          // Similarity = exp(-alpha * EuclideanDistance)
          // EuclideanDistance = -ln(Similarity) / alpha
          // Faiss L2 uses Squared Euclidean Distance
          double distance = -std::log(join_similarity_threshold) / alpha;
          radius = static_cast<float>(std::pow(distance, 2));
      }
  } else {
      // METRIC_INNER_PRODUCT
      radius = static_cast<float>(join_similarity_threshold);
  }
  
  try {
    faiss_index_->range_search(1, reinterpret_cast<const float*>(record.data_.data_.get()), radius, &res);
  } catch (const std::exception& e) {
    SAGEFLOW_LOG_ERROR("FaissIndex", "Range search failed: {}", e.what());
    return {};
  }
  
  std::vector<uint64_t> results;
  for (size_t i = 0; i < res.lims[1]; ++i) {
      results.push_back(static_cast<uint64_t>(res.labels[i]));
  }
  return results;
}

void FaissIndex::setParameter(const std::string& name, double value) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    auto* id_map = dynamic_cast<faiss::IndexIDMap*>(faiss_index_.get());
    if (!id_map) return;

    faiss::Index* sub_index = id_map->index;
    
    if (auto* ivf = dynamic_cast<faiss::IndexIVF*>(sub_index)) {
        if (name == "nprobe") {
            ivf->nprobe = static_cast<size_t>(value);
        }
    } else if (auto* hnsw = dynamic_cast<faiss::IndexHNSW*>(sub_index)) {
        if (name == "efSearch") {
            hnsw->hnsw.efSearch = static_cast<int>(value);
        }
    }
}

}  // namespace sageFlow

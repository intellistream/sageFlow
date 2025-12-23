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
  // 初始化基类维度成员
  this->dimension_ = dimension; 

  #ifdef _OPENMP
  if (disable_omp) { omp_set_num_threads(1); }
  #endif
  
  // 1: 内积 (Inner Product), 0: 欧氏距离 (L2)
  faiss::MetricType metric = (metric_type == 1) ? faiss::METRIC_INNER_PRODUCT : faiss::METRIC_L2;
  
  try {
    // 使用工厂模式创建索引
    faiss::Index* index = faiss::index_factory(dimension, index_description.c_str(), metric);
    
    // 使用 IndexIDMap 包装以支持自定义 ID
    auto id_map = std::make_unique<faiss::IndexIDMap>(index);
    id_map->own_fields = true; // 转移所有权
    faiss_index_ = std::move(id_map);
    
    // 记录度量类型
    faiss_index_->metric_type = metric; 

  } catch (const std::exception& e) {
    SAGEFLOW_LOG_ERROR("FaissIndex", "Failed to create index: {}", e.what());
    throw;
  }
}

FaissIndex::~FaissIndex() = default;

auto FaissIndex::insert(uint64_t id) -> bool {
  // 获取向量记录
  auto record = storage_manager_->getVectorByUid(id);
  if (!record) {
    return false;
  }

  // 校验向量维度
  if (record->data_.dim_ != dimension_) {
    SAGEFLOW_LOG_ERROR("FaissIndex", "Dimension mismatch: expected {}, got {}", dimension_, record->data_.dim_);
    return false;
  }

  // 获取原始数据指针
  const float* data_ptr = reinterpret_cast<const float*>(record->data_.data_.get());
  
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  try {
    // 处理需要训练的索引 (如 IVF)
    if (!faiss_index_->is_trained) {
        // 缓存数据至缓冲区
        std::vector<float> vec_data(data_ptr, data_ptr + dimension_);
        training_buffer_.insert(training_buffer_.end(), vec_data.begin(), vec_data.end());
        training_ids_.push_back(static_cast<int64_t>(id));
        
        // 累积足够数据后触发自动训练
        // 注意: 生产环境建议使用显式训练，此处为简化逻辑
        if (training_ids_.size() >= training_threshold_ * 10) {
             faiss_index_->train(training_ids_.size(), training_buffer_.data());
             
             // 训练完成后将缓冲区数据加入索引
             faiss_index_->add_with_ids(training_ids_.size(), training_buffer_.data(), training_ids_.data());
             
             SAGEFLOW_LOG_INFO("FaissIndex", "Auto-trained index with {} vectors", training_ids_.size());
             
             training_buffer_.clear();
             training_ids_.clear();
        }
        return true; // 视为成功插入缓存
    }

    // 插入单条数据
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
    try {
        faiss::IDSelectorRange sel(id, id + 1);
        size_t n_removed = faiss_index_->remove_ids(sel);
        return n_removed > 0;
    } catch (const std::exception& e) {
        // 部分索引类型不支持删除操作
        return false; 
    }
}

auto FaissIndex::query(const VectorRecord &record, int k) -> std::vector<uint64_t> {
  if (record.data_.dim_ != dimension_) {
      return {};
  }

  std::vector<float> distances(k);
  std::vector<faiss::idx_t> labels(k);

  std::shared_lock<std::shared_mutex> lock(mutex_);
  
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
  faiss::RangeSearchResult res(1);
  float search_radius = 0.0f;
  
  // Alpha 参数需与 ComputeEngine 保持一致
  constexpr double alpha = 0.1; 

  // 1. 根据度量类型转换搜索半径
  if (faiss_index_->metric_type == faiss::METRIC_L2) {
      // 转换公式: dist = -ln(Sim) / alpha
      // Faiss L2 Range Search 使用距离平方作为半径
      if (join_similarity_threshold >= 1.0) {
          search_radius = 0.0f; 
      } else if (join_similarity_threshold <= 0.0) {
          search_radius = std::numeric_limits<float>::max(); 
      } else {
          double distance = -std::log(join_similarity_threshold) / alpha;
          search_radius = static_cast<float>(distance * distance);
      }
  } else {
      // Inner Product: 直接使用相似度阈值
      search_radius = static_cast<float>(join_similarity_threshold);
  }

  std::vector<uint64_t> results;
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  try {
    // 2. 尝试执行原生范围搜索
    faiss_index_->range_search(1, reinterpret_cast<const float*>(record.data_.data_.get()), search_radius, &res);
    
    results.reserve(res.lims[1]);
    for (size_t i = 0; i < res.lims[1]; ++i) {
        results.push_back(static_cast<uint64_t>(res.labels[i]));
    }

  } catch (const std::exception& e) {
    // 3. 回退策略: HNSW 不支持 range_search，使用 KNN + 距离过滤模拟
    
    // 设置足够大的 K 值以覆盖潜在匹配
    const int k_fallback = 128; 
    
    std::vector<float> distances(k_fallback);
    std::vector<faiss::idx_t> labels(k_fallback);

    try {
        faiss_index_->search(1, reinterpret_cast<const float*>(record.data_.data_.get()), k_fallback, distances.data(), labels.data());

        for (int i = 0; i < k_fallback; ++i) {
            if (labels[i] == -1) continue;

            bool keep = false;
            if (faiss_index_->metric_type == faiss::METRIC_L2) {
                // L2: 需满足 distance_sq <= radius
                if (distances[i] <= search_radius) keep = true;
            } else {
                // IP: 需满足 score >= threshold
                if (distances[i] >= search_radius) keep = true;
            }

            if (keep) {
                results.push_back(static_cast<uint64_t>(labels[i]));
            }
        }
    } catch (const std::exception& ex) {
        SAGEFLOW_LOG_ERROR("FaissIndex", "Both range_search and knn fallback failed: {}", ex.what());
    }
  }
  
  return results;
}

void FaissIndex::setParameter(const std::string& name, double value) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    // 获取底层索引对象 (剥离 IndexIDMap)
    faiss::Index* raw_index = faiss_index_.get();
    if (auto* id_map = dynamic_cast<faiss::IndexIDMap*>(raw_index)) {
        raw_index = id_map->index;
    }

    // 设置特定算法参数
    if (name == "nprobe") {
        if (auto* ivf = dynamic_cast<faiss::IndexIVF*>(raw_index)) {
            ivf->nprobe = static_cast<size_t>(value);
        }
    } else if (name == "efSearch") {
        if (auto* hnsw = dynamic_cast<faiss::IndexHNSW*>(raw_index)) {
            hnsw->hnsw.efSearch = static_cast<int>(value);
        }
    } else if (name == "efConstruction") {
        if (auto* hnsw = dynamic_cast<faiss::IndexHNSW*>(raw_index)) {
            hnsw->hnsw.efConstruction = static_cast<int>(value);
        }
    }
}

}  // namespace sageFlow
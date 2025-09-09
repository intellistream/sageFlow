#include "index/ivf.h"

#include <algorithm>
#include <iostream>
#include <limits>
#include <queue>
#include <random>
#include <unordered_set>
#include <mutex>
#include <shared_mutex>
#include <numeric>
#include <chrono>

#include "utils/logger.h"

namespace candy {

void Ivf::debugDumpStateUnlocked() {
  // 调用方需已持有 global_mutex_ 锁
  size_t total_in_lists = 0;
  for (auto &kv : inverted_lists_) total_in_lists += kv.second.size();
  CANDY_LOG_WARN("INDEX", "DEBUG_DUMP size_={} total_in_lists={} deleted_uids={} nlists={} attempts={} success={} missing={} miss_in_storage={} miss_not_in_storage={} underflow={} ",
                 size_.load(), total_in_lists, deleted_uids_.size(), inverted_lists_.size(),
                 erase_attempts_.load(), erase_success_.load(), erase_missing_.load(),
                 erase_missing_in_storage_.load(), erase_missing_not_in_storage_.load(), erase_underflow_.load());
  // 采样输出前几个非空列表的部分内容
  int printed = 0;
  for (auto &kv : inverted_lists_) {
    if (kv.second.empty()) continue;
    std::string sample;
    size_t limit = std::min<size_t>(kv.second.size(), 5);
    for (size_t i = 0; i < limit; ++i) { sample += std::to_string(kv.second[i]); sample.push_back(','); }
    CANDY_LOG_INFO("INDEX", "list_id={} size={} sample=[{}]", kv.first, kv.second.size(), sample);
    if (++printed >= 5) break;
  }
}

Ivf::Ivf(int nlist, double rebuild_threshold, int nprobes)
    : nlist_(nlist),
      rebuild_threshold_(rebuild_threshold),
      nprobes_(nprobes),
      list_mutexes_(nlist) {
    // Initialize empty centroids and inverted lists
    centroids_.clear();
    try {
      inverted_lists_.reserve(nlist);
    } catch (const std::exception& e) {
      CANDY_LOG_ERROR("INDEX", "Ivf ctor reserve inverted_lists nlist={} error={} ", nlist, e.what());
      throw; // 继续抛出维持原有语义
    }
}

Ivf::~Ivf() = default;

void Ivf::rebuildIfNeeded(){
  // 关键：获取全局写锁，此期间所有其他读写操作都会被阻塞
  std::unique_lock<std::shared_mutex> lock(global_mutex_);

  // 调用内部的重建逻辑
  rebuildClustersInternal();

  // 重建成功后，清空软删除集合和计数器
  deleted_uids_.clear();
  vectors_since_last_rebuild_.store(0);
}

auto Ivf::needs_rebuild() const -> bool {
  const int current_size = size_.load(std::memory_order_relaxed);
  if (current_size == 0) {
    return false;
  }
  return centroids_.empty() || vectors_since_last_rebuild_.load(std::memory_order_relaxed) >= rebuild_threshold_ * current_size;
}

auto Ivf::assignToCluster(const VectorData& vec) const -> int { // Use trailing return type
    if (centroids_.empty()) {
        return -1; // No clusters yet
    }
    
    int best_cluster = 0;
    double min_distance = std::numeric_limits<double>::max();
    
    for (size_t i = 0; i < centroids_.size(); ++i) {
        double distance = storage_manager_->engine_->EuclideanDistance(vec, centroids_[i]);
        if (distance < min_distance) {
            min_distance = distance;
            best_cluster = static_cast<int>(i);
        }
    }
    
    return best_cluster;
}

namespace { // 使用匿名命名空间，将辅助函数的作用域限制在本文件内

// 模板函数 1: 将一个向量加到质心上
template <typename T>
void addToCentroid(char* centroid_data, const char* vector_data, int dim) {
  auto* centroid_ptr = reinterpret_cast<T*>(centroid_data);
  const auto* vector_ptr = reinterpret_cast<const T*>(vector_data);
  for (int d = 0; d < dim; ++d) {
    centroid_ptr[d] += vector_ptr[d];
  }
}

// 模板函数 2: 将质心除以簇的大小，完成求平均的过程
template <typename T>
void normalizeCentroid(char* centroid_data, int dim, int cluster_size) {
  if (cluster_size == 0) return;
  auto* centroid_ptr = reinterpret_cast<T*>(centroid_data);
  for (int d = 0; d < dim; ++d) {
    centroid_ptr[d] /= cluster_size;
  }
}

} // namespace

void Ivf::rebuildClustersInternal() {
  // 这个函数假定已经被外层的 rebuild() 方法用全局写锁保护

  // =======================================================================
  // 步骤 1: 从倒排列表中收集本索引的全部向量 UID
  // =======================================================================
  std::vector<uint64_t> all_uids_in_index;
  // 预估一个大小以减少内存重分配
  try {
    int logical_size = size_.load(std::memory_order_relaxed);
    if (logical_size < 0) {
      CANDY_LOG_ERROR("INDEX", "size_ negative={} forcing to 0 before reserve (possible erase of non-existent id)", logical_size);
      logical_size = 0; // 防止转换为 size_t 后变成巨大值
    }
    size_t target = static_cast<size_t>(logical_size) + deleted_uids_.size();
    all_uids_in_index.reserve(target);
  } catch (const std::exception& e) {
    CANDY_LOG_ERROR("INDEX", "reserve all_uids_in_index target_size={} error={} ",
                    size_.load(std::memory_order_relaxed) + deleted_uids_.size(), e.what());
    throw;
  }
 
  for (const auto& pair : inverted_lists_) {
    const auto& uids_in_list = pair.second;
    all_uids_in_index.insert(all_uids_in_index.end(), uids_in_list.begin(), uids_in_list.end());
  }
  // =======================================================================
  // 步骤 2: 过滤软删除的向量，并从全局存储中获取“存活”的向量数据
  // =======================================================================
  std::vector<std::shared_ptr<const VectorRecord>> live_records;
  try {
    live_records.reserve(all_uids_in_index.size());
  } catch (const std::exception& e) {
    CANDY_LOG_ERROR("INDEX", "reserve live_records target_size={} error={} ", all_uids_in_index.size(), e.what());
    throw;
  }

  for (const uint64_t uid : all_uids_in_index) {
    // 关键：检查该向量是否在软删除集合中
  if (deleted_uids_.find(uid) != deleted_uids_.end()) {
      continue;
    }

    // 从全局存储中获取向量数据
    if (auto record_sptr = storage_manager_->getVectorByUid(uid)) {
      live_records.push_back(std::move(record_sptr));
    }
  }

  // 如果过滤后没有任何有效数据，则清空索引并直接返回
  if (live_records.empty()) {
    centroids_.clear();
    inverted_lists_.clear();
    return;
  }
  const int live_size = live_records.size();
  const int actual_clusters = std::min(nlist_, live_size);
  if (actual_clusters <= 0) {
    return;
  }
  const int dim = live_records[0]->data_.dim_;
  const DataType type = live_records[0]->data_.type_;
  // =======================================================================
  // 步骤 3: K-Means++ 初始化质心
  // =======================================================================
  centroids_.clear();
  try {
    centroids_.reserve(actual_clusters);
  } catch (const std::exception& e) {
    CANDY_LOG_ERROR("INDEX", "reserve centroids actual_clusters={} error={} ", actual_clusters, e.what());
    throw;
  }

  std::random_device rd;
  std::mt19937 gen(rd());

  // 1. 随机选择第一个质心
  std::uniform_int_distribution<> distrib(0, live_size - 1);
  centroids_.push_back(live_records[distrib(gen)]->data_);

  std::vector<double> min_dist_sq(live_size, std::numeric_limits<double>::max());

  // 2. 智能地选择剩下的 k-1 个质心
  for (int i = 1; i < actual_clusters; ++i) {
    double total_dist_sq = 0.0;

    // a. 计算每个点到“已有”质心的最短距离
    for (int j = 0; j < live_size; ++j) {
      double dist_sq = storage_manager_->engine_->EuclideanDistance(live_records[j]->data_, centroids_.back());
      dist_sq *= dist_sq; // 计算平方距离
      min_dist_sq[j] = std::min(min_dist_sq[j], dist_sq);
      total_dist_sq += min_dist_sq[j];
    }

    // b. 以该距离为权重进行轮盘赌选择
    std::uniform_real_distribution<> dist_prob(0.0, total_dist_sq);
    double target = dist_prob(gen);

    double current_sum = 0.0;
    int next_centroid_idx = 0;
    for (int j = 0; j < live_size; ++j) {
      current_sum += min_dist_sq[j];
      if (current_sum >= target) {
        next_centroid_idx = j;
        break;
      }
    }
    centroids_.push_back(live_records[next_centroid_idx]->data_);
  }
  // =======================================================================
  // 步骤 3: K-Means 迭代
  // =======================================================================
  std::vector<int> assignments(live_size, -1);
  const int max_iterations = 20; // 或者设为配置项

  for (int iter = 0; iter < max_iterations; ++iter) {
    bool changed = false;

    // --- 分配步骤 (Assignment Step) ---
    // 将每个向量分配给距离最近的质心
    for (int i = 0; i < live_size; ++i) {
      int best_cluster = assignToCluster(live_records[i]->data_); // 复用 assignToCluster 函数
      if (assignments[i] != best_cluster) {
        assignments[i] = best_cluster;
        changed = true;
      }
    }

    if (!changed) {
      break; // 如果分配结果不再变化，提前结束
    }
    // --- 更新步骤 (Update Step) ---
    // 1. 准备新的质心存储和簇大小计数器
    std::vector<VectorData> new_centroids;
    try {
      new_centroids.reserve(actual_clusters);
    } catch (const std::exception& e) {
      CANDY_LOG_ERROR("INDEX", "reserve new_centroids actual_clusters={} error={} ", actual_clusters, e.what());
      throw;
    }
    for(int i = 0; i < actual_clusters; ++i) {
      new_centroids.emplace_back(dim, type); // 创建并用0初始化
    }
    std::vector<int> cluster_sizes(actual_clusters, 0);

    // 2. 累加每个簇内的所有向量
    for (int i = 0; i < live_size; ++i) {
      int cluster_idx = assignments[i];
      if (cluster_idx != -1) {
        // 使用模板函数，代码简洁且无重复
        if (type == DataType::Float32) {
          addToCentroid<float>(new_centroids[cluster_idx].data_.get(), live_records[i]->data_.data_.get(), dim);
        } else if (type == DataType::Float64) {
          addToCentroid<double>(new_centroids[cluster_idx].data_.get(), live_records[i]->data_.data_.get(), dim);
        }
        cluster_sizes[cluster_idx]++;
      }
    }

    // 3. 将累加和除以簇的大小，得到新的平均值（质心）
    for (int j = 0; j < actual_clusters; ++j) {
      // 使用模板函数，代码简洁且无重复
      if (type == DataType::Float32) {
        normalizeCentroid<float>(new_centroids[j].data_.get(), dim, cluster_sizes[j]);
      } else if (type == DataType::Float64) {
        normalizeCentroid<double>(new_centroids[j].data_.get(), dim, cluster_sizes[j]);
      }
    }

    // 4. 用新的质心替换旧的质心
    centroids_ = std::move(new_centroids);
  }

  // =======================================================================
  // 步骤 4: 重建倒排列表
  // =======================================================================
  inverted_lists_.clear();
  for (int i = 0; i < live_size; ++i) {
    int cluster_idx = assignments[i];
    if (cluster_idx != -1) {
      inverted_lists_[cluster_idx].push_back(live_records[i]->uid_);
    }
  }

  vectors_since_last_rebuild_.store(0);
  // 诊断：重建后验证 size_ 与实际元素数量是否一致
  size_t actual_total = 0;
  for (auto &kv : inverted_lists_) actual_total += kv.second.size();
  int logical_size = size_.load(std::memory_order_relaxed);
  if (logical_size < 0 || static_cast<size_t>(logical_size) != actual_total) {
    CANDY_LOG_WARN("INDEX", "post-rebuild size mismatch logical={} actual={} deleted_uids={} vectors_since_last_rebuild={} ",
                   logical_size, actual_total, deleted_uids_.size(), vectors_since_last_rebuild_.load());
  }
}

auto Ivf::insert(uint64_t id) -> bool {
  rebuildIfNeeded();
  int cluster_idx = -1;
  {
    // 步骤1：获取全局读锁，安全地读取质心(centroids_)
    std::shared_lock<std::shared_mutex> lock(global_mutex_);
    rebuild_cv_.wait(lock, [this]{ return !is_rebuilding_.load(); });
    if (centroids_.empty()) {
      return false; // 索引尚未初始化，插入失败
    }
    auto record = storage_manager_->getVectorByUid(id);
    if (!record) {
      return false;
    }
    cluster_idx = assignToCluster(record->data_);
  }

  if (cluster_idx < 0) {
    return false;
  }

  // 步骤2：只锁定需要修改的那一个列表（独占写锁）
  std::unique_lock<std::shared_mutex> list_lock(list_mutexes_[cluster_idx]);
  inverted_lists_[cluster_idx].push_back(id);

  size_.fetch_add(1, std::memory_order_relaxed);
  vectors_since_last_rebuild_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

auto Ivf::erase(uint64_t id) -> bool {
  // erase 也需要等待重建完成
  std::unique_lock<std::shared_mutex> lock(global_mutex_);
  rebuild_cv_.wait(lock, [this]{ return !is_rebuilding_.load(); });

  erase_attempts_.fetch_add(1, std::memory_order_relaxed);

  if (deleted_uids_.find(id) != deleted_uids_.end()) {
    erase_missing_.fetch_add(1, std::memory_order_relaxed); // 已标记删除再来一次
    return false; // 已经软删除
  }

  // 确认此向量确实存在：检查 inverted_lists_ 中是否出现
  bool found_in_lists = false;
  for (auto &kv : inverted_lists_) {
    auto &vec = kv.second;
    if (std::find(vec.begin(), vec.end(), id) != vec.end()) { found_in_lists = true; break; }
  }
  if (!found_in_lists) {
    // 判断是否仍在 storage 中存在
    bool in_storage = storage_manager_ && storage_manager_->getVectorByUid(id) != nullptr;
    erase_missing_.fetch_add(1, std::memory_order_relaxed);
    if (in_storage) {
      erase_missing_in_storage_.fetch_add(1, std::memory_order_relaxed);
    } else {
      erase_missing_not_in_storage_.fetch_add(1, std::memory_order_relaxed);
    }
    long long detail_seq = erase_detailed_logs_.fetch_add(1) + 1;
    if (detail_seq <= 50 || detail_seq % 1000 == 0) {
      CANDY_LOG_WARN("INDEX", "erase id={} not found lists size_={} in_storage={} attempts={} missing={} miss_in_storage={} miss_not_storage={} success={} underflow={} ",
                     id, size_.load(), in_storage, erase_attempts_.load(), erase_missing_.load(),
                     erase_missing_in_storage_.load(), erase_missing_not_in_storage_.load(),
                     erase_success_.load(), erase_underflow_.load());
      if (detail_seq <= 10 || detail_seq % 5000 == 0) {
        debugDumpStateUnlocked();
      }
    }
    return false;
  }

  deleted_uids_.insert(id);
  int before = size_.fetch_sub(1, std::memory_order_relaxed);
  if (before <= 0) {
    size_.store(0, std::memory_order_relaxed);
    erase_underflow_.fetch_add(1, std::memory_order_relaxed);
    CANDY_LOG_ERROR("INDEX", "size_ underflow detected (before={}) while erasing id={} -> corrected=0 attempts={} missing={} success={} underflow={} ",
                    before, id, erase_attempts_.load(), erase_missing_.load(), erase_success_.load(), erase_underflow_.load());
    return false;
  }
  erase_success_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

auto Ivf::query(const VectorRecord &record, int k) -> std::vector<uint64_t> {
    if (k <= 0) {
        return {};
    }

    // 使用最小堆来找到 nprobes_ 个最近的簇
    // pair<distance, cluster_id>
    std::priority_queue<std::pair<double, int>> closest_probes_pq;
    std::unordered_set<uint64_t> local_deleted_uids;

    // 步骤 1: 在全局读锁保护下，安全地获取探针和软删除列表
    {
        std::shared_lock<std::shared_mutex> lock(global_mutex_);
        rebuild_cv_.wait(lock, [this]{ return !is_rebuilding_.load(); });

        if (centroids_.empty()) {
            return {};
        }

        for (size_t i = 0; i < centroids_.size(); ++i) {
            double distance = storage_manager_->engine_->EuclideanDistance(record.data_, centroids_[i]);
            closest_probes_pq.emplace(distance, static_cast<int>(i));
            if (closest_probes_pq.size() > nprobes_) {
                closest_probes_pq.pop();
            }
        }
        local_deleted_uids = deleted_uids_;
    } // 全局读锁在这里被释放

    // 步骤 2: 遍历探查列表，对每个列表分别加锁并维护Top-K结果
    // 这个优先队列用来维护最终的 Top-K 结果
    std::priority_queue<UidAndDist> top_k_results;
    while (!closest_probes_pq.empty()) {
        auto cluster_idx = closest_probes_pq.top().second;
        closest_probes_pq.pop();
        std::shared_lock<std::shared_mutex> list_lock(list_mutexes_[cluster_idx]);

  if (inverted_lists_.find(cluster_idx) != inverted_lists_.end()) {
            const auto& candidate_ids = inverted_lists_.at(cluster_idx);
            for (const auto& id_val : candidate_ids) {
                if (local_deleted_uids.find(id_val) != local_deleted_uids.end()) {
                    continue; // 跳过被软删除的向量
                }

                if (auto candidate = storage_manager_->getVectorByUid(id_val)) {
                    double distance = storage_manager_->engine_->EuclideanDistance(record.data_, candidate->data_);

                    if (top_k_results.size() < k) {
                        top_k_results.emplace(id_val, distance);
                    } else if (distance < top_k_results.top().distance_) {
                        // 如果新邻居比当前最远的邻居还要近，就替换掉它
                        top_k_results.pop();
                        top_k_results.emplace(id_val, distance);
                    }
                }
            }
        }
    }

    // 步骤 3: 从优先队列中提取结果并返回
    std::vector<uint64_t> final_ids;
      try {
        final_ids.reserve(top_k_results.size());
      } catch (const std::exception& e) {
        CANDY_LOG_ERROR("INDEX", "reserve final_ids size_hint={} error={} ", top_k_results.size(), e.what());
        throw;
      }
    while (!top_k_results.empty()) {
        final_ids.push_back(top_k_results.top().uid_);
        top_k_results.pop();
    }

    // 优先队列得到的是从远到近的顺序，通常我们需要从近到远返回
  std::reverse(final_ids.begin(), final_ids.end());

    return final_ids;
}

auto Ivf::query_for_join(const VectorRecord &record, double join_similarity_threshold) -> std::vector<uint64_t> {
  std::priority_queue<std::pair<double, int>> probe_indices;
  std::unordered_set<uint64_t> local_deleted_uids;
  {
    std::shared_lock<std::shared_mutex> lock(global_mutex_);
    // 如果有其他线程正在重建，则在此等待
    rebuild_cv_.wait(lock, [this]{ return !is_rebuilding_.load(); });
    if (centroids_.empty()) {
      return {};
    }
    for (size_t i = 0; i < centroids_.size(); ++i) {
      double distance = storage_manager_->engine_->EuclideanDistance(record.data_, centroids_[i]);
      probe_indices.emplace(distance, static_cast<int>(i));
      if (probe_indices.size() > nprobes_) {
        probe_indices.pop();  // 保持前nprobes_个最近的质心
      }
    }
    local_deleted_uids = deleted_uids_;
  }
  // 步骤2：遍历探查列表，对每个列表分别加共享读锁
  std::vector<uint64_t> results;
  constexpr double epsilon = 1e-6;
  while (!probe_indices.empty()) {
    auto cluster_idx = probe_indices.top().second;
    probe_indices.pop();
    std::shared_lock<std::shared_mutex> list_lock(list_mutexes_[cluster_idx]);
  if (inverted_lists_.find(cluster_idx) != inverted_lists_.end()) {
      const auto& candidate_ids = inverted_lists_.at(cluster_idx);
      for (const auto& id_val : candidate_ids) {
  if (local_deleted_uids.find(id_val) != local_deleted_uids.end()) {
          continue;
        }
        if (auto candidate = storage_manager_->getVectorByUid(id_val)) {
          double similarity = storage_manager_->engine_->Similarity(record.data_, candidate->data_);
          if (similarity - join_similarity_threshold > epsilon) {
            results.emplace_back(id_val);
          }
        }
      }
    }
  }

  return results;
}
}  // namespace candy

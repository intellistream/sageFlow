#pragma once
#include "index/index.h"
#include <unordered_map>
#include <unordered_set>
#include <shared_mutex>
#include <condition_variable>
#include <vector>
#include <atomic>

namespace candy {
class Ivf final : public Index {
public:
  // Constructor
  explicit Ivf(int num_clusters = 1000, double rebuild_threshold = 0.5, int nprobes = 10);
  // Destructor
  ~Ivf() override;

  auto insert(uint64_t id) -> bool override;
  auto erase(uint64_t id) -> bool override;
  auto query(const VectorRecord &record, int k) -> std::vector<uint64_t> override;
  auto query_for_join(const VectorRecord &record,
                      double join_similarity_threshold) -> std::vector<uint64_t> override;

 private:
  // Number of clusters for K-means
  int nlist_;
  int nprobes_ = 1;
  // Cluster centroids
  std::vector<VectorData> centroids_;
  // Inverted lists mapping cluster ID to vector IDs
  std::unordered_map<int, std::vector<uint64_t>> inverted_lists_;
  std::unordered_set<uint64_t> deleted_uids_;       // 软删除集合
  // Threshold to trigger rebuilding of clusters (as a ratio)
  double rebuild_threshold_ = 0.5;  // Default value is 0.5
  // Counter for vectors added since last rebuild
  std::atomic<int> vectors_since_last_rebuild_{0}; // 改为原子变量
  std::atomic<int> size_{0};
  std::atomic<bool> is_rebuilding_{false};

  // === 并发控制与数据结构 ===
  mutable std::shared_mutex global_mutex_;          // 全局锁
  std::vector<std::shared_mutex> list_mutexes_;     // 每个倒排列表一把锁
  mutable std::condition_variable_any rebuild_cv_;

  auto needs_rebuild() const -> bool;
  // Perform k-means clustering
  void rebuildClustersInternal();
  // Assign a vector to a cluster
  auto assignToCluster(const VectorData& vec) const -> int;
  void rebuildIfNeeded();

};
}  // namespace candy

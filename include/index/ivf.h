#pragma once
#include <unordered_map>

#include "index/index.h"

namespace candy {
class Ivf final : public Index {
 private:
  // Number of clusters for K-means
  int num_clusters_;
  // Cluster centroids
  std::vector<VectorData> centroids_;
  // Inverted lists mapping cluster ID to vector IDs
  std::unordered_map<int, std::vector<uint64_t>> inverted_lists_;
  // Threshold to trigger rebuilding of clusters
  int rebuild_threshold_;
  // Counter for vectors added since last rebuild
  int vectors_since_last_rebuild_;

  // Perform k-means clustering
  void rebuildClusters();
  // Assign a vector to a cluster
  int assignToCluster(const VectorData& vec);

 public:
  // Constructor
  explicit Ivf(int num_clusters = 100, int rebuild_threshold = 1000);
  // Destructor
  ~Ivf() override;

  auto insert(uint64_t id) -> bool override;
  auto erase(uint64_t id) -> bool override;
  auto query(std::unique_ptr<VectorRecord>& record, int k) -> std::vector<uint64_t> override;
};
}  // namespace candy

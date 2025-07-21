#pragma once
#include "index/index.h"
#include <unordered_map>

#include "index/index.h"

namespace candy {
class Ivf final : public Index {
 private:
  // Number of clusters for K-means
  int nlist_;
  // Cluster centroids
  std::vector<VectorData> centroids_;
  // Inverted lists mapping cluster ID to vector IDs
  std::unordered_map<int, std::vector<uint64_t>> inverted_lists_;
  // Threshold to trigger rebuilding of clusters (as a ratio)
  double rebuild_threshold_ = 0.5;  // Default value is 0.5
  // Counter for vectors added since last rebuild
  int vectors_since_last_rebuild_;

  int nprobes_ = 1;
  int size_ = 0;
  // Perform k-means clustering
  void rebuildClusters();
  // Assign a vector to a cluster
  auto assignToCluster(const VectorData& vec) -> int;

 public:
        // Constructor
        explicit Ivf(int num_clusters = 1000, double rebuild_threshold = 0.5, int nprobes = 10);
        // Destructor
        ~Ivf() override;

        auto insert(uint64_t id) -> bool override;
        auto erase(uint64_t id) -> bool override;
        auto query(std::unique_ptr<VectorRecord>& record, int k) -> std::vector<uint64_t> override;
        auto query_for_join(std::unique_ptr<VectorRecord>& record,
                            double join_similarity_threshold) -> std::vector<uint64_t> override;
};
}  // namespace candy

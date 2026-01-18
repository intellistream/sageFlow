#pragma once

#include "test_utils/data_source/data_source_base.h"
#include <random>
#include <vector>

namespace sageFlow { namespace test {

/**
 * @brief Data source that generates vectors with Zipfian skew towards specific clusters
 * 
 * Generates K centroids.
 * Selects a centroid using Zipfian distribution.
 * Generates a vector near that centroid.
 */
class SkewedDataSource : public DataSourceBase {
public:
  struct Config {
    int vector_dim = 128;
    uint32_t seed = 42;
    int num_clusters = 100;    // Number of clusters (Worksets)
    double zipf_skew = 1.0;    // Skew parameter s (0 = uniform, >1 = highly skewed)
    double cluster_spread = 0.05; // Noise level around centroid
    int max_vectors = -1;
  };

  explicit SkewedDataSource(const Config& config);

  std::vector<float> getNextVector() override;
  int getDimension() const override { return config_.vector_dim; }
  bool hasMore() const override;
  void reset() override;
  int getTotalCount() const override { return config_.max_vectors; }
  
  // Helper for testing
  size_t getLastClusterIndex() const { return last_cluster_index_; }

private:
  Config config_;
  std::mt19937 rng_;
  int generated_count_ = 0;
  
  std::vector<std::vector<float>> centroids_;
  std::discrete_distribution<int> cluster_dist_;
  size_t last_cluster_index_ = 0;
  
  void initCentroids();
  void initDistribution();
  std::vector<float> generateRandomVector();
};

}} // namespace sageFlow::test

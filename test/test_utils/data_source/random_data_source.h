#pragma once

#include "test_utils/data_source/data_source_base.h"
#include <random>

namespace sageFlow { namespace test {

/**
 * @brief Data source that generates random normalized vectors
 * 
 * This is the default data generation method used in the original TestDataGenerator.
 */
class RandomDataSource : public DataSourceBase {
public:
  struct Config {
    int vector_dim = 128;
    uint32_t seed = 42;
    int max_vectors = -1;  // -1 means unlimited
  };

  explicit RandomDataSource(const Config& config);

  std::vector<float> getNextVector() override;
  int getDimension() const override { return config_.vector_dim; }
  bool hasMore() const override;
  void reset() override;
  int getTotalCount() const override { return config_.max_vectors; }

private:
  Config config_;
  std::mt19937 rng_;
  int generated_count_;
};

}} // namespace sageFlow::test

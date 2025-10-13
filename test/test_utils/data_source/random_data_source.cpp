#include "test_utils/data_source/random_data_source.h"
#include <cmath>

namespace sageFlow { namespace test {

RandomDataSource::RandomDataSource(const Config& config)
    : config_(config), rng_(config.seed), generated_count_(0) {}

std::vector<float> RandomDataSource::getNextVector() {
  if (!hasMore()) {
    return std::vector<float>();
  }

  std::vector<float> vec(config_.vector_dim);
  std::normal_distribution<float> dist(0.0f, 1.0f);
  
  for (int i = 0; i < config_.vector_dim; ++i) {
    vec[i] = dist(rng_);
  }

  // Normalize the vector
  float norm = 0.0f;
  for (float v : vec) {
    norm += v * v;
  }
  norm = std::sqrt(norm);

  if (norm > 1e-6f) {
    for (float& v : vec) {
      v /= norm;
    }
  }

  generated_count_++;
  return vec;
}

bool RandomDataSource::hasMore() const {
  if (config_.max_vectors < 0) {
    return true;  // Unlimited
  }
  return generated_count_ < config_.max_vectors;
}

void RandomDataSource::reset() {
  generated_count_ = 0;
  rng_.seed(config_.seed);
}

}} // namespace sageFlow::test

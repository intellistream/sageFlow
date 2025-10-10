#pragma once

#include "test_utils/data_source/data_source_base.h"
#include <string>

namespace sageFlow { namespace test {

/**
 * @brief Data source that reads vectors from fvecs dataset files
 * 
 * Reads vector data from standard fvecs format files (commonly used in vector search benchmarks).
 * The fvecs format stores vectors as: [dimension(int)][vector_data(floats)]...
 */
class DatasetDataSource : public DataSourceBase {
public:
  struct Config {
    std::string file_path;
    bool loop = false;  // If true, loop back to start when reaching end
    int expected_dim = -1;  // Expected dimension, -1 means auto-detect
  };

  explicit DatasetDataSource(const Config& config);

  std::vector<float> getNextVector() override;
  int getDimension() const override { return dimension_; }
  bool hasMore() const override;
  void reset() override;
  int getTotalCount() const override { return static_cast<int>(vectors_.size()); }

private:
  void loadVectors();

  Config config_;
  std::vector<std::vector<float>> vectors_;
  int dimension_;
  size_t current_index_;
};

}} // namespace sageFlow::test

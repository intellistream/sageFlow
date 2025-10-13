#pragma once

#include "test_utils/data_source/data_source_base.h"
#include <string>
#include <fstream>

namespace sageFlow { namespace test {

/**
 * @brief Data source that reads vectors from JSON files
 * 
 * Reads vector data from JSON format files for easy debugging and visualization.
 * JSON format: {"dimension": N, "count": M, "vectors": [[...], [...], ...]}
 */
class JsonDataSource : public DataSourceBase {
public:
  struct Config {
    std::string file_path;
    bool loop = false;  // If true, loop back to start when reaching end
  };

  explicit JsonDataSource(const Config& config);

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

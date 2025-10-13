#pragma once

#include "test_utils/data_source/data_source_base.h"
#include <vector>

namespace sageFlow { namespace test {

/**
 * @brief Data source that provides vectors from an in-memory list
 * 
 * This is a simple adapter that wraps a vector of float vectors and
 * provides them through the DataSourceBase interface. Useful for
 * testing and for wrapping generated data.
 */
class VectorListSource : public DataSourceBase {
public:
  explicit VectorListSource(const std::vector<std::vector<float>>& vectors)
      : vectors_(vectors), index_(0) {}
  
  std::vector<float> getNextVector() override {
    if (index_ >= vectors_.size()) return {};
    return vectors_[index_++];
  }
  
  int getDimension() const override {
    return vectors_.empty() ? 0 : static_cast<int>(vectors_[0].size());
  }
  
  bool hasMore() const override {
    return index_ < vectors_.size();
  }
  
  void reset() override {
    index_ = 0;
  }
  
  int getTotalCount() const override {
    return static_cast<int>(vectors_.size());
  }

private:
  std::vector<std::vector<float>> vectors_;
  size_t index_;
};

}} // namespace sageFlow::test

#pragma once

#include <vector>
#include <memory>
#include "common/data_types.h"

namespace sageFlow { namespace test {

/**
 * @brief Base class for data sources in testing
 * 
 * Provides a unified interface for obtaining vector data from different sources
 * (random generation, datasets, etc.)
 */
class DataSourceBase {
public:
  virtual ~DataSourceBase() = default;

  /**
   * @brief Get the next vector from the data source
   * @return A vector of floats, or empty vector if no more data
   */
  virtual std::vector<float> getNextVector() = 0;

  /**
   * @brief Get the dimension of vectors from this data source
   * @return The vector dimension
   */
  virtual int getDimension() const = 0;

  /**
   * @brief Check if more data is available
   * @return true if more vectors can be obtained, false otherwise
   */
  virtual bool hasMore() const = 0;

  /**
   * @brief Reset the data source to start from the beginning
   */
  virtual void reset() = 0;

  /**
   * @brief Get total number of vectors available (if known)
   * @return Number of vectors, or -1 if unknown
   */
  virtual int getTotalCount() const { return -1; }
};

}} // namespace sageFlow::test

#pragma once

#include <vector>
#include <memory>
#include "common/data_types.h"
#include "test_utils/data_source/data_source_base.h"

namespace sageFlow { namespace test {

/**
 * @brief Configuration for join data source pair
 * 
 * Defines how to create left and right data streams for join testing.
 * Supports multiple modes:
 * - Duplicate: Same data source duplicated to both sides
 * - Separate: Different data sources for left and right
 * - Generated: Use TestDataGenerator with optional UID offset
 */
struct JoinDataSourceConfig {
  enum class Mode {
    Duplicate,    // Duplicate one source to both sides
    Separate,     // Use two separate sources
    Generated     // Use generated data (backward compatible)
  };

  Mode mode = Mode::Generated;
  
  // For Duplicate mode
  std::shared_ptr<DataSourceBase> single_source;
  
  // For Separate mode  
  std::shared_ptr<DataSourceBase> left_source;
  std::shared_ptr<DataSourceBase> right_source;
  
  // Common options
  bool apply_right_uid_offset = true;   // Add offset to right stream UIDs
  uint64_t right_uid_offset = 500000;   // Default UID offset for right stream
  int64_t base_timestamp = 1000000;     // Starting timestamp
  int64_t time_interval = 100;          // Time increment between records
};

/**
 * @brief Manages a pair of data sources for join testing
 * 
 * Provides a unified interface for creating left and right data streams
 * from various sources. Supports:
 * - Duplicating a single source to both sides
 * - Using separate sources for left and right
 * - Applying UID offsets to distinguish streams
 * - Generating VectorRecords with proper timestamps
 */
class JoinDataSourcePair {
public:
  explicit JoinDataSourcePair(const JoinDataSourceConfig& config);

  /**
   * @brief Generate left and right record streams
   * @param max_records Maximum records to generate (0 = all available)
   * @return Pair of (left_records, right_records)
   */
  std::pair<std::vector<std::unique_ptr<VectorRecord>>, 
            std::vector<std::unique_ptr<VectorRecord>>> 
  generateStreams(size_t max_records = 0);

  /**
   * @brief Get the dimension of vectors in this pair
   */
  int getDimension() const;

  /**
   * @brief Get total available records (from smaller source if separate)
   */
  int getTotalCount() const;

  /**
   * @brief Reset both sources to beginning
   */
  void reset();

private:
  JoinDataSourceConfig config_;
  uint64_t next_left_uid_ = 1;
  uint64_t next_right_uid_ = 1;
  
  std::unique_ptr<VectorRecord> createRecord(uint64_t uid, const std::vector<float>& data, int64_t timestamp);
};

/**
 * @brief Factory for creating common join data source configurations
 */
class JoinDataSourceFactory {
public:
  /**
   * @brief Create config that duplicates a single source to both sides
   * @param source Data source to duplicate
   * @param apply_uid_offset Whether to offset right stream UIDs
   */
  static JoinDataSourceConfig createDuplicated(
      std::shared_ptr<DataSourceBase> source,
      bool apply_uid_offset = true);

  /**
   * @brief Create config using separate sources for left and right
   * @param left_source Source for left stream
   * @param right_source Source for right stream  
   * @param apply_uid_offset Whether to offset right stream UIDs
   */
  static JoinDataSourceConfig createSeparate(
      std::shared_ptr<DataSourceBase> left_source,
      std::shared_ptr<DataSourceBase> right_source,
      bool apply_uid_offset = false);

  /**
   * @brief Create config using TestDataGenerator (backward compatible)
   * 
   * This is the default mode that maintains compatibility with existing tests.
   * Data is generated once and duplicated to both streams.
   */
  static JoinDataSourceConfig createGenerated(
      std::shared_ptr<DataSourceBase> source,
      bool apply_uid_offset = true);
};

}} // namespace sageFlow::test

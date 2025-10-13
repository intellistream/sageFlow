#pragma once

#include "test_utils/join_data_source.h"
#include "test_utils/test_data_generator.h"
#include "test_utils/data_source/random_data_source.h"

namespace sageFlow { namespace test {

/**
 * @brief Helper functions for creating join test data
 * 
 * Provides convenient wrappers for common join testing scenarios,
 * maintaining backward compatibility with existing tests.
 */
class JoinTestHelper {
public:
  /**
   * @brief Create join streams from TestDataGenerator (backward compatible)
   * 
   * This is the standard pattern used in existing tests:
   * 1. Generate data with TestDataGenerator
   * 2. Duplicate to left and right streams
   * 3. Apply UID offset to right stream
   * 
   * @param generator TestDataGenerator instance
   * @param apply_uid_offset Whether to offset right UIDs (default: true)
   * @return Pair of (left_records, right_records)
   */
  static std::pair<std::vector<std::unique_ptr<VectorRecord>>,
                   std::vector<std::unique_ptr<VectorRecord>>>
  generateJoinStreamsFromGenerator(
      TestDataGenerator& generator,
      bool apply_uid_offset = true);

  /**
   * @brief Create join streams using a data source pair
   * 
   * @param pair JoinDataSourcePair to generate from
   * @param max_records Maximum records (0 = all available)
   * @return Pair of (left_records, right_records)
   */
  static std::pair<std::vector<std::unique_ptr<VectorRecord>>,
                   std::vector<std::unique_ptr<VectorRecord>>>
  generateJoinStreams(
      JoinDataSourcePair& pair,
      size_t max_records = 0);

  /**
   * @brief Create join streams by duplicating a single data source
   * 
   * Useful for testing with dataset files or specific patterns.
   * 
   * @param source Data source to duplicate
   * @param apply_uid_offset Whether to offset right UIDs
   * @param max_records Maximum records (0 = all available)
   * @return Pair of (left_records, right_records)
   */
  static std::pair<std::vector<std::unique_ptr<VectorRecord>>,
                   std::vector<std::unique_ptr<VectorRecord>>>
  generateJoinStreamsFromSource(
      std::shared_ptr<DataSourceBase> source,
      bool apply_uid_offset = true,
      size_t max_records = 0);

  /**
   * @brief Create join streams from separate left and right sources
   * 
   * Allows testing with different data distributions on each side.
   * 
   * @param left_source Source for left stream
   * @param right_source Source for right stream
   * @param apply_uid_offset Whether to offset right UIDs
   * @param max_records Maximum records (0 = all available)
   * @return Pair of (left_records, right_records)
   */
  static std::pair<std::vector<std::unique_ptr<VectorRecord>>,
                   std::vector<std::unique_ptr<VectorRecord>>>
  generateJoinStreamsFromSeparateSources(
      std::shared_ptr<DataSourceBase> left_source,
      std::shared_ptr<DataSourceBase> right_source,
      bool apply_uid_offset = false,
      size_t max_records = 0);
};

}} // namespace sageFlow::test

#include <gtest/gtest.h>
#include "test_utils/join_data_source.h"
#include "test_utils/join_test_helper.h"
#include "test_utils/test_data_generator.h"
#include "test_utils/data_source/random_data_source.h"
#include "test_utils/data_source/dataset_data_source.h"
#include "test_utils/test_data_adapter.h"
#include <memory>
#include <fstream>

namespace sageFlow {
namespace test {

class JoinDataSourceTest : public ::testing::Test {
protected:
  void SetUp() override {}
  void TearDown() override {}
};

// Test basic duplication mode
TEST_F(JoinDataSourceTest, DuplicateMode) {
  // Create a simple random source
  RandomDataSource::Config config;
  config.vector_dim = 64;
  config.seed = 42;
  config.max_vectors = 50;
  auto source = std::make_shared<RandomDataSource>(config);

  // Create join pair in duplicate mode
  auto join_config = JoinDataSourceFactory::createDuplicated(source, true);
  JoinDataSourcePair pair(join_config);

  // Generate streams
  auto [left_records, right_records] = pair.generateStreams();

  // Verify
  EXPECT_EQ(left_records.size(), 50);
  EXPECT_EQ(right_records.size(), 50);
  EXPECT_EQ(pair.getDimension(), 64);

  // Verify UIDs are offset for right stream
  for (size_t i = 0; i < left_records.size(); ++i) {
    EXPECT_LT(left_records[i]->uid_, right_records[i]->uid_);
    // Check that vectors are the same (duplicated)
    auto left_vec = extractFloatVector(*left_records[i]);
    auto right_vec = extractFloatVector(*right_records[i]);
    ASSERT_EQ(left_vec.size(), right_vec.size());
    for (size_t j = 0; j < left_vec.size(); ++j) {
      EXPECT_FLOAT_EQ(left_vec[j], right_vec[j]);
    }
  }
}

// Test separate sources mode
TEST_F(JoinDataSourceTest, SeparateMode) {
  // Create two different random sources with different seeds
  RandomDataSource::Config config1;
  config1.vector_dim = 32;
  config1.seed = 111;
  config1.max_vectors = 30;
  auto left_source = std::make_shared<RandomDataSource>(config1);

  RandomDataSource::Config config2;
  config2.vector_dim = 32;
  config2.seed = 222;
  config2.max_vectors = 30;
  auto right_source = std::make_shared<RandomDataSource>(config2);

  // Create join pair in separate mode
  auto join_config = JoinDataSourceFactory::createSeparate(
      left_source, right_source, false);
  JoinDataSourcePair pair(join_config);

  // Generate streams
  auto [left_records, right_records] = pair.generateStreams();

  // Verify
  EXPECT_EQ(left_records.size(), 30);
  EXPECT_EQ(right_records.size(), 30);
  EXPECT_EQ(pair.getDimension(), 32);

  // Verify vectors are different (separate sources)
  bool found_difference = false;
  for (size_t i = 0; i < std::min(left_records.size(), right_records.size()); ++i) {
    auto left_vec = extractFloatVector(*left_records[i]);
    auto right_vec = extractFloatVector(*right_records[i]);
    
    for (size_t j = 0; j < left_vec.size(); ++j) {
      if (std::abs(left_vec[j] - right_vec[j]) > 0.001f) {
        found_difference = true;
        break;
      }
    }
    if (found_difference) break;
  }
  EXPECT_TRUE(found_difference) << "Expected different vectors from separate sources";
}

// Test helper function for TestDataGenerator (backward compatible)
TEST_F(JoinDataSourceTest, HelperWithTestDataGenerator) {
  TestDataGenerator::Config config;
  config.vector_dim = 128;
  config.positive_pairs = 10;
  config.near_threshold_pairs = 0;  // Set to 0 to avoid defaults
  config.negative_pairs = 10;
  config.random_tail = 20;
  config.seed = 99;

  TestDataGenerator generator(config);
  
  // Use helper to generate join streams
  auto [left_records, right_records] = 
      JoinTestHelper::generateJoinStreamsFromGenerator(generator, true);

  // Verify counts
  int expected = (10 + 0 + 10) * 2 + 20;  // pairs * 2 + tail
  EXPECT_EQ(left_records.size(), expected);
  EXPECT_EQ(right_records.size(), expected);

  // Verify dimension
  for (const auto& rec : left_records) {
    EXPECT_EQ(rec->data_.dim_, 128);
  }
}

// Test helper with single source
TEST_F(JoinDataSourceTest, HelperWithSingleSource) {
  RandomDataSource::Config config;
  config.vector_dim = 64;
  config.seed = 777;
  config.max_vectors = 25;
  auto source = std::make_shared<RandomDataSource>(config);

  // Use helper to generate join streams
  auto [left_records, right_records] = 
      JoinTestHelper::generateJoinStreamsFromSource(source, true, 25);

  EXPECT_EQ(left_records.size(), 25);
  EXPECT_EQ(right_records.size(), 25);
}

// Test helper with separate sources
TEST_F(JoinDataSourceTest, HelperWithSeparateSources) {
  RandomDataSource::Config config1;
  config1.vector_dim = 32;
  config1.seed = 123;
  config1.max_vectors = 15;
  auto left_source = std::make_shared<RandomDataSource>(config1);

  RandomDataSource::Config config2;
  config2.vector_dim = 32;
  config2.seed = 456;
  config2.max_vectors = 15;
  auto right_source = std::make_shared<RandomDataSource>(config2);

  // Use helper
  auto [left_records, right_records] = 
      JoinTestHelper::generateJoinStreamsFromSeparateSources(
          left_source, right_source, false, 15);

  EXPECT_EQ(left_records.size(), 15);
  EXPECT_EQ(right_records.size(), 15);
}

// Test with dataset file (if available)
TEST_F(JoinDataSourceTest, WithDatasetSource) {
  std::string dataset_path = PROJECT_DIR "/data/siftsmall/siftsmall_query.fvecs";
  std::ifstream test_file(dataset_path);
  if (!test_file.good()) {
    GTEST_SKIP() << "Dataset file not found: " << dataset_path;
  }
  test_file.close();

  // Load dataset
  DatasetDataSource::Config config;
  config.file_path = dataset_path;
  config.expected_dim = 128;
  config.loop = false;
  auto source = std::make_shared<DatasetDataSource>(config);

  // Use with join data source
  auto [left_records, right_records] = 
      JoinTestHelper::generateJoinStreamsFromSource(source, true, 50);

  EXPECT_EQ(left_records.size(), 50);
  EXPECT_EQ(right_records.size(), 50);
  
  // Verify all records have correct dimension
  for (const auto& rec : left_records) {
    EXPECT_EQ(rec->data_.dim_, 128);
  }
}

// Test max_records limiting
TEST_F(JoinDataSourceTest, MaxRecordsLimit) {
  RandomDataSource::Config config;
  config.vector_dim = 32;
  config.seed = 999;
  config.max_vectors = 100;
  auto source = std::make_shared<RandomDataSource>(config);

  auto join_config = JoinDataSourceFactory::createDuplicated(source, true);
  JoinDataSourcePair pair(join_config);

  // Generate with limit
  auto [left_records, right_records] = pair.generateStreams(25);

  EXPECT_EQ(left_records.size(), 25);
  EXPECT_EQ(right_records.size(), 25);
}

// Test reset functionality
TEST_F(JoinDataSourceTest, ResetFunctionality) {
  RandomDataSource::Config config;
  config.vector_dim = 16;
  config.seed = 555;
  config.max_vectors = 10;
  auto source = std::make_shared<RandomDataSource>(config);

  auto join_config = JoinDataSourceFactory::createDuplicated(source, true);
  JoinDataSourcePair pair(join_config);

  // Generate first time
  auto [left1, right1] = pair.generateStreams();
  EXPECT_EQ(left1.size(), 10);

  // Reset and generate again
  pair.reset();
  auto [left2, right2] = pair.generateStreams();
  EXPECT_EQ(left2.size(), 10);

  // Verify data is the same (same seed, reset source)
  for (size_t i = 0; i < left1.size(); ++i) {
    auto vec1 = extractFloatVector(*left1[i]);
    auto vec2 = extractFloatVector(*left2[i]);
    ASSERT_EQ(vec1.size(), vec2.size());
    for (size_t j = 0; j < vec1.size(); ++j) {
      EXPECT_FLOAT_EQ(vec1[j], vec2[j]);
    }
  }
}

}} // namespace sageFlow::test

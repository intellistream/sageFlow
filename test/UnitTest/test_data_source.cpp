#include <gtest/gtest.h>
#include "test_utils/test_data_generator.h"
#include "test_utils/data_source/random_data_source.h"
#include "test_utils/data_source/dataset_data_source.h"
#include <memory>
#include <fstream>

namespace sageFlow {
namespace test {

class DataSourceTest : public ::testing::Test {
protected:
  void SetUp() override {}
  void TearDown() override {}
};

// Test RandomDataSource
TEST_F(DataSourceTest, RandomDataSourceBasic) {
  RandomDataSource::Config config;
  config.vector_dim = 64;
  config.seed = 42;
  config.max_vectors = 10;

  auto data_source = std::make_shared<RandomDataSource>(config);

  EXPECT_EQ(data_source->getDimension(), 64);
  EXPECT_TRUE(data_source->hasMore());
  EXPECT_EQ(data_source->getTotalCount(), 10);

  int count = 0;
  while (data_source->hasMore()) {
    auto vec = data_source->getNextVector();
    EXPECT_EQ(vec.size(), 64);
    
    // Check that vector is normalized
    float norm = 0.0f;
    for (float v : vec) {
      norm += v * v;
    }
    norm = std::sqrt(norm);
    EXPECT_NEAR(norm, 1.0f, 1e-5f);
    
    count++;
  }
  EXPECT_EQ(count, 10);

  // After reset, should be able to get more vectors
  data_source->reset();
  EXPECT_TRUE(data_source->hasMore());
  auto vec = data_source->getNextVector();
  EXPECT_EQ(vec.size(), 64);
}

// Test DatasetDataSource with siftsmall dataset
TEST_F(DataSourceTest, DatasetDataSourceBasic) {
  // Check if the dataset file exists
  std::string project_dir = PROJECT_DIR;
  std::string dataset_path = project_dir + "/data/siftsmall/siftsmall_query.fvecs";
  std::ifstream test_file(dataset_path);
  if (!test_file.good()) {
    GTEST_SKIP() << "Dataset file not found: " << dataset_path;
  }
  test_file.close();

  DatasetDataSource::Config config;
  config.file_path = dataset_path;
  config.expected_dim = 128;
  config.loop = false;

  auto data_source = std::make_shared<DatasetDataSource>(config);

  EXPECT_EQ(data_source->getDimension(), 128);
  EXPECT_TRUE(data_source->hasMore());
  EXPECT_GT(data_source->getTotalCount(), 0);

  int initial_count = data_source->getTotalCount();
  std::cout << "Loaded " << initial_count << " vectors from dataset" << std::endl;

  // Get a few vectors
  int count = 0;
  while (data_source->hasMore() && count < 5) {
    auto vec = data_source->getNextVector();
    EXPECT_EQ(vec.size(), 128);
    count++;
  }
  EXPECT_EQ(count, 5);

  // Reset and read again
  data_source->reset();
  EXPECT_TRUE(data_source->hasMore());
  auto vec = data_source->getNextVector();
  EXPECT_EQ(vec.size(), 128);
}

// Test TestDataGenerator with custom data source
TEST_F(DataSourceTest, TestDataGeneratorWithRandomDataSource) {
  // Create a random data source
  RandomDataSource::Config ds_config;
  ds_config.vector_dim = 64;
  ds_config.seed = 123;
  ds_config.max_vectors = -1;  // Unlimited

  auto data_source = std::make_shared<RandomDataSource>(ds_config);

  // Create TestDataGenerator with the data source
  TestDataGenerator::Config gen_config;
  gen_config.vector_dim = 64;
  gen_config.positive_pairs = 10;
  gen_config.negative_pairs = 10;
  gen_config.near_threshold_pairs = 5;
  gen_config.random_tail = 20;
  gen_config.similarity_threshold = 0.8;

  TestDataGenerator generator(gen_config, data_source);

  auto [records, expected_matches] = generator.generateData();

  // Verify data was generated
  int expected_records = (10 + 5 + 10) * 2 + 20;
  EXPECT_EQ(records.size(), expected_records);
  EXPECT_GE(expected_matches.size(), 10);  // At least positive pairs
}

// Test TestDataGenerator with dataset data source
TEST_F(DataSourceTest, TestDataGeneratorWithDatasetDataSource) {
  // Check if the dataset file exists
  std::string project_dir = PROJECT_DIR;
  std::string dataset_path = project_dir + "/data/siftsmall/siftsmall_query.fvecs";
  std::ifstream test_file(dataset_path);
  if (!test_file.good()) {
    GTEST_SKIP() << "Dataset file not found: " << dataset_path;
  }
  test_file.close();

  // Create a dataset data source with looping enabled
  DatasetDataSource::Config ds_config;
  ds_config.file_path = dataset_path;
  ds_config.expected_dim = 128;
  ds_config.loop = true;  // Enable looping to allow reuse

  auto data_source = std::make_shared<DatasetDataSource>(ds_config);

  // Create TestDataGenerator with the data source
  TestDataGenerator::Config gen_config;
  gen_config.vector_dim = 128;
  gen_config.positive_pairs = 5;
  gen_config.negative_pairs = 5;
  gen_config.near_threshold_pairs = 2;
  gen_config.random_tail = 10;
  gen_config.similarity_threshold = 0.8;

  TestDataGenerator generator(gen_config, data_source);

  auto [records, expected_matches] = generator.generateData();

  // Verify data was generated
  int expected_records = (5 + 2 + 5) * 2 + 10;
  EXPECT_EQ(records.size(), expected_records);
  
  // All records should have dimension 128
  for (const auto& record : records) {
    EXPECT_EQ(record->data_.dim_, 128);
  }
}

// Test backward compatibility - default constructor still works
TEST_F(DataSourceTest, BackwardCompatibility) {
  TestDataGenerator::Config config;
  config.vector_dim = 64;
  config.positive_pairs = 5;
  config.negative_pairs = 5;
  config.near_threshold_pairs = 2;
  config.random_tail = 10;

  // Use default constructor (should create random data source internally)
  TestDataGenerator generator(config);

  auto [records, expected_matches] = generator.generateData();

  int expected_records = (5 + 2 + 5) * 2 + 10;
  EXPECT_EQ(records.size(), expected_records);
}

}} // namespace sageFlow::test

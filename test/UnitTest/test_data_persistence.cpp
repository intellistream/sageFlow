#include <gtest/gtest.h>
#include "test_utils/test_data_generator.h"
#include "test_utils/data_source/random_data_source.h"
#include "test_utils/data_source/dataset_data_source.h"
#include "test_utils/data_source/json_data_source.h"
#include "test_utils/data_writer/fvecs_writer.h"
#include "test_utils/data_writer/json_writer.h"
#include <memory>
#include <fstream>
#include <filesystem>

namespace sageFlow {
namespace test {

class DataPersistenceTest : public ::testing::Test {
protected:
  void SetUp() override {
    test_dir_ = "/tmp/sageflow_test_data";
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    // Clean up test files
    std::filesystem::remove_all(test_dir_);
  }

  std::string test_dir_;
};

// Test saving generated data to fvecs format
TEST_F(DataPersistenceTest, SaveToFvecsFormat) {
  // Generate test data
  TestDataGenerator::Config config;
  config.vector_dim = 64;
  config.positive_pairs = 10;
  config.negative_pairs = 10;
  config.random_tail = 20;
  config.seed = 42;

  TestDataGenerator generator(config);
  auto [records, matches] = generator.generateData();

  // Save to fvecs
  std::string fvecs_path = test_dir_ + "/test_data.fvecs";
  auto writer = std::make_shared<FvecsWriter>();
  bool success = generator.saveGeneratedVectors(fvecs_path, writer);

  EXPECT_TRUE(success);
  EXPECT_TRUE(std::filesystem::exists(fvecs_path));

  // Verify file is not empty
  std::ifstream check(fvecs_path, std::ios::binary | std::ios::ate);
  EXPECT_TRUE(check.is_open());
  auto file_size = check.tellg();
  EXPECT_GT(file_size, 0);
  check.close();

  // Expected: (10 + 10 + 10) * 2 + 20 = 80 vectors
  int expected_vectors = (config.positive_pairs + config.near_threshold_pairs + config.negative_pairs) * 2 + config.random_tail;
  EXPECT_EQ(records.size(), expected_vectors);
}

// Test saving generated data to JSON format
TEST_F(DataPersistenceTest, SaveToJsonFormat) {
  // Generate test data
  TestDataGenerator::Config config;
  config.vector_dim = 32;
  config.positive_pairs = 5;
  config.negative_pairs = 5;
  config.random_tail = 10;
  config.seed = 123;

  TestDataGenerator generator(config);
  auto [records, matches] = generator.generateData();

  // Save to JSON
  std::string json_path = test_dir_ + "/test_data.json";
  auto writer = std::make_shared<JsonWriter>();
  bool success = generator.saveGeneratedVectors(json_path, writer);

  EXPECT_TRUE(success);
  EXPECT_TRUE(std::filesystem::exists(json_path));

  // Verify JSON is human-readable
  std::ifstream json_file(json_path);
  std::string first_line;
  std::getline(json_file, first_line);
  EXPECT_TRUE(first_line.find("{") != std::string::npos);
  json_file.close();
}

// Test round-trip: save to fvecs, load back, and verify
TEST_F(DataPersistenceTest, RoundTripFvecs) {
  // Generate and save
  TestDataGenerator::Config config;
  config.vector_dim = 128;
  config.positive_pairs = 10;
  config.negative_pairs = 10;
  config.random_tail = 20;
  config.seed = 42;

  TestDataGenerator generator(config);
  auto [records, matches] = generator.generateData();
  auto original_vectors = generator.getLastGeneratedVectors();

  std::string fvecs_path = test_dir_ + "/roundtrip.fvecs";
  auto writer = std::make_shared<FvecsWriter>();
  ASSERT_TRUE(generator.saveGeneratedVectors(fvecs_path, writer));

  // Load back
  DatasetDataSource::Config ds_config;
  ds_config.file_path = fvecs_path;
  ds_config.expected_dim = 128;
  auto data_source = std::make_shared<DatasetDataSource>(ds_config);

  // Verify dimension
  EXPECT_EQ(data_source->getDimension(), 128);
  EXPECT_EQ(data_source->getTotalCount(), original_vectors.size());

  // Verify vectors match
  int count = 0;
  while (data_source->hasMore() && count < static_cast<int>(original_vectors.size())) {
    auto loaded_vec = data_source->getNextVector();
    ASSERT_EQ(loaded_vec.size(), original_vectors[count].size());
    
    // Check values match (with floating point tolerance)
    for (size_t i = 0; i < loaded_vec.size(); ++i) {
      EXPECT_NEAR(loaded_vec[i], original_vectors[count][i], 1e-5);
    }
    count++;
  }
  EXPECT_EQ(count, original_vectors.size());
}

// Test round-trip: save to JSON, load back, and verify
TEST_F(DataPersistenceTest, RoundTripJson) {
  // Generate and save
  TestDataGenerator::Config config;
  config.vector_dim = 64;
  config.positive_pairs = 5;
  config.negative_pairs = 5;
  config.random_tail = 10;
  config.seed = 999;

  TestDataGenerator generator(config);
  auto [records, matches] = generator.generateData();
  auto original_vectors = generator.getLastGeneratedVectors();

  std::string json_path = test_dir_ + "/roundtrip.json";
  auto writer = std::make_shared<JsonWriter>();
  ASSERT_TRUE(generator.saveGeneratedVectors(json_path, writer));

  // Load back
  JsonDataSource::Config ds_config;
  ds_config.file_path = json_path;
  auto data_source = std::make_shared<JsonDataSource>(ds_config);

  // Verify dimension
  EXPECT_EQ(data_source->getDimension(), 64);
  EXPECT_EQ(data_source->getTotalCount(), original_vectors.size());

  // Verify vectors match
  int count = 0;
  while (data_source->hasMore() && count < static_cast<int>(original_vectors.size())) {
    auto loaded_vec = data_source->getNextVector();
    ASSERT_EQ(loaded_vec.size(), original_vectors[count].size());
    
    // Check values match (JSON has 6 decimal precision)
    for (size_t i = 0; i < loaded_vec.size(); ++i) {
      EXPECT_NEAR(loaded_vec[i], original_vectors[count][i], 1e-5);
    }
    count++;
  }
  EXPECT_EQ(count, original_vectors.size());
}

// Test using loaded data with TestDataGenerator
TEST_F(DataPersistenceTest, GenerateFromSavedData) {
  // First generate and save some data
  TestDataGenerator::Config config1;
  config1.vector_dim = 64;
  config1.positive_pairs = 10;
  config1.negative_pairs = 10;
  config1.random_tail = 20;
  config1.seed = 42;

  TestDataGenerator generator1(config1);
  auto [records1, matches1] = generator1.generateData();

  std::string fvecs_path = test_dir_ + "/source_data.fvecs";
  auto writer = std::make_shared<FvecsWriter>();
  ASSERT_TRUE(generator1.saveGeneratedVectors(fvecs_path, writer));

  // Now load that data and use it with TestDataGenerator
  DatasetDataSource::Config ds_config;
  ds_config.file_path = fvecs_path;
  ds_config.expected_dim = 64;
  ds_config.loop = true;  // Enable looping for reuse
  auto data_source = std::make_shared<DatasetDataSource>(ds_config);

  TestDataGenerator::Config config2;
  config2.similarity_threshold = 0.8;
  config2.positive_pairs = 5;
  config2.near_threshold_pairs = 0;  // Set to 0 to avoid defaults
  config2.negative_pairs = 5;
  config2.random_tail = 10;

  TestDataGenerator generator2(config2, data_source);
  auto [records2, matches2] = generator2.generateData();

  // Verify records were created
  int expected_records2 = (5 + 0 + 5) * 2 + 10;
  EXPECT_EQ(records2.size(), expected_records2);
  
  // All records should use dimension from loaded data
  for (const auto& record : records2) {
    EXPECT_EQ(record->data_.dim_, 64);
  }
}

}} // namespace sageFlow::test

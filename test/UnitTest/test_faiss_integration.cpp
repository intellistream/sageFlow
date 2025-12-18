#include <gtest/gtest.h>
#include "utils/logger.h"
#include <memory>
#include <vector>
#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"
#include "test_utils/test_data_generator.h"
#include "common/data_types.h"

namespace sageFlow {
namespace test {

class FaissIndexTest : public ::testing::Test {
protected:
  void SetUp() override {
    auto storage = std::make_shared<StorageManager>();
    concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage);
    
    generator_config_.vector_dim = 128;
    generator_config_.similarity_threshold = 0.8;
    generator_config_.seed = 42;
    generator_config_.positive_pairs = 100;
    generator_config_.negative_pairs = 0;
  }

  void TearDown() override {
  }

protected:
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  TestDataGenerator::Config generator_config_;
};

TEST_F(FaissIndexTest, IVFBasicOperations) {
  // 1. Create Faiss IVF Index
  FaissIVFParameters params;
  params.nlist = 10;
  params.nprobe = 5;
  
  int index_id = concurrency_manager_->create_index("faiss_ivf", IndexType::FaissIVF, 128, params);
  ASSERT_GE(index_id, 0);

  // 2. Generate Data
  TestDataGenerator generator(generator_config_);
  auto [records, expected_matches] = generator.generateData();
  
  // 3. Insert Data
  for (auto& record : records) {
    bool success = concurrency_manager_->insert(index_id, std::move(record));
    ASSERT_TRUE(success);
  }

  // 4. Query Data
  generator_config_.seed = 42;
  TestDataGenerator generator2(generator_config_);
  auto [records2, _] = generator2.generateData();
  
  auto& query_record = records2[0];
  
  auto results = concurrency_manager_->query(index_id, *query_record, 5);
  
  ASSERT_FALSE(results.empty());
  EXPECT_EQ(results[0]->uid_, query_record->uid_);
}

TEST_F(FaissIndexTest, HNSWBasicOperations) {
  // 1. Create Faiss HNSW Index
  FaissHNSWParameters params;
  params.M = 16;
  params.efConstruction = 40;
  params.efSearch = 16;
  
  int index_id = concurrency_manager_->create_index("faiss_hnsw", IndexType::FaissHNSW, 128, params);
  ASSERT_GE(index_id, 0);

  // 2. Generate Data
  TestDataGenerator generator(generator_config_);
  auto [records, expected_matches] = generator.generateData();
  
  // 3. Insert Data
  for (auto& record : records) {
    bool success = concurrency_manager_->insert(index_id, std::move(record));
    ASSERT_TRUE(success);
  }

  // 4. Query Data
  generator_config_.seed = 42;
  TestDataGenerator generator2(generator_config_);
  auto [records2, _] = generator2.generateData();
  
  auto& query_record = records2[0];
  
  auto results = concurrency_manager_->query(index_id, *query_record, 5);
  
  ASSERT_FALSE(results.empty());
  EXPECT_EQ(results[0]->uid_, query_record->uid_);
}

TEST_F(FaissIndexTest, RangeSearchForJoin) {
  // 1. Create Faiss IVF Index
  int index_id = concurrency_manager_->create_index("faiss_ivf_join", IndexType::FaissIVF, 128);
  ASSERT_GE(index_id, 0);

  // 2. Generate Data
  TestDataGenerator generator(generator_config_);
  auto [records, expected_matches] = generator.generateData();
  
  // 3. Insert Data
  for (auto& record : records) {
    concurrency_manager_->insert(index_id, std::move(record));
  }

  // 4. Range Query
  generator_config_.seed = 42;
  TestDataGenerator generator2(generator_config_);
  auto [records2, _] = generator2.generateData();
  auto& query_record = records2[0];
  
  double threshold = 1000.0; 
  auto results = concurrency_manager_->query_for_join(index_id, *query_record, threshold);
  
  bool found_self = false;
  for(auto& res : results) {
      if(res->uid_ == query_record->uid_) found_self = true;
  }
  EXPECT_TRUE(found_self);
}

} // namespace test
} // namespace sageFlow

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <vector>

#include "concurrency/concurrency_manager.h"
#include "index/index.h"
#include "storage/storage_manager.h"
#include "test_utils/test_data_adapter.h"

namespace sageFlow::test {

namespace {

auto uidsOf(const std::vector<std::shared_ptr<const VectorRecord>>& records) -> std::vector<uint64_t> {
  std::vector<uint64_t> uids;
  uids.reserve(records.size());
  for (const auto& record : records) {
    if (record) {
      uids.push_back(record->uid_);
    }
  }
  std::sort(uids.begin(), uids.end());
  return uids;
}

}  // namespace

TEST(KnnIndexTest, BruteForceQueryForJoinOnlyScansInsertedIndexUids) {
  auto storage = std::make_shared<StorageManager>();
  auto manager = std::make_shared<ConcurrencyManager>(storage);

  const int left_index = manager->create_index("left_local", IndexType::BruteForce, 2);
  const int right_index = manager->create_index("right_local", IndexType::BruteForce, 2);
  ASSERT_GE(left_index, 0);
  ASSERT_GE(right_index, 0);

  ASSERT_TRUE(manager->insert(left_index, createVectorRecord(1, 100, {0.0F, 0.0F})));
  ASSERT_TRUE(manager->insert(right_index, createVectorRecord(2, 100, {0.0F, 0.0F})));

  const auto query = createVectorRecord(999, 200, {0.0F, 0.0F});
  auto left_results = manager->query_for_join(left_index, *query, 0.99, 0.1);
  auto right_results = manager->query_for_join(right_index, *query, 0.99, 0.1);

  EXPECT_EQ(uidsOf(left_results), std::vector<uint64_t>({1}));
  EXPECT_EQ(uidsOf(right_results), std::vector<uint64_t>({2}));
}

TEST(KnnIndexTest, BruteForceEraseRemovesOnlyThatIndexMembership) {
  auto storage = std::make_shared<StorageManager>();
  auto manager = std::make_shared<ConcurrencyManager>(storage);

  const int left_index = manager->create_index("left_local", IndexType::BruteForce, 2);
  const int right_index = manager->create_index("right_local", IndexType::BruteForce, 2);
  ASSERT_GE(left_index, 0);
  ASSERT_GE(right_index, 0);

  ASSERT_TRUE(manager->insert(left_index, createVectorRecord(1, 100, {0.0F, 0.0F})));
  ASSERT_TRUE(manager->insert(left_index, createVectorRecord(3, 100, {1.0F, 1.0F})));
  ASSERT_TRUE(manager->insert(right_index, createVectorRecord(2, 100, {0.0F, 0.0F})));
  ASSERT_TRUE(manager->erase(left_index, 1));

  const auto query = createVectorRecord(999, 200, {0.0F, 0.0F});
  auto left_results = manager->query_for_join(left_index, *query, 0.99, 0.1);
  auto right_results = manager->query_for_join(right_index, *query, 0.99, 0.1);

  EXPECT_TRUE(uidsOf(left_results).empty());
  EXPECT_EQ(uidsOf(right_results), std::vector<uint64_t>({2}));
}

}  // namespace sageFlow::test

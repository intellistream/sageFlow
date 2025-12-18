#include <gtest/gtest.h>

#include "operator/join_operator_methods/diskann.h"
#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"
#include "test_utils/test_data_adapter.h"

namespace sageFlow {
namespace test {

class DiskANNJoinMethodTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_ = std::make_shared<StorageManager>();
    concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage_);
  }

  std::shared_ptr<StorageManager> storage_;
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};

TEST_F(DiskANNJoinMethodTest, ExecuteEagerReturnsInsertedCandidate) {
  const int dim = 8;
  FreshDiskANNParameters params;
  params.L = 32;
  params.R = 16;
  params.alpha = 1.2f;

  int left_idx = concurrency_manager_->create_index("fdisk_left", IndexType::FreshDiskANN, dim, params);
  int right_idx = concurrency_manager_->create_index("fdisk_right", IndexType::FreshDiskANN, dim, params);
  ASSERT_GE(left_idx, 0);
  ASSERT_GE(right_idx, 0);

  DiskANNJoinMethod method(left_idx, right_idx, /*threshold=*/0.0, concurrency_manager_);

  // Insert one candidate on the right side
  std::vector<float> base_vec(dim, 0.5f);
  auto cand = createVectorRecord(5001, /*ts=*/1000, base_vec);
  bool inserted = concurrency_manager_->insert(right_idx, std::make_unique<VectorRecord>(*cand));
  ASSERT_TRUE(inserted);

  // Query from left side should return the inserted right record
  auto query = createVectorRecord(1, /*ts=*/1000, base_vec);
  auto results = method.ExecuteEager(*query, /*query_slot=*/0);

  ASSERT_FALSE(results.empty());
  bool found = false;
  for (auto& r : results) {
    if (r && r->uid_ == 5001) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found) << "FreshDiskANN join method did not return the inserted candidate";
}

}  // namespace test
}  // namespace sageFlow

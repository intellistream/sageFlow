#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <unordered_set>
#include <vector>

#include "common/data_types.h"
#include "concurrency/concurrency_manager.h"
#include "execution/runtime_context.h"
#include "operator/join_operator_methods/vsjoin_method.h"
#include "state/two_tier_window_state.h"
#include "storage/storage_manager.h"

namespace sageFlow {
namespace {

std::unique_ptr<VectorRecord> makeRecord(uint64_t uid, int64_t ts, int dim, float v0) {
  std::vector<float> values(static_cast<size_t>(dim), 0.0f);
  values[0] = v0;

  auto data = std::make_unique<char[]>(static_cast<size_t>(dim) * sizeof(float));
  std::memcpy(data.get(), values.data(), static_cast<size_t>(dim) * sizeof(float));

  VectorData vec_data(dim, DataType::Float32, data.release());
  return std::make_unique<VectorRecord>(uid, ts, std::move(vec_data));
}

class VSJoinMethodIntegrationTest : public ::testing::Test {
protected:
  static constexpr int kDim = 4;

  void SetUp() override {
    storage_ = std::make_shared<StorageManager>();
    cm_ = std::make_shared<ConcurrencyManager>(storage_);

    global_left_id_ = cm_->create_index("vsjoin_test_global_left", IndexType::BruteForce, kDim);
    global_right_id_ = cm_->create_index("vsjoin_test_global_right", IndexType::BruteForce, kDim);

    local_left_id_ = cm_->create_index("vsjoin_test_local_left_p0", IndexType::BruteForce, kDim);
    local_right_id_ = cm_->create_index("vsjoin_test_local_right_p0", IndexType::BruteForce, kDim);

    ASSERT_GE(global_left_id_, 0);
    ASSERT_GE(global_right_id_, 0);
    ASSERT_GE(local_left_id_, 0);
    ASSERT_GE(local_right_id_, 0);

    RuntimeContext ctx(0, 1);
    method_.initialize(ctx, cm_);

    method_.setGlobalIndexIds(global_left_id_, global_right_id_);
    method_.setLocalIndexIds({local_left_id_}, {local_right_id_});

    left_state_ = std::make_unique<TwoTierWindowState>(/*parallelism=*/1, /*compact_threshold=*/100);
    right_state_ = std::make_unique<TwoTierWindowState>(/*parallelism=*/1, /*compact_threshold=*/100);
    method_.setWindowStates(left_state_.get(), right_state_.get());
  }

  std::shared_ptr<StorageManager> storage_;
  std::shared_ptr<ConcurrencyManager> cm_;

  int global_left_id_ = -1;
  int global_right_id_ = -1;
  int local_left_id_ = -1;
  int local_right_id_ = -1;

  VSJoinMethod method_;
  std::unique_ptr<TwoTierWindowState> left_state_;
  std::unique_ptr<TwoTierWindowState> right_state_;
};

TEST_F(VSJoinMethodIntegrationTest, EmptyWhenNoIndexCandidates) {
  auto query = *makeRecord(999, 123, kDim, 0.0f);
  auto results = method_.ExecuteEager(query, /*query_slot=*/0, /*subtask_index=*/0);
  EXPECT_TRUE(results.empty());
}

TEST_F(VSJoinMethodIntegrationTest, MergeAndDedupeGlobalAndLocal) {
  // query_slot=0 => 查右侧（global_right + local_right_p0）
  // 在 global_right 插入 10/11，在 local_right 插入 11/12，窗口右侧放 10/11/12

  // 注意：ConcurrencyManager::insert 会写入 StorageManager，无需手动 storage_->insert
  ASSERT_TRUE(cm_->insert(global_right_id_, makeRecord(10, 100, kDim, 1.0f)));
  ASSERT_TRUE(cm_->insert(global_right_id_, makeRecord(11, 100, kDim, 1.0f)));
  ASSERT_TRUE(cm_->insert(local_right_id_, makeRecord(11, 100, kDim, 1.0f)));
  ASSERT_TRUE(cm_->insert(local_right_id_, makeRecord(12, 100, kDim, 1.0f)));

  right_state_->addRecord(makeRecord(10, 100, kDim, 1.0f), 0);
  right_state_->addRecord(makeRecord(11, 100, kDim, 1.0f), 0);
  right_state_->addRecord(makeRecord(12, 100, kDim, 1.0f), 0);

  auto query = *makeRecord(999, 123, kDim, 1.0f);
  auto results = method_.ExecuteEager(query, /*query_slot=*/0, /*subtask_index=*/0);

  std::unordered_set<uint64_t> uids;
  for (const auto& r : results) {
    uids.insert(r->uid_);
  }

  EXPECT_EQ(uids.size(), 3u);
  EXPECT_TRUE(uids.count(10));
  EXPECT_TRUE(uids.count(11));
  EXPECT_TRUE(uids.count(12));
}

TEST_F(VSJoinMethodIntegrationTest, FiltersExpiredUids) {
  ASSERT_TRUE(cm_->insert(global_right_id_, makeRecord(10, 1, kDim, 1.0f)));
  ASSERT_TRUE(cm_->insert(global_right_id_, makeRecord(11, 1, kDim, 1.0f)));

  right_state_->addRecord(makeRecord(10, 1, kDim, 1.0f), 0);
  right_state_->addRecord(makeRecord(11, 1, kDim, 1.0f), 0);

  // 让它们都过期
  right_state_->evictExpired(/*current_timestamp=*/1000, /*window_size=*/1, 0);

  auto query = *makeRecord(999, 123, kDim, 1.0f);
  auto results = method_.ExecuteEager(query, /*query_slot=*/0, /*subtask_index=*/0);
  EXPECT_TRUE(results.empty());
}

}  // namespace
}  // namespace sageFlow

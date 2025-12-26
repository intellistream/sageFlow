#include <gtest/gtest.h>
#include <memory>
#include <vector>

#include "operator/join_operator_methods/lsh_method.h"
#include "state/shared_window_state.h"
#include "execution/runtime_context.h"
#include "test_utils/test_data_adapter.h"

namespace sageFlow {
namespace test {

class LSHMethodTest : public ::testing::Test {
 protected:
  void SetUp() override {
    left_state_ = std::make_unique<SharedWindowState>();
    right_state_ = std::make_unique<SharedWindowState>();
    context_ = std::make_unique<RuntimeContext>(0, 1);

    LSHMethod::Config cfg;
    cfg.similarity_threshold = 0.8;
    cfg.num_tables = 2;
    cfg.num_hashes = 8;
    cfg.dimension = 4;
    cfg.seed = 7;
    method_ = std::make_unique<LSHMethod>(cfg);
    method_->open(*context_, left_state_.get(), right_state_.get());
  }

  std::unique_ptr<VectorRecord> makeRecord(uint64_t uid, const std::vector<float>& vec) {
    return createVectorRecord(uid, /*timestamp*/0, vec);
  }

  std::unique_ptr<LSHMethod> method_;
  std::unique_ptr<SharedWindowState> left_state_;
  std::unique_ptr<SharedWindowState> right_state_;
  std::unique_ptr<RuntimeContext> context_;
};

// 验证相同向量在不同 UID 下能够命中同一桶并通过相似度阈值
TEST_F(LSHMethodTest, BasicMatch) {
  auto candidate = makeRecord(1, {1.0f, 0.0f, 0.0f, 0.0f});
  method_->onRecordAdded(*candidate, /*slot=*/1);
  right_state_->addRecord(std::move(candidate), /*subtask_index=*/0);

  VectorRecord query = *makeRecord(2, {1.0f, 0.0f, 0.0f, 0.0f});
  auto results = method_->ExecuteEager(query, /*query_slot=*/0);

  ASSERT_EQ(results.size(), 1u);
  EXPECT_EQ(results.front()->uid_, 1u);
}

// 验证相似度低的向量不会通过过滤
TEST_F(LSHMethodTest, BelowThresholdNoMatch) {
  auto candidate = makeRecord(3, {1.0f, 0.0f, 0.0f, 0.0f});
  method_->onRecordAdded(*candidate, /*slot=*/1);
  right_state_->addRecord(std::move(candidate), /*subtask_index=*/0);

  VectorRecord query = *makeRecord(4, {-3.0f, 0.0f, 0.0f, 0.0f});
  auto results = method_->ExecuteEager(query, /*query_slot=*/0);

  EXPECT_TRUE(results.empty());
}

}  // namespace test
}  // namespace sageFlow

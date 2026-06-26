#include <gtest/gtest.h>

#include <cmath>
#include <memory>
#include <vector>

#include "concurrency/concurrency_manager.h"
#include "common/data_types.h"
#include "execution/collector.h"
#include "execution/runtime_context.h"
#include "operator/join_operator_components/join_result_emitter.h"
#include "operator/join_operator.h"
#include "operator/utils/join_strategy_config.h"
#include "function/join_function.h"
#include "storage/storage_manager.h"
#include "test_utils/test_data_adapter.h"

namespace sageFlow::test {

namespace {

// A concat join function mirroring the datasource test: produces a 2*dim record.
std::unique_ptr<JoinFunction> makeConcatJoinFunction(int dim) {
  auto fn = std::make_unique<JoinFunction>(
      "ConcatJoin",
      [](std::unique_ptr<VectorRecord>& left,
         std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
        auto lv = extractFloatVector(*left);
        auto rv = extractFloatVector(*right);
        std::vector<float> out;
        out.reserve(lv.size() + rv.size());
        out.insert(out.end(), lv.begin(), lv.end());
        out.insert(out.end(), rv.begin(), rv.end());
        const uint64_t id = left->uid_ * 1000000 + right->uid_ % 1000000;
        return createVectorRecord(id, std::max(left->timestamp_, right->timestamp_), out);
      },
      dim);
  return fn;
}

RecordView makeView(uint64_t uid, int64_t ts, const std::vector<float>& v) {
  // Construct a shared, immutable view (make_shared: single combined allocation).
  auto owned = createVectorRecord(uid, ts, v);
  return RecordView(std::move(owned));
}

}  // namespace

// PAIR_PASSTHROUGH: the emitter must package (left, right, similarity) as shared
// references with the SAME underlying objects (zero deep copy), oriented by slot.
TEST(JoinPairMaterializationTest, PairPassthroughCarriesSharedRefsNoCopy) {
  const int left_slot = 0;
  const int right_slot = 1;
  JoinResultEmitter emitter(nullptr, left_slot, MaterializationMode::PAIR_PASSTHROUGH);

  RecordView probe = makeView(10, 100, {1.0F, 2.0F});
  RecordView cand = makeView(20, 110, {3.0F, 4.0F});
  const VectorRecord* probe_addr = probe.get();
  const VectorRecord* cand_addr = cand.get();

  std::vector<JoinOutputItem> out;

  // Case A: probe arrives on the LEFT slot -> left=probe, right=cand.
  emitter.appendPair(probe, cand, left_slot, 0.87, out);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out[0].second.type_, ResponseType::RecordPair);
  ASSERT_NE(out[0].second.pair_, nullptr);
  EXPECT_EQ(out[0].second.pair_->left.get(), probe_addr);   // same object, not a copy
  EXPECT_EQ(out[0].second.pair_->right.get(), cand_addr);
  EXPECT_EQ(out[0].second.pair_->left->uid_, 10u);
  EXPECT_EQ(out[0].second.pair_->right->uid_, 20u);
  EXPECT_DOUBLE_EQ(out[0].second.pair_->similarity, 0.87);

  // Case B: probe arrives on the RIGHT slot -> left=cand, right=probe.
  out.clear();
  emitter.appendPair(probe, cand, right_slot, 0.5, out);
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].second.pair_->left.get(), cand_addr);
  EXPECT_EQ(out[0].second.pair_->right.get(), probe_addr);
  EXPECT_EQ(out[0].second.pair_->left->uid_, 20u);
  EXPECT_EQ(out[0].second.pair_->right->uid_, 10u);

  // The source records are still alive and unmodified (shared, not consumed).
  EXPECT_EQ(probe->uid_, 10u);
  EXPECT_EQ(cand->uid_, 20u);
}

// Zero deep copy: the underlying VectorRecord use_count rises only from shared
// references, and the data pointer is identical to the source (no char[] copy).
TEST(JoinPairMaterializationTest, PairPassthroughNoVectorDataCopy) {
  JoinResultEmitter emitter(nullptr, 0, MaterializationMode::PAIR_PASSTHROUGH);

  RecordView probe = makeView(1, 100, {1.0F, 2.0F, 3.0F});
  RecordView cand = makeView(2, 100, {4.0F, 5.0F, 6.0F});
  const char* probe_data = probe->data_.data_.get();
  const char* cand_data = cand->data_.data_.get();

  std::vector<JoinOutputItem> out;
  emitter.appendPair(probe, cand, 0, -1.0, out);

  ASSERT_EQ(out.size(), 1u);
  // Same VectorData char[] buffer => no deep copy on emit.
  EXPECT_EQ(out[0].second.pair_->left->data_.data_.get(), probe_data);
  EXPECT_EQ(out[0].second.pair_->right->data_.data_.get(), cand_data);
}

TEST(JoinPairMaterializationTest, PairPassthroughCarriesComputedSimilarity) {
  auto storage = std::make_shared<StorageManager>();
  auto concurrency = std::make_shared<ConcurrencyManager>(storage);

  std::unique_ptr<Function> join_fn = std::make_unique<JoinFunction>("PairScoreJoin", 2);
  JoinStrategyConfig config;
  config.algorithm = JoinAlgorithm::BRUTEFORCE;
  config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
  config.window_state_type = WindowStateType::SHARED;
  config.index_strategy = IndexStrategy::SHARED;
  config.materialization_mode = MaterializationMode::PAIR_PASSTHROUGH;
  config.similarity_threshold = 0.05;
  config.similarity_alpha = 0.5;
  config.dimension = 2;
  config.hdr_projected_dim = 1;
  config.window_size_ms = 1000;
  config.step_size_ms = 100;

  JoinOperator op(join_fn, concurrency, config);
  RuntimeContext ctx(0, 1);
  op.open(ctx);

  std::vector<Response> out;
  Collector collector([&](std::unique_ptr<Response> response, int) {
    out.emplace_back(std::move(*response));
  });

  op.apply(Response{ResponseType::Record, createVectorRecord(10, 100, {0.0F, 0.0F})},
           0,
           collector,
           ctx);
  op.apply(Response{ResponseType::Record, createVectorRecord(20, 100, {3.0F, 4.0F})},
           1,
           collector,
           ctx);

  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out[0].type_, ResponseType::RecordPair);
  ASSERT_NE(out[0].pair_, nullptr);
  EXPECT_EQ(out[0].pair_->left->uid_, 10u);
  EXPECT_EQ(out[0].pair_->right->uid_, 20u);
  EXPECT_NEAR(out[0].pair_->similarity, std::exp(-0.5 * 5.0), 1e-12);
}

// CONCAT mode (default) must still produce a single Record via the join function.
TEST(JoinPairMaterializationTest, ConcatModeStillProducesRecord) {
  const int dim = 2;
  auto join_fn = makeConcatJoinFunction(dim);
  JoinResultEmitter emitter(join_fn.get(), 0, MaterializationMode::CONCAT);

  auto probe = createVectorRecord(10, 100, {1.0F, 2.0F});
  auto cand = createVectorRecord(20, 110, {3.0F, 4.0F});

  std::vector<JoinOutputItem> out;
  emitter.appendJoinedResult(*probe, *cand, 0, out);

  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].second.type_, ResponseType::Record);
  ASSERT_NE(out[0].second.record_, nullptr);
  EXPECT_EQ(out[0].second.pair_, nullptr);
  // Concatenated vector has 2*dim elements.
  EXPECT_EQ(out[0].second.record_->data_.dim_, 2 * dim);
}

}  // namespace sageFlow::test

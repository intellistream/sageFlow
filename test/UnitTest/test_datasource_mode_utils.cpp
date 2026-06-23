#include <gtest/gtest.h>

#include <vector>

#include "test_utils/datasource_modes/ground_truth.h"
#include "test_utils/datasource_modes/splitter.h"
#include "test_utils/test_data_adapter.h"

namespace sageFlow {
namespace test {

TEST(DatasourceModeSplitterTest, DuplicateCopiesRightSideWithUidOffset) {
  std::vector<std::unique_ptr<VectorRecord>> base_records;
  base_records.push_back(createVectorRecord(1, 1000, {1.0F, 2.0F}));
  base_records.push_back(createVectorRecord(2, 1010, {3.0F, 4.0F}));

  auto split = splitDatasourceRecords(std::move(base_records), "duplicate");

  ASSERT_EQ(split.left.size(), 2U);
  ASSERT_EQ(split.right.size(), 2U);
  EXPECT_EQ(split.left[0]->uid_, 1U);
  EXPECT_EQ(split.right[0]->uid_, 1U + kDatasourceRightUidOffset);
  EXPECT_EQ(split.right[1]->timestamp_, split.left[1]->timestamp_);
}

TEST(DatasourceModeSplitterTest, InterleavedSplitsEvenLeftOddRight) {
  std::vector<std::unique_ptr<VectorRecord>> base_records;
  base_records.push_back(createVectorRecord(1, 1000, {1.0F}));
  base_records.push_back(createVectorRecord(2, 1010, {2.0F}));
  base_records.push_back(createVectorRecord(3, 1020, {3.0F}));

  auto split = splitDatasourceRecords(std::move(base_records), "interleaved");

  ASSERT_EQ(split.left.size(), 2U);
  ASSERT_EQ(split.right.size(), 1U);
  EXPECT_EQ(split.left[0]->uid_, 1U);
  EXPECT_EQ(split.left[1]->uid_, 3U);
  EXPECT_EQ(split.right[0]->uid_, 2U + kDatasourceRightUidOffset);
}

TEST(DatasourceModeGroundTruthTest, ComputesExpectedPairsWithWindowAndAlpha) {
  std::vector<std::unique_ptr<VectorRecord>> left_records;
  std::vector<std::unique_ptr<VectorRecord>> right_records;
  left_records.push_back(createVectorRecord(1, 1000, {1.0F, 1.0F}));
  right_records.push_back(createVectorRecord(500001, 1005, {1.0F, 1.0F}));
  right_records.push_back(createVectorRecord(500002, 3000, {1.0F, 1.0F}));

  const auto expected = computeExpectedPairsByTraversal(
      left_records,
      right_records,
      0.99,
      100,
      "fixed_alpha",
      0.1,
      kDatasourceModuloBase);

  ASSERT_EQ(expected.size(), 1U);
  EXPECT_TRUE(expected.count({1U, 500001U}) > 0);
}

TEST(DatasourceModeGroundTruthTest, NormalizedSimilarityHandlesZeroNorm) {
  const double similarity = computeDatasourceSimilarity(
      {0.0F, 0.0F}, {1.0F, 0.0F}, "normalized", 0.1);

  EXPECT_DOUBLE_EQ(similarity, 0.0);
}

}  // namespace test
}  // namespace sageFlow

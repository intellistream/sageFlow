#include <gtest/gtest.h>

#include <cmath>
#include <memory>
#include <vector>

#include "common/data_types.h"
#include "operator/join_operator_methods/vsjoin_components/distance_verifier.h"

namespace sageFlow {
namespace {

// 辅助函数：创建测试用的 VectorRecord
std::unique_ptr<VectorRecord> createTestRecord(uint64_t uid, int64_t timestamp, const std::vector<float>& values) {
  int32_t dim = static_cast<int32_t>(values.size());
  auto data = std::make_unique<char[]>(dim * sizeof(float));
  std::memcpy(data.get(), values.data(), dim * sizeof(float));
  VectorData vec_data(dim, DataType::Float32, data.release());
  return std::make_unique<VectorRecord>(uid, timestamp, std::move(vec_data));
}

// 辅助函数：计算两个向量之间的 L2 距离
double computeExpectedL2Distance(const std::vector<float>& a, const std::vector<float>& b) {
  double sum = 0.0;
  for (size_t i = 0; i < a.size(); ++i) {
    double diff = a[i] - b[i];
    sum += diff * diff;
  }
  return std::sqrt(sum);
}

// 辅助函数：计算相似度
double computeExpectedSimilarity(double distance, double alpha) { return std::exp(-alpha * distance); }

// ============================================================================
// 基本功能测试
// ============================================================================

TEST(DistanceVerifierTest, ConstructorInitializesCorrectly) {
  DistanceVerifier verifier(0.8, 0.1);

  EXPECT_DOUBLE_EQ(verifier.getThreshold(), 0.8);
  EXPECT_DOUBLE_EQ(verifier.getAlpha(), 0.1);
  EXPECT_EQ(verifier.getEarlyTerminationDims(), 0);
}

TEST(DistanceVerifierTest, DistanceToSimilarityConversion) {
  DistanceVerifier verifier(0.5, 0.1);

  // 距离为 0 时相似度应该为 1
  EXPECT_DOUBLE_EQ(verifier.distanceToSimilarity(0.0), 1.0);

  // 距离越大相似度越小
  double dist1 = 1.0;
  double dist2 = 2.0;
  EXPECT_GT(verifier.distanceToSimilarity(dist1), verifier.distanceToSimilarity(dist2));

  // 验证公式正确性 similarity = exp(-alpha * distance)
  double alpha = 0.1;
  double distance = 5.0;
  double expected = std::exp(-alpha * distance);
  EXPECT_DOUBLE_EQ(verifier.distanceToSimilarity(distance), expected);
}

TEST(DistanceVerifierTest, SimilarityToDistanceConversion) {
  DistanceVerifier verifier(0.5, 0.1);

  // 相似度为 1 时距离应该为 0
  EXPECT_DOUBLE_EQ(verifier.similarityToDistance(1.0), 0.0);

  // 验证转换的可逆性
  double original_distance = 3.5;
  double similarity = verifier.distanceToSimilarity(original_distance);
  double recovered_distance = verifier.similarityToDistance(similarity);
  EXPECT_NEAR(recovered_distance, original_distance, 1e-10);

  // 无效的相似度值
  EXPECT_EQ(verifier.similarityToDistance(0.0), std::numeric_limits<double>::max());
  EXPECT_EQ(verifier.similarityToDistance(-0.5), std::numeric_limits<double>::max());
}

// ============================================================================
// 单个候选验证测试
// ============================================================================

TEST(DistanceVerifierTest, VerifySingleCandidate_Passes) {
  DistanceVerifier verifier(0.8, 0.1);

  // 创建两个非常相似的向量（距离很小）
  std::vector<float> query_values = {1.0f, 2.0f, 3.0f, 4.0f};
  std::vector<float> candidate_values = {1.0f, 2.0f, 3.0f, 4.0f};  // 完全相同

  auto query = createTestRecord(1, 100, query_values);
  auto candidate = createTestRecord(2, 101, candidate_values);

  auto result = verifier.verify(*query, *candidate);

  EXPECT_EQ(result.candidate_uid, 2);
  EXPECT_DOUBLE_EQ(result.distance, 0.0);
  EXPECT_DOUBLE_EQ(result.similarity, 1.0);
  EXPECT_TRUE(result.passed);
}

TEST(DistanceVerifierTest, VerifySingleCandidate_Fails) {
  DistanceVerifier verifier(0.95, 0.1);  // 高阈值

  // 创建两个不同的向量
  std::vector<float> query_values = {0.0f, 0.0f, 0.0f, 0.0f};
  std::vector<float> candidate_values = {10.0f, 10.0f, 10.0f, 10.0f};

  auto query = createTestRecord(1, 100, query_values);
  auto candidate = createTestRecord(2, 101, candidate_values);

  auto result = verifier.verify(*query, *candidate);

  EXPECT_EQ(result.candidate_uid, 2);
  EXPECT_GT(result.distance, 0.0);
  EXPECT_LT(result.similarity, 0.95);
  EXPECT_FALSE(result.passed);
}

TEST(DistanceVerifierTest, VerifySingleCandidate_DistanceCalculation) {
  DistanceVerifier verifier(0.5, 0.1);

  std::vector<float> query_values = {1.0f, 2.0f, 3.0f};
  std::vector<float> candidate_values = {4.0f, 6.0f, 8.0f};

  auto query = createTestRecord(1, 100, query_values);
  auto candidate = createTestRecord(2, 101, candidate_values);

  double expected_distance = computeExpectedL2Distance(query_values, candidate_values);
  double expected_similarity = computeExpectedSimilarity(expected_distance, 0.1);

  auto result = verifier.verify(*query, *candidate);

  EXPECT_NEAR(result.distance, expected_distance, 1e-6);
  EXPECT_NEAR(result.similarity, expected_similarity, 1e-6);
  EXPECT_EQ(result.passed, expected_similarity >= 0.5);
}

// ============================================================================
// 批量验证测试
// ============================================================================

TEST(DistanceVerifierTest, BatchVerification_EmptyList) {
  DistanceVerifier verifier(0.8, 0.1);

  std::vector<float> query_values = {1.0f, 2.0f, 3.0f};
  auto query = createTestRecord(1, 100, query_values);

  std::vector<std::unique_ptr<VectorRecord>> candidates;

  auto results = verifier.verifyBatch(*query, candidates);

  EXPECT_TRUE(results.empty());
}

TEST(DistanceVerifierTest, BatchVerification_MultipleCandidates) {
  DistanceVerifier verifier(0.5, 0.1);

  std::vector<float> query_values = {1.0f, 2.0f, 3.0f};
  auto query = createTestRecord(1, 100, query_values);

  std::vector<std::unique_ptr<VectorRecord>> candidates;
  candidates.push_back(createTestRecord(2, 101, {1.0f, 2.0f, 3.0f}));  // 相同
  candidates.push_back(createTestRecord(3, 102, {1.5f, 2.5f, 3.5f}));  // 稍有不同
  candidates.push_back(createTestRecord(4, 103, {10.0f, 20.0f, 30.0f}));  // 差异很大

  auto results = verifier.verifyBatch(*query, candidates);

  EXPECT_EQ(results.size(), 3);

  // 验证每个结果
  EXPECT_EQ(results[0].candidate_uid, 2);
  EXPECT_TRUE(results[0].passed);  // 完全相同应该通过

  EXPECT_EQ(results[1].candidate_uid, 3);
  // 稍有不同，可能通过也可能不通过，取决于阈值

  EXPECT_EQ(results[2].candidate_uid, 4);
  EXPECT_FALSE(results[2].passed);  // 差异很大应该不通过
}

TEST(DistanceVerifierTest, BatchVerification_ConsistentWithSingleVerification) {
  DistanceVerifier verifier(0.6, 0.15);

  std::vector<float> query_values = {2.0f, 4.0f, 6.0f, 8.0f};
  auto query = createTestRecord(1, 100, query_values);

  std::vector<std::vector<float>> candidate_values_list = {
      {2.1f, 4.1f, 6.1f, 8.1f}, {3.0f, 5.0f, 7.0f, 9.0f}, {0.0f, 0.0f, 0.0f, 0.0f}};

  std::vector<std::unique_ptr<VectorRecord>> candidates;
  for (size_t i = 0; i < candidate_values_list.size(); ++i) {
    candidates.push_back(createTestRecord(i + 2, i + 101, candidate_values_list[i]));
  }

  // 批量验证
  auto batch_results = verifier.verifyBatch(*query, candidates);

  // 单个验证并比较
  for (size_t i = 0; i < candidate_values_list.size(); ++i) {
    auto single_candidate = createTestRecord(i + 2, i + 101, candidate_values_list[i]);
    auto single_result = verifier.verify(*query, *single_candidate);

    EXPECT_EQ(batch_results[i].candidate_uid, single_result.candidate_uid);
    EXPECT_NEAR(batch_results[i].distance, single_result.distance, 1e-10);
    EXPECT_NEAR(batch_results[i].similarity, single_result.similarity, 1e-10);
    EXPECT_EQ(batch_results[i].passed, single_result.passed);
  }
}

TEST(DistanceVerifierTest, BatchVerification_HandlesNullCandidates) {
  DistanceVerifier verifier(0.5, 0.1);

  std::vector<float> query_values = {1.0f, 2.0f, 3.0f};
  auto query = createTestRecord(1, 100, query_values);

  std::vector<std::unique_ptr<VectorRecord>> candidates;
  candidates.push_back(createTestRecord(2, 101, {1.0f, 2.0f, 3.0f}));
  candidates.push_back(nullptr);  // 空指针
  candidates.push_back(createTestRecord(4, 103, {1.5f, 2.5f, 3.5f}));

  auto results = verifier.verifyBatch(*query, candidates);

  // 空指针应该被跳过
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0].candidate_uid, 2);
  EXPECT_EQ(results[1].candidate_uid, 4);
}

// ============================================================================
// 过滤候选测试
// ============================================================================

TEST(DistanceVerifierTest, FilterCandidates_KeepsOnlyPassed) {
  DistanceVerifier verifier(0.9, 0.1);

  std::vector<float> query_values = {1.0f, 2.0f, 3.0f, 4.0f};
  auto query = createTestRecord(1, 100, query_values);

  std::vector<std::unique_ptr<VectorRecord>> candidates;
  candidates.push_back(createTestRecord(2, 101, {1.0f, 2.0f, 3.0f, 4.0f}));  // 应该通过
  candidates.push_back(createTestRecord(3, 102, {1.01f, 2.01f, 3.01f, 4.01f}));  // 应该通过（很接近）
  candidates.push_back(createTestRecord(4, 103, {100.0f, 200.0f, 300.0f, 400.0f}));  // 不应该通过

  auto filtered = verifier.filterCandidates(*query, std::move(candidates));

  // 验证结果
  EXPECT_EQ(filtered.size(), 2);

  // 检查 UID
  std::vector<uint64_t> filtered_uids;
  for (const auto& record : filtered) {
    filtered_uids.push_back(record->uid_);
  }
  EXPECT_NE(std::find(filtered_uids.begin(), filtered_uids.end(), 2), filtered_uids.end());
  EXPECT_NE(std::find(filtered_uids.begin(), filtered_uids.end(), 3), filtered_uids.end());
}

TEST(DistanceVerifierTest, FilterCandidates_MoveSemantics) {
  DistanceVerifier verifier(0.5, 0.1);

  std::vector<float> query_values = {1.0f, 2.0f, 3.0f};
  auto query = createTestRecord(1, 100, query_values);

  std::vector<std::unique_ptr<VectorRecord>> candidates;
  candidates.push_back(createTestRecord(2, 101, {1.0f, 2.0f, 3.0f}));

  size_t original_size = candidates.size();
  auto filtered = verifier.filterCandidates(*query, std::move(candidates));

  // 原始候选列表应该被移动
  EXPECT_GT(original_size, 0);
  EXPECT_FALSE(filtered.empty());
}

TEST(DistanceVerifierTest, FilterCandidates_AllPass) {
  DistanceVerifier verifier(0.1, 0.1);  // 很低的阈值

  std::vector<float> query_values = {1.0f, 2.0f, 3.0f};
  auto query = createTestRecord(1, 100, query_values);

  std::vector<std::unique_ptr<VectorRecord>> candidates;
  candidates.push_back(createTestRecord(2, 101, {1.5f, 2.5f, 3.5f}));
  candidates.push_back(createTestRecord(3, 102, {2.0f, 3.0f, 4.0f}));
  candidates.push_back(createTestRecord(4, 103, {0.5f, 1.5f, 2.5f}));

  auto filtered = verifier.filterCandidates(*query, std::move(candidates));

  EXPECT_EQ(filtered.size(), 3);
}

TEST(DistanceVerifierTest, FilterCandidates_NonePasses) {
  DistanceVerifier verifier(0.999, 0.1);  // 非常高的阈值

  std::vector<float> query_values = {0.0f, 0.0f, 0.0f};
  auto query = createTestRecord(1, 100, query_values);

  std::vector<std::unique_ptr<VectorRecord>> candidates;
  candidates.push_back(createTestRecord(2, 101, {10.0f, 10.0f, 10.0f}));
  candidates.push_back(createTestRecord(3, 102, {20.0f, 20.0f, 20.0f}));

  auto filtered = verifier.filterCandidates(*query, std::move(candidates));

  EXPECT_TRUE(filtered.empty());
}

// ============================================================================
// 早期终止测试
// ============================================================================

TEST(DistanceVerifierTest, EarlyTermination_SetAndGet) {
  DistanceVerifier verifier(0.8, 0.1);

  EXPECT_EQ(verifier.getEarlyTerminationDims(), 0);

  verifier.setEarlyTerminationDims(10);
  EXPECT_EQ(verifier.getEarlyTerminationDims(), 10);

  verifier.setEarlyTerminationDims(0);
  EXPECT_EQ(verifier.getEarlyTerminationDims(), 0);
}

TEST(DistanceVerifierTest, EarlyTermination_CorrectRejection) {
  DistanceVerifier verifier(0.95, 0.1);  // 高阈值
  verifier.setEarlyTerminationDims(4);

  // 创建维度为 128 的向量
  std::vector<float> query_values(128, 0.0f);
  std::vector<float> candidate_values(128, 10.0f);  // 每个维度差异都很大

  auto query = createTestRecord(1, 100, query_values);
  auto candidate = createTestRecord(2, 101, candidate_values);

  // 即使只检查前 4 维，也应该能正确拒绝
  auto result = verifier.verify(*query, *candidate);

  EXPECT_FALSE(result.passed);
}

TEST(DistanceVerifierTest, EarlyTermination_NoFalseRejection) {
  // 确保早期终止不会错误地拒绝应该通过的候选
  DistanceVerifier verifier_with_et(0.5, 0.1);
  verifier_with_et.setEarlyTerminationDims(4);

  DistanceVerifier verifier_without_et(0.5, 0.1);

  // 创建一个应该通过验证的向量对
  std::vector<float> query_values = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f};
  std::vector<float> candidate_values = {1.1f, 2.1f, 3.1f, 4.1f, 5.1f, 6.1f, 7.1f, 8.1f};

  auto query = createTestRecord(1, 100, query_values);
  auto candidate = createTestRecord(2, 101, candidate_values);

  auto result_with_et = verifier_with_et.verify(*query, *candidate);
  auto result_without_et = verifier_without_et.verify(*query, *candidate);

  // 两种方式的结果应该一致
  EXPECT_EQ(result_with_et.passed, result_without_et.passed);

  // 如果通过了，距离和相似度也应该相同
  if (result_with_et.passed && result_without_et.passed) {
    EXPECT_NEAR(result_with_et.distance, result_without_et.distance, 1e-10);
    EXPECT_NEAR(result_with_et.similarity, result_without_et.similarity, 1e-10);
  }
}

TEST(DistanceVerifierTest, EarlyTermination_BatchVerification) {
  DistanceVerifier verifier(0.7, 0.1);
  verifier.setEarlyTerminationDims(8);

  std::vector<float> query_values(32, 1.0f);
  auto query = createTestRecord(1, 100, query_values);

  std::vector<std::unique_ptr<VectorRecord>> candidates;

  // 添加一些相似的候选
  std::vector<float> similar_values(32, 1.1f);
  candidates.push_back(createTestRecord(2, 101, similar_values));

  // 添加一些不相似的候选
  std::vector<float> dissimilar_values(32, 100.0f);
  candidates.push_back(createTestRecord(3, 102, dissimilar_values));

  auto results = verifier.verifyBatch(*query, candidates);

  EXPECT_EQ(results.size(), 2);
  // 相似的应该通过
  EXPECT_TRUE(results[0].passed);
  // 不相似的应该不通过
  EXPECT_FALSE(results[1].passed);
}

TEST(DistanceVerifierTest, EarlyTermination_FilterCandidates) {
  DistanceVerifier verifier(0.8, 0.1);
  verifier.setEarlyTerminationDims(16);

  std::vector<float> query_values(64, 0.0f);
  auto query = createTestRecord(1, 100, query_values);

  std::vector<std::unique_ptr<VectorRecord>> candidates;

  // 非常相似的候选
  std::vector<float> very_similar(64, 0.01f);
  candidates.push_back(createTestRecord(2, 101, very_similar));

  // 非常不同的候选（应该被早期拒绝）
  std::vector<float> very_different(64, 50.0f);
  candidates.push_back(createTestRecord(3, 102, very_different));

  auto filtered = verifier.filterCandidates(*query, std::move(candidates));

  // 只有相似的候选应该通过
  EXPECT_EQ(filtered.size(), 1);
  EXPECT_EQ(filtered[0]->uid_, 2);
}

// ============================================================================
// 边界情况测试
// ============================================================================

TEST(DistanceVerifierTest, ThresholdBoundary) {
  double alpha = 0.1;
  double target_distance = 2.0;
  double threshold = std::exp(-alpha * target_distance);  // 正好在边界

  DistanceVerifier verifier(threshold, alpha);

  // 创建距离正好等于 target_distance 的向量对
  std::vector<float> query_values = {0.0f, 0.0f};
  std::vector<float> candidate_values = {std::sqrt(2.0f), std::sqrt(2.0f)};  // L2 距离 = 2.0

  auto query = createTestRecord(1, 100, query_values);
  auto candidate = createTestRecord(2, 101, candidate_values);

  auto result = verifier.verify(*query, *candidate);

  // 边界情况：相似度 >= 阈值 应该通过
  EXPECT_TRUE(result.passed);
}

TEST(DistanceVerifierTest, HighDimensionalVectors) {
  DistanceVerifier verifier(0.5, 0.1);

  // 创建 256 维的向量
  std::vector<float> query_values(256, 1.0f);
  std::vector<float> candidate_values(256, 1.5f);

  auto query = createTestRecord(1, 100, query_values);
  auto candidate = createTestRecord(2, 101, candidate_values);

  auto result = verifier.verify(*query, *candidate);

  // 验证计算完成且结果合理
  EXPECT_GT(result.distance, 0.0);
  EXPECT_LT(result.similarity, 1.0);
  EXPECT_GT(result.similarity, 0.0);
}

TEST(DistanceVerifierTest, DifferentAlphaValues) {
  std::vector<float> query_values = {0.0f, 0.0f, 0.0f};
  std::vector<float> candidate_values = {1.0f, 1.0f, 1.0f};

  auto query = createTestRecord(1, 100, query_values);
  auto candidate = createTestRecord(2, 101, candidate_values);

  // 不同的 alpha 值应该产生不同的相似度
  DistanceVerifier verifier1(0.5, 0.05);
  DistanceVerifier verifier2(0.5, 0.1);
  DistanceVerifier verifier3(0.5, 0.2);

  auto result1 = verifier1.verify(*query, *candidate);
  auto result2 = verifier2.verify(*query, *candidate);
  auto result3 = verifier3.verify(*query, *candidate);

  // 距离应该相同
  EXPECT_NEAR(result1.distance, result2.distance, 1e-10);
  EXPECT_NEAR(result2.distance, result3.distance, 1e-10);

  // alpha 越大，相似度衰减越快
  EXPECT_GT(result1.similarity, result2.similarity);
  EXPECT_GT(result2.similarity, result3.similarity);
}

}  // namespace
}  // namespace sageFlow

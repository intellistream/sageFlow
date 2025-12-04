#include <compute_engine/simd_distance.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <random>
#include <vector>

using namespace sageFlow;

/**
 * @brief 辅助函数：生成随机向量
 */
static std::vector<float> generateRandomVector(size_t dim, unsigned seed = 42) {
  std::mt19937 gen(seed);
  std::uniform_real_distribution<float> dist(-10.0f, 10.0f);
  std::vector<float> vec(dim);
  for (size_t i = 0; i < dim; ++i) {
    vec[i] = dist(gen);
  }
  return vec;
}

/**
 * @brief 辅助函数：标量参考实现 - L2 距离平方
 */
static float referenceL2DistanceSquared(const float* a, const float* b, size_t dim) {
  double sum = 0.0;
  for (size_t i = 0; i < dim; ++i) {
    double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
    sum += diff * diff;
  }
  return static_cast<float>(sum);
}

/**
 * @brief 辅助函数：标量参考实现 - 点积
 */
static float referenceDotProduct(const float* a, const float* b, size_t dim) {
  double sum = 0.0;
  for (size_t i = 0; i < dim; ++i) {
    sum += static_cast<double>(a[i]) * static_cast<double>(b[i]);
  }
  return static_cast<float>(sum);
}

/**
 * @brief 辅助函数：标量参考实现 - 余弦相似度
 */
static float referenceCosineSimilarity(const float* a, const float* b, size_t dim) {
  float dot = referenceDotProduct(a, b, dim);
  float norm_a = std::sqrt(referenceDotProduct(a, a, dim));
  float norm_b = std::sqrt(referenceDotProduct(b, b, dim));
  if (norm_a < 1e-10f || norm_b < 1e-10f) {
    return 0.0f;
  }
  return dot / (norm_a * norm_b);
}

// ============================================================================
// SIMD 检测测试
// ============================================================================

TEST(SIMDDistanceTest, DetectSIMDLevel) {
  SIMDDistance::SIMDLevel level = SIMDDistance::detectSIMDLevel();
  std::cout << "Detected SIMD Level: " << SIMDDistance::simdLevelToString(level) << std::endl;

  // 至少应该检测到某个级别（可能是 NONE）
  EXPECT_TRUE(level >= SIMDDistance::SIMDLevel::NONE);
  EXPECT_TRUE(level <= SIMDDistance::SIMDLevel::AVX512);
}

TEST(SIMDDistanceTest, SIMDLevelToString) {
  EXPECT_STREQ(SIMDDistance::simdLevelToString(SIMDDistance::SIMDLevel::NONE), "None (Scalar)");
  EXPECT_STREQ(SIMDDistance::simdLevelToString(SIMDDistance::SIMDLevel::SSE), "SSE");
  EXPECT_STREQ(SIMDDistance::simdLevelToString(SIMDDistance::SIMDLevel::AVX), "AVX");
  EXPECT_STREQ(SIMDDistance::simdLevelToString(SIMDDistance::SIMDLevel::AVX2), "AVX2");
  EXPECT_STREQ(SIMDDistance::simdLevelToString(SIMDDistance::SIMDLevel::AVX512), "AVX-512");
}

// ============================================================================
// L2 距离正确性测试
// ============================================================================

TEST(SIMDDistanceTest, L2DistanceZeroDimension) {
  float a[] = {};
  float b[] = {};
  EXPECT_FLOAT_EQ(SIMDDistance::l2Distance(a, b, 0), 0.0f);
  EXPECT_FLOAT_EQ(SIMDDistance::l2DistanceSquared(a, b, 0), 0.0f);
}

TEST(SIMDDistanceTest, L2DistanceIdenticalVectors) {
  std::vector<float> vec = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
  EXPECT_FLOAT_EQ(SIMDDistance::l2Distance(vec.data(), vec.data(), vec.size()), 0.0f);
}

TEST(SIMDDistanceTest, L2DistanceKnownValues) {
  // 3-4-5 三角形
  std::vector<float> a = {0.0f, 0.0f};
  std::vector<float> b = {3.0f, 4.0f};
  EXPECT_FLOAT_EQ(SIMDDistance::l2Distance(a.data(), b.data(), 2), 5.0f);
  EXPECT_FLOAT_EQ(SIMDDistance::l2DistanceSquared(a.data(), b.data(), 2), 25.0f);
}

TEST(SIMDDistanceTest, L2DistanceCorrectnessSmallDim) {
  // 测试小维度（可能不是 SIMD 寄存器大小的倍数）
  for (size_t dim = 1; dim <= 16; ++dim) {
    auto vec_a = generateRandomVector(dim, 100 + dim);
    auto vec_b = generateRandomVector(dim, 200 + dim);

    float expected = referenceL2DistanceSquared(vec_a.data(), vec_b.data(), dim);
    float actual = SIMDDistance::l2DistanceSquared(vec_a.data(), vec_b.data(), dim);

    EXPECT_NEAR(actual, expected, 1e-4f) << "Failed at dim=" << dim;
  }
}

TEST(SIMDDistanceTest, L2DistanceCorrectnessLargeDim) {
  // 测试大维度（常见的向量嵌入维度）
  std::vector<size_t> dims = {32, 64, 128, 256, 512, 768, 1024, 1536};

  for (size_t dim : dims) {
    auto vec_a = generateRandomVector(dim, 300 + dim);
    auto vec_b = generateRandomVector(dim, 400 + dim);

    float expected = referenceL2DistanceSquared(vec_a.data(), vec_b.data(), dim);
    float actual = SIMDDistance::l2DistanceSquared(vec_a.data(), vec_b.data(), dim);

    // 对于大维度，允许稍大的误差
    float tolerance = expected * 1e-5f + 1e-4f;
    EXPECT_NEAR(actual, expected, tolerance) << "Failed at dim=" << dim;
  }
}

TEST(SIMDDistanceTest, L2DistanceNonAlignedDim) {
  // 测试非对齐维度（不是 4、8、16 的倍数）
  std::vector<size_t> dims = {3, 5, 7, 9, 11, 13, 15, 17, 33, 65, 127, 129};

  for (size_t dim : dims) {
    auto vec_a = generateRandomVector(dim, 500 + dim);
    auto vec_b = generateRandomVector(dim, 600 + dim);

    float expected = referenceL2DistanceSquared(vec_a.data(), vec_b.data(), dim);
    float actual = SIMDDistance::l2DistanceSquared(vec_a.data(), vec_b.data(), dim);

    float tolerance = expected * 1e-5f + 1e-4f;
    EXPECT_NEAR(actual, expected, tolerance) << "Failed at dim=" << dim;
  }
}

// ============================================================================
// 余弦相似度正确性测试
// ============================================================================

TEST(SIMDDistanceTest, CosineSimilarityZeroDimension) {
  float a[] = {};
  float b[] = {};
  EXPECT_FLOAT_EQ(SIMDDistance::cosineSimilarity(a, b, 0), 0.0f);
}

TEST(SIMDDistanceTest, CosineSimilarityIdenticalVectors) {
  std::vector<float> vec = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
  EXPECT_NEAR(SIMDDistance::cosineSimilarity(vec.data(), vec.data(), vec.size()), 1.0f, 1e-6f);
}

TEST(SIMDDistanceTest, CosineSimilarityOppositeVectors) {
  std::vector<float> a = {1.0f, 2.0f, 3.0f};
  std::vector<float> b = {-1.0f, -2.0f, -3.0f};
  EXPECT_NEAR(SIMDDistance::cosineSimilarity(a.data(), b.data(), 3), -1.0f, 1e-6f);
}

TEST(SIMDDistanceTest, CosineSimilarityOrthogonalVectors) {
  std::vector<float> a = {1.0f, 0.0f, 0.0f};
  std::vector<float> b = {0.0f, 1.0f, 0.0f};
  EXPECT_NEAR(SIMDDistance::cosineSimilarity(a.data(), b.data(), 3), 0.0f, 1e-6f);
}

TEST(SIMDDistanceTest, CosineSimilarityZeroVector) {
  std::vector<float> a = {1.0f, 2.0f, 3.0f};
  std::vector<float> b = {0.0f, 0.0f, 0.0f};
  EXPECT_FLOAT_EQ(SIMDDistance::cosineSimilarity(a.data(), b.data(), 3), 0.0f);
}

TEST(SIMDDistanceTest, CosineSimilarityCorrectness) {
  std::vector<size_t> dims = {3, 8, 16, 32, 64, 128, 256};

  for (size_t dim : dims) {
    auto vec_a = generateRandomVector(dim, 700 + dim);
    auto vec_b = generateRandomVector(dim, 800 + dim);

    float expected = referenceCosineSimilarity(vec_a.data(), vec_b.data(), dim);
    float actual = SIMDDistance::cosineSimilarity(vec_a.data(), vec_b.data(), dim);

    EXPECT_NEAR(actual, expected, 1e-5f) << "Failed at dim=" << dim;
  }
}

// ============================================================================
// 点积正确性测试
// ============================================================================

TEST(SIMDDistanceTest, DotProductCorrectness) {
  std::vector<size_t> dims = {1, 2, 3, 4, 5, 7, 8, 15, 16, 17, 32, 64, 128};

  for (size_t dim : dims) {
    auto vec_a = generateRandomVector(dim, 900 + dim);
    auto vec_b = generateRandomVector(dim, 1000 + dim);

    float expected = referenceDotProduct(vec_a.data(), vec_b.data(), dim);
    float actual = SIMDDistance::dotProduct(vec_a.data(), vec_b.data(), dim);

    float tolerance = std::abs(expected) * 1e-5f + 1e-4f;
    EXPECT_NEAR(actual, expected, tolerance) << "Failed at dim=" << dim;
  }
}

// ============================================================================
// 向量模长测试
// ============================================================================

TEST(SIMDDistanceTest, VectorNormCorrectness) {
  std::vector<float> vec = {3.0f, 4.0f};
  EXPECT_FLOAT_EQ(SIMDDistance::vectorNorm(vec.data(), 2), 5.0f);

  std::vector<float> unit = {1.0f, 0.0f, 0.0f};
  EXPECT_FLOAT_EQ(SIMDDistance::vectorNorm(unit.data(), 3), 1.0f);
}

// ============================================================================
// 批量计算测试
// ============================================================================

TEST(SIMDDistanceTest, BatchL2Distance) {
  const size_t dim = 128;
  const size_t num_candidates = 100;

  auto query = generateRandomVector(dim, 1100);

  std::vector<std::vector<float>> candidates_storage(num_candidates);
  std::vector<const float*> candidates(num_candidates);

  for (size_t i = 0; i < num_candidates; ++i) {
    candidates_storage[i] = generateRandomVector(dim, 1200 + i);
    candidates[i] = candidates_storage[i].data();
  }

  std::vector<float> results(num_candidates);
  SIMDDistance::l2DistanceBatch(query.data(), candidates.data(), num_candidates, dim, results.data());

  // 验证批量结果与单独计算一致
  for (size_t i = 0; i < num_candidates; ++i) {
    float expected = SIMDDistance::l2Distance(query.data(), candidates[i], dim);
    EXPECT_FLOAT_EQ(results[i], expected) << "Mismatch at candidate " << i;
  }
}

TEST(SIMDDistanceTest, BatchCosineSimilarity) {
  const size_t dim = 128;
  const size_t num_candidates = 100;

  auto query = generateRandomVector(dim, 1300);

  std::vector<std::vector<float>> candidates_storage(num_candidates);
  std::vector<const float*> candidates(num_candidates);

  for (size_t i = 0; i < num_candidates; ++i) {
    candidates_storage[i] = generateRandomVector(dim, 1400 + i);
    candidates[i] = candidates_storage[i].data();
  }

  std::vector<float> results(num_candidates);
  SIMDDistance::cosineSimilarityBatch(query.data(), candidates.data(), num_candidates, dim, results.data());

  // 验证批量结果与单独计算一致
  for (size_t i = 0; i < num_candidates; ++i) {
    float expected = SIMDDistance::cosineSimilarity(query.data(), candidates[i], dim);
    EXPECT_NEAR(results[i], expected, 1e-6f) << "Mismatch at candidate " << i;
  }
}

// ============================================================================
// 性能基准测试
// ============================================================================

class SIMDDistancePerformanceTest : public ::testing::Test {
 protected:
  static constexpr size_t kDim = 128;
  static constexpr size_t kNumVectors = 10000;
  static constexpr int kNumIterations = 100;

  std::vector<float> query_;
  std::vector<std::vector<float>> vectors_;

  void SetUp() override {
    query_ = generateRandomVector(kDim, 2000);
    vectors_.resize(kNumVectors);
    for (size_t i = 0; i < kNumVectors; ++i) {
      vectors_[i] = generateRandomVector(kDim, 3000 + i);
    }
  }
};

TEST_F(SIMDDistancePerformanceTest, L2DistancePerformance) {
  // 热身
  float dummy = 0.0f;
  for (size_t i = 0; i < 100; ++i) {
    dummy += SIMDDistance::l2Distance(query_.data(), vectors_[i].data(), kDim);
  }
  (void)dummy;  // 防止被优化掉

  auto start = std::chrono::high_resolution_clock::now();

  float total = 0.0f;
  for (int iter = 0; iter < kNumIterations; ++iter) {
    for (size_t i = 0; i < kNumVectors; ++i) {
      total += SIMDDistance::l2Distance(query_.data(), vectors_[i].data(), kDim);
    }
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  double total_ops = static_cast<double>(kNumIterations) * kNumVectors;
  double ops_per_sec = total_ops / (duration.count() / 1e6);

  std::cout << "L2 Distance Performance:" << std::endl;
  std::cout << "  SIMD Level: " << SIMDDistance::simdLevelToString(SIMDDistance::detectSIMDLevel()) << std::endl;
  std::cout << "  Total time: " << duration.count() / 1000.0 << " ms" << std::endl;
  std::cout << "  Operations: " << total_ops << std::endl;
  std::cout << "  Throughput: " << ops_per_sec / 1e6 << " M ops/sec" << std::endl;
  std::cout << "  (Result sum for validation: " << total << ")" << std::endl;

  // 确保测试实际执行了
  EXPECT_GT(total, 0.0f);
}

TEST_F(SIMDDistancePerformanceTest, CosineSimilarityPerformance) {
  // 热身
  float dummy = 0.0f;
  for (size_t i = 0; i < 100; ++i) {
    dummy += SIMDDistance::cosineSimilarity(query_.data(), vectors_[i].data(), kDim);
  }
  (void)dummy;  // 防止被优化掉

  auto start = std::chrono::high_resolution_clock::now();

  float total = 0.0f;
  for (int iter = 0; iter < kNumIterations; ++iter) {
    for (size_t i = 0; i < kNumVectors; ++i) {
      total += SIMDDistance::cosineSimilarity(query_.data(), vectors_[i].data(), kDim);
    }
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  double total_ops = static_cast<double>(kNumIterations) * kNumVectors;
  double ops_per_sec = total_ops / (duration.count() / 1e6);

  std::cout << "Cosine Similarity Performance:" << std::endl;
  std::cout << "  SIMD Level: " << SIMDDistance::simdLevelToString(SIMDDistance::detectSIMDLevel()) << std::endl;
  std::cout << "  Total time: " << duration.count() / 1000.0 << " ms" << std::endl;
  std::cout << "  Operations: " << total_ops << std::endl;
  std::cout << "  Throughput: " << ops_per_sec / 1e6 << " M ops/sec" << std::endl;

  EXPECT_TRUE(true);  // 性能测试不设置失败条件
}

// ============================================================================
// 边界条件测试
// ============================================================================

TEST(SIMDDistanceTest, LargeValues) {
  std::vector<float> a = {1e10f, 1e10f, 1e10f, 1e10f};
  std::vector<float> b = {-1e10f, -1e10f, -1e10f, -1e10f};

  float dist_sq = SIMDDistance::l2DistanceSquared(a.data(), b.data(), 4);
  EXPECT_GT(dist_sq, 0.0f);
  EXPECT_FALSE(std::isinf(dist_sq));
}

TEST(SIMDDistanceTest, SmallValues) {
  std::vector<float> a = {1e-10f, 1e-10f, 1e-10f, 1e-10f};
  std::vector<float> b = {2e-10f, 2e-10f, 2e-10f, 2e-10f};

  float dist = SIMDDistance::l2Distance(a.data(), b.data(), 4);
  EXPECT_GT(dist, 0.0f);
  EXPECT_FALSE(std::isnan(dist));
}

TEST(SIMDDistanceTest, MixedSignValues) {
  std::vector<float> a = {-5.0f, 3.0f, -2.0f, 7.0f, -1.0f, 4.0f, -8.0f, 6.0f};
  std::vector<float> b = {2.0f, -4.0f, 5.0f, -1.0f, 8.0f, -3.0f, 2.0f, -5.0f};

  float expected = referenceL2DistanceSquared(a.data(), b.data(), 8);
  float actual = SIMDDistance::l2DistanceSquared(a.data(), b.data(), 8);

  EXPECT_NEAR(actual, expected, 1e-4f);
}

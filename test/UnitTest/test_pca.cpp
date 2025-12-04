// test_pca.cpp
#include <gtest/gtest.h>

#include <cmath>
#include <random>
#include <vector>

#include "compute_engine/pca.h"

namespace sageFlow {
namespace {

// 辅助函数：计算两个向量的欧氏距离
auto euclideanDistance(const std::vector<float>& v1, const std::vector<float>& v2) -> float {
  float sum = 0.0F;
  for (size_t i = 0; i < v1.size(); ++i) {
    float diff = v1[i] - v2[i];
    sum += diff * diff;
  }
  return std::sqrt(sum);
}

// 辅助函数：生成随机向量
auto generateRandomVectors(int n_samples, int dim, unsigned int seed = 42)
    -> std::vector<std::vector<float>> {
  std::mt19937 gen(seed);
  std::normal_distribution<float> dist(0.0F, 1.0F);

  std::vector<std::vector<float>> vectors(n_samples, std::vector<float>(dim));
  for (int i = 0; i < n_samples; ++i) {
    for (int j = 0; j < dim; ++j) {
      vectors[i][j] = dist(gen);
    }
  }
  return vectors;
}

// 辅助函数：生成具有结构的测试数据（前几个维度有较大方差）
auto generateStructuredVectors(int n_samples, int dim, int n_significant_dims, unsigned int seed = 42)
    -> std::vector<std::vector<float>> {
  std::mt19937 gen(seed);
  std::normal_distribution<float> high_var_dist(0.0F, 10.0F);
  std::normal_distribution<float> low_var_dist(0.0F, 0.1F);

  std::vector<std::vector<float>> vectors(n_samples, std::vector<float>(dim));
  for (int i = 0; i < n_samples; ++i) {
    for (int j = 0; j < dim; ++j) {
      if (j < n_significant_dims) {
        vectors[i][j] = high_var_dist(gen);
      } else {
        vectors[i][j] = low_var_dist(gen);
      }
    }
  }
  return vectors;
}

// ==================== 构造函数测试 ====================

TEST(PCATest, ConstructorValidDimensions) {
  EXPECT_NO_THROW(PCA(128, 32));
  EXPECT_NO_THROW(PCA(10, 10));
  EXPECT_NO_THROW(PCA(100, 1));
}

TEST(PCATest, ConstructorInvalidDimensions) {
  // target_dim > original_dim
  EXPECT_THROW(PCA(32, 128), std::invalid_argument);

  // 零或负数维度
  EXPECT_THROW(PCA(0, 10), std::invalid_argument);
  EXPECT_THROW(PCA(10, 0), std::invalid_argument);
  EXPECT_THROW(PCA(-1, 10), std::invalid_argument);
  EXPECT_THROW(PCA(10, -1), std::invalid_argument);
}

// ==================== Fit 测试 ====================

TEST(PCATest, FitWithValidData) {
  constexpr int kOriginalDim = 64;
  constexpr int kTargetDim = 16;
  constexpr int kNSamples = 100;

  PCA pca(kOriginalDim, kTargetDim);
  auto samples = generateRandomVectors(kNSamples, kOriginalDim);

  EXPECT_FALSE(pca.isFitted());
  EXPECT_NO_THROW(pca.fit(samples));
  EXPECT_TRUE(pca.isFitted());
}

TEST(PCATest, FitWithEmptyData) {
  PCA pca(64, 16);
  std::vector<std::vector<float>> empty_samples;

  EXPECT_THROW(pca.fit(empty_samples), std::invalid_argument);
}

TEST(PCATest, FitWithDimensionMismatch) {
  PCA pca(64, 16);
  // 创建维度不匹配的样本
  std::vector<std::vector<float>> samples = {{1.0F, 2.0F, 3.0F}};  // 只有3维

  EXPECT_THROW(pca.fit(samples), std::invalid_argument);
}

TEST(PCATest, FitWithInsufficientSamples) {
  constexpr int kOriginalDim = 64;
  constexpr int kTargetDim = 16;

  PCA pca(kOriginalDim, kTargetDim);
  // 样本数小于目标维度
  auto samples = generateRandomVectors(10, kOriginalDim);  // 10 < 16

  EXPECT_THROW(pca.fit(samples), std::invalid_argument);
}

// ==================== Transform 测试 ====================

TEST(PCATest, TransformOutputDimension) {
  constexpr int kOriginalDim = 128;
  constexpr int kTargetDim = 32;
  constexpr int kNSamples = 200;

  PCA pca(kOriginalDim, kTargetDim);
  auto samples = generateRandomVectors(kNSamples, kOriginalDim);
  pca.fit(samples);

  auto test_vector = generateRandomVectors(1, kOriginalDim)[0];
  auto transformed = pca.transform(test_vector);

  EXPECT_EQ(static_cast<int>(transformed.size()), kTargetDim);
}

TEST(PCATest, TransformWithoutFit) {
  PCA pca(64, 16);
  std::vector<float> test_vector(64, 1.0F);

  EXPECT_THROW(pca.transform(test_vector), std::runtime_error);
}

TEST(PCATest, TransformDimensionMismatch) {
  constexpr int kOriginalDim = 64;
  constexpr int kTargetDim = 16;

  PCA pca(kOriginalDim, kTargetDim);
  auto samples = generateRandomVectors(100, kOriginalDim);
  pca.fit(samples);

  std::vector<float> wrong_dim_vector(32, 1.0F);  // 错误的维度
  EXPECT_THROW(pca.transform(wrong_dim_vector), std::invalid_argument);
}

// ==================== TransformBatch 测试 ====================

TEST(PCATest, TransformBatch) {
  constexpr int kOriginalDim = 64;
  constexpr int kTargetDim = 16;
  constexpr int kNSamples = 100;
  constexpr int kNTestVectors = 50;

  PCA pca(kOriginalDim, kTargetDim);
  auto samples = generateRandomVectors(kNSamples, kOriginalDim);
  pca.fit(samples);

  auto test_vectors = generateRandomVectors(kNTestVectors, kOriginalDim, 123);
  auto transformed = pca.transformBatch(test_vectors);

  EXPECT_EQ(transformed.size(), static_cast<size_t>(kNTestVectors));
  for (const auto& vec : transformed) {
    EXPECT_EQ(static_cast<int>(vec.size()), kTargetDim);
  }
}

// ==================== 距离下界性质测试 ====================

TEST(PCATest, DistanceLowerBound) {
  // 验证 PCA 距离下界性质: ||P*x - P*y|| <= ||x - y||
  constexpr int kOriginalDim = 128;
  constexpr int kTargetDim = 32;
  constexpr int kNSamples = 500;
  constexpr int kNTestPairs = 100;

  PCA pca(kOriginalDim, kTargetDim);
  auto samples = generateRandomVectors(kNSamples, kOriginalDim);
  pca.fit(samples);

  auto test_vectors = generateRandomVectors(kNTestPairs * 2, kOriginalDim, 456);

  int violations = 0;
  constexpr float kTolerance = 1e-4F;  // 允许小的数值误差

  for (int i = 0; i < kNTestPairs; ++i) {
    const auto& x = test_vectors[i * 2];
    const auto& y = test_vectors[i * 2 + 1];

    float original_dist = euclideanDistance(x, y);

    auto px = pca.transform(x);
    auto py = pca.transform(y);
    float projected_dist = euclideanDistance(px, py);

    // 投影后的距离应该小于等于原始距离
    if (projected_dist > original_dist + kTolerance) {
      violations++;
    }
  }

  // 由于数值精度问题，允许极少数违规
  EXPECT_LE(violations, 2) << "Too many distance lower bound violations";
}

// ==================== 解释方差测试 ====================

TEST(PCATest, ExplainedVarianceRatio) {
  constexpr int kOriginalDim = 64;
  constexpr int kTargetDim = 16;
  constexpr int kNSamples = 200;

  PCA pca(kOriginalDim, kTargetDim);
  auto samples = generateRandomVectors(kNSamples, kOriginalDim);
  pca.fit(samples);

  auto ratio = pca.getExplainedVarianceRatio();

  EXPECT_EQ(static_cast<int>(ratio.size()), kTargetDim);

  // 解释方差比例应该是非负的
  for (float r : ratio) {
    EXPECT_GE(r, 0.0F);
  }

  // 解释方差比例之和应该等于 1（针对提取的主成分）
  float sum = 0.0F;
  for (float r : ratio) {
    sum += r;
  }
  EXPECT_NEAR(sum, 1.0F, 1e-5F);
}

TEST(PCATest, ExplainedVarianceDecreasing) {
  // 对于结构化数据，解释方差应该大致递减
  constexpr int kOriginalDim = 64;
  constexpr int kTargetDim = 16;
  constexpr int kNSamples = 500;
  constexpr int kNSignificantDims = 8;

  PCA pca(kOriginalDim, kTargetDim);
  auto samples = generateStructuredVectors(kNSamples, kOriginalDim, kNSignificantDims);
  pca.fit(samples);

  auto ratio = pca.getExplainedVarianceRatio();

  // 前几个主成分应该解释大部分方差
  float top_half_ratio = 0.0F;
  for (int i = 0; i < kTargetDim / 2; ++i) {
    top_half_ratio += ratio[i];
  }

  EXPECT_GT(top_half_ratio, 0.5F) << "Top half of components should explain majority of variance";
}

TEST(PCATest, ExplainedVarianceWithoutFit) {
  PCA pca(64, 16);
  EXPECT_THROW((void)pca.getExplainedVarianceRatio(), std::runtime_error);
}

// ==================== 主成分矩阵测试 ====================

TEST(PCATest, ComponentsOrthogonality) {
  constexpr int kOriginalDim = 64;
  constexpr int kTargetDim = 8;
  constexpr int kNSamples = 200;

  PCA pca(kOriginalDim, kTargetDim);
  auto samples = generateRandomVectors(kNSamples, kOriginalDim);
  pca.fit(samples);

  const auto& components = pca.getComponents();

  EXPECT_EQ(static_cast<int>(components.size()), kTargetDim);

  // 验证主成分是单位向量且相互正交
  constexpr float kTolerance = 1e-4F;

  for (int i = 0; i < kTargetDim; ++i) {
    // 检查单位向量
    float norm = 0.0F;
    for (float v : components[i]) {
      norm += v * v;
    }
    norm = std::sqrt(norm);
    EXPECT_NEAR(norm, 1.0F, kTolerance) << "Component " << i << " is not unit vector";

    // 检查正交性
    for (int j = i + 1; j < kTargetDim; ++j) {
      float dot = 0.0F;
      for (size_t k = 0; k < components[i].size(); ++k) {
        dot += components[i][k] * components[j][k];
      }
      EXPECT_NEAR(dot, 0.0F, kTolerance)
          << "Components " << i << " and " << j << " are not orthogonal";
    }
  }
}

// ==================== Mean 测试 ====================

TEST(PCATest, MeanComputation) {
  constexpr int kOriginalDim = 4;
  constexpr int kTargetDim = 2;

  // 创建已知均值的数据
  std::vector<std::vector<float>> samples = {
      {1.0F, 2.0F, 3.0F, 4.0F}, {3.0F, 4.0F, 5.0F, 6.0F}, {5.0F, 6.0F, 7.0F, 8.0F}};

  PCA pca(kOriginalDim, kTargetDim);
  pca.fit(samples);

  auto mean = pca.getMean();
  EXPECT_EQ(static_cast<int>(mean.size()), kOriginalDim);

  // 期望均值: (1+3+5)/3=3, (2+4+6)/3=4, (3+5+7)/3=5, (4+6+8)/3=6
  EXPECT_NEAR(mean[0], 3.0F, 1e-5F);
  EXPECT_NEAR(mean[1], 4.0F, 1e-5F);
  EXPECT_NEAR(mean[2], 5.0F, 1e-5F);
  EXPECT_NEAR(mean[3], 6.0F, 1e-5F);
}

// ==================== 性能测试 ====================

TEST(PCATest, PerformanceFit1000Samples128Dim) {
  constexpr int kOriginalDim = 128;
  constexpr int kTargetDim = 32;
  constexpr int kNSamples = 1000;

  PCA pca(kOriginalDim, kTargetDim);
  auto samples = generateRandomVectors(kNSamples, kOriginalDim);

  auto start = std::chrono::high_resolution_clock::now();
  pca.fit(samples);
  auto end = std::chrono::high_resolution_clock::now();

  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  // 要求 1000 样本 128 维 < 1s
  EXPECT_LT(duration.count(), 1000) << "PCA fit took too long: " << duration.count() << "ms";
}

TEST(PCATest, PerformanceTransformBatch) {
  constexpr int kOriginalDim = 128;
  constexpr int kTargetDim = 32;
  constexpr int kNSamples = 1000;
  constexpr int kNTestVectors = 10000;

  PCA pca(kOriginalDim, kTargetDim);
  auto samples = generateRandomVectors(kNSamples, kOriginalDim);
  pca.fit(samples);

  auto test_vectors = generateRandomVectors(kNTestVectors, kOriginalDim, 789);

  auto start = std::chrono::high_resolution_clock::now();
  auto transformed = pca.transformBatch(test_vectors);
  auto end = std::chrono::high_resolution_clock::now();

  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  // 验证结果正确
  EXPECT_EQ(transformed.size(), static_cast<size_t>(kNTestVectors));

  // 10000 向量变换应该在合理时间内完成
  EXPECT_LT(duration.count(), 500) << "Batch transform took too long: " << duration.count() << "ms";
}

// ==================== 边界情况测试 ====================

TEST(PCATest, SingleComponent) {
  constexpr int kOriginalDim = 64;
  constexpr int kTargetDim = 1;
  constexpr int kNSamples = 100;

  PCA pca(kOriginalDim, kTargetDim);
  auto samples = generateRandomVectors(kNSamples, kOriginalDim);
  pca.fit(samples);

  auto test_vector = generateRandomVectors(1, kOriginalDim)[0];
  auto transformed = pca.transform(test_vector);

  EXPECT_EQ(static_cast<int>(transformed.size()), 1);
}

TEST(PCATest, SameDimensionTransform) {
  constexpr int kDim = 32;
  constexpr int kNSamples = 100;

  PCA pca(kDim, kDim);
  auto samples = generateRandomVectors(kNSamples, kDim);
  pca.fit(samples);

  auto test_vector = generateRandomVectors(1, kDim)[0];
  auto transformed = pca.transform(test_vector);

  EXPECT_EQ(static_cast<int>(transformed.size()), kDim);
}

TEST(PCATest, Getters) {
  constexpr int kOriginalDim = 64;
  constexpr int kTargetDim = 16;

  PCA pca(kOriginalDim, kTargetDim);

  EXPECT_EQ(pca.getOriginalDim(), kOriginalDim);
  EXPECT_EQ(pca.getTargetDim(), kTargetDim);
  EXPECT_FALSE(pca.isFitted());

  auto samples = generateRandomVectors(100, kOriginalDim);
  pca.fit(samples);

  EXPECT_TRUE(pca.isFitted());
  EXPECT_EQ(static_cast<int>(pca.getComponents().size()), kTargetDim);
  EXPECT_EQ(static_cast<int>(pca.getMean().size()), kOriginalDim);
}

}  // namespace
}  // namespace sageFlow

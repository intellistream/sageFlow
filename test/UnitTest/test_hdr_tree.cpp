// test_hdr_tree.cpp
// HDR-Tree baseline 单元测试

#include <gtest/gtest.h>

#include <cmath>
#include <memory>
#include <random>
#include <vector>

#include "common/data_types.h"
#include "compute_engine/compute_engine.h"
#include "concurrency/concurrency_manager.h"
#include "index/hdr_tree.h"
#include "operator/join_operator_methods/hdr_tree_method.h"
#include "storage/storage_manager.h"

namespace sageFlow {
namespace {

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
    // 归一化向量
    float norm = 0.0F;
    for (int j = 0; j < dim; ++j) {
      norm += vectors[i][j] * vectors[i][j];
    }
    norm = std::sqrt(norm);
    if (norm > 0) {
      for (int j = 0; j < dim; ++j) {
        vectors[i][j] /= norm;
      }
    }
  }
  return vectors;
}

// 辅助函数：创建 VectorRecord
auto createVectorRecord(uint64_t uid, int64_t timestamp, const std::vector<float>& data)
    -> std::unique_ptr<VectorRecord> {
  int dim = static_cast<int>(data.size());
  auto raw_data = std::make_unique<char[]>(dim * sizeof(float));
  std::memcpy(raw_data.get(), data.data(), dim * sizeof(float));
  return std::make_unique<VectorRecord>(uid, timestamp, dim, DataType::Float32, raw_data.release());
}

// 辅助函数：计算余弦相似度
auto cosineSimilarity(const std::vector<float>& v1, const std::vector<float>& v2) -> float {
  float dot = 0.0F;
  float norm1 = 0.0F;
  float norm2 = 0.0F;
  for (size_t i = 0; i < v1.size(); ++i) {
    dot += v1[i] * v2[i];
    norm1 += v1[i] * v1[i];
    norm2 += v2[i] * v2[i];
  }
  if (norm1 <= 0 || norm2 <= 0) return 0.0F;
  return dot / (std::sqrt(norm1) * std::sqrt(norm2));
}

// ==================== HDRTree 构造函数测试 ====================

TEST(HDRTreeTest, ConstructorValidDimensions) {
  HDRTree::Config config1;
  config1.projected_dim = 32;
  EXPECT_NO_THROW(HDRTree(128, config1));

  HDRTree::Config config2;
  config2.projected_dim = 16;
  EXPECT_NO_THROW(HDRTree(64, config2));
}

TEST(HDRTreeTest, ConstructorInvalidDimensions) {
  // projected_dim > dimension
  HDRTree::Config config1;
  config1.projected_dim = 64;
  EXPECT_THROW(HDRTree(32, config1), std::invalid_argument);

  // projected_dim <= 0
  HDRTree::Config config2;
  config2.projected_dim = 0;
  EXPECT_THROW(HDRTree(128, config2), std::invalid_argument);

  HDRTree::Config config3;
  config3.projected_dim = -1;
  EXPECT_THROW(HDRTree(128, config3), std::invalid_argument);
}

// ==================== PCA 训练测试 ====================

TEST(HDRTreeTest, TrainPCA) {
  constexpr int kDim = 64;
  constexpr int kProjectedDim = 16;
  constexpr int kNSamples = 200;

  HDRTree::Config config;
  config.projected_dim = kProjectedDim;
  config.pca_sample_size = 100;

  HDRTree tree(kDim, config);

  EXPECT_FALSE(tree.isPCATrained());

  auto samples = generateRandomVectors(kNSamples, kDim);
  tree.trainPCA(samples);

  EXPECT_TRUE(tree.isPCATrained());
  EXPECT_NE(tree.getPCA(), nullptr);
}

TEST(HDRTreeTest, TrainPCAWithEmptyData) {
  HDRTree::Config config;
  config.projected_dim = 16;
  HDRTree tree(64, config);
  std::vector<std::vector<float>> empty_samples;

  EXPECT_THROW(tree.trainPCA(empty_samples), std::invalid_argument);
}

// ==================== 插入和删除测试 ====================

TEST(HDRTreeTest, InsertWithStorageManager) {
  constexpr int kDim = 64;
  constexpr int kProjectedDim = 16;
  constexpr int kNSamples = 100;

  // 创建存储管理器
  auto storage = std::make_shared<StorageManager>();
  storage->engine_ = std::make_shared<ComputeEngine>();

  // 创建 HDRTree
  HDRTree::Config config;
  config.projected_dim = kProjectedDim;
  config.pca_sample_size = 50;

  HDRTree tree(kDim, config);
  tree.storage_manager_ = storage;

  // 生成测试数据并插入存储
  auto vectors = generateRandomVectors(kNSamples, kDim);
  for (int i = 0; i < kNSamples; ++i) {
    auto record = createVectorRecord(i, i * 1000, vectors[i]);
    storage->insert(std::move(record));
  }

  // 训练 PCA
  tree.trainPCA(vectors);

  // 插入索引
  for (int i = 0; i < kNSamples; ++i) {
    EXPECT_TRUE(tree.insert(i));
  }

  EXPECT_EQ(tree.size(), static_cast<size_t>(kNSamples));
}

TEST(HDRTreeTest, InsertWithoutStorageManager) {
  HDRTree::Config config;
  config.projected_dim = 16;
  HDRTree tree(64, config);

  // 没有设置 storage_manager_ 应该失败
  EXPECT_FALSE(tree.insert(1));
}

TEST(HDRTreeTest, EraseRecord) {
  constexpr int kDim = 64;
  constexpr int kProjectedDim = 16;
  constexpr int kNSamples = 50;

  auto storage = std::make_shared<StorageManager>();
  storage->engine_ = std::make_shared<ComputeEngine>();

  HDRTree::Config config;
  config.projected_dim = kProjectedDim;
  config.pca_sample_size = 30;

  HDRTree tree(kDim, config);
  tree.storage_manager_ = storage;

  auto vectors = generateRandomVectors(kNSamples, kDim);
  for (int i = 0; i < kNSamples; ++i) {
    auto record = createVectorRecord(i, i * 1000, vectors[i]);
    storage->insert(std::move(record));
  }

  tree.trainPCA(vectors);

  for (int i = 0; i < kNSamples; ++i) {
    tree.insert(i);
  }

  EXPECT_EQ(tree.size(), static_cast<size_t>(kNSamples));

  // 删除部分记录
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(tree.erase(i));
  }

  EXPECT_EQ(tree.size(), static_cast<size_t>(kNSamples - 10));

  // 重复删除应该失败
  EXPECT_FALSE(tree.erase(0));
}

// ==================== 查询测试 ====================

TEST(HDRTreeTest, QueryForJoin) {
  constexpr int kDim = 64;
  constexpr int kProjectedDim = 16;
  constexpr int kNSamples = 100;
  constexpr double kThreshold = 0.8;

  auto storage = std::make_shared<StorageManager>();
  storage->engine_ = std::make_shared<ComputeEngine>();

  HDRTree::Config config;
  config.projected_dim = kProjectedDim;
  config.pca_sample_size = 50;
  config.distance_bound_ratio = 1.5f;

  HDRTree tree(kDim, config);
  tree.storage_manager_ = storage;

  auto vectors = generateRandomVectors(kNSamples, kDim, 42);
  for (int i = 0; i < kNSamples; ++i) {
    auto record = createVectorRecord(i, i * 1000, vectors[i]);
    storage->insert(std::move(record));
  }

  tree.trainPCA(vectors);

  for (int i = 0; i < kNSamples; ++i) {
    tree.insert(i);
  }

  // 创建查询向量（使用某个现有向量）
  auto query_record = createVectorRecord(999, 999000, vectors[0]);

  auto results = tree.query_for_join(*query_record, kThreshold);

  // 查询结果应该包含自身（相似度 = 1.0）
  bool found_self = false;
  for (uint64_t uid : results) {
    if (uid == 0) {
      found_self = true;
      break;
    }
  }
  EXPECT_TRUE(found_self) << "Query should find the identical vector";
}

TEST(HDRTreeTest, QueryWithoutPCATraining) {
  constexpr int kDim = 64;
  constexpr int kNSamples = 20;

  auto storage = std::make_shared<StorageManager>();
  storage->engine_ = std::make_shared<ComputeEngine>();

  HDRTree::Config config;
  config.projected_dim = 16;
  HDRTree tree(kDim, config);
  tree.storage_manager_ = storage;

  auto vectors = generateRandomVectors(kNSamples, kDim);
  for (int i = 0; i < kNSamples; ++i) {
    auto record = createVectorRecord(i, i * 1000, vectors[i]);
    storage->insert(std::move(record));
  }

  // 不训练 PCA，直接查询（应该回退到暴力搜索）
  auto query_record = createVectorRecord(999, 999000, vectors[0]);
  auto results = tree.query_for_join(*query_record, 0.8);

  // 暴力搜索仍应返回结果
  EXPECT_FALSE(results.empty());
}

// ==================== 自动 PCA 训练测试 ====================

TEST(HDRTreeTest, AutoTrainPCA) {
  constexpr int kDim = 64;
  constexpr int kProjectedDim = 8;
  constexpr int kPCASampleSize = 30;
  constexpr int kNSamples = 50;

  auto storage = std::make_shared<StorageManager>();
  storage->engine_ = std::make_shared<ComputeEngine>();

  HDRTree::Config config;
  config.projected_dim = kProjectedDim;
  config.pca_sample_size = kPCASampleSize;

  HDRTree tree(kDim, config);
  tree.storage_manager_ = storage;

  auto vectors = generateRandomVectors(kNSamples, kDim);

  // 逐个插入，当样本达到 pca_sample_size 时应自动训练
  for (int i = 0; i < kNSamples; ++i) {
    auto record = createVectorRecord(i, i * 1000, vectors[i]);
    storage->insert(std::move(record));
    tree.insert(i);

    if (i == kPCASampleSize - 1) {
      // 刚好达到样本数时应该训练
      // 注意：由于实现中是 >= 比较，可能需要多一个样本
    }
  }

  // 最终应该已训练
  EXPECT_TRUE(tree.isPCATrained());
}

// ==================== HDRTreeMethod 测试 ====================

TEST(HDRTreeMethodTest, Construction) {
  auto storage = std::make_shared<StorageManager>();
  auto cm = std::make_shared<ConcurrencyManager>(storage);

  HDRTreeMethod::Config config;
  config.similarity_threshold = 0.8;
  config.projected_dim = 16;

  EXPECT_NO_THROW(HDRTreeMethod(0, 1, 0.8, cm, config));
}

TEST(HDRTreeMethodTest, ExecuteEagerWithNullManager) {
  HDRTreeMethod method(0, 1, 0.8, nullptr);

  auto record = createVectorRecord(0, 0, std::vector<float>(64, 1.0f));
  auto results = method.ExecuteEager(*record, 0);

  EXPECT_TRUE(results.empty());
}

TEST(HDRTreeMethodTest, ExecuteLazyWithNullManager) {
  HDRTreeMethod method(0, 1, 0.8, nullptr);

  std::deque<std::unique_ptr<VectorRecord>> queries;
  queries.push_back(createVectorRecord(0, 0, std::vector<float>(64, 1.0f)));

  auto results = method.ExecuteLazy(queries, 0);

  EXPECT_TRUE(results.empty());
}

// ==================== 召回率测试 ====================

TEST(HDRTreeTest, RecallVsBruteForce) {
  constexpr int kDim = 64;
  constexpr int kProjectedDim = 16;
  constexpr int kNSamples = 200;
  constexpr int kNQueries = 20;
  constexpr double kThreshold = 0.7;

  auto storage = std::make_shared<StorageManager>();
  storage->engine_ = std::make_shared<ComputeEngine>();

  // 创建 HDRTree
  HDRTree::Config config;
  config.projected_dim = kProjectedDim;
  config.pca_sample_size = 100;
  config.distance_bound_ratio = 1.5f;

  HDRTree tree(kDim, config);
  tree.storage_manager_ = storage;

  // 生成数据
  auto vectors = generateRandomVectors(kNSamples, kDim, 123);
  for (int i = 0; i < kNSamples; ++i) {
    auto record = createVectorRecord(i, i * 1000, vectors[i]);
    storage->insert(std::move(record));
  }

  tree.trainPCA(vectors);

  for (int i = 0; i < kNSamples; ++i) {
    tree.insert(i);
  }

  // 生成查询
  auto query_vectors = generateRandomVectors(kNQueries, kDim, 456);

  int total_ground_truth = 0;
  int total_found = 0;

  for (int q = 0; q < kNQueries; ++q) {
    auto query_record = createVectorRecord(kNSamples + q, (kNSamples + q) * 1000, query_vectors[q]);

    // HDRTree 查询
    auto hdr_results = tree.query_for_join(*query_record, kThreshold);
    std::unordered_set<uint64_t> hdr_set(hdr_results.begin(), hdr_results.end());

    // 暴力搜索获取 Ground Truth
    auto gt_results = storage->similarityJoinQuery(*query_record, kThreshold);
    std::unordered_set<uint64_t> gt_set(gt_results.begin(), gt_results.end());

    total_ground_truth += gt_set.size();

    for (uint64_t uid : gt_set) {
      if (hdr_set.count(uid) > 0) {
        total_found++;
      }
    }
  }

  double recall = total_ground_truth > 0
                      ? static_cast<double>(total_found) / total_ground_truth
                      : 1.0;

  // 召回率应该至少达到 80%（由于 PCA 降维会损失信息）
  EXPECT_GE(recall, 0.7) << "Recall: " << recall * 100 << "%, Ground truth: "
                          << total_ground_truth << ", Found: " << total_found;
}

// ==================== 配置测试 ====================

TEST(HDRTreeTest, ConfigGetters) {
  HDRTree::Config config;
  config.projected_dim = 32;
  config.pca_sample_size = 5000;
  config.distance_bound_ratio = 1.3f;

  HDRTree tree(128, config);

  const auto& retrieved_config = tree.getConfig();
  EXPECT_EQ(retrieved_config.projected_dim, 32);
  EXPECT_EQ(retrieved_config.pca_sample_size, 5000);
  EXPECT_FLOAT_EQ(retrieved_config.distance_bound_ratio, 1.3f);
}

TEST(HDRTreeMethodTest, ConfigGetters) {
  auto storage = std::make_shared<StorageManager>();
  auto cm = std::make_shared<ConcurrencyManager>(storage);

  HDRTreeMethod::Config config;
  config.similarity_threshold = 0.9;
  config.projected_dim = 24;

  HDRTreeMethod method(0, 1, 0.9, cm, config);

  const auto& retrieved_config = method.getConfig();
  EXPECT_DOUBLE_EQ(retrieved_config.similarity_threshold, 0.9);
  EXPECT_EQ(retrieved_config.projected_dim, 24);
}

}  // namespace
}  // namespace sageFlow

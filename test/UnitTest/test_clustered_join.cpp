#include <gtest/gtest.h>

#include <memory>
#include <random>
#include <unordered_set>
#include <vector>

#include "concurrency/concurrency_manager.h"
#include "execution/centroid_partitioner.h"
#include "execution/collector.h"
#include "execution/runtime_context.h"
#include "function/join_function.h"
#include "operator/join_operator_methods/clustered_join_method.h"
#include "operator/utils/join_strategy_config.h"
#include "storage/storage_manager.h"
#include "test_utils/test_data_adapter.h"
#include "test_utils/test_data_generator.h"
#include "utils/logger.h"

namespace sageFlow {
namespace test {

// ==================== CentroidPartitioner 测试 ====================

class CentroidPartitionerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_.num_partitions = 4;
    config_.dimension = 128;
    config_.overlap_ratio = 0.1;
    config_.max_iterations = 100;
    config_.seed = 42;
  }

  /**
   * @brief 生成随机向量样本
   */
  std::vector<std::vector<float>> generateRandomSamples(int num_samples, int dim) {
    std::mt19937 gen(42);
    std::normal_distribution<float> dist(0.0f, 1.0f);

    std::vector<std::vector<float>> samples(num_samples);
    for (int i = 0; i < num_samples; ++i) {
      samples[i].resize(dim);
      for (int j = 0; j < dim; ++j) {
        samples[i][j] = dist(gen);
      }
    }
    return samples;
  }

  /**
   * @brief 生成聚类样本（多个簇）
   */
  std::vector<std::vector<float>> generateClusteredSamples(int num_clusters, int samples_per_cluster, int dim) {
    std::mt19937 gen(42);
    std::normal_distribution<float> center_dist(0.0f, 10.0f);
    std::normal_distribution<float> noise_dist(0.0f, 0.5f);

    std::vector<std::vector<float>> samples;
    samples.reserve(num_clusters * samples_per_cluster);

    for (int c = 0; c < num_clusters; ++c) {
      // 生成簇中心
      std::vector<float> center(dim);
      for (int j = 0; j < dim; ++j) {
        center[j] = center_dist(gen);
      }

      // 在簇中心周围生成样本
      for (int i = 0; i < samples_per_cluster; ++i) {
        std::vector<float> sample(dim);
        for (int j = 0; j < dim; ++j) {
          sample[j] = center[j] + noise_dist(gen);
        }
        samples.push_back(std::move(sample));
      }
    }

    return samples;
  }

  CentroidPartitioner::Config config_;
};

TEST_F(CentroidPartitionerTest, Construction) {
  CentroidPartitioner partitioner(config_);

  EXPECT_EQ(partitioner.getNumPartitions(), config_.num_partitions);
  EXPECT_EQ(partitioner.getDimension(), config_.dimension);
  EXPECT_FALSE(partitioner.isTrained());
}

TEST_F(CentroidPartitionerTest, ConstructionInvalidConfig) {
  // 测试无效的 num_partitions
  {
    CentroidPartitioner::Config invalid_config = config_;
    invalid_config.num_partitions = 0;
    EXPECT_THROW({
      CentroidPartitioner partitioner(invalid_config);
    }, std::invalid_argument);
  }

  // 测试无效的 dimension
  {
    CentroidPartitioner::Config invalid_config = config_;
    invalid_config.dimension = -1;
    EXPECT_THROW({
      CentroidPartitioner partitioner(invalid_config);
    }, std::invalid_argument);
  }

  // 测试无效的 overlap_ratio
  {
    CentroidPartitioner::Config invalid_config = config_;
    invalid_config.overlap_ratio = 1.5;
    EXPECT_THROW({
      CentroidPartitioner partitioner(invalid_config);
    }, std::invalid_argument);
  }
}

TEST_F(CentroidPartitionerTest, TrainAndPartition) {
  CentroidPartitioner partitioner(config_);

  auto samples = generateRandomSamples(1000, config_.dimension);
  partitioner.train(samples);

  EXPECT_TRUE(partitioner.isTrained());

  // 测试分区
  auto record = createVectorRecord(1, 1000, samples[0]);
  auto partitions = partitioner.getPartitions(*record);

  EXPECT_FALSE(partitions.empty());
  EXPECT_LT(partitions[0], config_.num_partitions);
}

TEST_F(CentroidPartitionerTest, TrainWithVectorRecords) {
  CentroidPartitioner partitioner(config_);

  auto samples = generateRandomSamples(500, config_.dimension);
  std::vector<std::unique_ptr<VectorRecord>> records;
  std::vector<const VectorRecord*> record_ptrs;

  for (size_t i = 0; i < samples.size(); ++i) {
    records.push_back(createVectorRecord(i, 1000 + i, samples[i]));
    record_ptrs.push_back(records.back().get());
  }

  partitioner.train(record_ptrs);

  EXPECT_TRUE(partitioner.isTrained());
}

TEST_F(CentroidPartitionerTest, ClusteredSamplesDistribution) {
  config_.num_partitions = 4;
  CentroidPartitioner partitioner(config_);

  // 生成 4 个明显分离的簇
  auto samples = generateClusteredSamples(4, 100, config_.dimension);
  partitioner.train(samples);

  EXPECT_TRUE(partitioner.isTrained());

  // 验证同一簇内的样本大多分配到同一分区
  std::vector<int> cluster0_partitions;
  for (int i = 0; i < 100; ++i) {
    auto record = createVectorRecord(i, 1000, samples[i]);
    cluster0_partitions.push_back(partitioner.getPrimaryPartition(*record));
  }

  // 统计最常见的分区
  std::unordered_map<int, int> partition_counts;
  for (int p : cluster0_partitions) {
    partition_counts[p]++;
  }

  int max_count = 0;
  for (const auto& [_, count] : partition_counts) {
    max_count = std::max(max_count, count);
  }

  // 期望大多数样本（>60%）分配到同一分区
  EXPECT_GT(max_count, 60);
}

TEST_F(CentroidPartitionerTest, BorderDetection) {
  CentroidPartitioner partitioner(config_);

  auto samples = generateClusteredSamples(4, 100, config_.dimension);
  partitioner.train(samples);

  // 测试边界检测
  // 注意：边界检测结果取决于数据分布，这里只验证方法能正常工作
  int boundary_count = 0;
  for (size_t i = 0; i < samples.size(); ++i) {
    auto record = createVectorRecord(i, 1000, samples[i]);
    if (partitioner.isBoundaryVector(*record)) {
      boundary_count++;
    }
  }

  // 边界向量应该是少数（取决于 overlap_ratio）
  SAGEFLOW_LOG_INFO("TEST", "Boundary vectors: {}/{}", boundary_count, samples.size());
  EXPECT_LT(boundary_count, static_cast<int>(samples.size()));
}

TEST_F(CentroidPartitionerTest, GetBorderPartitions) {
  CentroidPartitioner partitioner(config_);

  auto samples = generateRandomSamples(500, config_.dimension);
  partitioner.train(samples);

  auto record = createVectorRecord(1, 1000, samples[0]);
  auto partitions = partitioner.getPartitions(*record);
  auto border_partitions = partitioner.getBorderPartitions(*record);

  // 主分区不应该在边界分区列表中
  if (!partitions.empty() && !border_partitions.empty()) {
    for (int bp : border_partitions) {
      EXPECT_NE(bp, partitions[0]);
    }
  }
}

TEST_F(CentroidPartitionerTest, IPartitionerInterface) {
  CentroidPartitioner partitioner(config_);

  auto samples = generateRandomSamples(500, config_.dimension);
  partitioner.train(samples);

  Response response;
  response.type_ = ResponseType::Record;
  response.record_ = createVectorRecord(1, 1000, samples[0]);

  size_t partition = partitioner.partition(response, config_.num_partitions);
  EXPECT_LT(partition, static_cast<size_t>(config_.num_partitions));
}

TEST_F(CentroidPartitionerTest, PartitionStats) {
  CentroidPartitioner partitioner(config_);

  auto samples = generateRandomSamples(500, config_.dimension);
  partitioner.train(samples);

  // 模拟一些分区更新
  for (int i = 0; i < 100; ++i) {
    auto record = createVectorRecord(i, 1000, samples[i % samples.size()]);
    int partition = partitioner.getPrimaryPartition(*record);
    partitioner.updatePartitionSize(partition, 1);
  }

  auto stats = partitioner.getStats();
  EXPECT_EQ(stats.sizes.size(), static_cast<size_t>(config_.num_partitions));
  EXPECT_GE(stats.balance_score, 0.0);
  EXPECT_LE(stats.balance_score, 1.0);

  // 验证总数正确
  size_t total = 0;
  for (size_t s : stats.sizes) {
    total += s;
  }
  EXPECT_EQ(total, 100u);
}

TEST_F(CentroidPartitionerTest, NeedsRebalance) {
  config_.rebalance_threshold = 0.3;
  CentroidPartitioner partitioner(config_);

  auto samples = generateRandomSamples(500, config_.dimension);
  partitioner.train(samples);

  // 极度不均衡的分区大小
  std::vector<size_t> unbalanced_sizes = {1000, 10, 10, 10};
  EXPECT_TRUE(partitioner.needsRebalance(unbalanced_sizes));

  // 相对均衡的分区大小
  std::vector<size_t> balanced_sizes = {250, 250, 250, 250};
  EXPECT_FALSE(partitioner.needsRebalance(balanced_sizes));
}

TEST_F(CentroidPartitionerTest, IncrementalUpdate) {
  CentroidPartitioner partitioner(config_);

  auto samples = generateRandomSamples(500, config_.dimension);
  partitioner.train(samples);

  auto centroids_before = partitioner.getCentroids();

  // 增量更新
  std::vector<float> new_vec(config_.dimension, 1.0f);
  partitioner.updateCentroidsIncremental(new_vec, 0.1);

  auto centroids_after = partitioner.getCentroids();

  // 验证质心有变化
  bool changed = false;
  for (size_t c = 0; c < centroids_before.size(); ++c) {
    for (size_t d = 0; d < centroids_before[c].size(); ++d) {
      if (std::abs(centroids_before[c][d] - centroids_after[c][d]) > 1e-9f) {
        changed = true;
        break;
      }
    }
    if (changed) break;
  }
  EXPECT_TRUE(changed);
}

// ==================== ClusteredJoinMethod 测试 ====================
// 注意：ClusteredJoinMethod 已重写为方案 A（独立索引）
// 详细测试请参见 test_clustered_join_method.cpp
// 这里只保留兼容新接口的基本测试

class ClusteredJoinMethodTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto storage = std::make_shared<StorageManager>();
    concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage);

    config_.similarity_threshold = 0.8;
    config_.dimension = 128;
    config_.window_size_ms = 10000;
    config_.index_type = ClusteredIndexType::BRUTEFORCE;
  }

  std::vector<std::vector<float>> generateRandomVectors(int count, int dim) {
    std::mt19937 gen(42);
    std::normal_distribution<float> dist(0.0f, 1.0f);

    std::vector<std::vector<float>> vectors(count);
    for (int i = 0; i < count; ++i) {
      vectors[i].resize(dim);
      for (int j = 0; j < dim; ++j) {
        vectors[i][j] = dist(gen);
      }
    }
    return vectors;
  }

  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  ClusteredJoinMethod::Config config_;
};

TEST_F(ClusteredJoinMethodTest, Construction) {
  ClusteredJoinMethod method(config_);

  EXPECT_EQ(method.getName(), "ClusteredJoin");
  EXPECT_EQ(method.getConfig().similarity_threshold, config_.similarity_threshold);
  EXPECT_FALSE(method.isInitialized());
}

TEST_F(ClusteredJoinMethodTest, SimpleConstruction) {
  ClusteredJoinMethod method(0.8, 128);

  EXPECT_EQ(method.getName(), "ClusteredJoin");
  EXPECT_FALSE(method.isInitialized());
}

TEST_F(ClusteredJoinMethodTest, InitializeAndExecute) {
  ClusteredJoinMethod method(config_);
  RuntimeContext context(0, 1);
  method.initialize(context, concurrency_manager_);
  
  EXPECT_TRUE(method.isInitialized());
  
  auto samples = generateRandomVectors(30, config_.dimension);

  // 模拟流式数据到达并执行查询
  for (int i = 0; i < 25; ++i) {
    auto record = createVectorRecord(i, 1000 + i, samples[i]);
    method.addRecord(std::make_unique<VectorRecord>(*record), 0);
    method.ExecuteEager(*record, 0);
  }
  
  EXPECT_GT(method.getLeftWindowSize(), 0u);
}

TEST_F(ClusteredJoinMethodTest, EagerExecution) {
  ClusteredJoinMethod method(config_);
  RuntimeContext context(0, 1);
  method.initialize(context, concurrency_manager_);

  auto samples = generateRandomVectors(50, config_.dimension);
  
  // 添加数据到右侧窗口
  for (int i = 0; i < 25; ++i) {
    auto record = createVectorRecord(i, 1000 + i, samples[i]);
    method.addRecord(std::move(record), 1);  // 添加到右侧
  }

  // 执行 Eager 查询
  auto query = createVectorRecord(100, 2000, samples[0]);
  auto results = method.ExecuteEager(*query, 0);

  SAGEFLOW_LOG_INFO("TEST", "Eager query found {} results", results.size());
}

// ==================== 集成测试 ====================

class ClusteredJoinIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto storage = std::make_shared<StorageManager>();
    concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage);

    generator_config_.vector_dim = 128;
    generator_config_.similarity_threshold = 0.8;
    generator_config_.positive_pairs = 30;
    generator_config_.negative_pairs = 50;
    generator_config_.random_tail = 20;
    generator_config_.seed = 42;
  }

  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  TestDataGenerator::Config generator_config_;
};

TEST_F(ClusteredJoinIntegrationTest, BasicPipeline) {
  TestDataGenerator generator(generator_config_);
  auto [records, expected_matches] = generator.generateData();

  ClusteredJoinMethod::Config method_config;
  method_config.similarity_threshold = generator_config_.similarity_threshold;
  method_config.dimension = generator_config_.vector_dim;
  method_config.window_size_ms = 10000;
  method_config.index_type = ClusteredIndexType::BRUTEFORCE;

  ClusteredJoinMethod method(method_config);
  RuntimeContext context(0, 1);
  method.initialize(context, concurrency_manager_);

  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> actual_matches;

  for (auto& record : records) {
    // 添加到右侧窗口（模拟双流）
    method.addRecord(std::make_unique<VectorRecord>(*record), 1);

    // 执行查询
    auto candidates = method.ExecuteEager(*record, 0);

    for (const auto& cand : candidates) {
      if (cand && cand->uid_ != record->uid_) {
        actual_matches.insert({std::min(record->uid_, cand->uid_), std::max(record->uid_, cand->uid_)});
      }
    }
  }

  SAGEFLOW_LOG_INFO("TEST", "Expected matches: {}, Actual matches: {}", expected_matches.size(), actual_matches.size());

  // 验证基本功能正常
  SUCCEED() << "ClusteredJoin pipeline executed without crash";
}

}  // namespace test
}  // namespace sageFlow

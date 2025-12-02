#include <gtest/gtest.h>

#include "execution/partitioner_factory.h"
#include "execution/centroid_partitioner.h"
#include "execution/vector_space_partitioner.h"
#include "operator/join_strategy_config.h"
#include "test_utils/test_data_adapter.h"

#include <random>
#include <set>
#include <cmath>

namespace sageFlow {
namespace test {

// 辅助函数：创建 Response
inline Response makeResponse(std::unique_ptr<VectorRecord> record) {
  return Response(ResponseType::Record, std::move(record));
}

inline Response makeNullResponse() {
  return Response();  // 使用默认构造函数，type_ = None, record_ = nullptr
}

// =============================================================================
// PartitionerFactory 基本功能测试
// =============================================================================

class PartitionerFactoryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_.dimension = 128;
    config_.num_partitions = 4;
    config_.vsjoin_num_hash_functions = 8;
    config_.vsjoin_boundary_threshold = 0.1;
    config_.clustered_overlap_ratio = 0.1;
    config_.clustered_rebalance_threshold = 0.3;
  }

  JoinStrategyConfig config_;
};

// 测试创建 RoundRobin 分区器
TEST_F(PartitionerFactoryTest, CreateRoundRobin) {
  auto partitioner = PartitionerFactory::create(
      PartitionStrategy::ROUND_ROBIN, 128, 4, config_);
  ASSERT_NE(partitioner, nullptr);
  
  // 验证轮询行为
  std::vector<float> data(128, 1.0f);
  std::vector<size_t> partitions;
  for (int i = 0; i < 8; ++i) {
    auto record = createVectorRecord(i, 1000 + i, data);
    Response response = makeResponse(std::move(record));
    partitions.push_back(partitioner->partition(response, 4));
  }
  
  // 应该按顺序分配：0, 1, 2, 3, 0, 1, 2, 3
  EXPECT_EQ(partitions[0], 0);
  EXPECT_EQ(partitions[1], 1);
  EXPECT_EQ(partitions[2], 2);
  EXPECT_EQ(partitions[3], 3);
  EXPECT_EQ(partitions[4], 0);
  EXPECT_EQ(partitions[5], 1);
}

// 测试创建 KeyHash 分区器
TEST_F(PartitionerFactoryTest, CreateKeyHash) {
  auto partitioner = PartitionerFactory::create(
      PartitionStrategy::KEY_HASH, 128, 4, config_);
  ASSERT_NE(partitioner, nullptr);
  
  // 相同时间戳应该分配到相同分区
  std::vector<float> data(128, 1.0f);
  auto record1 = createVectorRecord(1, 1000, data);
  auto record2 = createVectorRecord(2, 1000, data);  // 不同 UID，相同时间戳
  
  Response response1 = makeResponse(std::move(record1));
  Response response2 = makeResponse(std::move(record2));
  
  EXPECT_EQ(partitioner->partition(response1, 4),
            partitioner->partition(response2, 4));
}

// 测试创建 VectorHash 分区器
TEST_F(PartitionerFactoryTest, CreateVectorHash) {
  auto partitioner = PartitionerFactory::create(
      PartitionStrategy::VECTOR_HASH, 128, 4, config_);
  ASSERT_NE(partitioner, nullptr);
  
  // 相同向量应该分配到相同分区
  std::vector<float> data(128, 1.0f);
  auto record1 = createVectorRecord(1, 1000, data);
  auto record2 = createVectorRecord(2, 2000, data);  // 不同时间戳，相同向量
  
  Response response1 = makeResponse(std::move(record1));
  Response response2 = makeResponse(std::move(record2));
  
  EXPECT_EQ(partitioner->partition(response1, 4),
            partitioner->partition(response2, 4));
}

// 测试创建 LSH 分区器
TEST_F(PartitionerFactoryTest, CreateLSH) {
  auto partitioner = PartitionerFactory::create(
      PartitionStrategy::LSH, 128, 4, config_);
  ASSERT_NE(partitioner, nullptr);
  
  // 验证可以正常分区
  std::vector<float> data(128, 1.0f);
  auto record = createVectorRecord(1, 1000, data);
  Response response = makeResponse(std::move(record));
  
  size_t partition = partitioner->partition(response, 4);
  EXPECT_LT(partition, 4);
}

// 测试创建 Centroid 分区器
TEST_F(PartitionerFactoryTest, CreateCentroid) {
  auto partitioner = PartitionerFactory::create(
      PartitionStrategy::CENTROID, 128, 4, config_);
  ASSERT_NE(partitioner, nullptr);
  
  // 未训练时应该使用后备策略（基于 UID 的哈希）
  std::vector<float> data(128, 1.0f);
  auto record = createVectorRecord(1, 1000, data);
  Response response = makeResponse(std::move(record));
  
  size_t partition = partitioner->partition(response, 4);
  EXPECT_LT(partition, 4);
}

// 测试使用简化接口创建分区器
TEST_F(PartitionerFactoryTest, CreateFromConfig) {
  config_.partition_strategy = PartitionStrategy::LSH;
  auto partitioner = PartitionerFactory::create(config_);
  ASSERT_NE(partitioner, nullptr);
}

// 测试空记录处理
TEST_F(PartitionerFactoryTest, HandleNullRecord) {
  auto partitioner = PartitionerFactory::create(
      PartitionStrategy::LSH, 128, 4, config_);
  
  Response response = makeNullResponse();
  EXPECT_EQ(partitioner->partition(response, 4), 0);
}

// =============================================================================
// LSHIPartitioner 详细测试
// =============================================================================

class LSHIPartitionerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    partitioner_ = std::make_unique<LSHIPartitioner>(
        128,  // dimension
        8,    // num_hash_functions
        16,   // num_partitions
        42,   // seed
        0.1   // boundary_threshold
    );
  }

  std::unique_ptr<LSHIPartitioner> partitioner_;

  // 创建随机向量
  std::vector<float> createRandomVector(int dim, std::mt19937& gen) {
    std::normal_distribution<float> dist(0.0f, 1.0f);
    std::vector<float> vec(dim);
    for (int i = 0; i < dim; ++i) {
      vec[i] = dist(gen);
    }
    return vec;
  }

  // 创建相似向量
  std::vector<float> createSimilarVector(const std::vector<float>& base,
                                          float noise_level,
                                          std::mt19937& gen) {
    std::normal_distribution<float> noise(0.0f, noise_level);
    std::vector<float> similar = base;
    for (size_t i = 0; i < similar.size(); ++i) {
      similar[i] += noise(gen);
    }
    return similar;
  }
};

// 测试相同向量分配到相同分区
TEST_F(LSHIPartitionerTest, SameVectorSamePartition) {
  std::vector<float> data(128, 0.5f);
  
  auto record1 = createVectorRecord(1, 1000, data);
  auto record2 = createVectorRecord(2, 2000, data);
  
  Response response1 = makeResponse(std::move(record1));
  Response response2 = makeResponse(std::move(record2));
  
  EXPECT_EQ(partitioner_->partition(response1, 16),
            partitioner_->partition(response2, 16));
}

// 测试相似向量的局部性保持
TEST_F(LSHIPartitionerTest, LocalityPreservation) {
  std::mt19937 gen(12345);
  const int num_tests = 100;
  int same_partition_count = 0;
  
  for (int i = 0; i < num_tests; ++i) {
    // 生成基础向量
    std::vector<float> base = createRandomVector(128, gen);
    
    // 生成相似向量（小噪声）
    std::vector<float> similar = createSimilarVector(base, 0.05f, gen);
    
    auto record_base = createVectorRecord(i * 2, 1000 + i, base);
    auto record_similar = createVectorRecord(i * 2 + 1, 1000 + i, similar);
    
    Response response_base = makeResponse(std::move(record_base));
    Response response_similar = makeResponse(std::move(record_similar));
    
    if (partitioner_->partition(response_base, 16) ==
        partitioner_->partition(response_similar, 16)) {
      same_partition_count++;
    }
  }
  
  // 相似向量同分区率应该 > 60%
  double same_partition_rate =
      static_cast<double>(same_partition_count) / num_tests;
  EXPECT_GT(same_partition_rate, 0.60)
      << "Similar vectors should have >60% same partition rate, got "
      << same_partition_rate * 100 << "%";
}

// 测试候选分区功能
TEST_F(LSHIPartitionerTest, CandidatePartitions) {
  std::vector<float> data(128, 0.5f);
  auto record = createVectorRecord(1, 1000, data);
  Response response = makeResponse(std::move(record));
  
  // 获取主分区
  size_t main_partition = partitioner_->partition(response, 16);
  
  // 重新创建 response 因为 partition 可能消耗了 record_
  auto record2 = createVectorRecord(1, 1000, data);
  Response response2 = makeResponse(std::move(record2));
  
  // 获取候选分区
  auto candidates = partitioner_->getCandidatePartitions(response2, 16, 3);
  
  // 候选分区应该包含主分区
  ASSERT_FALSE(candidates.empty());
  EXPECT_EQ(candidates[0], main_partition);
  
  // 请求3个候选分区，应该返回至少1个
  EXPECT_GE(candidates.size(), 1);
}

// 测试边界向量检测
TEST_F(LSHIPartitionerTest, BoundaryVectorDetection) {
  // 零向量应该被视为边界向量
  std::vector<float> zero_vec(128, 0.0f);
  auto zero_record = createVectorRecord(1, 1000, zero_vec);
  Response zero_response = makeResponse(std::move(zero_record));
  
  EXPECT_TRUE(partitioner_->isBoundaryVector(zero_response, 16));
}

// 测试不同分区数的兼容性
TEST_F(LSHIPartitionerTest, DifferentNumChannels) {
  std::vector<float> data(128, 0.5f);
  
  // 不同的 num_channels 应该都能工作
  for (size_t channels : {2, 4, 8, 16, 32}) {
    auto record = createVectorRecord(1, 1000, data);
    Response response = makeResponse(std::move(record));
    size_t partition = partitioner_->partition(response, channels);
    EXPECT_LT(partition, channels);
  }
}

// 测试获取内部 LSHPartitioner
TEST_F(LSHIPartitionerTest, GetLSHPartitioner) {
  auto lsh = partitioner_->getLSHPartitioner();
  ASSERT_NE(lsh, nullptr);
  EXPECT_EQ(lsh->getDimension(), 128);
  EXPECT_EQ(lsh->getNumHashFunctions(), 8);
}

// =============================================================================
// PartitionerFactory 工具函数测试
// =============================================================================

TEST(PartitionerFactoryUtilsTest, GetRecommendedPartitionCount) {
  // RoundRobin 应该返回并行度
  EXPECT_EQ(PartitionerFactory::getRecommendedPartitionCount(
                PartitionStrategy::ROUND_ROBIN, 4),
            4);
  
  // LSH 应该返回 2 的幂次方
  EXPECT_EQ(PartitionerFactory::getRecommendedPartitionCount(
                PartitionStrategy::LSH, 3),
            4);
  EXPECT_EQ(PartitionerFactory::getRecommendedPartitionCount(
                PartitionStrategy::LSH, 5),
            8);
  
  // Centroid 应该至少返回 8
  EXPECT_GE(PartitionerFactory::getRecommendedPartitionCount(
                PartitionStrategy::CENTROID, 4),
            8);
}

TEST(PartitionerFactoryUtilsTest, RequiresTraining) {
  EXPECT_FALSE(PartitionerFactory::requiresTraining(
                   PartitionStrategy::ROUND_ROBIN));
  EXPECT_FALSE(PartitionerFactory::requiresTraining(
                   PartitionStrategy::KEY_HASH));
  EXPECT_FALSE(PartitionerFactory::requiresTraining(
                   PartitionStrategy::VECTOR_HASH));
  EXPECT_FALSE(PartitionerFactory::requiresTraining(PartitionStrategy::LSH));
  EXPECT_TRUE(PartitionerFactory::requiresTraining(
                  PartitionStrategy::CENTROID));
}

TEST(PartitionerFactoryUtilsTest, GetDescription) {
  // 确保所有策略都有描述
  EXPECT_FALSE(PartitionerFactory::getDescription(
                   PartitionStrategy::ROUND_ROBIN)
                   .empty());
  EXPECT_FALSE(PartitionerFactory::getDescription(PartitionStrategy::KEY_HASH)
                   .empty());
  EXPECT_FALSE(PartitionerFactory::getDescription(
                   PartitionStrategy::VECTOR_HASH)
                   .empty());
  EXPECT_FALSE(PartitionerFactory::getDescription(PartitionStrategy::LSH)
                   .empty());
  EXPECT_FALSE(PartitionerFactory::getDescription(PartitionStrategy::CENTROID)
                   .empty());
}

// =============================================================================
// CentroidPartitioner 训练与分区测试
// =============================================================================

class CentroidPartitionerIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    CentroidPartitioner::Config config;
    config.num_partitions = 4;
    config.dimension = 8;  // 使用小维度便于测试
    config.overlap_ratio = 0.2;
    partitioner_ = std::make_unique<CentroidPartitioner>(config);
  }

  std::unique_ptr<CentroidPartitioner> partitioner_;
};

TEST_F(CentroidPartitionerIntegrationTest, TrainAndPartition) {
  // 创建训练样本 - 4 个明显分离的簇
  std::vector<std::vector<float>> samples;
  std::mt19937 gen(42);
  std::normal_distribution<float> noise(0.0f, 0.1f);
  
  // 簇中心
  std::vector<std::vector<float>> centers = {
      {1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
      {0.0f, 1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
      {0.0f, 0.0f, 1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
      {0.0f, 0.0f, 0.0f, 1.0f, 0.0f, 0.0f, 0.0f, 0.0f}};
  
  // 每个簇生成 10 个样本
  for (const auto& center : centers) {
    for (int i = 0; i < 10; ++i) {
      std::vector<float> sample = center;
      for (float& val : sample) {
        val += noise(gen);
      }
      samples.push_back(sample);
    }
  }
  
  // 训练
  partitioner_->train(samples);
  EXPECT_TRUE(partitioner_->isTrained());
  
  // 测试分区 - 相似向量应该分到相同分区
  std::vector<float> test1 = {0.9f, 0.1f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};
  std::vector<float> test2 = {0.95f, 0.05f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};
  
  auto record1 = createVectorRecord(1, 1000, test1);
  auto record2 = createVectorRecord(2, 1000, test2);
  
  int p1 = partitioner_->getPrimaryPartition(*record1);
  int p2 = partitioner_->getPrimaryPartition(*record2);
  
  EXPECT_EQ(p1, p2) << "Similar vectors should be in same partition";
}

TEST_F(CentroidPartitionerIntegrationTest, IPartitionerInterface) {
  // 创建训练样本
  std::vector<std::vector<float>> samples = {
      {1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
      {0.0f, 1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
      {0.0f, 0.0f, 1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
      {0.0f, 0.0f, 0.0f, 1.0f, 0.0f, 0.0f, 0.0f, 0.0f}};
  
  partitioner_->train(samples);
  
  // 通过 IPartitioner 接口测试
  std::vector<float> test_vec = {0.9f, 0.1f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};
  auto record = createVectorRecord(1, 1000, test_vec);
  Response response = makeResponse(std::move(record));
  
  IPartitioner* base_ptr = partitioner_.get();
  size_t partition = base_ptr->partition(response, 4);
  EXPECT_LT(partition, 4);
}

}  // namespace test
}  // namespace sageFlow

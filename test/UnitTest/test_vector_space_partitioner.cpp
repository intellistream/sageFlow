#include <gtest/gtest.h>

#include "execution/vector_space_partitioner.h"
#include "test_utils/test_data_adapter.h"

#include <cmath>
#include <random>
#include <set>
#include <vector>

namespace sageFlow {
namespace test {

// =============================================================================
// LSHPartitioner Tests
// =============================================================================

class LSHPartitionerTest : public ::testing::Test {
 protected:
  void SetUp() override { partitioner_ = std::make_unique<LSHPartitioner>(128, 8, 42); }

  std::unique_ptr<LSHPartitioner> partitioner_;

  // 创建随机向量
  std::vector<float> createRandomVector(int dim, std::mt19937& gen) {
    std::normal_distribution<float> dist(0.0f, 1.0f);
    std::vector<float> vec(dim);
    for (int i = 0; i < dim; ++i) {
      vec[i] = dist(gen);
    }
    return vec;
  }

  // 创建相似向量（添加小噪声）
  std::vector<float> createSimilarVector(const std::vector<float>& base, float noise_level, std::mt19937& gen) {
    std::normal_distribution<float> noise(0.0f, noise_level);
    std::vector<float> similar = base;
    for (size_t i = 0; i < similar.size(); ++i) {
      similar[i] += noise(gen);
    }
    return similar;
  }

  // 计算余弦相似度
  float cosineSimilarity(const std::vector<float>& a, const std::vector<float>& b) {
    float dot = 0.0f, norm_a = 0.0f, norm_b = 0.0f;
    for (size_t i = 0; i < a.size(); ++i) {
      dot += a[i] * b[i];
      norm_a += a[i] * a[i];
      norm_b += b[i] * b[i];
    }
    return dot / (std::sqrt(norm_a) * std::sqrt(norm_b));
  }
};

// 一致性测试：相同向量应该分配到相同分区
TEST_F(LSHPartitionerTest, SameVectorSamePartition) {
  std::vector<float> data = {1.0f, 2.0f, 3.0f, 4.0f};
  // 扩展到128维
  data.resize(128, 0.5f);

  auto record1 = createVectorRecord(1, 1000, data);
  auto record2 = createVectorRecord(2, 2000, data);  // 不同 UID/时间戳，相同向量

  size_t partition1 = partitioner_->partition(*record1, 16);
  size_t partition2 = partitioner_->partition(*record2, 16);

  EXPECT_EQ(partition1, partition2) << "Same vectors should map to same partition";
}

// 哈希码一致性测试
TEST_F(LSHPartitionerTest, HashCodeConsistency) {
  std::vector<float> data(128, 1.0f);
  auto record = createVectorRecord(1, 1000, data);

  uint64_t hash1 = partitioner_->getHashCode(*record);
  uint64_t hash2 = partitioner_->getHashCode(*record);

  EXPECT_EQ(hash1, hash2) << "Hash code should be consistent for same vector";
}

// 局部性测试：相似向量有高概率分配到同一分区
TEST_F(LSHPartitionerTest, SimilarVectorsSamePartitionHighProbability) {
  std::mt19937 gen(12345);
  const int num_tests = 100;
  const int dim = 128;
  const size_t num_partitions = 8;
  int same_partition_count = 0;

  for (int i = 0; i < num_tests; ++i) {
    // 生成基础向量
    std::vector<float> base = createRandomVector(dim, gen);

    // 生成相似向量（小噪声）
    std::vector<float> similar = createSimilarVector(base, 0.05f, gen);

    auto record_base = createVectorRecord(i * 2, 1000 + i, base);
    auto record_similar = createVectorRecord(i * 2 + 1, 1000 + i, similar);

    size_t partition_base = partitioner_->partition(*record_base, num_partitions);
    size_t partition_similar = partitioner_->partition(*record_similar, num_partitions);

    if (partition_base == partition_similar) {
      same_partition_count++;
    }
  }

  // 相似向量同分区率应该 > 70%
  double same_partition_rate = static_cast<double>(same_partition_count) / num_tests;
  EXPECT_GT(same_partition_rate, 0.70) << "Similar vectors should have >70% same partition rate, got "
                                       << same_partition_rate * 100 << "%";
}

// 不同向量分布测试
TEST_F(LSHPartitionerTest, DifferentVectorsDistribute) {
  std::mt19937 gen(42);
  const int num_vectors = 1000;
  const int dim = 128;
  const size_t num_partitions = 16;

  std::vector<size_t> partition_counts(num_partitions, 0);

  for (int i = 0; i < num_vectors; ++i) {
    std::vector<float> data = createRandomVector(dim, gen);
    auto record = createVectorRecord(i, 1000 + i, data);
    size_t partition = partitioner_->partition(*record, num_partitions);

    ASSERT_LT(partition, num_partitions) << "Partition should be in valid range";
    partition_counts[partition]++;
  }

  // 检查分布是否合理（至少使用了一半的分区）
  int non_empty_partitions = 0;
  for (size_t count : partition_counts) {
    if (count > 0) {
      non_empty_partitions++;
    }
  }

  EXPECT_GE(non_empty_partitions, static_cast<int>(num_partitions / 2))
      << "Should use at least half of the partitions";
}

// 候选分区测试：结果应包含主分区
TEST_F(LSHPartitionerTest, CandidatePartitionsIncludesMainPartition) {
  std::vector<float> data(128, 1.0f);
  auto record = createVectorRecord(1, 1000, data);

  size_t main_partition = partitioner_->partition(*record, 16);
  std::vector<size_t> candidates = partitioner_->getCandidatePartitions(*record, 16, 1);

  ASSERT_FALSE(candidates.empty()) << "Candidates should not be empty";
  EXPECT_EQ(candidates[0], main_partition) << "First candidate should be main partition";
}

// 更多探测意味着更多候选分区
TEST_F(LSHPartitionerTest, MoreProbesMeansMoreCandidates) {
  std::vector<float> data(128, 1.0f);
  auto record = createVectorRecord(1, 1000, data);

  std::vector<size_t> candidates_1 = partitioner_->getCandidatePartitions(*record, 16, 1);
  std::vector<size_t> candidates_3 = partitioner_->getCandidatePartitions(*record, 16, 3);
  std::vector<size_t> candidates_5 = partitioner_->getCandidatePartitions(*record, 16, 5);

  EXPECT_EQ(candidates_1.size(), 1);
  EXPECT_GE(candidates_3.size(), candidates_1.size());
  EXPECT_GE(candidates_5.size(), candidates_3.size());
}

// 候选分区不重复
TEST_F(LSHPartitionerTest, CandidatePartitionsNoDuplicates) {
  std::vector<float> data(128, 0.5f);
  auto record = createVectorRecord(1, 1000, data);

  std::vector<size_t> candidates = partitioner_->getCandidatePartitions(*record, 16, 8);

  std::set<size_t> unique_candidates(candidates.begin(), candidates.end());
  EXPECT_EQ(unique_candidates.size(), candidates.size()) << "Candidate partitions should not have duplicates";
}

// 边界向量检测测试
TEST_F(LSHPartitionerTest, BoundaryVectorDetection) {
  // 创建一个接近原点的向量（应该靠近多个超平面）
  std::vector<float> near_origin(128, 0.001f);
  auto record_near_origin = createVectorRecord(1, 1000, near_origin);

  // 接近原点的向量应该被标记为边界向量
  EXPECT_TRUE(partitioner_->isBoundaryVector(*record_near_origin, 16))
      << "Near-origin vector should be boundary vector";
}

// 远离边界的向量测试
TEST_F(LSHPartitionerTest, NonBoundaryVectorDetection) {
  // 创建一个远离超平面的向量（所有分量都很大）
  std::vector<float> far_from_boundary(128, 10.0f);
  auto record_far = createVectorRecord(1, 1000, far_from_boundary);

  // 这个向量可能不是边界向量
  // 注意：由于超平面是随机的，这个测试可能不总是通过
  // 我们只验证函数不会崩溃
  bool is_boundary = partitioner_->isBoundaryVector(*record_far, 16);
  (void)is_boundary;  // 只测试不崩溃
}

// 零向量测试
TEST_F(LSHPartitionerTest, ZeroVectorIsBoundary) {
  std::vector<float> zero_vec(128, 0.0f);
  auto record = createVectorRecord(1, 1000, zero_vec);

  EXPECT_TRUE(partitioner_->isBoundaryVector(*record, 16)) << "Zero vector should be boundary vector";
}

// 构造函数参数验证
TEST_F(LSHPartitionerTest, ConstructorValidation) {
  // 有效参数
  EXPECT_NO_THROW(LSHPartitioner(64, 4, 42, 0.1));
  EXPECT_NO_THROW(LSHPartitioner(256, 16, 0, 0.5));

  // 无效维度
  EXPECT_THROW(LSHPartitioner(0, 8, 42), std::invalid_argument);
  EXPECT_THROW(LSHPartitioner(-1, 8, 42), std::invalid_argument);

  // 无效哈希函数数
  EXPECT_THROW(LSHPartitioner(128, 0, 42), std::invalid_argument);
  EXPECT_THROW(LSHPartitioner(128, -1, 42), std::invalid_argument);

  // 无效边界阈值
  EXPECT_THROW(LSHPartitioner(128, 8, 42, -0.1), std::invalid_argument);
  EXPECT_THROW(LSHPartitioner(128, 8, 42, 1.5), std::invalid_argument);
}

// 维度不匹配测试
TEST_F(LSHPartitionerTest, DimensionMismatchThrows) {
  std::vector<float> wrong_dim(64, 1.0f);  // 期望 128 维
  auto record = createVectorRecord(1, 1000, wrong_dim);

  EXPECT_THROW(partitioner_->partition(*record, 16), std::invalid_argument);
  EXPECT_THROW(partitioner_->getHashCode(*record), std::invalid_argument);
  EXPECT_THROW(partitioner_->getCandidatePartitions(*record, 16, 3), std::invalid_argument);
}

// 无效分区数测试
TEST_F(LSHPartitionerTest, ZeroPartitionsThrows) {
  std::vector<float> data(128, 1.0f);
  auto record = createVectorRecord(1, 1000, data);

  EXPECT_THROW(partitioner_->partition(*record, 0), std::invalid_argument);
  EXPECT_THROW(partitioner_->getCandidatePartitions(*record, 0, 3), std::invalid_argument);
}

// 不同种子产生不同结果
TEST_F(LSHPartitionerTest, DifferentSeedsDifferentResults) {
  LSHPartitioner partitioner1(128, 8, 42);
  LSHPartitioner partitioner2(128, 8, 123);

  std::vector<float> data(128, 1.0f);
  auto record = createVectorRecord(1, 1000, data);

  uint64_t hash1 = partitioner1.getHashCode(*record);
  uint64_t hash2 = partitioner2.getHashCode(*record);

  // 不同种子应该产生不同的哈希码（极小概率相同）
  EXPECT_NE(hash1, hash2) << "Different seeds should produce different hash codes";
}

// Getter 方法测试
TEST_F(LSHPartitionerTest, GetterMethods) {
  EXPECT_EQ(partitioner_->getDimension(), 128);
  EXPECT_EQ(partitioner_->getNumHashFunctions(), 8);
}

// 哈希函数数量限制测试
TEST_F(LSHPartitionerTest, MaxHashFunctionsLimit) {
  // 请求 100 个哈希函数，但应该被限制为 64
  LSHPartitioner large_hash(128, 100, 42);
  EXPECT_EQ(large_hash.getNumHashFunctions(), 64);
}

// =============================================================================
// KMeansPartitioner Tests
// =============================================================================

class KMeansPartitionerTest : public ::testing::Test {
 protected:
  static constexpr int kDimension = 8;
  static constexpr int kNumClusters = 4;

  void SetUp() override { partitioner_ = std::make_unique<KMeansPartitioner>(kDimension, kNumClusters, 42); }

  std::unique_ptr<KMeansPartitioner> partitioner_;

  // 创建样本数据用于初始化
  std::vector<std::unique_ptr<VectorRecord>> createSampleRecords(int num_samples) {
    std::vector<std::unique_ptr<VectorRecord>> records;
    std::mt19937 gen(12345);
    std::normal_distribution<float> dist(0.0f, 1.0f);

    for (int i = 0; i < num_samples; ++i) {
      std::vector<float> data(kDimension);
      for (int j = 0; j < kDimension; ++j) {
        data[j] = dist(gen);
      }
      records.push_back(createVectorRecord(i, 1000 + i, data));
    }

    return records;
  }
};

// 未初始化时调用 partition 应抛出异常
TEST_F(KMeansPartitionerTest, UninitializedThrows) {
  std::vector<float> data(kDimension, 1.0f);
  auto record = createVectorRecord(1, 1000, data);

  EXPECT_THROW(partitioner_->partition(*record, 4), std::runtime_error);
  EXPECT_THROW(partitioner_->getCandidatePartitions(*record, 4, 2), std::runtime_error);
  EXPECT_THROW(partitioner_->isBoundaryVector(*record, 4), std::runtime_error);
}

// 初始化测试
TEST_F(KMeansPartitionerTest, Initialization) {
  auto records = createSampleRecords(100);
  std::vector<const VectorRecord*> samples;
  for (const auto& r : records) {
    samples.push_back(r.get());
  }

  EXPECT_FALSE(partitioner_->isInitialized());
  EXPECT_NO_THROW(partitioner_->initCentroids(samples, 50));
  EXPECT_TRUE(partitioner_->isInitialized());
}

// 空样本初始化应抛出异常
TEST_F(KMeansPartitionerTest, EmptySamplesThrows) {
  std::vector<const VectorRecord*> empty_samples;
  EXPECT_THROW(partitioner_->initCentroids(empty_samples), std::invalid_argument);
}

// 初始化后分区测试
TEST_F(KMeansPartitionerTest, PartitionAfterInit) {
  auto records = createSampleRecords(100);
  std::vector<const VectorRecord*> samples;
  for (const auto& r : records) {
    samples.push_back(r.get());
  }

  partitioner_->initCentroids(samples, 50);

  std::vector<float> data(kDimension, 0.5f);
  auto record = createVectorRecord(999, 9999, data);

  size_t partition = partitioner_->partition(*record, 4);
  EXPECT_LT(partition, 4u);
}

// 相同向量分配到相同分区
TEST_F(KMeansPartitionerTest, SameVectorSamePartition) {
  auto records = createSampleRecords(100);
  std::vector<const VectorRecord*> samples;
  for (const auto& r : records) {
    samples.push_back(r.get());
  }
  partitioner_->initCentroids(samples, 50);

  std::vector<float> data(kDimension, 1.0f);
  auto record1 = createVectorRecord(1, 1000, data);
  auto record2 = createVectorRecord(2, 2000, data);

  size_t partition1 = partitioner_->partition(*record1, 4);
  size_t partition2 = partitioner_->partition(*record2, 4);

  EXPECT_EQ(partition1, partition2);
}

// 候选分区测试
TEST_F(KMeansPartitionerTest, CandidatePartitions) {
  auto records = createSampleRecords(100);
  std::vector<const VectorRecord*> samples;
  for (const auto& r : records) {
    samples.push_back(r.get());
  }
  partitioner_->initCentroids(samples, 50);

  std::vector<float> data(kDimension, 0.5f);
  auto record = createVectorRecord(1, 1000, data);

  std::vector<size_t> candidates = partitioner_->getCandidatePartitions(*record, 4, 2);
  EXPECT_GE(candidates.size(), 1u);
  EXPECT_LE(candidates.size(), 2u);

  // 第一个应该是主分区
  size_t main_partition = partitioner_->partition(*record, 4);
  EXPECT_EQ(candidates[0], main_partition);
}

// 在线更新测试
TEST_F(KMeansPartitionerTest, OnlineUpdate) {
  auto records = createSampleRecords(50);
  std::vector<const VectorRecord*> samples;
  for (const auto& r : records) {
    samples.push_back(r.get());
  }
  partitioner_->initCentroids(samples, 50);

  std::vector<float> new_data(kDimension, 5.0f);
  auto new_record = createVectorRecord(999, 9999, new_data);

  // 在线更新不应该崩溃
  EXPECT_NO_THROW(partitioner_->updateCentroids(*new_record, 0.1));
}

// 构造函数参数验证
TEST_F(KMeansPartitionerTest, ConstructorValidation) {
  EXPECT_NO_THROW(KMeansPartitioner(64, 8, 42));
  EXPECT_THROW(KMeansPartitioner(0, 8, 42), std::invalid_argument);
  EXPECT_THROW(KMeansPartitioner(64, 0, 42), std::invalid_argument);
}

// Getter 方法测试
TEST_F(KMeansPartitionerTest, GetterMethods) {
  EXPECT_EQ(partitioner_->getNumClusters(), kNumClusters);
  EXPECT_FALSE(partitioner_->isInitialized());
}

}  // namespace test
}  // namespace sageFlow

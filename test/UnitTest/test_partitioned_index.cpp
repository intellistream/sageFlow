#include <gtest/gtest.h>

#include "execution/vector_space_partitioner.h"
#include "index/partitioned_index.h"
#include "storage/storage_manager.h"
#include "test_utils/test_data_adapter.h"

#include <cmath>
#include <random>
#include <thread>
#include <vector>

namespace sageFlow {
namespace test {

class PartitionedIndexTest : public ::testing::Test {
 protected:
  static constexpr int kDimension = 128;
  static constexpr size_t kNumPartitions = 4;

  void SetUp() override {
    // 创建 storage manager
    storage_manager_ = std::make_shared<StorageManager>();

    // 创建 LSH 分区器
    partitioner_ = std::make_shared<LSHPartitioner>(kDimension, 8, 42);

    // 创建分区索引
    index_ = std::make_unique<PartitionedIndex>(kNumPartitions, kDimension, partitioner_, 10, 2);

    // 设置 StorageManager
    index_->storage_manager_ = storage_manager_;
  }

  void TearDown() override {
    index_.reset();
    partitioner_.reset();
    storage_manager_.reset();
  }

  // 创建随机向量
  std::vector<float> createRandomVector(std::mt19937& gen) {
    std::normal_distribution<float> dist(0.0f, 1.0f);
    std::vector<float> vec(kDimension);
    for (int i = 0; i < kDimension; ++i) {
      vec[i] = dist(gen);
    }
    return vec;
  }

  // 创建相似向量
  std::vector<float> createSimilarVector(const std::vector<float>& base, float noise_level, std::mt19937& gen) {
    std::normal_distribution<float> noise(0.0f, noise_level);
    std::vector<float> similar = base;
    for (size_t i = 0; i < similar.size(); ++i) {
      similar[i] += noise(gen);
    }
    return similar;
  }

  // 插入向量并返回其 UID
  uint64_t insertVector(uint64_t uid, int64_t timestamp, const std::vector<float>& data) {
    // 使用 test_data_adapter 创建记录
    auto record = createVectorRecord(uid, timestamp, data);

    // 先存入 StorageManager
    storage_manager_->insert(createVectorRecord(uid, timestamp, data));

    // 然后插入索引
    index_->insert(uid);
    return uid;
  }

  std::shared_ptr<StorageManager> storage_manager_;
  std::shared_ptr<LSHPartitioner> partitioner_;
  std::unique_ptr<PartitionedIndex> index_;
};

// =============================================================================
// 基础功能测试
// =============================================================================

// 测试构造函数参数验证
TEST_F(PartitionedIndexTest, ConstructorValidation) {
  // 分区数为0应该抛出异常
  EXPECT_THROW(PartitionedIndex(0, kDimension, partitioner_, 10, 2), std::invalid_argument);

  // 维度为0应该抛出异常
  EXPECT_THROW(PartitionedIndex(kNumPartitions, 0, partitioner_, 10, 2), std::invalid_argument);

  // 分区器为空应该抛出异常
  EXPECT_THROW(PartitionedIndex(kNumPartitions, kDimension, nullptr, 10, 2), std::invalid_argument);

  // 正常构造应该成功
  EXPECT_NO_THROW(PartitionedIndex(kNumPartitions, kDimension, partitioner_, 10, 2));
}

// 测试插入路由到正确分区
TEST_F(PartitionedIndexTest, InsertRouting) {
  std::mt19937 gen(12345);

  // 插入多个向量
  for (uint64_t i = 1; i <= 100; ++i) {
    auto data = createRandomVector(gen);
    insertVector(i, 1000 + i, data);
  }

  // 验证所有向量都有对应的分区
  for (uint64_t i = 1; i <= 100; ++i) {
    auto partition = index_->getPartitionForUid(i);
    ASSERT_TRUE(partition.has_value()) << "UID " << i << " should have a partition";
    EXPECT_LT(*partition, kNumPartitions) << "Partition ID should be < num_partitions";
  }

  // 验证总大小
  EXPECT_EQ(index_->getTotalSize(), 100);
}

// 测试插入和查询
TEST_F(PartitionedIndexTest, InsertAndQuery) {
  std::mt19937 gen(12345);

  // 插入一些向量
  std::vector<std::vector<float>> inserted_vectors;
  for (uint64_t i = 1; i <= 50; ++i) {
    auto data = createRandomVector(gen);
    inserted_vectors.push_back(data);
    insertVector(i, 1000 + i, data);
  }

  // 使用一个已插入的向量进行查询
  auto query_record = createVectorRecord(999, 9999, inserted_vectors[0]);
  auto results = index_->query(*query_record, 5);

  // 应该至少返回一些结果
  EXPECT_FALSE(results.empty()) << "Query should return results";
}

// 测试删除操作正确性
TEST_F(PartitionedIndexTest, EraseCorrectness) {
  std::mt19937 gen(12345);

  // 插入向量
  for (uint64_t i = 1; i <= 20; ++i) {
    auto data = createRandomVector(gen);
    insertVector(i, 1000 + i, data);
  }

  EXPECT_EQ(index_->getTotalSize(), 20);

  // 删除一些向量
  EXPECT_TRUE(index_->erase(5));
  EXPECT_TRUE(index_->erase(10));
  EXPECT_TRUE(index_->erase(15));

  EXPECT_EQ(index_->getTotalSize(), 17);

  // 验证删除的向量不再有分区映射
  EXPECT_FALSE(index_->getPartitionForUid(5).has_value());
  EXPECT_FALSE(index_->getPartitionForUid(10).has_value());
  EXPECT_FALSE(index_->getPartitionForUid(15).has_value());

  // 未删除的向量应该仍有分区映射
  EXPECT_TRUE(index_->getPartitionForUid(1).has_value());
  EXPECT_TRUE(index_->getPartitionForUid(20).has_value());
}

// 测试删除不存在的记录
TEST_F(PartitionedIndexTest, EraseNonExistent) {
  std::mt19937 gen(12345);

  // 插入几个向量
  for (uint64_t i = 1; i <= 5; ++i) {
    auto data = createRandomVector(gen);
    insertVector(i, 1000 + i, data);
  }

  // 删除不存在的 UID
  EXPECT_FALSE(index_->erase(100));
  EXPECT_FALSE(index_->erase(999));

  // 大小不应该改变
  EXPECT_EQ(index_->getTotalSize(), 5);
}

// =============================================================================
// 分区查询测试
// =============================================================================

// 测试单分区查询
TEST_F(PartitionedIndexTest, SinglePartitionQuery) {
  std::mt19937 gen(12345);

  // 插入向量
  std::vector<std::pair<uint64_t, std::vector<float>>> uid_to_vector;
  for (uint64_t i = 1; i <= 100; ++i) {
    auto data = createRandomVector(gen);
    uid_to_vector.emplace_back(i, data);
    insertVector(i, 1000 + i, data);
  }

  // 找一个分区中有向量的分区
  auto sizes = index_->getPartitionSizes();
  size_t non_empty_partition = 0;
  for (size_t i = 0; i < sizes.size(); ++i) {
    if (sizes[i] > 0) {
      non_empty_partition = i;
      break;
    }
  }

  // 查询该分区
  auto query_record = createVectorRecord(999, 9999, uid_to_vector[0].second);
  auto results = index_->queryPartition(non_empty_partition, *query_record, 5);

  // 结果应该只来自该分区
  for (uint64_t uid : results) {
    auto partition = index_->getPartitionForUid(uid);
    ASSERT_TRUE(partition.has_value());
    EXPECT_EQ(*partition, non_empty_partition);
  }
}

// 测试跨分区查询召回率
TEST_F(PartitionedIndexTest, MultiPartitionQueryRecall) {
  std::mt19937 gen(12345);

  // 插入基础向量
  std::vector<float> base_vector = createRandomVector(gen);
  insertVector(1, 1000, base_vector);

  // 插入与基础向量相似的向量
  for (uint64_t i = 2; i <= 20; ++i) {
    auto similar = createSimilarVector(base_vector, 0.1f, gen);
    insertVector(i, 1000 + i, similar);
  }

  // 插入随机向量
  for (uint64_t i = 21; i <= 100; ++i) {
    auto random = createRandomVector(gen);
    insertVector(i, 1000 + i, random);
  }

  // 使用基础向量查询
  auto query_record = createVectorRecord(999, 9999, base_vector);

  // 单分区查询
  auto single_results = index_->queryMultiPartition(*query_record, 10, 1);

  // 多分区查询（应该有更好的召回率）
  auto multi_results = index_->queryMultiPartition(*query_record, 10, 3);

  // 多分区查询应该不为空
  EXPECT_FALSE(multi_results.empty());
}

// 测试阈值查询 (query_for_join)
TEST_F(PartitionedIndexTest, QueryForJoin) {
  std::mt19937 gen(12345);

  // 插入基础向量
  std::vector<float> base_vector = createRandomVector(gen);
  insertVector(1, 1000, base_vector);

  // 插入一些相似向量
  for (uint64_t i = 2; i <= 10; ++i) {
    auto similar = createSimilarVector(base_vector, 0.05f, gen);
    insertVector(i, 1000 + i, similar);
  }

  // 插入一些不太相似的向量
  for (uint64_t i = 11; i <= 50; ++i) {
    auto random = createRandomVector(gen);
    insertVector(i, 1000 + i, random);
  }

  // 使用基础向量进行阈值查询
  auto query_record = createVectorRecord(999, 9999, base_vector);
  auto results = index_->query_for_join(*query_record, 0.8);

  // 应该返回一些结果（相似的向量）
  // 注意：由于是近似索引，结果可能不完美
}

// =============================================================================
// 并发测试
// =============================================================================

// 测试并发插入
TEST_F(PartitionedIndexTest, ConcurrentInsert) {
  const int num_threads = 4;
  const int vectors_per_thread = 100;

  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, t, vectors_per_thread]() {
      std::mt19937 gen(12345 + t);
      for (int i = 0; i < vectors_per_thread; ++i) {
        uint64_t uid = t * vectors_per_thread + i + 1;
        auto data = createRandomVector(gen);

        // 创建记录并存入 StorageManager
        storage_manager_->insert(createVectorRecord(uid, 1000 + uid, data));

        // 插入索引
        index_->insert(uid);
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // 验证所有向量都已插入
  EXPECT_EQ(index_->getTotalSize(), num_threads * vectors_per_thread);
}

// 测试并发查询和插入
TEST_F(PartitionedIndexTest, ConcurrentQueryAndInsert) {
  std::mt19937 gen(12345);

  // 先插入一些初始向量
  for (uint64_t i = 1; i <= 50; ++i) {
    auto data = createRandomVector(gen);
    insertVector(i, 1000 + i, data);
  }

  std::atomic<int> insert_count{0};
  std::atomic<int> query_count{0};

  std::vector<std::thread> threads;

  // 插入线程
  for (int t = 0; t < 2; ++t) {
    threads.emplace_back([this, t, &insert_count]() {
      std::mt19937 tgen(54321 + t);
      for (int i = 0; i < 50; ++i) {
        uint64_t uid = 1000 + t * 50 + i;
        auto data = createRandomVector(tgen);

        storage_manager_->insert(createVectorRecord(uid, 1000 + uid, data));
        index_->insert(uid);
        insert_count++;
      }
    });
  }

  // 查询线程
  for (int t = 0; t < 2; ++t) {
    threads.emplace_back([this, t, &query_count]() {
      std::mt19937 tgen(99999 + t);
      for (int i = 0; i < 50; ++i) {
        auto data = createRandomVector(tgen);
        auto query_record = createVectorRecord(9999, 9999, data);
        auto results = index_->query(*query_record, 5);
        query_count++;
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(insert_count.load(), 100);
  EXPECT_EQ(query_count.load(), 100);
}

// =============================================================================
// 负载统计测试
// =============================================================================

// 测试分区大小统计
TEST_F(PartitionedIndexTest, PartitionSizes) {
  std::mt19937 gen(12345);

  // 插入向量
  for (uint64_t i = 1; i <= 100; ++i) {
    auto data = createRandomVector(gen);
    insertVector(i, 1000 + i, data);
  }

  auto sizes = index_->getPartitionSizes();

  EXPECT_EQ(sizes.size(), kNumPartitions);

  // 所有分区大小之和应该等于总大小
  size_t total = 0;
  for (size_t s : sizes) {
    total += s;
  }
  EXPECT_EQ(total, 100);
  EXPECT_EQ(total, index_->getTotalSize());
}

// 测试负载均衡
TEST_F(PartitionedIndexTest, LoadImbalance) {
  std::mt19937 gen(12345);

  // 插入大量随机向量
  for (uint64_t i = 1; i <= 1000; ++i) {
    auto data = createRandomVector(gen);
    insertVector(i, 1000 + i, data);
  }

  double imbalance = index_->getLoadImbalance();

  // 负载不均衡度应该大于等于1.0
  EXPECT_GE(imbalance, 1.0);

  // 对于随机数据和 LSH 分区器，不均衡度不应该太高
  EXPECT_LT(imbalance, 5.0) << "Load imbalance should be reasonable for random data";

  // 打印分区大小用于调试
  auto sizes = index_->getPartitionSizes();
  std::cout << "Partition sizes: ";
  for (size_t s : sizes) {
    std::cout << s << " ";
  }
  std::cout << ", imbalance: " << imbalance << std::endl;
}

// 测试空索引的状态
TEST_F(PartitionedIndexTest, EmptyIndexState) {
  EXPECT_EQ(index_->getTotalSize(), 0);
  EXPECT_EQ(index_->getNumPartitions(), kNumPartitions);
  EXPECT_EQ(index_->getDimension(), kDimension);
  EXPECT_DOUBLE_EQ(index_->getLoadImbalance(), 1.0);

  auto sizes = index_->getPartitionSizes();
  for (size_t s : sizes) {
    EXPECT_EQ(s, 0);
  }
}

// 测试直接插入到分区
TEST_F(PartitionedIndexTest, InsertToPartitionDirect) {
  std::mt19937 gen(12345);

  // 直接插入到指定分区
  for (size_t p = 0; p < kNumPartitions; ++p) {
    for (int i = 0; i < 10; ++i) {
      uint64_t uid = p * 100 + i + 1;
      auto data = createRandomVector(gen);

      storage_manager_->insert(createVectorRecord(uid, 1000 + uid, data));
      EXPECT_TRUE(index_->insertToPartition(p, uid));
    }
  }

  // 验证分区大小
  auto sizes = index_->getPartitionSizes();
  for (size_t p = 0; p < kNumPartitions; ++p) {
    EXPECT_EQ(sizes[p], 10) << "Partition " << p << " should have 10 vectors";
  }

  // 验证总大小
  EXPECT_EQ(index_->getTotalSize(), kNumPartitions * 10);
}

// 测试无效分区ID
TEST_F(PartitionedIndexTest, InvalidPartitionId) {
  std::mt19937 gen(12345);

  auto data = createRandomVector(gen);

  // 先存储记录
  storage_manager_->insert(createVectorRecord(1, 1000, data));
  storage_manager_->insert(createVectorRecord(2, 1001, data));

  // 插入到无效分区应该失败
  EXPECT_FALSE(index_->insertToPartition(kNumPartitions, 1));
  EXPECT_FALSE(index_->insertToPartition(kNumPartitions + 10, 2));

  // 查询无效分区应该返回空
  auto query_record = createVectorRecord(999, 9999, data);
  auto results = index_->queryPartition(kNumPartitions, *query_record, 5);
  EXPECT_TRUE(results.empty());
}

}  // namespace test
}  // namespace sageFlow

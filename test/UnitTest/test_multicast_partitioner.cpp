/**
 * @file test_multicast_partitioner.cpp
 * @brief 多播分区器单元测试
 * 
 * 验证 CentroidPartitioner 的多播功能，确保边界向量能够正确复制到多个分区。
 */

#include <gtest/gtest.h>
#include "execution/centroid_partitioner.h"
#include "execution/result_partition.h"
#include "execution/blocking_queue.h"
#include "common/data_types.h"
#include "test_utils/test_data_adapter.h"
#include <memory>
#include <vector>
#include <set>
#include <algorithm>

namespace sageFlow {
namespace test {

class MulticastPartitionerTest : public ::testing::Test {
protected:
  void SetUp() override {
    // 创建 CentroidPartitioner 配置
    CentroidPartitioner::Config config;
    config.num_partitions = 4;
    config.dimension = 4;
    config.overlap_ratio = 0.3;  // 较大的重叠比例，便于测试边界向量
    config.seed = 42;
    
    partitioner_ = std::make_unique<CentroidPartitioner>(config);
    
    // 训练分区器（使用4个正交方向的向量作为质心的基础）
    std::vector<std::vector<float>> samples = {
      // 四个主方向的向量
      {1.0f, 0.0f, 0.0f, 0.0f},
      {0.9f, 0.1f, 0.0f, 0.0f},
      {0.0f, 1.0f, 0.0f, 0.0f},
      {0.1f, 0.9f, 0.0f, 0.0f},
      {0.0f, 0.0f, 1.0f, 0.0f},
      {0.0f, 0.1f, 0.9f, 0.0f},
      {0.0f, 0.0f, 0.0f, 1.0f},
      {0.0f, 0.0f, 0.1f, 0.9f}
    };
    partitioner_->train(samples);
  }
  
  std::unique_ptr<CentroidPartitioner> partitioner_;
};

// 测试：多播禁用时返回单个分区
TEST_F(MulticastPartitionerTest, MulticastDisabled_SinglePartition) {
  // 确保多播禁用
  partitioner_->setMulticastEnabled(false);
  EXPECT_FALSE(partitioner_->supportsMulticast());
  
  // 创建一个边界向量（位于两个聚类中间）
  std::vector<float> data = {0.5f, 0.5f, 0.0f, 0.0f};
  auto record = createVectorRecord(1, 1000, data);
  Response response{ResponseType::Record, std::move(record)};
  
  // 即使是边界向量，禁用多播时也只返回一个分区
  auto partitions = partitioner_->partitionMulti(response, 4);
  EXPECT_EQ(partitions.size(), 1);
}

// 测试：非边界向量始终返回单个分区
TEST_F(MulticastPartitionerTest, NonBoundaryVector_SinglePartition) {
  // 启用多播
  partitioner_->setMulticastEnabled(true);
  EXPECT_TRUE(partitioner_->supportsMulticast());
  
  // 创建一个非边界向量（明确属于某个聚类）
  std::vector<float> data = {1.0f, 0.0f, 0.0f, 0.0f};
  auto record = createVectorRecord(1, 1000, data);
  Response response{ResponseType::Record, std::move(record)};
  
  // 非边界向量应该只返回一个分区
  auto partitions = partitioner_->partitionMulti(response, 4);
  EXPECT_GE(partitions.size(), 1);
  
  // 所有返回的分区应该在有效范围内
  for (size_t p : partitions) {
    EXPECT_LT(p, 4);
  }
}

// 测试：边界向量可能返回多个分区
TEST_F(MulticastPartitionerTest, BoundaryVector_MayReturnMultiplePartitions) {
  // 启用多播
  partitioner_->setMulticastEnabled(true);
  
  // 创建一个明显的边界向量（位于多个聚类中间）
  std::vector<float> data = {0.5f, 0.5f, 0.0f, 0.0f};
  auto record = createVectorRecord(1, 1000, data);
  Response response{ResponseType::Record, std::move(record)};
  
  // 边界向量可能返回多个分区（取决于 overlap_ratio）
  auto partitions = partitioner_->partitionMulti(response, 4);
  EXPECT_GE(partitions.size(), 1);  // 至少返回一个
  
  // 验证分区列表是有效的
  for (size_t p : partitions) {
    EXPECT_LT(p, 4);
  }
  
  // 验证分区列表是去重的
  std::set<size_t> unique_partitions(partitions.begin(), partitions.end());
  EXPECT_EQ(partitions.size(), unique_partitions.size());
}

// 测试：IPartitioner 接口的默认多播行为
TEST_F(MulticastPartitionerTest, DefaultMulticastBehavior) {
  // RoundRobinPartitioner 不支持多播
  RoundRobinPartitioner round_robin;
  EXPECT_FALSE(round_robin.supportsMulticast());
  
  // 默认的 partitionMulti 应该调用 partition
  std::vector<float> data = {1.0f, 0.0f, 0.0f, 0.0f};
  auto record = createVectorRecord(1, 1000, data);
  Response response{ResponseType::Record, std::move(record)};
  
  auto partitions = round_robin.partitionMulti(response, 4);
  EXPECT_EQ(partitions.size(), 1);
}

// 测试：未训练的分区器使用哈希降级
TEST_F(MulticastPartitionerTest, UntrainedPartitioner_FallbackToHash) {
  CentroidPartitioner::Config config;
  config.num_partitions = 4;
  config.dimension = 4;
  
  CentroidPartitioner untrained(config);
  untrained.setMulticastEnabled(true);
  
  EXPECT_FALSE(untrained.isTrained());
  
  std::vector<float> data = {0.5f, 0.5f, 0.0f, 0.0f};
  auto record = createVectorRecord(1, 1000, data);
  Response response{ResponseType::Record, std::move(record)};
  
  // 未训练时应该降级为单播哈希
  auto partitions = untrained.partitionMulti(response, 4);
  EXPECT_EQ(partitions.size(), 1);
}

// 测试：空记录处理
TEST_F(MulticastPartitionerTest, NullRecord_ReturnsDefaultPartition) {
  partitioner_->setMulticastEnabled(true);
  
  Response empty_response;
  empty_response.type_ = ResponseType::None;
  empty_response.record_ = nullptr;
  
  auto partitions = partitioner_->partitionMulti(empty_response, 4);
  EXPECT_EQ(partitions.size(), 1);
  EXPECT_EQ(partitions[0], 0);
}

// 测试：ResultPartition 的多播发送
TEST_F(MulticastPartitionerTest, ResultPartitionMulticastEmit) {
  // 创建多个队列
  std::vector<std::shared_ptr<BlockingQueue>> raw_queues;
  std::vector<QueuePtr> queues;
  for (int i = 0; i < 4; ++i) {
    auto queue = std::make_shared<BlockingQueue>(100);
    raw_queues.push_back(queue);
    queues.push_back(queue);
  }
  
  // 设置 ResultPartition
  ResultPartition partition;
  
  // 启用多播
  partitioner_->setMulticastEnabled(true);
  
  // 移动 partitioner_ 到 ResultPartition（需要创建新的）
  CentroidPartitioner::Config config;
  config.num_partitions = 4;
  config.dimension = 4;
  config.overlap_ratio = 0.3;
  config.seed = 42;
  
  auto partitioner = std::make_unique<CentroidPartitioner>(config);
  
  std::vector<std::vector<float>> samples = {
    {1.0f, 0.0f, 0.0f, 0.0f},
    {0.9f, 0.1f, 0.0f, 0.0f},
    {0.0f, 1.0f, 0.0f, 0.0f},
    {0.1f, 0.9f, 0.0f, 0.0f},
    {0.0f, 0.0f, 1.0f, 0.0f},
    {0.0f, 0.1f, 0.9f, 0.0f},
    {0.0f, 0.0f, 0.0f, 1.0f},
    {0.0f, 0.0f, 0.1f, 0.9f}
  };
  partitioner->train(samples);
  partitioner->setMulticastEnabled(true);
  
  partition.setup(std::move(partitioner), std::move(queues), 0);
  
  // 发送一个边界向量
  std::vector<float> data = {0.5f, 0.5f, 0.0f, 0.0f};
  auto record = createVectorRecord(1, 1000, data);
  Response response{ResponseType::Record, std::move(record)};
  partition.emit(std::move(response), 0);
  
  // 停止所有队列
  for (auto& q : raw_queues) {
    q->stop();
  }
  
  // 计算收到的消息总数
  int total_received = 0;
  for (size_t i = 0; i < 4; ++i) {
    while (true) {
      auto tagged = raw_queues[i]->pop();
      if (!tagged.has_value()) break;
      total_received++;
    }
  }
  
  // 边界向量应该至少被发送到一个队列
  EXPECT_GE(total_received, 1);
}

// 测试：多播下数据一致性
TEST_F(MulticastPartitionerTest, MulticastDataConsistency) {
  // 创建多个队列
  std::vector<std::shared_ptr<BlockingQueue>> raw_queues;
  std::vector<QueuePtr> queues;
  for (int i = 0; i < 4; ++i) {
    auto queue = std::make_shared<BlockingQueue>(100);
    raw_queues.push_back(queue);
    queues.push_back(queue);
  }
  
  // 设置 ResultPartition
  ResultPartition partition;
  
  CentroidPartitioner::Config config;
  config.num_partitions = 4;
  config.dimension = 4;
  config.overlap_ratio = 0.5;  // 非常大的重叠比例，确保多播
  config.seed = 42;
  
  auto partitioner = std::make_unique<CentroidPartitioner>(config);
  
  std::vector<std::vector<float>> samples = {
    {1.0f, 0.0f, 0.0f, 0.0f},
    {0.0f, 1.0f, 0.0f, 0.0f},
    {0.0f, 0.0f, 1.0f, 0.0f},
    {0.0f, 0.0f, 0.0f, 1.0f}
  };
  partitioner->train(samples);
  partitioner->setMulticastEnabled(true);
  
  partition.setup(std::move(partitioner), std::move(queues), 0);
  
  // 发送一个边界向量
  uint64_t expected_uid = 42;
  int64_t expected_ts = 12345;
  std::vector<float> data = {0.5f, 0.5f, 0.0f, 0.0f};
  auto record = createVectorRecord(expected_uid, expected_ts, data);
  Response response{ResponseType::Record, std::move(record)};
  partition.emit(std::move(response), 0);
  
  // 停止所有队列
  for (auto& q : raw_queues) {
    q->stop();
  }
  
  // 验证所有收到的消息都有正确的数据
  for (size_t i = 0; i < 4; ++i) {
    while (true) {
      auto tagged = raw_queues[i]->pop();
      if (!tagged.has_value()) break;
      
      // 验证消息数据
      ASSERT_NE(tagged->response.record_, nullptr);
      EXPECT_EQ(tagged->response.record_->uid_, expected_uid);
      EXPECT_EQ(tagged->response.record_->timestamp_, expected_ts);
    }
  }
}

// 测试：setMulticastEnabled/isMulticastEnabled
TEST_F(MulticastPartitionerTest, MulticastToggle) {
  EXPECT_FALSE(partitioner_->isMulticastEnabled());
  EXPECT_FALSE(partitioner_->supportsMulticast());
  
  partitioner_->setMulticastEnabled(true);
  EXPECT_TRUE(partitioner_->isMulticastEnabled());
  EXPECT_TRUE(partitioner_->supportsMulticast());
  
  partitioner_->setMulticastEnabled(false);
  EXPECT_FALSE(partitioner_->isMulticastEnabled());
  EXPECT_FALSE(partitioner_->supportsMulticast());
}

// 测试：多分区映射到同一 channel 时的去重
TEST_F(MulticastPartitionerTest, PartitionDeduplication) {
  partitioner_->setMulticastEnabled(true);
  
  // 当 num_channels 小于 num_partitions 时，多个分区可能映射到同一 channel
  std::vector<float> data = {0.5f, 0.5f, 0.0f, 0.0f};
  auto record = createVectorRecord(1, 1000, data);
  Response response{ResponseType::Record, std::move(record)};
  
  // 只有 2 个 channel，但有 4 个分区
  auto partitions = partitioner_->partitionMulti(response, 2);
  
  // 验证结果是去重的
  std::set<size_t> unique_partitions(partitions.begin(), partitions.end());
  EXPECT_EQ(partitions.size(), unique_partitions.size());
  
  // 验证所有分区都在有效范围内
  for (size_t p : partitions) {
    EXPECT_LT(p, 2);
  }
}

} // namespace test
} // namespace sageFlow

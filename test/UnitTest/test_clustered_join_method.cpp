/**
 * @file test_clustered_join_method.cpp
 * @brief ClusteredJoinMethod 单元测试 - 方案 A（独立索引）
 * 
 * 测试 Task 1.2 重写的 ClusteredJoinMethod:
 * 1. 构造函数和初始化
 * 2. Owner-Computes 去重规则
 * 3. 独立索引创建
 * 4. ExecuteEager 基本功能
 * 5. 窗口状态管理（addRecord, evictExpired）
 */

#include <gtest/gtest.h>

#include <memory>
#include <random>
#include <set>
#include <unordered_set>
#include <vector>

#include "concurrency/concurrency_manager.h"
#include "execution/runtime_context.h"
#include "operator/join_operator_methods/clustered_join_method.h"
#include "storage/storage_manager.h"
#include "test_utils/test_data_adapter.h"
#include "utils/logger.h"

namespace sageFlow {
namespace test {

// ==================== ClusteredJoinMethod 测试（方案 A） ====================

class ClusteredJoinMethodTestV2 : public ::testing::Test {
 protected:
  void SetUp() override {
    auto storage = std::make_shared<StorageManager>();
    concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage);

    // 配置
    config_.similarity_threshold = 0.8;
    config_.dimension = 8;  // 小维度便于测试
    config_.window_size_ms = 10000;
    config_.index_type = ClusteredIndexType::BRUTEFORCE;
    config_.ivf_nlist = 10;
    config_.ivf_nprobes = 5;
  }

  /**
   * @brief 生成随机向量
   */
  std::vector<float> generateRandomVector(int dim, unsigned seed) {
    std::mt19937 gen(seed);
    std::normal_distribution<float> dist(0.0f, 1.0f);

    std::vector<float> vec(dim);
    float norm = 0.0f;
    for (int i = 0; i < dim; ++i) {
      vec[i] = dist(gen);
      norm += vec[i] * vec[i];
    }
    // 归一化
    norm = std::sqrt(norm);
    for (int i = 0; i < dim; ++i) {
      vec[i] /= norm;
    }
    return vec;
  }

  /**
   * @brief 生成相似向量（与 base 的相似度 > threshold）
   */
  std::vector<float> generateSimilarVector(const std::vector<float>& base, 
                                           double target_similarity,
                                           unsigned seed) {
    std::mt19937 gen(seed);
    std::normal_distribution<float> noise_dist(0.0f, 0.1f);

    std::vector<float> vec(base.size());
    float mix_ratio = static_cast<float>(std::sqrt(target_similarity));
    
    for (size_t i = 0; i < base.size(); ++i) {
      vec[i] = mix_ratio * base[i] + (1 - mix_ratio) * noise_dist(gen);
    }
    
    // 归一化
    float norm = 0.0f;
    for (size_t i = 0; i < vec.size(); ++i) {
      norm += vec[i] * vec[i];
    }
    norm = std::sqrt(norm);
    for (size_t i = 0; i < vec.size(); ++i) {
      vec[i] /= norm;
    }
    return vec;
  }

  /**
   * @brief 计算余弦相似度
   */
  double cosineSimilarity(const std::vector<float>& a, const std::vector<float>& b) {
    double dot = 0.0, norm_a = 0.0, norm_b = 0.0;
    for (size_t i = 0; i < a.size(); ++i) {
      dot += a[i] * b[i];
      norm_a += a[i] * a[i];
      norm_b += b[i] * b[i];
    }
    return dot / (std::sqrt(norm_a) * std::sqrt(norm_b));
  }

  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  ClusteredJoinMethod::Config config_;
};

// ==================== 构造和初始化测试 ====================

TEST_F(ClusteredJoinMethodTestV2, ConfigConstruction) {
  ClusteredJoinMethod method(config_);

  EXPECT_EQ(method.getName(), "ClusteredJoin");
  EXPECT_DOUBLE_EQ(method.getConfig().similarity_threshold, config_.similarity_threshold);
  EXPECT_EQ(method.getConfig().dimension, config_.dimension);
  EXPECT_FALSE(method.isInitialized());
}

TEST_F(ClusteredJoinMethodTestV2, SimpleConstruction) {
  ClusteredJoinMethod method(0.75, 64);

  EXPECT_EQ(method.getName(), "ClusteredJoin");
  EXPECT_DOUBLE_EQ(method.getConfig().similarity_threshold, 0.75);
  EXPECT_EQ(method.getConfig().dimension, 64);
  EXPECT_FALSE(method.isInitialized());
}

TEST_F(ClusteredJoinMethodTestV2, InitializeSingleSubtask) {
  ClusteredJoinMethod method(config_);
  RuntimeContext context(0, 1);  // subtask 0 of 1
  
  method.initialize(context, concurrency_manager_);

  EXPECT_TRUE(method.isInitialized());
  EXPECT_EQ(method.getSubtaskIndex(), 0);
  EXPECT_EQ(method.getParallelism(), 1);
  EXPECT_GE(method.getLeftIndexId(), 0);
  EXPECT_GE(method.getRightIndexId(), 0);
}

TEST_F(ClusteredJoinMethodTestV2, InitializeMultipleSubtasks) {
  // 测试多个 subtask 创建独立索引
  constexpr size_t parallelism = 4;
  std::vector<std::unique_ptr<ClusteredJoinMethod>> methods;
  std::set<int> all_left_ids, all_right_ids;
  
  for (size_t i = 0; i < parallelism; ++i) {
    methods.push_back(std::make_unique<ClusteredJoinMethod>(config_));
    RuntimeContext context(i, parallelism);
    methods.back()->initialize(context, concurrency_manager_);
    
    EXPECT_TRUE(methods.back()->isInitialized());
    EXPECT_EQ(methods.back()->getSubtaskIndex(), i);
    
    // 收集索引 ID
    all_left_ids.insert(methods.back()->getLeftIndexId());
    all_right_ids.insert(methods.back()->getRightIndexId());
  }
  
  // 验证每个 subtask 有独立的索引
  EXPECT_EQ(all_left_ids.size(), parallelism);
  EXPECT_EQ(all_right_ids.size(), parallelism);
}

TEST_F(ClusteredJoinMethodTestV2, DoubleInitializeIgnored) {
  ClusteredJoinMethod method(config_);
  RuntimeContext context(0, 2);
  
  method.initialize(context, concurrency_manager_);
  int first_left_id = method.getLeftIndexId();
  
  // 再次初始化应该被忽略
  RuntimeContext context2(1, 2);
  method.initialize(context2, concurrency_manager_);
  
  // 应该保持第一次初始化的值
  EXPECT_EQ(method.getLeftIndexId(), first_left_id);
  EXPECT_EQ(method.getSubtaskIndex(), 0);  // 未改变
}

// ==================== Owner-Computes 规则测试 ====================

TEST_F(ClusteredJoinMethodTestV2, OwnerComputesDeduplication) {
  // 测试 Owner-Computes 去重规则
  // 设置: parallelism = 4, 模拟相同记录在多个 subtask 上
  constexpr size_t parallelism = 4;
  
  // 创建相似的向量对
  auto base_vec = generateRandomVector(config_.dimension, 100);
  auto similar_vec = generateSimilarVector(base_vec, 0.95, 200);
  
  uint64_t left_uid = 100;
  uint64_t right_uid = 101;
  int64_t timestamp = 1000;
  
  // 预计算 owner: min(100, 101) % 4 = 100 % 4 = 0
  size_t expected_owner = std::min(left_uid, right_uid) % parallelism;
  EXPECT_EQ(expected_owner, 0u);
  
  // 创建 4 个 subtask，每个都持有相同的数据（模拟多播）
  std::vector<std::unique_ptr<ClusteredJoinMethod>> methods;
  int total_outputs = 0;
  
  for (size_t i = 0; i < parallelism; ++i) {
    methods.push_back(std::make_unique<ClusteredJoinMethod>(config_));
    RuntimeContext context(i, parallelism);
    methods.back()->initialize(context, concurrency_manager_);
    
    // 添加相同的记录到每个 subtask
    auto left_record = createVectorRecord(left_uid, timestamp, base_vec);
    auto right_record = createVectorRecord(right_uid, timestamp, similar_vec);
    
    methods.back()->addRecord(std::move(left_record), 0);   // left stream
    methods.back()->addRecord(std::move(right_record), 1);  // right stream
    
    // 用左侧记录查询右侧
    auto query = createVectorRecord(left_uid, timestamp, base_vec);
    auto results = methods.back()->ExecuteEager(*query, 0);
    
    total_outputs += static_cast<int>(results.size());
    
    // 验证只有 owner subtask 产生输出
    if (i == expected_owner) {
      // subtask 0 应该是 owner，应该有输出（如果相似度足够）
      SAGEFLOW_LOG_INFO("TEST", "Subtask {} (owner): {} results", i, results.size());
    } else {
      // 其他 subtask 不应该输出这个匹配对
      EXPECT_EQ(results.size(), 0u) 
          << "Subtask " << i << " should not output, but got " << results.size() << " results";
    }
  }
  
  // 总输出应该 <= 1（去重后）
  EXPECT_LE(total_outputs, 1);
}

// ==================== 状态管理测试 ====================

TEST_F(ClusteredJoinMethodTestV2, AddRecordAndQuery) {
  ClusteredJoinMethod method(config_);
  RuntimeContext context(0, 1);
  method.initialize(context, concurrency_manager_);
  
  // 创建相似向量对
  auto vec1 = generateRandomVector(config_.dimension, 1);
  auto vec2 = generateSimilarVector(vec1, 0.95, 2);
  
  auto record1 = createVectorRecord(1, 1000, vec1);
  auto record2 = createVectorRecord(2, 1001, vec2);
  
  // 添加 record2 到右侧
  method.addRecord(std::make_unique<VectorRecord>(*record2), 1);
  
  EXPECT_EQ(method.getRightWindowSize(), 1u);
  EXPECT_EQ(method.getLeftWindowSize(), 0u);
  
  // 用 record1 查询应该找到 record2
  auto results = method.ExecuteEager(*record1, 0);
  
  SAGEFLOW_LOG_INFO("TEST", "Query found {} results", results.size());
  // 在 p=1 情况下，任何 owner 计算都会是 subtask 0，所以应该有输出
}

TEST_F(ClusteredJoinMethodTestV2, EvictExpired) {
  ClusteredJoinMethod method(config_);
  RuntimeContext context(0, 1);
  method.initialize(context, concurrency_manager_);
  
  auto vec = generateRandomVector(config_.dimension, 1);
  
  // 添加几条记录，时间戳跨越窗口
  for (int i = 0; i < 5; ++i) {
    auto record = createVectorRecord(static_cast<uint64_t>(i), 1000 + i * 3000, vec);
    method.addRecord(std::move(record), 0);
  }
  
  EXPECT_EQ(method.getLeftWindowSize(), 5u);
  
  // 驱逐过期记录 (window_size = 10000ms)
  // current_timestamp = 15000, threshold = 15000 - 10000 = 5000
  // 记录时间戳: 1000, 4000, 7000, 10000, 13000
  // 1000 < 5000 -> 驱逐
  // 4000 < 5000 -> 驱逐
  // 7000 >= 5000 -> 保留
  method.evictExpired(15000);
  
  EXPECT_LT(method.getLeftWindowSize(), 5u);
  SAGEFLOW_LOG_INFO("TEST", "After eviction: {} records", method.getLeftWindowSize());
}

TEST_F(ClusteredJoinMethodTestV2, Close) {
  ClusteredJoinMethod method(config_);
  RuntimeContext context(0, 1);
  method.initialize(context, concurrency_manager_);
  
  auto vec = generateRandomVector(config_.dimension, 1);
  auto record = createVectorRecord(1, 1000, vec);
  method.addRecord(std::move(record), 0);
  
  EXPECT_TRUE(method.isInitialized());
  EXPECT_EQ(method.getLeftWindowSize(), 1u);
  
  method.close();
  
  EXPECT_FALSE(method.isInitialized());
  EXPECT_EQ(method.getLeftWindowSize(), 0u);
  EXPECT_EQ(method.getRightWindowSize(), 0u);
}

// ==================== IVF 索引类型测试 ====================

TEST_F(ClusteredJoinMethodTestV2, IVFIndexType) {
  config_.index_type = ClusteredIndexType::IVF;
  ClusteredJoinMethod method(config_);
  RuntimeContext context(0, 1);
  
  // IVF 索引创建应该成功
  EXPECT_NO_THROW(method.initialize(context, concurrency_manager_));
  EXPECT_TRUE(method.isInitialized());
}

// ==================== ExecuteEager 未初始化测试 ====================

TEST_F(ClusteredJoinMethodTestV2, ExecuteEagerNotInitialized) {
  ClusteredJoinMethod method(config_);
  
  auto vec = generateRandomVector(config_.dimension, 1);
  auto record = createVectorRecord(1, 1000, vec);
  
  // 未初始化时调用应该返回空结果
  auto results = method.ExecuteEager(*record, 0);
  EXPECT_TRUE(results.empty());
}

// ==================== 边界情况测试 ====================

TEST_F(ClusteredJoinMethodTestV2, EmptyWindowQuery) {
  ClusteredJoinMethod method(config_);
  RuntimeContext context(0, 1);
  method.initialize(context, concurrency_manager_);
  
  auto vec = generateRandomVector(config_.dimension, 1);
  auto record = createVectorRecord(1, 1000, vec);
  
  // 空窗口查询应该返回空结果
  auto results = method.ExecuteEager(*record, 0);
  EXPECT_TRUE(results.empty());
}

TEST_F(ClusteredJoinMethodTestV2, DuplicateRecordIgnored) {
  ClusteredJoinMethod method(config_);
  RuntimeContext context(0, 1);
  method.initialize(context, concurrency_manager_);
  
  auto vec = generateRandomVector(config_.dimension, 1);
  
  // 添加同一个 UID 两次
  auto record1 = createVectorRecord(1, 1000, vec);
  auto record2 = createVectorRecord(1, 2000, vec);  // 同 UID，不同时间戳
  
  method.addRecord(std::move(record1), 0);
  method.addRecord(std::move(record2), 0);  // 应该被忽略
  
  EXPECT_EQ(method.getLeftWindowSize(), 1u);  // 只有一条
}

}  // namespace test
}  // namespace sageFlow

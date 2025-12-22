#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "common/data_types.h"
#include "index/hnsw.h"
#include "operator/join_operator_methods/vsjoin_components/async_candidate_generator.h"
#include "operator/join_operator_methods/vsjoin_components/distance_verifier.h"
#include "storage/storage_manager.h"

namespace sageFlow {
namespace {

// 辅助函数：创建测试用的 VectorRecord
std::unique_ptr<VectorRecord> createTestRecord(uint64_t uid, int64_t timestamp,
                                                const std::vector<float>& values) {
  int32_t dim = static_cast<int32_t>(values.size());
  auto data = std::make_unique<char[]>(dim * sizeof(float));
  std::memcpy(data.get(), values.data(), dim * sizeof(float));
  VectorData vec_data(dim, DataType::Float32, data.release());
  return std::make_unique<VectorRecord>(uid, timestamp, std::move(vec_data));
}

// 辅助函数：创建随机向量
std::vector<float> createRandomVector(int dim, std::mt19937& rng) {
  std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
  std::vector<float> vec(dim);
  for (int i = 0; i < dim; ++i) {
    vec[i] = dist(rng);
  }
  return vec;
}

class AsyncCandidateGeneratorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_ = std::make_shared<StorageManager>();
    storage_->engine_ = std::make_shared<ComputeEngine>();

    // 创建 HNSW 索引
    index_ = std::make_shared<HNSW>(16, 100, 50);
    index_->storage_manager_ = storage_;
    index_->dimension_ = kDimension;

    // 插入测试数据
    std::mt19937 rng(42);
    for (int i = 0; i < kNumTestRecords; ++i) {
      auto values = createRandomVector(kDimension, rng);
      auto record = createTestRecord(static_cast<uint64_t>(i + 1), 1000 + i, values);
      storage_->insert(std::move(record));
      index_->insert(static_cast<uint64_t>(i + 1));
    }
  }

  void TearDown() override {
    if (generator_) {
      generator_->shutdown();
      generator_.reset();
    }
  }

  static constexpr int kDimension = 128;
  static constexpr int kNumTestRecords = 100;

  std::shared_ptr<StorageManager> storage_;
  std::shared_ptr<HNSW> index_;
  std::unique_ptr<AsyncCandidateGenerator> generator_;
};

// ============================================================================
// 构造函数测试
// ============================================================================

TEST_F(AsyncCandidateGeneratorTest, ConstructorWithValidParams) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 4, 100);

  EXPECT_TRUE(generator_->isRunning());
  EXPECT_FALSE(generator_->isShutdownRequested());
  EXPECT_EQ(generator_->getNumThreads(), 4);
  EXPECT_EQ(generator_->getMaxQueueSize(), 100);
  EXPECT_EQ(generator_->getCompletedCount(), 0);
  EXPECT_EQ(generator_->getPendingCount(), 0);
}

TEST_F(AsyncCandidateGeneratorTest, ConstructorWithNullIndex) {
  EXPECT_THROW(std::make_unique<AsyncCandidateGenerator>(nullptr, 4, 100), std::invalid_argument);
}

TEST_F(AsyncCandidateGeneratorTest, ConstructorWithZeroThreads) {
  EXPECT_THROW(std::make_unique<AsyncCandidateGenerator>(index_, 0, 100), std::invalid_argument);
}

TEST_F(AsyncCandidateGeneratorTest, ConstructorWithUnlimitedQueue) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 2, 0);

  EXPECT_TRUE(generator_->isRunning());
  EXPECT_EQ(generator_->getMaxQueueSize(), 0);
}

// ============================================================================
// 单个查询测试
// ============================================================================

TEST_F(AsyncCandidateGeneratorTest, SingleQueryReturnsResult) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 4, 100);

  std::mt19937 rng(123);
  auto query_values = createRandomVector(kDimension, rng);
  auto query = createTestRecord(999, 5000, query_values);

  auto future = generator_->submitQuery(*query, 10);
  auto result = future.get();

  // 应该返回一些结果（具体数量取决于索引实现）
  EXPECT_LE(result.size(), 10);
  EXPECT_EQ(generator_->getCompletedCount(), 1);
}

TEST_F(AsyncCandidateGeneratorTest, SingleQueryWithZeroK) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 4, 100);

  std::mt19937 rng(123);
  auto query_values = createRandomVector(kDimension, rng);
  auto query = createTestRecord(999, 5000, query_values);

  auto future = generator_->submitQuery(*query, 0);
  auto result = future.get();

  EXPECT_TRUE(result.empty());
}

TEST_F(AsyncCandidateGeneratorTest, SingleQueryWithNegativeK) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 4, 100);

  std::mt19937 rng(123);
  auto query_values = createRandomVector(kDimension, rng);
  auto query = createTestRecord(999, 5000, query_values);

  auto future = generator_->submitQuery(*query, -5);
  auto result = future.get();

  EXPECT_TRUE(result.empty());
}

// ============================================================================
// 批量查询测试
// ============================================================================

TEST_F(AsyncCandidateGeneratorTest, BatchQueryReturnsResults) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 4, 100);

  std::mt19937 rng(456);
  std::vector<std::unique_ptr<VectorRecord>> queries;
  std::vector<const VectorRecord*> query_ptrs;

  for (int i = 0; i < 10; ++i) {
    auto values = createRandomVector(kDimension, rng);
    queries.push_back(createTestRecord(1000 + i, 6000 + i, values));
    query_ptrs.push_back(queries.back().get());
  }

  auto futures = generator_->submitBatch(query_ptrs, 5);
  EXPECT_EQ(futures.size(), 10);

  for (auto& future : futures) {
    auto result = future.get();
    EXPECT_LE(result.size(), 5);
  }

  EXPECT_EQ(generator_->getCompletedCount(), 10);
}

TEST_F(AsyncCandidateGeneratorTest, BatchQueryWithNullPointer) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 4, 100);

  std::mt19937 rng(789);
  auto values = createRandomVector(kDimension, rng);
  auto query1 = createTestRecord(1001, 7000, values);

  std::vector<const VectorRecord*> query_ptrs = {query1.get(), nullptr, query1.get()};

  auto futures = generator_->submitBatch(query_ptrs, 5);
  EXPECT_EQ(futures.size(), 3);

  // 第一个应该成功
  auto result1 = futures[0].get();
  EXPECT_LE(result1.size(), 5);

  // 第二个应该抛出异常
  EXPECT_THROW(futures[1].get(), std::invalid_argument);

  // 第三个应该成功
  auto result3 = futures[2].get();
  EXPECT_LE(result3.size(), 5);
}

// ============================================================================
// 验证器集成测试
// ============================================================================

TEST_F(AsyncCandidateGeneratorTest, QueryWithVerification) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 4, 100);

  // 创建一个高阈值的验证器，过滤掉大部分结果
  auto verifier = std::make_shared<DistanceVerifier>(0.99, 0.1);

  std::mt19937 rng(321);
  auto query_values = createRandomVector(kDimension, rng);
  auto query = createTestRecord(999, 5000, query_values);

  auto future = generator_->submitQueryWithVerification(*query, 20, verifier);
  auto result = future.get();

  // 由于高阈值，应该过滤掉大部分或全部结果
  EXPECT_LE(result.size(), 20);
  EXPECT_EQ(generator_->getCompletedCount(), 1);
}

TEST_F(AsyncCandidateGeneratorTest, QueryWithNullVerifier) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 4, 100);

  std::mt19937 rng(654);
  auto query_values = createRandomVector(kDimension, rng);
  auto query = createTestRecord(999, 5000, query_values);

  auto future = generator_->submitQueryWithVerification(*query, 10, nullptr);
  auto result = future.get();

  // 无验证器时，应该返回索引的原始结果
  EXPECT_LE(result.size(), 10);
}

// ============================================================================
// 并发测试
// ============================================================================

TEST_F(AsyncCandidateGeneratorTest, ConcurrentSubmit) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 4, 1000);

  std::atomic<int> completed_count{0};
  const int num_submitters = 4;
  const int queries_per_submitter = 25;

  std::vector<std::thread> submitters;
  std::vector<std::future<std::vector<std::unique_ptr<VectorRecord>>>> all_futures;
  std::mutex futures_mutex;

  for (int t = 0; t < num_submitters; ++t) {
    submitters.emplace_back([&, t]() {
      std::mt19937 rng(t * 1000);
      for (int i = 0; i < queries_per_submitter; ++i) {
        auto values = createRandomVector(kDimension, rng);
        auto query = createTestRecord(10000 + t * 100 + i, 8000 + i, values);
        auto future = generator_->submitQuery(*query, 5);

        std::lock_guard<std::mutex> lock(futures_mutex);
        all_futures.push_back(std::move(future));
      }
    });
  }

  for (auto& t : submitters) {
    t.join();
  }

  // 等待所有结果
  for (auto& future : all_futures) {
    auto result = future.get();
    EXPECT_LE(result.size(), 5);
    completed_count++;
  }

  EXPECT_EQ(completed_count.load(), num_submitters * queries_per_submitter);
  EXPECT_EQ(generator_->getCompletedCount(), num_submitters * queries_per_submitter);
}

TEST_F(AsyncCandidateGeneratorTest, HighConcurrencyStress) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 8, 500);

  const int total_queries = 200;
  std::vector<std::future<std::vector<std::unique_ptr<VectorRecord>>>> futures;
  futures.reserve(total_queries);

  std::mt19937 rng(999);
  std::vector<std::unique_ptr<VectorRecord>> queries;

  for (int i = 0; i < total_queries; ++i) {
    auto values = createRandomVector(kDimension, rng);
    queries.push_back(createTestRecord(20000 + i, 9000 + i, values));
    futures.push_back(generator_->submitQuery(*queries.back(), 3));
  }

  int success_count = 0;
  for (auto& future : futures) {
    try {
      auto result = future.get();
      EXPECT_LE(result.size(), 3);
      success_count++;
    } catch (const std::exception& e) {
      ADD_FAILURE() << "Unexpected exception: " << e.what();
    }
  }

  EXPECT_EQ(success_count, total_queries);
  EXPECT_EQ(generator_->getCompletedCount(), total_queries);
}

// ============================================================================
// 生命周期测试
// ============================================================================

TEST_F(AsyncCandidateGeneratorTest, GracefulShutdown) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 4, 100);

  std::mt19937 rng(111);
  std::vector<std::future<std::vector<std::unique_ptr<VectorRecord>>>> futures;

  // 提交一些查询
  for (int i = 0; i < 20; ++i) {
    auto values = createRandomVector(kDimension, rng);
    auto query = createTestRecord(30000 + i, 10000 + i, values);
    futures.push_back(generator_->submitQuery(*query, 5));
  }

  // 优雅关闭
  generator_->shutdown();

  EXPECT_FALSE(generator_->isRunning());
  EXPECT_TRUE(generator_->isShutdownRequested());

  // 所有已提交的查询应该完成
  for (auto& future : futures) {
    EXPECT_NO_THROW(future.get());
  }

  EXPECT_EQ(generator_->getCompletedCount(), 20);
}

TEST_F(AsyncCandidateGeneratorTest, ShutdownNow) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 2, 1000);

  std::mt19937 rng(222);
  std::vector<std::future<std::vector<std::unique_ptr<VectorRecord>>>> futures;

  // 快速提交大量查询
  for (int i = 0; i < 100; ++i) {
    auto values = createRandomVector(kDimension, rng);
    auto query = createTestRecord(40000 + i, 11000 + i, values);
    futures.push_back(generator_->submitQuery(*query, 5));
  }

  // 强制关闭
  generator_->shutdownNow();

  EXPECT_FALSE(generator_->isRunning());
  EXPECT_TRUE(generator_->isShutdownRequested());

  // 一些查询可能完成，一些可能被取消
  int completed = 0;
  int cancelled = 0;
  for (auto& future : futures) {
    try {
      future.get();
      completed++;
    } catch (const std::runtime_error&) {
      cancelled++;
    }
  }

  EXPECT_GE(completed + cancelled, 100);
}

TEST_F(AsyncCandidateGeneratorTest, SubmitAfterShutdown) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 4, 100);
  generator_->shutdown();

  std::mt19937 rng(333);
  auto values = createRandomVector(kDimension, rng);
  auto query = createTestRecord(50000, 12000, values);

  auto future = generator_->submitQuery(*query, 5);

  EXPECT_THROW(future.get(), std::runtime_error);
}

// ============================================================================
// 统计测试
// ============================================================================

TEST_F(AsyncCandidateGeneratorTest, PendingCountAccuracy) {
  // 使用少量线程和小队列来观察待处理计数
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 1, 100);

  std::mt19937 rng(444);

  // 提交一些查询
  for (int i = 0; i < 5; ++i) {
    auto values = createRandomVector(kDimension, rng);
    auto query = createTestRecord(60000 + i, 13000 + i, values);
    generator_->submitQuery(*query, 5);
  }

  // 待处理计数应该在 0 到 5 之间（取决于处理速度）
  EXPECT_LE(generator_->getPendingCount(), 5);
}

TEST_F(AsyncCandidateGeneratorTest, CompletedCountAccuracy) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 4, 100);

  EXPECT_EQ(generator_->getCompletedCount(), 0);

  std::mt19937 rng(555);
  std::vector<std::future<std::vector<std::unique_ptr<VectorRecord>>>> futures;

  for (int i = 0; i < 10; ++i) {
    auto values = createRandomVector(kDimension, rng);
    auto query = createTestRecord(70000 + i, 14000 + i, values);
    futures.push_back(generator_->submitQuery(*query, 5));
  }

  // 等待所有完成
  for (auto& future : futures) {
    future.get();
  }

  EXPECT_EQ(generator_->getCompletedCount(), 10);
}

// ============================================================================
// 边界条件测试
// ============================================================================

TEST_F(AsyncCandidateGeneratorTest, SingleThreadExecution) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 1, 100);

  std::mt19937 rng(666);
  std::vector<std::future<std::vector<std::unique_ptr<VectorRecord>>>> futures;

  for (int i = 0; i < 20; ++i) {
    auto values = createRandomVector(kDimension, rng);
    auto query = createTestRecord(80000 + i, 15000 + i, values);
    futures.push_back(generator_->submitQuery(*query, 5));
  }

  for (auto& future : futures) {
    auto result = future.get();
    EXPECT_LE(result.size(), 5);
  }

  EXPECT_EQ(generator_->getCompletedCount(), 20);
}

TEST_F(AsyncCandidateGeneratorTest, LargeK) {
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 4, 100);

  std::mt19937 rng(777);
  auto values = createRandomVector(kDimension, rng);
  auto query = createTestRecord(90000, 16000, values);

  // k 大于数据库中的记录数
  auto future = generator_->submitQuery(*query, kNumTestRecords * 2);
  auto result = future.get();

  // 结果不应超过数据库中的记录数
  EXPECT_LE(result.size(), kNumTestRecords);
}

// ============================================================================
// 队列满测试
// ============================================================================

TEST_F(AsyncCandidateGeneratorTest, QueueFullBlocking) {
  // 小队列大小
  generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 1, 5);

  std::mt19937 rng(888);
  std::atomic<bool> submit_completed{false};

  // 在后台线程提交大量查询
  std::thread submitter([&]() {
    for (int i = 0; i < 20; ++i) {
      auto values = createRandomVector(kDimension, rng);
      auto query = createTestRecord(100000 + i, 17000 + i, values);
      generator_->submitQuery(*query, 5);
    }
    submit_completed = true;
  });

  // 等待提交完成
  submitter.join();
  EXPECT_TRUE(submit_completed);

  // 关闭并等待
  generator_->shutdown();
  EXPECT_EQ(generator_->getCompletedCount(), 20);
}

}  // namespace
}  // namespace sageFlow

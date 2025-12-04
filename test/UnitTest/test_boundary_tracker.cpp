#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <thread>
#include <vector>

#include "coordination/boundary_tracker.h"

namespace sageFlow {
namespace {

// ============================================================================
// 基本功能测试
// ============================================================================

TEST(BoundaryTrackerTest, MarkAndCheck) {
  BoundaryTracker tracker;
  tracker.markAsBoundary(100, 0);
  EXPECT_TRUE(tracker.isBoundaryVector(100));
  EXPECT_FALSE(tracker.isBoundaryVector(200));
}

TEST(BoundaryTrackerTest, UnmarkRemovesBoundary) {
  BoundaryTracker tracker;
  tracker.markAsBoundary(100, 0);
  EXPECT_TRUE(tracker.isBoundaryVector(100));

  tracker.unmark(100);
  EXPECT_FALSE(tracker.isBoundaryVector(100));
}

TEST(BoundaryTrackerTest, UnmarkNonExistent) {
  BoundaryTracker tracker;
  // 对不存在的向量调用 unmark 不应崩溃
  EXPECT_NO_THROW(tracker.unmark(999));
  EXPECT_EQ(tracker.size(), 0);
}

TEST(BoundaryTrackerTest, GetBoundaryVectorsForPartition) {
  BoundaryTracker tracker;
  tracker.markAsBoundary(100, 0);
  tracker.markAsBoundary(101, 0);
  tracker.markAsBoundary(200, 1);

  auto partition0 = tracker.getBoundaryVectorsForPartition(0);
  EXPECT_EQ(partition0.size(), 2);

  // 检查 100 和 101 都在结果中
  EXPECT_TRUE(std::find(partition0.begin(), partition0.end(), 100) != partition0.end());
  EXPECT_TRUE(std::find(partition0.begin(), partition0.end(), 101) != partition0.end());

  auto partition1 = tracker.getBoundaryVectorsForPartition(1);
  EXPECT_EQ(partition1.size(), 1);
  EXPECT_EQ(partition1[0], 200);
}

TEST(BoundaryTrackerTest, GetBoundaryVectorsForNonExistentPartition) {
  BoundaryTracker tracker;
  tracker.markAsBoundary(100, 0);

  auto result = tracker.getBoundaryVectorsForPartition(999);
  EXPECT_TRUE(result.empty());
}

TEST(BoundaryTrackerTest, GetPartition) {
  BoundaryTracker tracker;
  tracker.markAsBoundary(100, 5);
  tracker.markAsBoundary(200, 10);

  EXPECT_EQ(tracker.getPartition(100), 5);
  EXPECT_EQ(tracker.getPartition(200), 10);
  EXPECT_EQ(tracker.getPartition(300), -1);  // 不存在
}

TEST(BoundaryTrackerTest, Size) {
  BoundaryTracker tracker;
  EXPECT_EQ(tracker.size(), 0);

  tracker.markAsBoundary(100, 0);
  EXPECT_EQ(tracker.size(), 1);

  tracker.markAsBoundary(200, 1);
  EXPECT_EQ(tracker.size(), 2);

  tracker.unmark(100);
  EXPECT_EQ(tracker.size(), 1);
}

TEST(BoundaryTrackerTest, GetPartitionStats) {
  BoundaryTracker tracker;
  tracker.markAsBoundary(100, 0);
  tracker.markAsBoundary(101, 0);
  tracker.markAsBoundary(102, 0);
  tracker.markAsBoundary(200, 1);
  tracker.markAsBoundary(201, 1);

  auto stats = tracker.getPartitionStats();
  EXPECT_EQ(stats.size(), 2);
  EXPECT_EQ(stats[0], 3);
  EXPECT_EQ(stats[1], 2);
}

TEST(BoundaryTrackerTest, Clear) {
  BoundaryTracker tracker;
  tracker.markAsBoundary(100, 0);
  tracker.markAsBoundary(200, 1);
  EXPECT_EQ(tracker.size(), 2);

  tracker.clear();
  EXPECT_EQ(tracker.size(), 0);
  EXPECT_FALSE(tracker.isBoundaryVector(100));
  EXPECT_FALSE(tracker.isBoundaryVector(200));
  EXPECT_TRUE(tracker.getBoundaryVectorsForPartition(0).empty());
}

// ============================================================================
// 批量操作测试
// ============================================================================

TEST(BoundaryTrackerTest, UnmarkBatch) {
  BoundaryTracker tracker;
  tracker.markAsBoundary(100, 0);
  tracker.markAsBoundary(101, 0);
  tracker.markAsBoundary(102, 0);
  tracker.markAsBoundary(200, 1);

  std::vector<uint64_t> to_remove = {100, 102, 999};  // 999 不存在
  tracker.unmarkBatch(to_remove);

  EXPECT_FALSE(tracker.isBoundaryVector(100));
  EXPECT_TRUE(tracker.isBoundaryVector(101));
  EXPECT_FALSE(tracker.isBoundaryVector(102));
  EXPECT_TRUE(tracker.isBoundaryVector(200));
  EXPECT_EQ(tracker.size(), 2);
}

TEST(BoundaryTrackerTest, UnmarkBatchEmptyList) {
  BoundaryTracker tracker;
  tracker.markAsBoundary(100, 0);

  std::vector<uint64_t> empty_list;
  EXPECT_NO_THROW(tracker.unmarkBatch(empty_list));
  EXPECT_EQ(tracker.size(), 1);
}

// ============================================================================
// 边界情况测试
// ============================================================================

TEST(BoundaryTrackerTest, RemarkSamePartition) {
  BoundaryTracker tracker;
  tracker.markAsBoundary(100, 0);
  tracker.markAsBoundary(100, 0);  // 重复标记相同分区

  EXPECT_EQ(tracker.size(), 1);
  EXPECT_EQ(tracker.getPartition(100), 0);
  EXPECT_EQ(tracker.getBoundaryVectorsForPartition(0).size(), 1);
}

TEST(BoundaryTrackerTest, RemarkDifferentPartition) {
  BoundaryTracker tracker;
  tracker.markAsBoundary(100, 0);
  EXPECT_EQ(tracker.getPartition(100), 0);

  // 标记到不同分区，应该更新
  tracker.markAsBoundary(100, 5);
  EXPECT_EQ(tracker.getPartition(100), 5);
  EXPECT_EQ(tracker.size(), 1);

  // 原分区应该为空
  EXPECT_TRUE(tracker.getBoundaryVectorsForPartition(0).empty());
  EXPECT_EQ(tracker.getBoundaryVectorsForPartition(5).size(), 1);
}

TEST(BoundaryTrackerTest, LargePartitionId) {
  BoundaryTracker tracker;
  size_t large_partition = 1000000;
  tracker.markAsBoundary(100, large_partition);

  EXPECT_TRUE(tracker.isBoundaryVector(100));
  EXPECT_EQ(tracker.getPartition(100), static_cast<int64_t>(large_partition));
}

TEST(BoundaryTrackerTest, LargeVectorUid) {
  BoundaryTracker tracker;
  uint64_t large_uid = UINT64_MAX - 1;
  tracker.markAsBoundary(large_uid, 0);

  EXPECT_TRUE(tracker.isBoundaryVector(large_uid));
  EXPECT_EQ(tracker.getPartition(large_uid), 0);
}

// ============================================================================
// 并发访问测试
// ============================================================================

TEST(BoundaryTrackerTest, ConcurrentMarking) {
  BoundaryTracker tracker;
  constexpr int kNumThreads = 4;
  constexpr int kOpsPerThread = 1000;

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);

  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&tracker, t]() {
      for (int i = 0; i < kOpsPerThread; ++i) {
        uint64_t uid = static_cast<uint64_t>(t * kOpsPerThread + i);
        tracker.markAsBoundary(uid, static_cast<size_t>(t));
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(tracker.size(), kNumThreads * kOpsPerThread);
}

TEST(BoundaryTrackerTest, ConcurrentReadWrite) {
  BoundaryTracker tracker;
  std::atomic<bool> stop_flag{false};
  std::atomic<int> errors{0};

  // 预先填充一些数据
  for (uint64_t i = 0; i < 100; ++i) {
    tracker.markAsBoundary(i, i % 4);
  }

  // 写线程：不断标记和取消标记
  std::thread writer([&]() {
    uint64_t counter = 100;
    while (!stop_flag.load()) {
      tracker.markAsBoundary(counter, counter % 4);
      if (counter > 100) {
        tracker.unmark(counter - 50);
      }
      ++counter;
    }
  });

  // 读线程：不断查询
  std::vector<std::thread> readers;
  for (int i = 0; i < 3; ++i) {
    readers.emplace_back([&]() {
      while (!stop_flag.load()) {
        // 各种读操作 - 使用 volatile 避免被优化掉
        volatile bool b = tracker.isBoundaryVector(50);
        volatile int64_t p = tracker.getPartition(25);
        auto v = tracker.getBoundaryVectorsForPartition(0);
        volatile size_t s = tracker.size();
        auto stats = tracker.getPartitionStats();
        (void)b;
        (void)p;
        (void)s;
      }
    });
  }

  // 运行一段时间
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  stop_flag.store(true);

  writer.join();
  for (auto& reader : readers) {
    reader.join();
  }

  EXPECT_EQ(errors.load(), 0);
}

TEST(BoundaryTrackerTest, ConcurrentUnmarkBatch) {
  BoundaryTracker tracker;

  // 预先填充数据
  for (uint64_t i = 0; i < 1000; ++i) {
    tracker.markAsBoundary(i, i % 10);
  }

  constexpr int kNumThreads = 4;
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);

  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&tracker, t]() {
      std::vector<uint64_t> to_remove;
      for (uint64_t i = static_cast<uint64_t>(t); i < 1000; i += kNumThreads) {
        to_remove.push_back(i);
      }
      tracker.unmarkBatch(to_remove);
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(tracker.size(), 0);
}

// ============================================================================
// 分区清理测试
// ============================================================================

TEST(BoundaryTrackerTest, PartitionCleanupOnUnmark) {
  BoundaryTracker tracker;
  tracker.markAsBoundary(100, 5);

  // 获取分区统计，应该有分区 5
  auto stats = tracker.getPartitionStats();
  EXPECT_EQ(stats.count(5), 1);

  // 取消标记后，分区应该被清理
  tracker.unmark(100);
  stats = tracker.getPartitionStats();
  EXPECT_EQ(stats.count(5), 0);
}

TEST(BoundaryTrackerTest, PartitionCleanupOnBatchUnmark) {
  BoundaryTracker tracker;
  tracker.markAsBoundary(100, 5);
  tracker.markAsBoundary(101, 5);
  tracker.markAsBoundary(200, 6);

  std::vector<uint64_t> to_remove = {100, 101};
  tracker.unmarkBatch(to_remove);

  auto stats = tracker.getPartitionStats();
  EXPECT_EQ(stats.count(5), 0);  // 分区 5 应该被清理
  EXPECT_EQ(stats.count(6), 1);  // 分区 6 仍存在
}

}  // namespace
}  // namespace sageFlow

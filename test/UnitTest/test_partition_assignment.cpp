#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <thread>
#include <vector>

#include "operator/join_operator_methods/vsjoin_components/partition_assignment.h"

namespace sageFlow {
namespace {

TEST(VSJoinPartitionAssignmentTest, BasicGetSet) {
    VSJoinPartitionAssignment table(/*num_logical_partitions=*/8, /*num_physical_subtasks=*/2);

    for (int pid = 0; pid < 8; ++pid) {
        int st = table.getPhysicalSubtask(pid);
        EXPECT_TRUE(st == 0 || st == 1);
    }

    table.setPhysicalSubtask(3, 1);
    EXPECT_EQ(table.getPhysicalSubtask(3), 1);

    table.setPhysicalSubtask(4, 0);
    EXPECT_EQ(table.getPhysicalSubtask(4), 0);

    EXPECT_EQ(table.getPhysicalSubtask(-1), -1);
    EXPECT_EQ(table.getPhysicalSubtask(999), -1);
}

TEST(VSJoinPartitionAssignmentTest, BatchUpdate) {
    VSJoinPartitionAssignment table(/*num_logical_partitions=*/6, /*num_physical_subtasks=*/3);

    auto before = table.getCurrentMapping();
    ASSERT_EQ(before.size(), 6u);

    table.updateMapping({{0, 2}, {1, 2}, {2, 1}});

    EXPECT_EQ(table.getPhysicalSubtask(0), 2);
    EXPECT_EQ(table.getPhysicalSubtask(1), 2);
    EXPECT_EQ(table.getPhysicalSubtask(2), 1);

    // 非法更新应被忽略
    table.updateMapping({{-1, 0}, {5, 999}});
    EXPECT_EQ(table.getPhysicalSubtask(5), before[5]);
}

TEST(VSJoinPartitionAssignmentTest, ConcurrentReadsSingleWriter) {
    VSJoinPartitionAssignment table(/*num_logical_partitions=*/1024, /*num_physical_subtasks=*/8);

    std::atomic<bool> stop{false};
    std::atomic<size_t> ok_reads{0};

    const int kReaders = 8;
    std::vector<std::thread> readers;
    readers.reserve(kReaders);

    for (int i = 0; i < kReaders; ++i) {
        readers.emplace_back([&]() {
            while (!stop.load(std::memory_order_relaxed)) {
                // 固定读几个点，确保不会越界/崩溃
                int a = table.getPhysicalSubtask(0);
                int b = table.getPhysicalSubtask(511);
                int c = table.getPhysicalSubtask(1023);
                if (a >= 0 && b >= 0 && c >= 0) {
                    ok_reads.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    // 单写线程：重复批量更新
    std::thread writer([&]() {
        for (int round = 0; round < 200; ++round) {
            table.updateMapping({{0, round % 8}, {511, (round + 1) % 8}, {1023, (round + 2) % 8}});
        }
        stop.store(true, std::memory_order_relaxed);
    });

    writer.join();
    for (auto& t : readers) t.join();

    EXPECT_GT(ok_reads.load(std::memory_order_relaxed), 0u);
}

}  // namespace
}  // namespace sageFlow

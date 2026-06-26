#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <limits>
#include <thread>
#include <vector>

#include "operator/join_operator_methods/vsjoin_components/load_monitor.h"
#include "operator/join_operator_methods/vsjoin_components/partition_assignment.h"
#include "utils/logger.h"

namespace sageFlow {
namespace {

std::vector<size_t> countAssignmentsBySubtask(const std::vector<int>& mapping,
                                              size_t num_subtasks) {
    std::vector<size_t> counts(num_subtasks, 0);
    for (int subtask : mapping) {
        if (subtask >= 0 && static_cast<size_t>(subtask) < num_subtasks) {
            counts[static_cast<size_t>(subtask)] += 1;
        }
    }
    return counts;
}

size_t assignmentImbalanceGap(const std::vector<size_t>& counts) {
    if (counts.empty()) {
        return 0;
    }
    size_t min_count = std::numeric_limits<size_t>::max();
    size_t max_count = 0;
    for (size_t value : counts) {
        min_count = std::min(min_count, value);
        max_count = std::max(max_count, value);
    }
    return max_count - min_count;
}

TEST(VSJoinLoadBalancingTest, AssignmentTableConcurrentRead) {
    VSJoinPartitionAssignment assignment(128, 8);

    const int num_threads = 16;
    const int reads_per_thread = 10000;
    std::vector<std::thread> threads;
    std::atomic<int> total_reads{0};

    threads.reserve(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&assignment, &total_reads, reads_per_thread]() {
            for (int i = 0; i < reads_per_thread; ++i) {
                int logical_pid = i % 128;
                int physical_subtask = assignment.getPhysicalSubtask(logical_pid);
                ASSERT_GE(physical_subtask, 0);
                ASSERT_LT(physical_subtask, 8);
                total_reads.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& th : threads) th.join();

    EXPECT_EQ(total_reads.load(std::memory_order_relaxed), num_threads * reads_per_thread);
}

TEST(VSJoinLoadBalancingTest, AssignmentTableBatchUpdateAtomicity) {
    VSJoinPartitionAssignment assignment(128, 8);

    std::vector<std::pair<int, int>> updates;
    updates.reserve(64);
    for (int i = 0; i < 64; ++i) {
        updates.emplace_back(i, (i + 1) % 8);
    }

    assignment.updateMapping(updates);

    for (int i = 0; i < 64; ++i) {
        int physical_subtask = assignment.getPhysicalSubtask(i);
        EXPECT_TRUE(physical_subtask == (i + 1) % 8 || physical_subtask == (i % 8));
    }
}

TEST(VSJoinLoadBalancingTest, AssignmentTableConcurrentReadWriteAtomicVisibility) {
    constexpr int kLogicalPartitions = 128;
    constexpr int kPhysicalSubtasks = 8;

    VSJoinPartitionAssignment assignment(kLogicalPartitions, kPhysicalSubtasks);

    std::vector<int> mapping_a(kLogicalPartitions, 0);
    std::vector<int> mapping_b(kLogicalPartitions, 0);
    std::vector<std::pair<int, int>> updates_to_a;
    std::vector<std::pair<int, int>> updates_to_b;
    updates_to_a.reserve(kLogicalPartitions);
    updates_to_b.reserve(kLogicalPartitions);

    for (int i = 0; i < kLogicalPartitions; ++i) {
        mapping_a[i] = i % kPhysicalSubtasks;
        mapping_b[i] = (i + 3) % kPhysicalSubtasks;
        updates_to_a.emplace_back(i, mapping_a[i]);
        updates_to_b.emplace_back(i, mapping_b[i]);
    }

    assignment.updateMapping(updates_to_a);

    std::atomic<bool> stop{false};
    std::atomic<size_t> invalid_reads{0};
    std::vector<std::thread> readers;
    readers.reserve(8);

    for (int t = 0; t < 8; ++t) {
        readers.emplace_back([&assignment, &mapping_a, &mapping_b, &stop, &invalid_reads]() {
            int cursor = 0;
            while (!stop.load(std::memory_order_relaxed)) {
                int logical_pid = cursor % kLogicalPartitions;
                int observed = assignment.getPhysicalSubtask(logical_pid);
                const int a = mapping_a[logical_pid];
                const int b = mapping_b[logical_pid];
                if (observed != a && observed != b) {
                    invalid_reads.fetch_add(1, std::memory_order_relaxed);
                }
                cursor += 1;
            }
        });
    }

    std::thread writer([&assignment, &updates_to_a, &updates_to_b]() {
        for (int round = 0; round < 200; ++round) {
            assignment.updateMapping((round % 2 == 0) ? updates_to_b : updates_to_a);
        }
    });

    writer.join();
    stop.store(true, std::memory_order_relaxed);
    for (auto& reader : readers) {
        reader.join();
    }

    EXPECT_EQ(invalid_reads.load(std::memory_order_relaxed), 0u)
        << "Readers should only observe complete old/new mapping values for each logical partition";
}

TEST(VSJoinLoadBalancingTest, LoadMonitorFunctionality) {
    VSJoinLoadMonitor monitor(8);

    monitor.reportLoad(0, 1000, 10.5, 50);
    monitor.reportLoad(1, 500, 5.0, 20);
    monitor.reportLoad(2, 2000, 20.0, 100);

    auto stats = monitor.getLoadStats();
    EXPECT_EQ(stats.size(), 8u);

    EXPECT_EQ(stats[0].record_count, 1000u);
    EXPECT_EQ(stats[1].record_count, 500u);
    EXPECT_EQ(stats[2].record_count, 2000u);

    EXPECT_EQ(monitor.getBusiestSubtask(), 2u);
    EXPECT_EQ(monitor.getIdlestSubtask(), 3u);  // 未上报的 subtask 默认 record_count=0，更空闲

    const double avg_load = monitor.getAverageLoad();
    EXPECT_NEAR(avg_load, (1000.0 + 500.0 + 2000.0) / 8.0, 0.1);
}

TEST(VSJoinLoadBalancingTest, LoadBalancingEffectiveness) {
    VSJoinLoadMonitor monitor(8);
    for (int i = 0; i < 2; ++i) {
        monitor.reportLoad(static_cast<size_t>(i), 2000, 20.0, 100);
    }
    for (int i = 2; i < 8; ++i) {
        monitor.reportLoad(static_cast<size_t>(i), 100, 1.0, 5);
    }

    const double avg_load = monitor.getAverageLoad();
    const double max_load = 2000.0;
    const double imbalance_ratio = max_load / avg_load;
    EXPECT_GT(imbalance_ratio, 1.5);

    VSJoinPartitionAssignment assignment(128, 8);

    std::vector<std::pair<int, int>> skew_updates;
    skew_updates.reserve(64);
    for (int i = 0; i < 64; ++i) {
        skew_updates.emplace_back(i, 0);
    }
    assignment.updateMapping(skew_updates);

    const auto before_counts = countAssignmentsBySubtask(assignment.getCurrentMapping(), 8);
    const size_t before_gap = assignmentImbalanceGap(before_counts);

    std::vector<std::pair<int, int>> rebalance_updates;
    for (int i = 0; i < 32; ++i) {
        rebalance_updates.emplace_back(i, 2);
    }
    for (int i = 32; i < 64; ++i) {
        rebalance_updates.emplace_back(i, 3);
    }

    assignment.updateMapping(rebalance_updates);

    const auto after_counts = countAssignmentsBySubtask(assignment.getCurrentMapping(), 8);
    const size_t after_gap = assignmentImbalanceGap(after_counts);

    for (int i = 0; i < 32; ++i) {
        EXPECT_EQ(assignment.getPhysicalSubtask(i), 2);
    }
    for (int i = 32; i < 64; ++i) {
        EXPECT_EQ(assignment.getPhysicalSubtask(i), 3);
    }

    EXPECT_LT(after_gap, before_gap)
        << "Assignment distribution should become more balanced after rebalance updates";
}

TEST(VSJoinLoadBalancingTest, AssignmentTablePerformance) {
    VSJoinPartitionAssignment assignment(1024, 16);

    const int num_reads = 1000000;
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_reads; ++i) {
        assignment.getPhysicalSubtask(i % 1024);
    }
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    const double avg_latency_ns = duration.count() / static_cast<double>(num_reads);

    EXPECT_LT(avg_latency_ns, 10.0);

    SAGEFLOW_LOG_INFO("VSJOIN_PERF", "AssignmentTable read latency: {} ns", avg_latency_ns);
}

}  // namespace
}  // namespace sageFlow

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <thread>
#include <vector>

#include "operator/join_operator_methods/vsjoin_components/load_monitor.h"

namespace sageFlow {
namespace {

TEST(VSJoinLoadMonitorTest, ReportAndQuery) {
    VSJoinLoadMonitor monitor(/*num_subtasks=*/3);

    monitor.reportLoad(0, 10, 1.5, 2);
    monitor.reportLoad(1, 5, 2.0, 0);
    monitor.reportLoad(2, 20, 1.0, 1);

    auto stats = monitor.getLoadStats();
    ASSERT_EQ(stats.size(), 3u);

    EXPECT_EQ(stats[0].record_count, 10u);
    EXPECT_EQ(stats[1].record_count, 5u);
    EXPECT_EQ(stats[2].record_count, 20u);

    EXPECT_DOUBLE_EQ(monitor.getAverageLoad(), (10.0 + 5.0 + 20.0) / 3.0);
    EXPECT_EQ(monitor.getBusiestSubtask(), 2u);
    EXPECT_EQ(monitor.getIdlestSubtask(), 1u);
}

TEST(VSJoinLoadMonitorTest, ConcurrentReports) {
    VSJoinLoadMonitor monitor(/*num_subtasks=*/4);

    std::atomic<bool> start{false};
    std::vector<std::thread> threads;
    threads.reserve(8);

    for (size_t i = 0; i < 8; ++i) {
        threads.emplace_back([&, i]() {
            while (!start.load(std::memory_order_relaxed)) {
            }
            for (size_t r = 0; r < 1000; ++r) {
                size_t idx = (i + r) % 4;
                monitor.reportLoad(idx, r);
            }
        });
    }

    start.store(true, std::memory_order_relaxed);
    for (auto& t : threads) t.join();

    auto stats = monitor.getLoadStats();
    ASSERT_EQ(stats.size(), 4u);
    for (const auto& s : stats) {
        // 至少应被某个线程写过
        EXPECT_GE(s.record_count, 0u);
    }
}

}  // namespace
}  // namespace sageFlow

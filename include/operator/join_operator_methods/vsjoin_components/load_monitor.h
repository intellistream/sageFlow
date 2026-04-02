#pragma once

#include <chrono>
#include <cstddef>
#include <mutex>
#include <vector>

namespace sageFlow {

struct LoadStat {
    size_t subtask_index = 0;
    size_t record_count = 0;          // latest sampled record count
    double avg_latency_ms = 0.0;      // EWMA latency
    size_t queue_backlog = 0;         // EWMA backlog (rounded)
    size_t sample_count = 0;          // number of reports received
    size_t total_records = 0;         // cumulative reported records
    double total_latency_ms = 0.0;    // cumulative reported latency
    size_t total_backlog = 0;         // cumulative reported backlog
    std::chrono::steady_clock::time_point last_update{};
};

class VSJoinLoadMonitor {
public:
    explicit VSJoinLoadMonitor(size_t num_subtasks);

    void reportLoad(size_t subtask_index,
                    size_t record_count,
                    double avg_latency_ms = 0.0,
                    size_t queue_backlog = 0);

    std::vector<LoadStat> getLoadStats() const;

    double getAverageLoad() const;

    size_t getBusiestSubtask() const;
    size_t getIdlestSubtask() const;

    /// Get EWMA-smoothed load for a subtask (latency + weighted backlog)
    double getSmoothedLoad(size_t subtask_index, double backlog_weight = 0.25) const;

private:
    size_t num_subtasks_;
    mutable std::mutex stats_mutex_;
    std::vector<LoadStat> subtask_loads_;
};

}  // namespace sageFlow

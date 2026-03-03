#pragma once

#include <chrono>
#include <cstddef>
#include <mutex>
#include <vector>

namespace sageFlow {

struct LoadStat {
    size_t subtask_index = 0;
    size_t record_count = 0;
    double avg_latency_ms = 0.0;
    size_t queue_backlog = 0;
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

private:
    size_t num_subtasks_;
    mutable std::mutex stats_mutex_;
    std::vector<LoadStat> subtask_loads_;
};

}  // namespace sageFlow

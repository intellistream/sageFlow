#include "operator/join_operator_methods/vsjoin_components/load_monitor.h"

#include "utils/logger.h"

#include <algorithm>
#include <numeric>

namespace sageFlow {

VSJoinLoadMonitor::VSJoinLoadMonitor(size_t num_subtasks)
    : num_subtasks_(num_subtasks), subtask_loads_(num_subtasks) {
    for (size_t i = 0; i < num_subtasks_; ++i) {
        subtask_loads_[i].subtask_index = i;
        subtask_loads_[i].last_update = std::chrono::steady_clock::now();
    }
    SAGEFLOW_LOG_DEBUG("VSJOIN_LOAD_MONITOR", "init load monitor num_subtasks=%zu", num_subtasks_);
}

void VSJoinLoadMonitor::reportLoad(size_t subtask_index,
                                  size_t record_count,
                                  double avg_latency_ms,
                                  size_t queue_backlog) {
    std::lock_guard<std::mutex> lock(stats_mutex_);

    if (subtask_index >= subtask_loads_.size()) {
        return;
    }

    auto& stat = subtask_loads_[subtask_index];
    stat.subtask_index = subtask_index;
    stat.record_count = record_count;
    stat.avg_latency_ms = avg_latency_ms;
    stat.queue_backlog = queue_backlog;
    stat.last_update = std::chrono::steady_clock::now();
}

std::vector<LoadStat> VSJoinLoadMonitor::getLoadStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return subtask_loads_;
}

double VSJoinLoadMonitor::getAverageLoad() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    if (subtask_loads_.empty()) return 0.0;

    const size_t sum = std::accumulate(
        subtask_loads_.begin(), subtask_loads_.end(), static_cast<size_t>(0),
        [](size_t acc, const LoadStat& s) { return acc + s.record_count; });

    return static_cast<double>(sum) / static_cast<double>(subtask_loads_.size());
}

size_t VSJoinLoadMonitor::getBusiestSubtask() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    if (subtask_loads_.empty()) return 0;

    auto it = std::max_element(subtask_loads_.begin(), subtask_loads_.end(),
                               [](const LoadStat& a, const LoadStat& b) {
                                   if (a.record_count != b.record_count) return a.record_count < b.record_count;
                                   return a.queue_backlog < b.queue_backlog;
                               });
    return it->subtask_index;
}

size_t VSJoinLoadMonitor::getIdlestSubtask() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    if (subtask_loads_.empty()) return 0;

    auto it = std::min_element(subtask_loads_.begin(), subtask_loads_.end(),
                               [](const LoadStat& a, const LoadStat& b) {
                                   if (a.record_count != b.record_count) return a.record_count < b.record_count;
                                   return a.queue_backlog < b.queue_backlog;
                               });
    return it->subtask_index;
}

}  // namespace sageFlow

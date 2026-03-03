#include "operator/join_operator_methods/vsjoin_components/partition_assignment.h"

#include "utils/logger.h"

namespace sageFlow {

VSJoinPartitionAssignment::VSJoinPartitionAssignment(size_t num_logical_partitions,
                                                     size_t num_physical_subtasks)
    : num_logical_(num_logical_partitions),
      num_physical_(num_physical_subtasks),
            valid_(num_logical_partitions > 0 && num_physical_subtasks > 0),
      current_table_(std::make_unique<std::vector<int>>(num_logical_partitions, -1)),
      next_table_(std::make_unique<std::vector<int>>(num_logical_partitions, -1)),
      current_ptr_(current_table_.get()) {
        if (!valid_) {
                current_ptr_.store(current_table_.get(), std::memory_order_release);
                SAGEFLOW_LOG_WARN("VSJOIN_ASSIGNMENT", "invalid assignment table config logical=%zu physical=%zu",
                                                    num_logical_, num_physical_);
                return;
        }

    for (size_t i = 0; i < num_logical_; ++i) {
        (*current_table_)[i] = static_cast<int>(i % num_physical_);
        (*next_table_)[i] = (*current_table_)[i];
    }
    current_ptr_.store(current_table_.get(), std::memory_order_release);

    SAGEFLOW_LOG_DEBUG("VSJOIN_ASSIGNMENT", "init assignment table logical=%zu physical=%zu", num_logical_,
                      num_physical_);
}

int VSJoinPartitionAssignment::getPhysicalSubtask(int logical_pid) const {
    if (!valid_) {
        return -1;
    }

    auto* table = current_ptr_.load(std::memory_order_acquire);

    if (!table) return -1;
    if (logical_pid < 0 || static_cast<size_t>(logical_pid) >= num_logical_) {
        return -1;
    }

    return (*table)[static_cast<size_t>(logical_pid)];
}

void VSJoinPartitionAssignment::updateMapping(const std::vector<std::pair<int, int>>& updates) {
    if (!valid_ || updates.empty()) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(write_mutex_);
        auto* current_snapshot = current_ptr_.load(std::memory_order_acquire);
        if (!current_snapshot) {
            return;
        }

        *next_table_ = *current_snapshot;

        for (const auto& update : updates) {
            const int logical_pid = update.first;
            const int physical_subtask = update.second;

            if (logical_pid >= 0 && static_cast<size_t>(logical_pid) < num_logical_ && physical_subtask >= 0 &&
                static_cast<size_t>(physical_subtask) < num_physical_) {
                (*next_table_)[static_cast<size_t>(logical_pid)] = physical_subtask;
            }
        }

        current_ptr_.store(next_table_.get(), std::memory_order_release);
        std::swap(current_table_, next_table_);
    }

    SAGEFLOW_LOG_DEBUG("VSJOIN_ASSIGNMENT", "update mapping size=%zu", updates.size());
}

void VSJoinPartitionAssignment::setPhysicalSubtask(int logical_pid, int physical_subtask) {
    updateMapping({{logical_pid, physical_subtask}});
}

std::vector<int> VSJoinPartitionAssignment::getCurrentMapping() const {
    if (!valid_) {
        return {};
    }

    auto* table = current_ptr_.load(std::memory_order_acquire);
    if (!table) return {};
    return *table;
}

}  // namespace sageFlow

#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace sageFlow {

class VSJoinPartitionAssignment {
public:
    explicit VSJoinPartitionAssignment(size_t num_logical_partitions,
                                      size_t num_physical_subtasks);

    // ==================== 读操作（高频，完全无锁） ====================
    int getPhysicalSubtask(int logical_pid) const;

    // ==================== 写操作（低频，批量更新） ====================
    void updateMapping(const std::vector<std::pair<int, int>>& updates);
    void setPhysicalSubtask(int logical_pid, int physical_subtask);

    // 获取当前映射表快照（用于调试）
    std::vector<int> getCurrentMapping() const;

private:
    size_t num_logical_;
    size_t num_physical_;
    bool valid_;

    // 双缓冲：两个映射表实例
    std::unique_ptr<std::vector<int>> current_table_;  // 当前版本（读）
    std::unique_ptr<std::vector<int>> next_table_;     // 准备版本（写）

    // 原子指针：指向当前可读的映射表
    std::atomic<std::vector<int>*> current_ptr_;

    // 写互斥锁：保护 next_table_ 的更新过程（避免并发写冲突）
    mutable std::mutex write_mutex_;
};

}  // namespace sageFlow

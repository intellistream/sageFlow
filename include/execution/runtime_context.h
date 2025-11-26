//
// Created for sageFlow architecture refactoring
//

#pragma once

#include <cstddef>
#include <string>

namespace sageFlow {

/**
 * @brief 运行时上下文，提供算子实例的执行环境信息
 * 
 * RuntimeContext 为算子提供关键的运行时信息：
 * - 子任务索引：标识当前算子实例在并行实例组中的位置
 * - 并行度：当前算子的总并行实例数
 * - 连接策略类型：指示使用的连接策略（分区/共享队列）
 * 
 * 这些信息使算子能够：
 * 1. 识别自己的身份（哪个并发实例）
 * 2. 路由到正确的状态分片
 * 3. 根据连接策略调整行为
 */
class RuntimeContext {
public:
    RuntimeContext(size_t subtask_index, size_t parallelism)
        : subtask_index_(subtask_index)
        , parallelism_(parallelism) {}

    /**
     * @brief 获取当前子任务索引（0-based）
     * @return 子任务索引，范围 [0, parallelism)
     */
    size_t getSubtaskIndex() const { return subtask_index_; }

    /**
     * @brief 获取算子的并行度
     * @return 并行实例总数
     */
    size_t getParallelism() const { return parallelism_; }

    /**
     * @brief 获取任务名称（用于调试和日志）
     * @return 格式化的任务名称，如 "Task[2/8]"
     */
    std::string getTaskName() const {
        return "Task[" + std::to_string(subtask_index_) + "/" + 
               std::to_string(parallelism_) + "]";
    }

private:
    size_t subtask_index_;  // 当前子任务索引
    size_t parallelism_;     // 并行度
};

} // namespace sageFlow

#pragma once

#include "execution/vector_space_partitioner.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <vector>

namespace sageFlow {

/**
 * @brief 自适应调整历史记录
 */
struct AdaptHistory {
    std::chrono::steady_clock::time_point timestamp;
    std::string action;  // "split", "merge", "rebalance"
    int partition_id;
    std::string details;
};

/**
 * @brief 自适应分区器配置
 */
struct AdaptivePartitionerConfig {
    int initial_partitions = 16;        ///< 初始分区数
    int64_t adapt_interval_ms = 1000;   ///< 自适应调整间隔（毫秒）
    double load_threshold = 0.3;        ///< 负载不均衡阈值
    double split_threshold = 2.0;       ///< 分裂阈值（相对均值）
    double merge_threshold = 0.3;       ///< 合并阈值（相对均值）
    int min_partitions = 2;             ///< 最小分区数
    int max_partitions = 64;            ///< 最大分区数
};

/**
 * @brief 每个分区的统计信息
 */
struct PartitionStats {
    std::atomic<size_t> count{0};           ///< 处理的记录数
    std::atomic<int64_t> total_latency_us{0}; ///< 总延迟（微秒）
    std::atomic<size_t> data_size{0};       ///< 数据量
    
    PartitionStats() = default;
    
    // 拷贝构造函数（从原子变量读取值）
    PartitionStats(const PartitionStats& other) 
        : count(other.count.load(std::memory_order_relaxed)),
          total_latency_us(other.total_latency_us.load(std::memory_order_relaxed)),
          data_size(other.data_size.load(std::memory_order_relaxed)) {}
    
    // 移动构造函数（与拷贝相同）
    PartitionStats(PartitionStats&& other) noexcept
        : count(other.count.load(std::memory_order_relaxed)),
          total_latency_us(other.total_latency_us.load(std::memory_order_relaxed)),
          data_size(other.data_size.load(std::memory_order_relaxed)) {}
    
    // 拷贝赋值运算符
    PartitionStats& operator=(const PartitionStats& other) {
        if (this != &other) {
            count.store(other.count.load(std::memory_order_relaxed), std::memory_order_relaxed);
            total_latency_us.store(other.total_latency_us.load(std::memory_order_relaxed), std::memory_order_relaxed);
            data_size.store(other.data_size.load(std::memory_order_relaxed), std::memory_order_relaxed);
        }
        return *this;
    }
    
    // 移动赋值运算符
    PartitionStats& operator=(PartitionStats&& other) noexcept {
        if (this != &other) {
            count.store(other.count.load(std::memory_order_relaxed), std::memory_order_relaxed);
            total_latency_us.store(other.total_latency_us.load(std::memory_order_relaxed), std::memory_order_relaxed);
            data_size.store(other.data_size.load(std::memory_order_relaxed), std::memory_order_relaxed);
        }
        return *this;
    }
    
    void reset() {
        count.store(0, std::memory_order_relaxed);
        total_latency_us.store(0, std::memory_order_relaxed);
        data_size.store(0, std::memory_order_relaxed);
    }
    
    double getAverageLatencyUs() const {
        size_t c = count.load(std::memory_order_relaxed);
        if (c == 0) return 0.0;
        return static_cast<double>(total_latency_us.load(std::memory_order_relaxed)) / c;
    }
};

/**
 * @brief 自适应分区器
 * 
 * 根据数据分布和负载自动调整分区策略。
 * 基于 DEBS'23 S3J 论文的自适应分区思想。
 * 
 * 核心功能：
 * 1. 动态分裂过载分区
 * 2. 动态合并低负载分区
 * 3. 负载均衡监控
 * 4. 自适应调整历史记录
 */
class AdaptivePartitioner : public KMeansPartitioner {
    // [TODO-S3J] 废弃 Split/Merge 策略
    // 论文明确指出 S3J 不改变分区数量，而是移动 Workset。
    // 动作：
    // 1. 标记 splitPartition() 和 mergePartitions() 为 [DEPRECATED]。
    // 2. 它们将被 migrateWorkset(workset_id, src, dest) 取代。

    // [TODO-S3J] 实现 Algorithm 1 (Greedy Balancing)
    // 1. 计算不平衡度 DI (Degree of Imbalance)。
    // 2. 识别 Overloaded Worker 和 Underloaded Worker。
    // 3. 贪心选择 benefit 最大的 Workset 进行迁移。
    // void rebalanceWorksets(const std::vector<PartitionStats>& global_stats);
public:
    /**
     * @brief 构造函数
     * @param dimension 向量维度
     * @param config 自适应配置
     * @param seed 随机种子
     */
    explicit AdaptivePartitioner(int dimension, 
                                  const AdaptivePartitionerConfig& config = AdaptivePartitionerConfig(),
                                  int seed = 42);
    
    ~AdaptivePartitioner() override = default;
    
    // 禁用拷贝
    AdaptivePartitioner(const AdaptivePartitioner&) = delete;
    AdaptivePartitioner& operator=(const AdaptivePartitioner&) = delete;
    
    /**
     * @brief 更新分区统计
     * @param partition 分区 ID
     * @param latency_us 处理延迟（微秒）
     * @param data_size 数据大小
     */
    void updateStats(size_t partition, int64_t latency_us, size_t data_size);
    
    /**
     * @brief 检查并执行自适应调整
     * @return 是否进行了调整
     */
    bool checkAndAdapt();
    
    /**
     * @brief 强制执行自适应调整（忽略时间间隔）
     * @return 是否进行了调整
     */
    bool forceAdapt();
    
    /**
     * @brief 获取当前分区数
     */
    int getCurrentNumPartitions() const;
    
    /**
     * @brief 获取自适应历史
     */
    std::vector<AdaptHistory> getHistory() const;
    
    /**
     * @brief 清除自适应历史
     */
    void clearHistory();
    
    /**
     * @brief 获取负载不均衡度
     * @return 不均衡度 (最大负载/平均负载 - 1)
     */
    double computeLoadImbalance() const;
    
    /**
     * @brief 获取分区统计信息
     * @param partition 分区 ID
     * @return 分区统计
     */
    const PartitionStats& getPartitionStats(size_t partition) const;
    
    /**
     * @brief 重置所有统计信息
     */
    void resetStats();
    
    /**
     * @brief 获取配置
     */
    const AdaptivePartitionerConfig& getConfig() const { return adapt_config_; }

private:
    AdaptivePartitionerConfig adapt_config_;
    std::atomic<int64_t> last_adapt_time_ms_;
    
    // 每分区统计
    mutable std::mutex stats_mutex_;
    std::vector<PartitionStats> partition_stats_;
    
    // 自适应历史
    mutable std::mutex history_mutex_;
    std::vector<AdaptHistory> history_;
    static constexpr size_t kMaxHistorySize = 1000;
    
    // 当前分区数（可能与初始值不同）
    std::atomic<int> current_num_partitions_;
    
    /**
     * @brief 分裂过载分区
     * @param partition 要分裂的分区
     * @return 是否成功分裂
     */
    bool splitPartition(size_t partition);
    
    /**
     * @brief 合并低负载分区
     * @param partition1 第一个分区
     * @param partition2 第二个分区
     * @return 是否成功合并
     */
    bool mergePartitions(size_t partition1, size_t partition2);
    
    /**
     * @brief 找到最大负载分区
     */
    size_t findMaxLoadPartition() const;
    
    /**
     * @brief 找到最小负载分区
     */
    size_t findMinLoadPartition() const;
    
    /**
     * @brief 找到相邻分区（用于合并）
     */
    size_t findNeighborPartition(size_t partition) const;
    
    /**
     * @brief 记录自适应历史
     */
    void recordHistory(const std::string& action, int partition_id, const std::string& details = "");
    
    /**
     * @brief 获取当前时间戳（毫秒）
     */
    static int64_t getCurrentTimeMs();
};

}  // namespace sageFlow

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "utils/metrics/join_metrics.h"

namespace sageFlow {
namespace metrics {

/**
 * @brief 单次 Join 执行的统计信息快照
 *
 * 汇总来自 JoinMetrics 的原始计数器，并提供计算指标（召回率、精确率等）。
 */
struct JoinExecutionStats {
    // ==================== 时间指标（纳秒） ====================
    std::chrono::nanoseconds total_time{0};
    std::chrono::nanoseconds index_build_time{0};
    std::chrono::nanoseconds query_time{0};
    std::chrono::nanoseconds window_eviction_time{0};
    std::chrono::nanoseconds similarity_time{0};
    std::chrono::nanoseconds join_function_time{0};
    std::chrono::nanoseconds emit_time{0};
    std::chrono::nanoseconds lock_wait_time{0};

    // ==================== 数据规模 ====================
    int64_t left_records_processed = 0;
    int64_t right_records_processed = 0;
    int64_t total_records_in_window = 0;

    // ==================== 匹配统计 ====================
    int64_t total_comparisons = 0;   ///< 总比较次数
    int64_t candidate_pairs = 0;     ///< 候选对数
    int64_t output_matches = 0;      ///< 输出匹配数

    // ==================== 准确性指标 ====================
    int64_t true_positives = 0;
    int64_t false_positives = 0;
    int64_t false_negatives = 0;

    // ==================== 索引统计 ====================
    int64_t index_inserts = 0;
    int64_t index_deletes = 0;
    int64_t index_queries = 0;
    int64_t index_rebuilds = 0;

    // ==================== 计算指标 ====================

    /**
     * @brief 计算召回率
     * @return 召回率 (true_positives / (true_positives + false_negatives))
     */
    [[nodiscard]] double recall() const {
        int64_t relevant = true_positives + false_negatives;
        return relevant > 0 ? static_cast<double>(true_positives) / static_cast<double>(relevant) : 0.0;
    }

    /**
     * @brief 计算精确率
     * @return 精确率 (true_positives / (true_positives + false_positives))
     */
    [[nodiscard]] double precision() const {
        int64_t retrieved = true_positives + false_positives;
        return retrieved > 0 ? static_cast<double>(true_positives) / static_cast<double>(retrieved) : 0.0;
    }

    /**
     * @brief 计算 F1 分数
     * @return F1 = 2 * precision * recall / (precision + recall)
     */
    [[nodiscard]] double f1Score() const {
        double r = recall();
        double p = precision();
        return (r + p > 0) ? 2 * r * p / (r + p) : 0.0;
    }

    /**
     * @brief 计算吞吐量（记录/秒）
     * @return 每秒处理的记录数
     */
    [[nodiscard]] double throughputRecordsPerSec() const {
        double sec = std::chrono::duration<double>(total_time).count();
        return sec > 0 ? static_cast<double>(left_records_processed + right_records_processed) / sec : 0.0;
    }

    /**
     * @brief 计算平均查询时间（微秒）
     * @return 平均每次查询的耗时
     */
    [[nodiscard]] double avgQueryTimeUs() const {
        double us = std::chrono::duration<double, std::micro>(query_time).count();
        return index_queries > 0 ? us / static_cast<double>(index_queries) : 0.0;
    }

    /**
     * @brief 计算平均端到端延迟（微秒）
     * @param e2e_latency_ns 累计端到端延迟（纳秒）
     * @param e2e_latency_count 延迟测量次数
     * @return 平均端到端延迟
     */
    [[nodiscard]] static double avgE2ELatencyUs(uint64_t e2e_latency_ns, uint64_t e2e_latency_count) {
        return e2e_latency_count > 0
                   ? static_cast<double>(e2e_latency_ns) / static_cast<double>(e2e_latency_count) / 1000.0
                   : 0.0;
    }
};

/**
 * @brief Join 指标收集器
 *
 * 线程安全的指标收集器，支持增量更新和快照获取。
 * 扩展现有 JoinMetrics 单例，提供更丰富的统计功能。
 */
class JoinMetricsCollector {
  public:
    using Clock = std::chrono::high_resolution_clock;

    /**
     * @brief 构造函数
     * @param name 收集器名称（通常是算法名或 subtask 标识）
     */
    explicit JoinMetricsCollector(std::string name);

    /**
     * @brief 获取收集器名称
     */
    [[nodiscard]] const std::string& name() const { return name_; }

    // ==================== 时间记录 ====================

    /**
     * @brief 开始计时
     * @param phase 阶段名（如 "index_build", "query", "total"）
     */
    void startTimer(const std::string& phase);

    /**
     * @brief 停止计时并累加到对应指标
     * @param phase 阶段名
     */
    void stopTimer(const std::string& phase);

    /**
     * @brief RAII 计时器
     */
    class ScopedTimer {
      public:
        ScopedTimer(JoinMetricsCollector& collector, std::string phase);
        ~ScopedTimer();

        // 禁止拷贝
        ScopedTimer(const ScopedTimer&) = delete;
        ScopedTimer& operator=(const ScopedTimer&) = delete;

        // 允许移动
        ScopedTimer(ScopedTimer&& other) noexcept;
        ScopedTimer& operator=(ScopedTimer&& other) noexcept;

      private:
        JoinMetricsCollector* collector_;
        std::string phase_;
        bool active_;
    };

    /**
     * @brief 创建 RAII 计时器
     * @param phase 阶段名
     * @return RAII 计时器对象
     */
    [[nodiscard]] ScopedTimer scopedTimer(const std::string& phase) { return ScopedTimer(*this, phase); }

    // ==================== 计数更新 ====================

    void recordLeftProcessed(int64_t count = 1);
    void recordRightProcessed(int64_t count = 1);
    void recordComparison(int64_t count = 1);
    void recordCandidate(int64_t count = 1);
    void recordMatch(int64_t count = 1);

    void recordIndexInsert(int64_t count = 1);
    void recordIndexDelete(int64_t count = 1);
    void recordIndexQuery(int64_t count = 1);
    void recordIndexRebuild();

    // ==================== 准确性记录 ====================

    /**
     * @brief 批量更新准确性指标
     * @param tp 真阳性数
     * @param fp 假阳性数
     * @param fn 假阴性数
     */
    void updateAccuracyMetrics(int64_t tp, int64_t fp, int64_t fn);

    /**
     * @brief 增量记录真阳性
     */
    void recordTruePositive(int64_t count = 1);

    /**
     * @brief 增量记录假阳性
     */
    void recordFalsePositive(int64_t count = 1);

    /**
     * @brief 增量记录假阴性
     */
    void recordFalseNegative(int64_t count = 1);

    // ==================== 快照获取 ====================

    /**
     * @brief 获取当前统计快照
     * @return 当前统计的不可变副本
     */
    [[nodiscard]] JoinExecutionStats snapshot() const;

    /**
     * @brief 从全局 JoinMetrics 单例创建快照
     * @return 基于全局指标的统计快照
     */
    [[nodiscard]] static JoinExecutionStats snapshotFromGlobal();

    /**
     * @brief 重置所有统计
     */
    void reset();

  private:
    std::string name_;
    mutable std::mutex mutex_;

    // 本地统计（独立于全局 JoinMetrics）
    JoinExecutionStats stats_;

    // 活动计时器
    std::unordered_map<std::string, Clock::time_point> active_timers_;
};

/**
 * @brief 全局指标注册表
 *
 * 管理多个 JoinMetricsCollector 实例，支持按名称查找。
 */
class JoinMetricsRegistry {
  public:
    /**
     * @brief 获取单例
     */
    static JoinMetricsRegistry& instance();

    /**
     * @brief 注册或获取收集器
     * @param name 收集器名称
     * @return 收集器的共享指针
     */
    std::shared_ptr<JoinMetricsCollector> getOrCreate(const std::string& name);

    /**
     * @brief 获取已存在的收集器
     * @param name 收集器名称
     * @return 收集器指针，不存在时返回 nullptr
     */
    std::shared_ptr<JoinMetricsCollector> get(const std::string& name);

    /**
     * @brief 获取所有收集器名称
     */
    [[nodiscard]] std::vector<std::string> getCollectorNames() const;

    /**
     * @brief 获取所有收集器的快照
     */
    [[nodiscard]] std::unordered_map<std::string, JoinExecutionStats> allSnapshots() const;

    /**
     * @brief 重置所有收集器
     */
    void resetAll();

    /**
     * @brief 移除指定收集器
     * @param name 收集器名称
     * @return 是否成功移除
     */
    bool remove(const std::string& name);

    /**
     * @brief 移除所有收集器
     */
    void clear();

  private:
    JoinMetricsRegistry() = default;

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<JoinMetricsCollector>> collectors_;
};

}  // namespace metrics
}  // namespace sageFlow

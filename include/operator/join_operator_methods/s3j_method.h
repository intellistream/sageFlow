#pragma once

#include "operator/join_operator_methods/base_method.h"
#include "operator/join_operator_methods/s3j_components/adaptive_partitioner.h"
#include "operator/join_operator_methods/s3j_components/adaptive_index_selector.h"
#include "concurrency/concurrency_manager.h"
#include "state/window_state.h"
#include "execution/runtime_context.h"
#include "index/partitioned_index.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace sageFlow {

/**
 * @brief S3J 方法配置
 */
struct S3JConfig {
    double similarity_threshold = 0.8;   ///< 相似度阈值
    int num_partitions = 16;             ///< 分区数量
    int64_t adapt_interval_ms = 1000;    ///< 自适应调整间隔（毫秒）
    double load_threshold = 0.3;         ///< 负载不均衡阈值
    double index_switch_threshold = 0.2; ///< 索引切换阈值
    bool enable_metrics = true;          ///< 启用指标收集
    bool enable_adaptive = true;         ///< 启用自适应调整
    int dimension = 128;                 ///< 向量维度
    int nlist = 100;                     ///< IVF 聚类数
    int nprobes = 10;                    ///< IVF 探测数
};

/**
 * @brief S3J 运行时指标
 */
struct S3JMetrics {
    double avg_latency_ms = 0.0;
    double throughput_qps = 0.0;
    double recall_estimate = 0.0;
    int current_partitions = 0;
    std::string current_index_type;
    std::vector<AdaptHistory> adapt_history;
    size_t total_queries = 0;
    size_t total_matches = 0;
};

/**
 * @brief S3J 方法
 * 
 * DEBS'23 论文的实现：自适应分布式流式相似度 Join
 * 
 * 核心特性：
 * 1. 自适应分区：根据数据分布动态调整分区策略
 * 2. 自适应索引选择：根据数据特性选择最佳索引类型
 * 3. 滑动窗口：高效的窗口状态维护
 * 4. 负载感知：实时监控和调整
 * 
 * 推荐配置：
 * - partition_strategy: centroid（质心分区）
 * - window_state_type: partitioned（分区状态）
 * - index_strategy: adaptive（自适应索引）
 */
class S3JMethod final : public BaseMethod {
public:
    /**
     * @brief 构造函数
     * @param left_index_id 左流索引 ID
     * @param right_index_id 右流索引 ID
     * @param threshold 相似度阈值
     * @param concurrency_manager 并发管理器
     * @param config S3J 配置
     */
    S3JMethod(int left_index_id,
              int right_index_id,
              double threshold,
              const std::shared_ptr<ConcurrencyManager>& concurrency_manager,
              const S3JConfig& config = S3JConfig());
    
    /**
     * @brief 简化构造函数
     * @param threshold 相似度阈值
     * @param config S3J 配置
     */
    explicit S3JMethod(double threshold, const S3JConfig& config = S3JConfig());
    
    ~S3JMethod() override = default;
    
    // 禁用拷贝
    S3JMethod(const S3JMethod&) = delete;
    S3JMethod& operator=(const S3JMethod&) = delete;
    
    /**
     * @brief 获取方法名称
     */
    std::string getName() const { return "S3J"; }
    
    /**
     * @brief 初始化方法
     * @param context 运行时上下文
     * @param left_state 左流窗口状态
     * @param right_state 右流窗口状态
     */
    void open(const RuntimeContext& context,
              WindowState* left_state,
              WindowState* right_state);
    
    /**
     * @brief Eager 模式：对单个查询向量执行匹配
     * @param query_record 查询向量记录
     * @param query_slot 查询来源槽位 (0=左流, 1=右流)
     * @return 匹配结果列表
     */
    std::vector<RecordView> ExecuteEager(
        const VectorRecord& query_record,
        int query_slot,
        size_t subtask_index = 0) override;
    
    /**
     * @brief 关闭方法
     */
    void close();
    
    /**
     * @brief 获取运行时指标
     */
    S3JMetrics getMetrics() const;
    
    /**
     * @brief 强制触发自适应调整
     */
    void forceAdapt();
    
    /**
     * @brief 设置并发管理器
     */
    void setConcurrencyManager(const std::shared_ptr<ConcurrencyManager>& manager);
    
    /**
     * @brief 设置窗口状态
     */
    void setWindowStates(WindowState* left_state, WindowState* right_state);
    
    /**
     * @brief 获取配置
     */
    const S3JConfig& getConfig() const { return config_; }
    
    /**
     * @brief 检查是否已初始化
     */
    bool isInitialized() const { return initialized_; }

private:
    S3JConfig config_;
    
    // 索引 ID
    int left_index_id_ = -1;
    int right_index_id_ = -1;
    
    // 窗口状态（非拥有）
    WindowState* left_state_ = nullptr;
    WindowState* right_state_ = nullptr;
    
    // 运行时信息
    size_t subtask_index_ = 0;
    size_t parallelism_ = 1;
    bool initialized_ = false;
    
    // 核心组件
    std::shared_ptr<AdaptivePartitioner> partitioner_;
    std::shared_ptr<AdaptiveIndexSelector> index_selector_;
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
    
    // 当前索引类型
    IndexType current_index_type_ = IndexType::IVF;
    
    // 指标收集
    struct MetricsCollector {
        std::atomic<size_t> query_count{0};
        std::atomic<int64_t> total_latency_us{0};
        std::atomic<size_t> match_count{0};
        std::chrono::steady_clock::time_point start_time;
        
        void reset() {
            query_count.store(0);
            total_latency_us.store(0);
            match_count.store(0);
            start_time = std::chrono::steady_clock::now();
        }
    };
    mutable MetricsCollector metrics_collector_;
    
    /**
     * @brief 获取对侧索引 ID
     */
    int otherIndexId(int slot) const;
    
    /**
     * @brief 执行自适应检查
     */
    void maybeAdapt();
    
    /**
     * @brief 切换索引类型
     * @param new_type 新的索引类型
     * @return 是否成功切换
     */
    bool switchIndex(IndexType new_type);
    
    /**
     * @brief 在分区内搜索
     */
    std::vector<std::shared_ptr<const VectorRecord>> searchInPartition(
        const VectorRecord& query, int slot, double threshold);
    
    /**
     * @brief 使用窗口状态执行搜索
     */
    std::vector<RecordView> searchInWindowState(
        const VectorRecord& query, int slot);
    
    /**
     * @brief 计算余弦相似度
     */
    double computeCosineSimilarity(
        const std::vector<float>& a, 
        const std::vector<float>& b) const;
    
    /**
     * @brief 提取浮点向量
     */
    std::vector<float> extractFloatVector(const VectorRecord& record) const;
};

}  // namespace sageFlow

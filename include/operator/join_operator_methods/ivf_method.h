#pragma once

#include <deque>
#include <memory>
#include <vector>
#include <atomic>
#include "operator/join_operator_methods/base_method.h"
#include "operator/utils/join_strategy_config.h"
#include "state/window_state.h"
#include "execution/runtime_context.h"
#include "concurrency/concurrency_manager.h"

namespace sageFlow {

/**
 * @brief IVF (Inverted File) Enhanced Join 方法
 * 
 * 基于 IVF 倒排索引的近似 Join 实现，利用聚类分区进行高效的近似最近邻搜索。
 * 
 * 算法原理：
 * 1. 聚类分区：使用 k-means 将向量空间划分为 nlist 个簇
 * 2. 倒排索引：每个簇维护一个倒排列表，存储属于该簇的向量
 * 3. 多路搜索：查询时搜索 nprobes 个最近的簇
 * 4. 精确计算：在候选集中计算精确距离并过滤
 * 
 * 算法复杂度：
 * - 查询复杂度: O(D * nlist + D * nprobes * N/nlist)
 *   - 第一项：质心距离计算
 *   - 第二项：候选向量精确距离计算
 * - 索引构建: O(N * D * nlist * iterations) (k-means)
 * 
 * 论文参考：
 * - Faiss: https://github.com/facebookresearch/faiss
 * - IEEE TBD 2017: Billion-scale similarity search with GPUs
 * 
 * 推荐配置：
 * - partition_strategy: round_robin（负载均衡分发）
 * - window_state_type: shared（共享状态）
 * - index_strategy: ivf
 */
class IVFMethod final : public BaseMethod {
public:
    /**
     * @brief IVF 方法配置
     */
    struct Config {
        double similarity_threshold = 0.8;  ///< 相似度阈值
        int nlist = 100;                    ///< 聚类数量
        int nprobes = 10;                   ///< 搜索的簇数量
        double rebuild_threshold = 0.2;    ///< 重建阈值（数据变化比例）
        bool use_existing_index = true;    ///< 是否复用已有索引
    };
    
    /**
     * @brief 索引统计信息
     */
    struct IndexStats {
        size_t num_elements = 0;           ///< 索引中的元素数量
        size_t num_clusters = 0;           ///< 实际聚类数量
        std::vector<size_t> cluster_sizes; ///< 各簇的大小
        double cluster_balance = 0.0;      ///< 簇大小均衡度 (标准差/均值)
    };
    
    /**
     * @brief 构造函数
     * @param config IVF 配置
     */
    explicit IVFMethod(const Config& config);
    
    /**
     * @brief 使用默认配置的构造函数
     * @param threshold 相似度阈值
     */
    explicit IVFMethod(double threshold);
    
    ~IVFMethod() override = default;
    
    // 禁用拷贝
    IVFMethod(const IVFMethod&) = delete;
    IVFMethod& operator=(const IVFMethod&) = delete;
    
    // 允许移动
    IVFMethod(IVFMethod&&) = default;
    IVFMethod& operator=(IVFMethod&&) = default;
    
    /**
     * @brief 获取方法名称
     * @return 方法名称 "IVF"
     */
    std::string getName() const { return "IVF"; }
    
    /**
     * @brief 初始化方法
     * @param context 运行时上下文
     * @param left_state 左流窗口状态
     * @param right_state 右流窗口状态
     * @param concurrency_manager 并发管理器
     */
    void open(const RuntimeContext& context,
              WindowState* left_state,
              WindowState* right_state,
              ConcurrencyManager* concurrency_manager);
    
    /**
     * @brief 兼容性 open 方法（不使用索引）
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
     * @param subtask_index 当前执行的 subtask 索引
     * @return 匹配结果列表
     */
    std::vector<RecordView> ExecuteEager(
        const VectorRecord& query_record,
        int query_slot,
        size_t subtask_index = 0) override;
    
    /**
     * @brief 关闭方法，释放资源
     */
    void close();
    
    /**
     * @brief 获取配置
     * @return 当前配置的常量引用
     */
    const Config& getConfig() const { return config_; }
    
    /**
     * @brief 设置 nprobes
     * @param nprobes 新的 nprobes 值
     */
    void setNprobes(int nprobes);
    
    /**
     * @brief 获取索引统计信息
     * @return 索引统计
     */
    IndexStats getStats() const;
    
    /**
     * @brief 检查是否已初始化
     * @return true 如果已调用 open()
     */
    bool isInitialized() const { return initialized_; }
    
    /**
     * @brief 检查是否使用索引模式
     * @return true 如果使用 ConcurrencyManager 管理的索引
     */
    bool isUsingIndex() const { return concurrency_manager_ != nullptr; }
    
    /**
     * @brief 设置索引 ID
     * @param left_index_id 左侧索引 ID
     * @param right_index_id 右侧索引 ID
     */
    void setIndexIds(int32_t left_index_id, int32_t right_index_id) {
        left_index_id_ = left_index_id;
        right_index_id_ = right_index_id;
    }

    /**
     * @brief 设置 ConcurrencyManager
     * @param cm ConcurrencyManager 指针
     */
    void setConcurrencyManager(ConcurrencyManager* cm) {
        concurrency_manager_ = cm;
    }
    
    void setConcurrencyManager(std::shared_ptr<ConcurrencyManager> cm) {
        concurrency_manager_ = cm.get();
    }

private:
    Config config_;
    
    // 窗口状态（非拥有）
    WindowState* left_state_ = nullptr;
    WindowState* right_state_ = nullptr;
    
    // 运行时信息
    size_t subtask_index_ = 0;
    size_t parallelism_ = 1;
    
    // 索引管理（可选）
    ConcurrencyManager* concurrency_manager_ = nullptr;
    int32_t left_index_id_ = -1;
    int32_t right_index_id_ = -1;
    
    // 重建追踪
    std::atomic<size_t> last_rebuild_size_{0};
    std::atomic<size_t> current_size_{0};
    
    // 初始化标志
    bool initialized_ = false;
    
    /**
     * @brief 通过索引执行范围搜索
     * @param query 查询向量
     * @param index_id 索引ID
     * @return 满足阈值的候选记录
     */
    std::vector<std::shared_ptr<const VectorRecord>> rangeSearchWithIndex(
        const VectorRecord& query,
        int32_t index_id);
    
    /**
     * @brief 通过窗口状态快照执行暴力范围搜索（线程安全版本）
     * @param query 查询向量
     * @param records 待搜索的记录快照
     * @return 满足阈值的匹配记录
     */
    std::vector<RecordView> rangeSearchBruteForceSnapshot(
        const VectorRecord& query,
        const std::vector<RecordView>& records);
    
    /**
     * @brief 通过窗口状态执行暴力范围搜索（降级模式）
     * @param query 查询向量
     * @param records 待搜索的记录集
     * @return 满足阈值的匹配记录
     */
    std::vector<RecordView> rangeSearchBruteForce(
        const VectorRecord& query,
        const std::deque<RecordView>& records);
    
    /**
     * @brief 计算两个向量的相似度
     * 使用 L2 距离 + 指数衰减 (exp(-alpha * dist))，与 ComputeEngine::Similarity 一致
     * @param a 第一个向量
     * @param b 第二个向量
     * @return 相似度值，范围 [0.0, 1.0]
     */
    double computeSimilarity(
        const std::vector<float>& a,
        const std::vector<float>& b) const;
    
    /**
     * @brief 获取对侧的索引ID
     * @param slot 当前槽位
     * @return 对侧索引ID
     */
    int32_t getOppositeIndexId(int slot) const {
        return (slot == 0) ? right_index_id_ : left_index_id_;
    }
};

} // namespace sageFlow

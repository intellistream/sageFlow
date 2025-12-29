#pragma once

#include <deque>
#include <memory>
#include <vector>
#include "operator/join_operator_methods/base_method.h"
#include "state/window_state.h"
#include "execution/runtime_context.h"

namespace sageFlow {

/**
 * @brief BruteForce Ground Truth 方法
 * 
 * 精确匹配实现，用于：
 * 1. 作为 Ground Truth 验证其他方法的召回率
 * 2. 小规模数据的精确 Join（窗口大小 < 1000）
 * 3. 调试时作为对照组
 * 
 * 算法复杂度：
 * - 时间复杂度: O(N * M * D)，N=查询数量，M=窗口记录数，D=向量维度
 * - 空间复杂度: O(M * D)，仅存储窗口内记录
 * 
 * 推荐配置：
 * - partition_strategy: round_robin（负载均衡分发）
 * - window_state_type: shared（共享状态保证100%召回）
 * - index_strategy: none（精确匹配无需索引）
 */
class BruteForceBaseline final : public BaseMethod {
public:
    /**
     * @brief 构造函数
     * @param threshold 相似度阈值，范围 [0.0, 1.0]
     */
    explicit BruteForceBaseline(double threshold);
    
    ~BruteForceBaseline() override = default;
    
    // 禁用拷贝
    BruteForceBaseline(const BruteForceBaseline&) = delete;
    BruteForceBaseline& operator=(const BruteForceBaseline&) = delete;
    
    // 允许移动
    BruteForceBaseline(BruteForceBaseline&&) = default;
    BruteForceBaseline& operator=(BruteForceBaseline&&) = default;
    
    /**
     * @brief 获取方法名称
     * @return 方法名称 "BruteForce"
     */
    std::string getName() const { return "BruteForce"; }
    
    /**
     * @brief 初始化方法
     * @param context 运行时上下文，提供 subtask_index 等信息
     * @param left_state 左流的窗口状态
     * @param right_state 右流的窗口状态
     */
    void open(const RuntimeContext& context,
              WindowState* left_state,
              WindowState* right_state);
    
    /**
     * @brief Eager 模式：对单个查询向量执行匹配
     * 
     * 遍历对侧窗口内所有记录，计算余弦相似度，返回满足阈值的结果
     * 
     * @param query_record 查询向量记录
     * @param query_slot 查询来源槽位 (0=左流, 1=右流)
     * @param subtask_index 当前执行的 subtask 索引
     * @return 匹配结果列表（记录副本）
     */
    std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query_record,
        int query_slot,
        size_t subtask_index = 0) override;
    
    /**
     * @brief 关闭方法，释放资源
     */
    void close();
    
    /**
     * @brief 获取阈值
     * @return 当前相似度阈值
     */
    double getThreshold() const { return join_similarity_threshold_; }
    
    /**
     * @brief 设置阈值
     * @param threshold 新的相似度阈值
     */
    void setThreshold(double threshold) { join_similarity_threshold_ = threshold; }
    
    /**
     * @brief 检查是否已初始化
     * @return true 如果已调用 open()
     */
    bool isInitialized() const { return initialized_; }

private:
    // 左右流的窗口状态（非拥有）
    WindowState* left_state_ = nullptr;
    WindowState* right_state_ = nullptr;
    
    // 运行时信息
    size_t subtask_index_ = 0;
    size_t parallelism_ = 1;
    
    // 初始化标志
    bool initialized_ = false;
    
    /**
     * @brief 计算两个向量的相似度
     * 使用 L2 距离 + 指数衰减 (exp(-alpha * dist))，与 ComputeEngine::Similarity 一致
     * @param a 第一个向量（从 VectorRecord 提取）
     * @param b 第二个向量（从 VectorRecord 提取）
     * @return 相似度值，范围 [0.0, 1.0]
     */
    double computeSimilarity(
        const std::vector<float>& a, 
        const std::vector<float>& b) const;
    
    /**
     * @brief 在给定记录快照中搜索满足阈值的匹配（线程安全版本）
     * @param query 查询向量记录
     * @param records 待搜索的记录快照（shared_ptr 版本）
     * @return 匹配结果列表（记录副本）
     */
    std::vector<std::unique_ptr<VectorRecord>> searchInRecordsSnapshot(
        const VectorRecord& query,
        const std::vector<std::shared_ptr<const VectorRecord>>& records) const;
    
    /**
     * @brief 在给定记录集中搜索满足阈值的匹配
     * @param query 查询向量记录
     * @param records 待搜索的记录集
     * @return 匹配结果列表（记录副本）
     */
    std::vector<std::unique_ptr<VectorRecord>> searchInRecords(
        const VectorRecord& query,
        const std::deque<std::unique_ptr<VectorRecord>>& records) const;
};

} // namespace sageFlow

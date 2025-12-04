#pragma once

#include "index/index.h"

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace sageFlow {

/**
 * @brief 索引性能评估结果
 */
struct IndexPerformance {
    double avg_latency_us = 0.0;    ///< 平均延迟（微秒）
    double recall = 0.0;            ///< 召回率
    double memory_mb = 0.0;         ///< 内存使用（MB）
    size_t sample_count = 0;        ///< 采样数量
    
    bool isValid() const { return sample_count > 0; }
};

/**
 * @brief 自适应索引选择器配置
 */
struct AdaptiveIndexSelectorConfig {
    double switch_threshold = 0.2;   ///< 性能差距阈值（相对改进）
    int sample_size = 100;           ///< 采样大小
    int eval_interval = 1000;        ///< 评估间隔（记录数）
    bool enable_auto_switch = true;  ///< 启用自动切换
};

/**
 * @brief 自适应索引选择器
 * 
 * 根据数据特性选择最佳索引类型。
 * 基于 DEBS'23 S3J 论文的自适应索引思想。
 * 
 * 支持的索引类型：
 * - BruteForce: 小规模数据，精确查询
 * - IVF: 中等规模数据，平衡速度和召回
 * - HNSW: 大规模数据，高速近似查询
 */
class AdaptiveIndexSelector {
public:
    /**
     * @brief 构造函数
     * @param config 配置参数
     */
    explicit AdaptiveIndexSelector(const AdaptiveIndexSelectorConfig& config = AdaptiveIndexSelectorConfig());
    
    ~AdaptiveIndexSelector() = default;
    
    // 禁用拷贝，允许移动
    AdaptiveIndexSelector(const AdaptiveIndexSelector&) = delete;
    AdaptiveIndexSelector& operator=(const AdaptiveIndexSelector&) = delete;
    AdaptiveIndexSelector(AdaptiveIndexSelector&&) = default;
    AdaptiveIndexSelector& operator=(AdaptiveIndexSelector&&) = default;
    
    /**
     * @brief 选择最佳索引类型
     * @param dimension 向量维度
     * @param data_size 数据规模
     * @param query_rate 查询频率（QPS）
     * @return 推荐的索引类型
     */
    IndexType selectBestIndex(int dimension, size_t data_size, double query_rate) const;
    
    /**
     * @brief 根据当前性能判断是否应该切换索引
     * @param current_type 当前索引类型
     * @param current_perf 当前索引性能
     * @param data_size 数据规模
     * @param dimension 向量维度
     * @return 推荐的索引类型（如果不需要切换则返回 current_type）
     */
    IndexType shouldSwitchIndex(IndexType current_type,
                                 const IndexPerformance& current_perf,
                                 size_t data_size,
                                 int dimension) const;
    
    /**
     * @brief 更新索引性能缓存
     * @param type 索引类型
     * @param perf 性能数据
     */
    void updatePerformanceCache(IndexType type, const IndexPerformance& perf);
    
    /**
     * @brief 获取索引的推荐参数
     * @param type 索引类型
     * @param dimension 向量维度
     * @param expected_size 预期数据规模
     * @return 参数映射 (key -> value)
     */
    std::map<std::string, std::string> getRecommendedParams(IndexType type,
                                                             int dimension,
                                                             size_t expected_size) const;
    
    /**
     * @brief 获取缓存的性能数据
     * @param type 索引类型
     * @return 性能数据（如果不存在则返回空的 IndexPerformance）
     */
    IndexPerformance getCachedPerformance(IndexType type) const;
    
    /**
     * @brief 清除性能缓存
     */
    void clearCache();
    
    /**
     * @brief 获取配置
     */
    const AdaptiveIndexSelectorConfig& getConfig() const { return config_; }
    
    /**
     * @brief 索引类型转字符串
     */
    static std::string indexTypeToString(IndexType type);
    
    /**
     * @brief 字符串转索引类型
     */
    static IndexType stringToIndexType(const std::string& str);

private:
    AdaptiveIndexSelectorConfig config_;
    
    // 历史性能缓存
    mutable std::unordered_map<IndexType, IndexPerformance> perf_cache_;
    
    /**
     * @brief 估算索引在给定条件下的理论性能
     * @param type 索引类型
     * @param dimension 向量维度
     * @param data_size 数据规模
     * @return 估算的性能
     */
    IndexPerformance estimatePerformance(IndexType type, int dimension, size_t data_size) const;
    
    /**
     * @brief 根据数据规模确定索引类型阈值
     */
    struct SizeThresholds {
        size_t bruteforce_max = 1000;      ///< BruteForce 最大数据量
        size_t ivf_preferred_min = 500;    ///< IVF 推荐最小数据量
        size_t ivf_preferred_max = 100000; ///< IVF 推荐最大数据量
        size_t hnsw_preferred_min = 10000; ///< HNSW 推荐最小数据量
    };
    SizeThresholds thresholds_;
};

}  // namespace sageFlow

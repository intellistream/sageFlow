/**
 * @file join_integration_pipeline_helper.h
 * @brief E-03: Join 集成测试 Pipeline 辅助类
 * 
 * 提供从配置到可执行 Pipeline 的完整转换流程，简化集成测试的 Pipeline 构建。
 */

#pragma once

#include "operator/utils/join_strategy_config.h"
#include "common/data_types.h"
#include "stream/data_stream_source/data_stream_source.h"
#include "function/sink_function.h"

#include <memory>
#include <vector>
#include <set>
#include <unordered_set>
#include <mutex>
#include <functional>
#include <chrono>

namespace sageFlow {

// Forward declarations
class StreamEnvironment;
class ConcurrencyManager;

namespace test {

/**
 * @brief 匹配结果对
 * 
 * 存储 Join 操作的一对匹配结果，包含左右 UID 和相似度分数。
 */
struct MatchPair {
    uint64_t left_uid;
    uint64_t right_uid;
    double similarity;
    
    bool operator==(const MatchPair& other) const {
        return left_uid == other.left_uid && right_uid == other.right_uid;
    }
    
    bool operator<(const MatchPair& other) const {
        if (left_uid != other.left_uid) return left_uid < other.left_uid;
        return right_uid < other.right_uid;
    }
    
    /**
     * @brief 用于 unordered_set 的哈希函数
     */
    struct Hash {
        size_t operator()(const MatchPair& p) const noexcept {
            uint64_t a = std::min(p.left_uid, p.right_uid);
            uint64_t b = std::max(p.left_uid, p.right_uid);
            uint64_t mix = a * 1315423911u ^ ((b << 13) | (b >> 7));
            return std::hash<uint64_t>{}(mix);
        }
    };
};

/**
 * @brief Pipeline 执行结果
 * 
 * 包含执行状态、性能指标和结果数据。
 */
struct PipelineExecutionResult {
    /// 匹配结果列表
    std::vector<MatchPair> matches;
    
    /// 执行时间（毫秒）
    double execution_time_ms = 0.0;

    /// Join 算法完成时间（毫秒）：以 JoinOperator emits stable 的时间点为准（并行 makespan）
    /// 注意：该时间不包含 Sink 追赶等待阶段，更接近“算法计算完成”的现实口径。
    double join_time_ms = 0.0;

    /// Sink 追赶等待耗时（毫秒）：从 emits stable 到等待结束（追平/稳定/超时）
    double sink_wait_ms = 0.0;

    /// Join 输出的总 emits 数（包含重复）
    uint64_t total_emits = 0;
    
    /// Sink 已处理的唯一输出数（去重后）
    uint64_t sink_processed = 0;
    
    /// Sink 去重拦截的输出数
    uint64_t sink_dedup = 0;
    
    /// 左流处理的记录数
    int64_t left_processed = 0;
    
    /// 右流处理的记录数
    int64_t right_processed = 0;
    
    /// 执行是否成功
    bool success = false;
    
    /// 错误信息（如果 success=false）
    std::string error_message;
    
    /// Sink 去重拦截的记录数（用于诊断 multicast 问题）
    int64_t dedup_count = 0;
    
    /**
     * @brief 获取匹配数量
     */
    [[nodiscard]] size_t matchCount() const { return matches.size(); }
};

/**
 * @brief 可执行的测试 Pipeline 接口
 * 
 * 抽象接口，用于执行 Join Pipeline 并获取结果。
 */
class ExecutableTestPipeline {
public:
    virtual ~ExecutableTestPipeline() = default;
    
    /**
     * @brief 执行 Pipeline 并返回结果
     * @return 执行结果
     */
    virtual PipelineExecutionResult execute() = 0;
    
    /**
     * @brief 获取已收集的匹配结果
     * @return 匹配结果列表
     */
    virtual std::vector<MatchPair> getMatches() const = 0;
};

/**
 * @brief 结果收集器 Sink
 * 
 * 用于收集 Join 输出结果的 Sink 实现。
 * 支持多线程安全访问。
 */
class MatchCollectorSink {
public:
    MatchCollectorSink() = default;
    ~MatchCollectorSink() = default;
    
    // 禁止拷贝，允许移动
    MatchCollectorSink(const MatchCollectorSink&) = delete;
    MatchCollectorSink& operator=(const MatchCollectorSink&) = delete;
    MatchCollectorSink(MatchCollectorSink&&) = default;
    MatchCollectorSink& operator=(MatchCollectorSink&&) = default;
    
    /**
     * @brief 打开收集器，重置状态
     */
    void open();
    
    /**
     * @brief 处理一条记录
     * @param record 输入记录（Join 输出的合并记录）
     */
    void invoke(std::unique_ptr<VectorRecord>& record);
    
    /**
     * @brief 关闭收集器
     */
    void close();
    
    /**
     * @brief 获取收集的匹配对
     * @return 匹配对列表（线程安全拷贝）
     */
    [[nodiscard]] std::vector<MatchPair> getMatches() const;
    
    /**
     * @brief 获取处理的记录数
     * @return 处理的记录总数
     */
    [[nodiscard]] int64_t getProcessedCount() const;
    
    /**
     * @brief 重置收集器状态
     */
    void reset();
    
    /**
     * @brief 获取去重拦截的记录数
     * @return 被去重拦截的记录数
     */
    [[nodiscard]] int64_t getDedupCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return dedup_count_;
    }

private:
    std::vector<MatchPair> matches_;
    struct PairKey {
        uint64_t left_uid = 0;
        uint64_t right_uid = 0;
        bool operator==(const PairKey& other) const {
            return left_uid == other.left_uid && right_uid == other.right_uid;
        }
    };
    struct PairKeyHash {
        size_t operator()(const PairKey& k) const noexcept {
            uint64_t x = k.left_uid ^ (k.right_uid + 0x9e3779b97f4a7c15ULL + (k.left_uid << 6) + (k.left_uid >> 2));
            return static_cast<size_t>(x);
        }
    };
    std::unordered_set<PairKey, PairKeyHash> seen_pairs_;  ///< 已见过的 (left_uid,right_uid)（用于 Sink 去重）
    int64_t processed_count_ = 0;
    int64_t dedup_count_ = 0;  ///< 被去重拦截的记录数
    mutable std::mutex mutex_;
};

/**
 * @brief Join 集成测试 Pipeline 辅助类
 * 
 * 提供从配置到可执行 Pipeline 的完整转换。
 * 
 * 使用示例:
 * @code
 * JoinStrategyConfig config;
 * config.algorithm = JoinAlgorithm::BRUTEFORCE;
 * config.similarity_threshold = 0.8;
 * 
 * auto pipeline = JoinIntegrationPipelineHelper::createPipeline(
 *     std::move(left_records), std::move(right_records), config, 4);
 * 
 * auto result = pipeline->execute();
 * std::cout << "Matches: " << result.matches.size() << std::endl;
 * @endcode
 */
class JoinIntegrationPipelineHelper {
public:
    /**
     * @brief 从 VectorRecord 向量创建 Pipeline
     * 
     * @param left_stream 左流数据
     * @param right_stream 右流数据
     * @param config Join 策略配置
     * @param parallelism 并行度（默认=1）
     * @return 可执行的 Pipeline
     */
    static std::unique_ptr<ExecutableTestPipeline> createPipeline(
        std::vector<std::unique_ptr<VectorRecord>> left_stream,
        std::vector<std::unique_ptr<VectorRecord>> right_stream,
        const JoinStrategyConfig& config,
        int parallelism = 1);
    
    /**
     * @brief 从 DataStreamSource 创建 Pipeline
     * 
     * @param left_source 左流数据源
     * @param right_source 右流数据源
     * @param config Join 策略配置
     * @param parallelism 并行度（默认=1）
     * @return 可执行的 Pipeline
     */
    static std::unique_ptr<ExecutableTestPipeline> createPipeline(
        std::shared_ptr<DataStreamSource> left_source,
        std::shared_ptr<DataStreamSource> right_source,
        const JoinStrategyConfig& config,
        int parallelism = 1);
    
    /**
     * @brief 创建自 Join Pipeline（同一流与自身 Join）
     * 
     * @param stream 输入流数据
     * @param config Join 策略配置
     * @param parallelism 并行度（默认=1）
     * @return 可执行的 Pipeline
     */
    static std::unique_ptr<ExecutableTestPipeline> createSelfJoinPipeline(
        std::vector<std::unique_ptr<VectorRecord>> stream,
        const JoinStrategyConfig& config,
        int parallelism = 1);
    
    /**
     * @brief 验证配置并创建 Pipeline
     * 
     * 会先使用 JoinConfigValidator 验证配置，
     * 如果验证失败则抛出异常。
     * 
     * @param left_stream 左流数据
     * @param right_stream 右流数据
     * @param config Join 策略配置
     * @param parallelism 并行度（默认=1）
     * @return 可执行的 Pipeline
     * @throws std::runtime_error 如果配置验证失败
     */
    static std::unique_ptr<ExecutableTestPipeline> createValidatedPipeline(
        std::vector<std::unique_ptr<VectorRecord>> left_stream,
        std::vector<std::unique_ptr<VectorRecord>> right_stream,
        const JoinStrategyConfig& config,
        int parallelism = 1);
    
    /**
     * @brief 获取 Join 方法字符串（用于 Stream::join()）
     * 
     * @param config Join 策略配置
     * @return Join 方法字符串（如 "bruteforce_eager"）
     */
    static std::string getJoinMethodString(const JoinStrategyConfig& config);
};

// ==================== 便捷工具函数 ====================

/**
 * @brief 便捷函数：执行 Join 并返回匹配数
 * 
 * @param left 左流数据
 * @param right 右流数据  
 * @param config Join 策略配置
 * @return 匹配对数量
 */
inline int64_t executeJoinAndCount(
    std::vector<std::unique_ptr<VectorRecord>> left,
    std::vector<std::unique_ptr<VectorRecord>> right,
    const JoinStrategyConfig& config) {
    
    auto pipeline = JoinIntegrationPipelineHelper::createPipeline(
        std::move(left), std::move(right), config);
    
    auto result = pipeline->execute();
    return static_cast<int64_t>(result.matches.size());
}

/**
 * @brief 计算召回率
 * 
 * @param actual 实际匹配结果
 * @param expected 预期匹配结果（Ground Truth）
 * @return 召回率 [0.0, 1.0]
 */
inline double computeRecall(
    const std::vector<MatchPair>& actual,
    const std::vector<MatchPair>& expected) {
    
    if (expected.empty()) return 1.0;
    
    std::set<MatchPair> expected_set(expected.begin(), expected.end());
    std::set<MatchPair> actual_set(actual.begin(), actual.end());
    
    int64_t true_positives = 0;
    for (const auto& match : actual_set) {
        if (expected_set.count(match) > 0) {
            true_positives++;
        }
    }
    
    return static_cast<double>(true_positives) / static_cast<double>(expected.size());
}

/**
 * @brief 计算精确率
 * 
 * @param actual 实际匹配结果
 * @param expected 预期匹配结果（Ground Truth）
 * @return 精确率 [0.0, 1.0]
 */
inline double computePrecision(
    const std::vector<MatchPair>& actual,
    const std::vector<MatchPair>& expected) {
    
    if (actual.empty()) return 1.0;
    
    std::set<MatchPair> expected_set(expected.begin(), expected.end());
    
    int64_t true_positives = 0;
    for (const auto& match : actual) {
        if (expected_set.count(match) > 0) {
            true_positives++;
        }
    }
    
    return static_cast<double>(true_positives) / static_cast<double>(actual.size());
}

/**
 * @brief 计算 F1 分数
 * 
 * @param recall 召回率
 * @param precision 精确率
 * @return F1 分数 [0.0, 1.0]
 */
inline double computeF1Score(double recall, double precision) {
    if (recall + precision == 0.0) return 0.0;
    return 2.0 * recall * precision / (recall + precision);
}

}  // namespace test
}  // namespace sageFlow

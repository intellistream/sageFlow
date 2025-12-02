#pragma once

#include "operator/join_strategy_config.h"
#include "operator/join_operator_methods/base_method.h"
#include "state/window_state.h"
#include "execution/partitioner.h"
#include "execution/vector_space_partitioner.h"
#include "concurrency/concurrency_manager.h"

#include <memory>
#include <optional>
#include <string>

namespace sageFlow {

// Forward declarations to avoid circular includes
class PartitionCoordinator;
class AsyncCandidateGenerator;
class DistanceVerifier;
class CentroidPartitioner;
class Index;

/**
 * @brief Join 策略工厂
 * 
 * 根据配置创建完整的 Join 策略组件，包括：
 * - JoinMethod (候选生成和执行逻辑)
 * - WindowState (左右窗口状态)
 * - Partitioner (上游到 Join 算子的分区器)
 * - Index (共享或分区索引)
 * - VSJoin/S3J 专用组件
 */
class JoinStrategyFactory {
public:
    /**
     * @brief 策略组件集合
     * 
     * 包含 Join 操作所需的所有组件。
     * 每个组件根据配置创建，不使用的组件为 nullptr。
     */
    struct StrategyComponents {
        // ==================== 核心组件 ====================
        
        /// Join 方法实现
        std::unique_ptr<BaseMethod> join_method;
        
        /// 左流窗口状态
        std::unique_ptr<WindowState> left_state;
        
        /// 右流窗口状态
        std::unique_ptr<WindowState> right_state;
        
        /// 数据分区器
        std::unique_ptr<IPartitioner> partitioner;
        
        // ==================== 索引配置 ====================
        
        /// 左流索引 ID（共享索引模式）
        int left_index_id = -1;
        
        /// 右流索引 ID（共享索引模式）
        int right_index_id = -1;
        
        /// 左流分区索引（分区索引模式）
        std::shared_ptr<Index> left_partitioned_index;
        
        /// 右流分区索引（分区索引模式）
        std::shared_ptr<Index> right_partitioned_index;
        
        // ==================== VSJoin 专用组件 ====================
        
        /// 向量空间分区器
        std::shared_ptr<VectorSpacePartitioner> vector_partitioner;
        
        /// 分区协调器
        std::shared_ptr<PartitionCoordinator> coordinator;
        
        /// 左流异步候选生成器
        std::shared_ptr<AsyncCandidateGenerator> left_async_gen;
        
        /// 右流异步候选生成器
        std::shared_ptr<AsyncCandidateGenerator> right_async_gen;
        
        /// 距离验证器
        std::shared_ptr<DistanceVerifier> verifier;
        
        // ==================== S3J/ClusteredJoin 专用组件 ====================
        
        /// 质心分区器（用于 S3J 和 ClusteredJoin）
        std::shared_ptr<CentroidPartitioner> centroid_partitioner;
        
        // ==================== 辅助方法 ====================
        
        /// 检查核心组件是否有效
        [[nodiscard]] bool isValid() const {
            return join_method != nullptr;
        }
        
        /// 获取配置摘要
        [[nodiscard]] std::string summary() const;
    };
    
    /**
     * @brief 根据配置创建策略组件
     * 
     * 这是工厂的主入口，会根据配置自动创建所有必要的组件。
     * 
     * @param config 策略配置
     * @param concurrency_manager 并发管理器
     * @param parallelism 算子并行度
     * @return 策略组件集合
     * @throws std::runtime_error 如果配置无效或组件创建失败
     */
    static StrategyComponents create(
        const JoinStrategyConfig& config,
        std::shared_ptr<ConcurrencyManager> concurrency_manager,
        size_t parallelism);
    
    /**
     * @brief 仅创建 JoinMethod
     * 
     * 在需要复用现有索引和状态的情况下使用。
     * 
     * @param config 策略配置
     * @param concurrency_manager 并发管理器
     * @param left_index_id 左流索引 ID（-1 表示不使用共享索引）
     * @param right_index_id 右流索引 ID（-1 表示不使用共享索引）
     * @return JoinMethod 实例
     */
    static std::unique_ptr<BaseMethod> createJoinMethod(
        const JoinStrategyConfig& config,
        std::shared_ptr<ConcurrencyManager> concurrency_manager,
        int left_index_id = -1,
        int right_index_id = -1);
    
    /**
     * @brief 仅创建 WindowState
     * 
     * @param config 策略配置
     * @param parallelism 并行度
     * @return WindowState 实例
     */
    static std::unique_ptr<WindowState> createWindowState(
        const JoinStrategyConfig& config,
        size_t parallelism);
    
    /**
     * @brief 仅创建 Partitioner
     * 
     * @param config 策略配置
     * @return Partitioner 实例
     */
    static std::unique_ptr<IPartitioner> createPartitioner(
        const JoinStrategyConfig& config);
    
    /**
     * @brief 创建向量空间分区器
     * 
     * 用于 VSJoin 的 LSH 分区或 S3J/ClusteredJoin 的质心分区。
     * 
     * @param config 策略配置
     * @return VectorSpacePartitioner 实例
     */
    static std::shared_ptr<VectorSpacePartitioner> createVectorSpacePartitioner(
        const JoinStrategyConfig& config);
    
    /**
     * @brief 创建质心分区器
     * 
     * 用于 S3J 和 ClusteredJoin。
     * 
     * @param config 策略配置
     * @return CentroidPartitioner 实例
     */
    static std::shared_ptr<CentroidPartitioner> createCentroidPartitioner(
        const JoinStrategyConfig& config);
    
    /**
     * @brief 创建索引对
     * 
     * 根据配置创建左右流的共享索引或分区索引。
     * 
     * @param config 策略配置
     * @param concurrency_manager 并发管理器
     * @param out_left_id 输出左流索引 ID
     * @param out_right_id 输出右流索引 ID
     * @return true 如果成功创建
     */
    static bool createIndexPair(
        const JoinStrategyConfig& config,
        std::shared_ptr<ConcurrencyManager> concurrency_manager,
        int& out_left_id,
        int& out_right_id);

private:
    // 内部辅助方法
    static std::unique_ptr<BaseMethod> createBruteForceMethod(
        const JoinStrategyConfig& config,
        std::shared_ptr<ConcurrencyManager> cm,
        int left_idx, int right_idx);
    
    static std::unique_ptr<BaseMethod> createIvfMethod(
        const JoinStrategyConfig& config,
        std::shared_ptr<ConcurrencyManager> cm,
        int left_idx, int right_idx);
    
    static std::unique_ptr<BaseMethod> createHnswMethod(
        const JoinStrategyConfig& config,
        std::shared_ptr<ConcurrencyManager> cm,
        int left_idx, int right_idx);
    
    static std::unique_ptr<BaseMethod> createHdrTreeMethod(
        const JoinStrategyConfig& config,
        std::shared_ptr<ConcurrencyManager> cm,
        int left_idx, int right_idx);
    
    static std::unique_ptr<BaseMethod> createClusteredJoinMethod(
        const JoinStrategyConfig& config,
        std::shared_ptr<ConcurrencyManager> cm,
        int left_idx, int right_idx);
    
    static std::unique_ptr<BaseMethod> createS3JMethod(
        const JoinStrategyConfig& config,
        std::shared_ptr<ConcurrencyManager> cm,
        int left_idx, int right_idx);
    
    static std::unique_ptr<BaseMethod> createVSJoinMethod(
        const JoinStrategyConfig& config,
        std::shared_ptr<ConcurrencyManager> cm,
        int left_idx, int right_idx);
    
    // 获取索引类型
    static IndexType getIndexType(const JoinStrategyConfig& config);
    
    // 获取索引参数
    static IndexParameters getIndexParameters(const JoinStrategyConfig& config);
};

}  // namespace sageFlow

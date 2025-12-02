//
// Created for sageFlow architecture refactoring - Phase 2
// Task C-04: WindowStateFactory 窗口状态自适应选择
//

#pragma once

#include "state/window_state.h"
#include "operator/join_strategy_config.h"

#include <memory>

namespace sageFlow {

// 前向声明
class VectorSpacePartitioner;

/**
 * @brief 窗口状态工厂
 *
 * 根据配置创建适当的窗口状态实例。支持以下窗口状态类型：
 *
 * | 类型                    | 描述                        | 适用场景               |
 * |------------------------|----------------------------|----------------------|
 * | SharedWindowState      | 共享状态，所有实例共享        | RoundRobin 分区       |
 * | PartitionedWindowState | 分区状态，每个 subtask 独立   | 内容分区（Key/Vector） |
 * | TwoTierWindowState     | 双层结构（写友好层+紧凑层）    | 高吞吐写入场景          |
 * | PartitionedVectorState | 向量空间分区状态              | VSJoin 专用           |
 *
 * @note 本工厂类专注于 WindowState 的创建，与 JoinStrategyFactory 解耦，
 *       以便在不同上下文中复用。
 */
class WindowStateFactory {
public:
    /**
     * @brief 根据类型创建窗口状态
     *
     * 根据指定的 WindowStateType 创建相应的窗口状态实例。
     *
     * @param type 窗口状态类型
     * @param parallelism 并行度（用于分区状态）
     * @param config 完整配置（用于获取特定参数，如压缩阈值等）
     * @param partitioner 向量分区器（仅 PARTITIONED_VECTOR 需要）
     * @return 窗口状态实例
     * @throws std::runtime_error 如果类型无效或缺少必要参数
     */
    static std::unique_ptr<WindowState> create(
        WindowStateType type,
        size_t parallelism,
        const JoinStrategyConfig& config,
        std::shared_ptr<VectorSpacePartitioner> partitioner = nullptr);

    /**
     * @brief 根据配置自动推断并创建窗口状态
     *
     * 使用 config.window_state_type 确定要创建的窗口状态类型。
     * 这是创建窗口状态的首选方法。
     *
     * @param config 策略配置
     * @param parallelism 并行度
     * @param partitioner 向量分区器（仅 VSJoin 等需要）
     * @return 窗口状态实例
     */
    static std::unique_ptr<WindowState> createFromConfig(
        const JoinStrategyConfig& config,
        size_t parallelism,
        std::shared_ptr<VectorSpacePartitioner> partitioner = nullptr);

    /**
     * @brief 使用默认配置创建窗口状态
     *
     * 使用默认的 JoinStrategyConfig 创建窗口状态，适用于简单场景。
     *
     * @param type 窗口状态类型
     * @param parallelism 并行度
     * @return 窗口状态实例
     */
    static std::unique_ptr<WindowState> createWithDefaults(
        WindowStateType type,
        size_t parallelism);

    /**
     * @brief 验证窗口状态类型与分区策略的兼容性
     *
     * 检查给定的窗口状态类型是否与分区策略兼容。
     * 例如：RoundRobin 分区应使用 SharedWindowState。
     *
     * @param state_type 窗口状态类型
     * @param partition_strategy 分区策略
     * @return true 如果兼容
     */
    static bool isCompatible(
        WindowStateType state_type,
        PartitionStrategy partition_strategy);

    /**
     * @brief 根据分区策略推荐窗口状态类型
     *
     * 根据给定的分区策略返回推荐的窗口状态类型。
     *
     * @param partition_strategy 分区策略
     * @return 推荐的窗口状态类型
     */
    static WindowStateType recommendStateType(PartitionStrategy partition_strategy);
};

}  // namespace sageFlow

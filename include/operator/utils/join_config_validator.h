//
// Created for sageFlow architecture refactoring - Phase 2
// Task C-06: JoinConfigValidator 配置验证与错误处理
//

#pragma once

#include "operator/utils/join_strategy_config.h"

#include <string>
#include <vector>

namespace sageFlow {

/**
 * @brief 配置验证器
 *
 * 验证 JoinStrategyConfig 的合法性和一致性。
 * 提供详细的错误信息和性能警告，帮助用户正确配置 Join 策略。
 *
 * 主要验证规则：
 * 1. 分区-窗口兼容性：确保分区策略与窗口状态类型匹配
 * 2. 算法-策略兼容性：确保算法与分区/窗口/索引策略一致
 * 3. 参数范围检查：验证所有参数在有效范围内
 * 4. 组件依赖检查：检查算法所需的组件是否可用
 * 5. 性能提示：对可能影响性能的配置给出警告
 *
 * @see JoinStrategyConfig
 * @see JoinStrategyFactory
 */
class JoinConfigValidator {
public:
    /**
     * @brief 验证结果
     *
     * 包含验证是否通过、错误信息列表和警告信息列表。
     */
    struct ValidationResult {
        bool valid;                          ///< 是否有效
        std::vector<std::string> errors;     ///< 错误信息列表
        std::vector<std::string> warnings;   ///< 警告信息列表

        /**
         * @brief 转换为字符串格式
         * @return 格式化的验证结果
         */
        [[nodiscard]] std::string toString() const;

        /**
         * @brief 检查是否有警告
         * @return 是否有警告信息
         */
        [[nodiscard]] bool hasWarnings() const { return !warnings.empty(); }

        /**
         * @brief 检查是否有错误
         * @return 是否有错误信息
         */
        [[nodiscard]] bool hasErrors() const { return !errors.empty(); }

        /**
         * @brief 添加错误信息
         * @param error 错误描述
         */
        void addError(const std::string& error);

        /**
         * @brief 添加警告信息
         * @param warning 警告描述
         */
        void addWarning(const std::string& warning);
    };

    /**
     * @brief 验证配置
     *
     * 执行所有验证检查，返回包含错误和警告的验证结果。
     *
     * @param config 策略配置
     * @return 验证结果
     */
    static ValidationResult validate(const JoinStrategyConfig& config);

    /**
     * @brief 验证并在无效时抛出异常
     *
     * 如果配置无效，抛出 std::runtime_error，异常信息包含所有错误。
     *
     * @param config 策略配置
     * @throws std::runtime_error 如果配置无效
     */
    static void throwIfInvalid(const JoinStrategyConfig& config);

    /**
     * @brief 验证配置并记录警告日志
     *
     * 验证配置，如果有效但有警告，将警告输出到日志。
     * 如果无效，将错误输出到日志并返回 false。
     *
     * @param config 策略配置
     * @return 配置是否有效
     */
    static bool validateAndLog(const JoinStrategyConfig& config);

    /**
     * @brief 检查分区策略与窗口状态的兼容性
     *
     * @param partition_strategy 分区策略
     * @param window_state_type 窗口状态类型
     * @return 是否兼容
     */
    static bool isCompatible(PartitionStrategy partition_strategy,
                             WindowStateType window_state_type);

    /**
     * @brief 获取分区策略推荐的窗口状态类型
     *
     * @param partition_strategy 分区策略
     * @return 推荐的窗口状态类型列表
     */
    static std::vector<WindowStateType> getRecommendedWindowStates(
        PartitionStrategy partition_strategy);

    /**
     * @brief 获取算法推荐的分区策略
     *
     * @param algorithm Join 算法
     * @return 推荐的分区策略
     */
    static PartitionStrategy getRecommendedPartitionStrategy(JoinAlgorithm algorithm);

private:
    /**
     * @brief 检查分区-窗口兼容性
     * @param config 策略配置
     * @param result 验证结果（会被修改）
     */
    static void checkPartitionWindowCompatibility(
        const JoinStrategyConfig& config,
        ValidationResult& result);

    /**
     * @brief 检查算法-策略兼容性
     * @param config 策略配置
     * @param result 验证结果（会被修改）
     */
    static void checkAlgorithmStrategyCompatibility(
        const JoinStrategyConfig& config,
        ValidationResult& result);

    /**
     * @brief 检查参数范围
     * @param config 策略配置
     * @param result 验证结果（会被修改）
     */
    static void checkParameterRanges(
        const JoinStrategyConfig& config,
        ValidationResult& result);

    /**
     * @brief 检查组件依赖
     * @param config 策略配置
     * @param result 验证结果（会被修改）
     */
    static void checkDependencies(
        const JoinStrategyConfig& config,
        ValidationResult& result);

    /**
     * @brief 检查潜在的性能问题
     * @param config 策略配置
     * @param result 验证结果（会被修改）
     */
    static void checkPerformanceHints(
        const JoinStrategyConfig& config,
        ValidationResult& result);

    /**
     * @brief 检查冷启动训练配置
     * 
     * 验证冷启动相关参数的合法性和一致性。
     * 仅在 enable_cold_start = true 时执行验证。
     * 
     * @param config 策略配置
     * @param result 验证结果（会被修改）
     */
    static void checkColdStartConfig(
        const JoinStrategyConfig& config,
        ValidationResult& result);
};

}  // namespace sageFlow

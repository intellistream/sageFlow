#pragma once

#include "operator/join_strategy_config.h"

#include <optional>
#include <string>
#include <vector>

namespace sageFlow {
namespace test {

/**
 * @brief JoinStrategyConfig 加载器
 *
 * 从 TOML 配置文件加载 Join 策略配置，支持单个加载、批量加载和配置合并。
 * 封装现有 loadJoinStrategyConfig() 功能，为测试提供更便捷的接口。
 */
class JoinConfigLoader {
public:
    /**
     * @brief 从 TOML 文件加载单个配置
     *
     * 加载文件根级别配置或默认配置。
     *
     * @param config_path 配置文件路径（可以是相对路径或绝对路径）
     * @return JoinStrategyConfig 实例
     * @throws std::runtime_error 如果加载或解析失败
     */
    static JoinStrategyConfig loadFromFile(const std::string& config_path);

    /**
     * @brief 从 TOML 文件加载指定名称的策略配置
     *
     * 在 [strategies.xxx] 节点下查找指定名称的策略配置。
     *
     * @param config_path 配置文件路径
     * @param strategy_name 策略名称（如 "bruteforce_baseline"）
     * @return JoinStrategyConfig 实例
     * @throws std::runtime_error 如果加载失败或策略不存在
     */
    static JoinStrategyConfig loadByName(const std::string& config_path,
                                         const std::string& strategy_name);

    /**
     * @brief 从 TOML 文件加载多个配置（用于参数化测试）
     *
     * 加载 [strategies] 节点下的所有策略配置。
     *
     * @param config_path 配置文件路径
     * @return 配置列表，每个策略一个 JoinStrategyConfig
     */
    static std::vector<JoinStrategyConfig> loadAllFromFile(const std::string& config_path);

    /**
     * @brief 从 TOML 文件加载指定的多个策略配置
     *
     * @param config_path 配置文件路径
     * @param strategy_names 策略名称列表
     * @return 配置列表
     */
    static std::vector<JoinStrategyConfig> loadByNames(
        const std::string& config_path,
        const std::vector<std::string>& strategy_names);

    /**
     * @brief 按算法类型加载配置
     *
     * @param config_path 配置文件路径
     * @param algorithm 算法类型
     * @return 匹配该算法类型的配置列表
     */
    static std::vector<JoinStrategyConfig> loadByAlgorithm(
        const std::string& config_path,
        JoinAlgorithm algorithm);

    /**
     * @brief 合并两个配置（override 覆盖 base）
     *
     * 将 override 中非默认值的字段覆盖到 base 配置中。
     *
     * @param base 基础配置
     * @param override_config 覆盖配置
     * @return 合并后的配置
     */
    static JoinStrategyConfig merge(const JoinStrategyConfig& base,
                                    const JoinStrategyConfig& override_config);

    /**
     * @brief 将配置保存到 TOML 文件
     *
     * @param config 配置实例
     * @param output_path 输出文件路径
     */
    static void saveToFile(const JoinStrategyConfig& config,
                           const std::string& output_path);

    /**
     * @brief 列出配置文件中所有可用的策略名称
     *
     * @param config_path 配置文件路径
     * @return 策略名称列表
     */
    static std::vector<std::string> listStrategyNames(const std::string& config_path);

    /**
     * @brief 检查配置文件是否存在且可解析
     *
     * @param config_path 配置文件路径
     * @return true 如果文件存在且可解析
     */
    static bool isValidConfigFile(const std::string& config_path);

    /**
     * @brief 获取默认配置文件路径
     *
     * @return 默认配置文件路径 (config/join_strategies.toml)
     */
    static std::string getDefaultConfigPath();

private:
    /**
     * @brief 解析项目相对路径为绝对路径
     *
     * @param path 输入路径
     * @return 解析后的绝对路径
     */
    static std::string resolvePath(const std::string& path);

    /**
     * @brief 判断配置是否使用默认值
     *
     * @param config 配置实例
     * @return true 如果配置全部使用默认值
     */
    static bool isDefaultConfig(const JoinStrategyConfig& config);
};

}  // namespace test
}  // namespace sageFlow

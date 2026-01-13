#pragma once

#include "operator/utils/join_strategy_config.h"

#include <optional>
#include <string>
#include <vector>

// Forward declaration for toml types
namespace toml {
inline namespace v3 {
class table;
class array;
}  // namespace v3
}  // namespace toml

namespace sageFlow {
namespace test {

/**
 * @brief 集成测试用例配置
 *
 * 包含执行一个完整集成测试所需的所有配置参数。
 * 支持从 TOML 文件加载，并继承通用配置。
 */
struct IntegrationTestCase {
    // ==================== 基本信息 ====================
    std::string name;         ///< 测试名称
    std::string description;  ///< 测试描述
    bool enabled = true;      ///< 是否启用

    // ==================== 策略配置 ====================
    JoinStrategyConfig strategy;  ///< Join 策略配置

    // ==================== 数据配置 ====================
    int vector_dim = 128;              ///< 向量维度
    std::vector<int> data_sizes;       ///< 测试的数据规模列表
    std::vector<int> parallelism;      ///< 测试的并行度列表

    // ==================== 数据生成/数据源配置 ====================
    double positive_ratio = 0.10;      ///< 正样本比例（相似度 > threshold）
    double negative_ratio = 0.60;      ///< 负样本比例（相似度 < threshold - 0.1）
    int64_t time_interval_ms = 10;     ///< 记录间时间间隔（毫秒）
    uint32_t seed = 42;                ///< 随机种子
    int64_t base_timestamp = 1000000;  ///< 基础时间戳
    
    // ==================== 数据生成高级配置 ====================
    int positive_pairs = 500;          ///< 正样本对数量
    int near_threshold_pairs = 50;     ///< 接近阈值的样本对数量
    int negative_pairs = 500;          ///< 负样本对数量
    int random_tail = 2000;            ///< 随机尾部数据量
    double alpha = 0.1;                ///< 相似度计算的 alpha 参数
    std::string similarity_mode = "fixed_alpha"; ///< 相似度模式：fixed_alpha/adaptive_alpha/normalized
    
    /// 数据源选择：
    /// - "generator" (默认)：使用 TestDataGenerator 生成数据
    /// - "dataset": 直接从数据集文件加载（支持 fvecs/json）
    std::string data_source_type = "generator";
    std::string data_source_file_path;      ///< 数据集文件路径（dataset 模式）
    int data_source_expected_dim = 0;       ///< 期望维度（0=不校验）
    
    /// 数据切分模式（dataset/生成数据通用）：
    /// - duplicate: 左右流相同
    /// - half_split: 前半 -> 左，后半 -> 右
    /// - interleaved: 偶数 -> 左，奇数 -> 右
    std::string split_mode = "duplicate";
    
    // ==================== 数据源模式配置 ====================
    /// 数据源模式：
    /// - "duplicate": 复制所有向量到两个流（自连接，左右向量相同）
    /// - "paired": 分割配对数据（左流=base向量，右流=perturbed向量，向量不同但相似）
    /// paired 模式更适合测试 multicast_k 等分区策略的效果
    std::string data_mode = "duplicate";

    // ==================== 验证配置 ====================
    double expected_min_recall = 0.0;      ///< 期望最小召回率
    double expected_min_precision = 0.0;   ///< 期望最小精确率
    bool compare_with_ground_truth = true; ///< 是否与 Ground Truth 对比
    bool allow_approximate_match = false;  ///< 是否允许近似匹配

    // ==================== 输出配置 ====================
    bool save_results = true;        ///< 是否保存结果
    std::string result_output_dir;   ///< 结果输出目录
    bool generate_report = true;     ///< 是否生成报告

    // ==================== 辅助方法 ====================

    /**
     * @brief 获取配置摘要
     * @return 配置摘要字符串
     */
    [[nodiscard]] std::string summary() const;

    /**
     * @brief 验证配置是否完整
     * @return 错误信息列表，空表示验证通过
     */
    [[nodiscard]] std::vector<std::string> validate() const;
    
    /**
     * @brief 检查配置是否有效
     * @return true 如果配置有效
     */
    [[nodiscard]] bool isValid() const;
};

/**
 * @brief 集成测试配置加载器
 *
 * 从 TOML 文件加载测试用例配置。
 * 支持通用配置继承、按算法过滤、按名称查找等功能。
 */
class IntegrationTestConfigLoader {
public:
    /**
     * @brief 加载所有测试用例
     * @param config_path 配置文件路径
     * @return 测试用例列表
     * @throws std::runtime_error 如果加载失败
     */
    static std::vector<IntegrationTestCase> loadFromFile(const std::string& config_path);

    /**
     * @brief 加载特定算法的测试用例
     * @param config_path 配置文件路径
     * @param algorithm 算法类型
     * @return 测试用例列表
     */
    static std::vector<IntegrationTestCase> loadByAlgorithm(const std::string& config_path,
                                                            JoinAlgorithm algorithm);

    /**
     * @brief 根据名称加载单个测试用例
     * @param config_path 配置文件路径
     * @param test_name 测试名称
     * @return 测试用例（如果存在）
     */
    static std::optional<IntegrationTestCase> loadByName(const std::string& config_path,
                                                         const std::string& test_name);

    /**
     * @brief 加载通用配置（应用到所有测试用例）
     * @param config_path 配置文件路径
     * @return 通用配置
     */
    static IntegrationTestCase loadCommonConfig(const std::string& config_path);

    /**
     * @brief 列出配置文件中所有测试用例名称
     * @param config_path 配置文件路径
     * @return 测试用例名称列表
     */
    static std::vector<std::string> listTestCaseNames(const std::string& config_path);

    /**
     * @brief 检查配置文件是否有效
     * @param config_path 配置文件路径
     * @return true 如果配置文件存在且可解析
     */
    static bool isValidConfigFile(const std::string& config_path);

    /**
     * @brief 设置配置文件路径（优先于默认路径和环境变量）
     * @param path 配置文件路径
     */
    static void setConfigPath(const std::string& path);

    /**
     * @brief 从环境变量获取配置文件路径
     * 
     * 检查环境变量 SAGEFLOW_TEST_CONFIG_PATH，如果设置则使用该路径。
     * 优先级：setConfigPath() > 环境变量 > 默认路径
     * 
     * @return 配置文件路径（如果环境变量未设置则返回空字符串）
     */
    static std::string getConfigPathFromEnv();

    /**
     * @brief 获取默认集成测试配置文件路径
     * @return 默认配置文件路径
     */
    static std::string getDefaultConfigPath();

    /**
     * @brief 加载启用的测试用例
     * @param config_path 配置文件路径
     * @return 启用的测试用例列表
     */
    static std::vector<IntegrationTestCase> loadEnabledTests(const std::string& config_path);

    /**
     * @brief 按数据规模范围过滤测试用例
     * @param test_cases 测试用例列表
     * @param min_size 最小数据规模
     * @param max_size 最大数据规模
     * @return 过滤后的测试用例列表
     */
    static std::vector<IntegrationTestCase> filterByDataSize(
        const std::vector<IntegrationTestCase>& test_cases,
        int min_size,
        int max_size);

private:
    /**
     * @brief 解析单个测试用例
     * @param table TOML 表
     * @param common 通用配置
     * @return 测试用例
     */
    static IntegrationTestCase parseTestCase(const toml::table& table,
                                             const IntegrationTestCase& common);

    /**
     * @brief 解析策略配置
     * @param table TOML 表
     * @return 策略配置
     */
    static JoinStrategyConfig parseStrategyConfig(const toml::table& table);

    /**
     * @brief 解析项目相对路径为绝对路径
     * @param path 输入路径
     * @return 解析后的绝对路径
     */
    static std::string resolvePath(const std::string& path);
    
    /**
     * @brief 解析整数数组
     * @param arr TOML 数组
     * @return 整数向量
     */
    static std::vector<int> parseIntArray(const toml::array& arr);

    /// 自定义配置文件路径（通过 setConfigPath 设置）
    static std::string custom_config_path_;
};

}  // namespace test
}  // namespace sageFlow

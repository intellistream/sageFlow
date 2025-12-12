#pragma once

#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "metrics/join_metrics_collector.h"

namespace sageFlow {
namespace metrics {

/**
 * @brief 验证规则
 *
 * 定义单个验证条件，包含检查逻辑和错误消息生成。
 */
struct ValidationRule {
    std::string name;         ///< 规则名称
    std::string description;  ///< 规则描述
    std::function<bool(const JoinExecutionStats&)> check;  ///< 检查函数，返回 true 表示通过
    std::function<std::string(const JoinExecutionStats&)> errorMessage;  ///< 错误消息生成函数
    bool is_warning = false;  ///< true = 警告, false = 错误
};

/**
 * @brief 验证结果
 */
struct ValidationResult {
    bool passed = true;                 ///< 是否通过所有错误级别检查
    std::vector<std::string> errors;    ///< 错误消息列表
    std::vector<std::string> warnings;  ///< 警告消息列表

    [[nodiscard]] bool hasErrors() const { return !errors.empty(); }
    [[nodiscard]] bool hasWarnings() const { return !warnings.empty(); }

    /**
     * @brief 合并另一个验证结果
     * @param other 要合并的结果
     */
    void merge(const ValidationResult& other) {
        if (!other.passed) {
            passed = false;
        }
        errors.insert(errors.end(), other.errors.begin(), other.errors.end());
        warnings.insert(warnings.end(), other.warnings.begin(), other.warnings.end());
    }

    /**
     * @brief 获取格式化的结果摘要
     */
    [[nodiscard]] std::string summary() const;
};

/**
 * @brief 验证异常
 */
class ValidationException : public std::runtime_error {
  public:
    explicit ValidationException(const ValidationResult& result)
        : std::runtime_error(result.summary()), result_(result) {}

    [[nodiscard]] const ValidationResult& result() const { return result_; }

  private:
    ValidationResult result_;
};

/**
 * @brief 指标验证器
 *
 * 根据预定义规则验证 Join 执行统计。
 */
class MetricsValidator {
  public:
    MetricsValidator() = default;

    /**
     * @brief 使用默认规则创建验证器
     * @return 配置了常用规则的验证器
     */
    static MetricsValidator createDefault();

    /**
     * @brief 添加规则
     * @param rule 验证规则
     */
    void addRule(ValidationRule rule);

    /**
     * @brief 添加多条规则
     * @param rules 验证规则列表
     */
    void addRules(std::vector<ValidationRule> rules);

    /**
     * @brief 验证统计
     * @param stats 要验证的统计数据
     * @return 验证结果
     */
    [[nodiscard]] ValidationResult validate(const JoinExecutionStats& stats) const;

    /**
     * @brief 验证并抛出异常（如果有错误）
     * @param stats 要验证的统计数据
     * @throws ValidationException 如果验证失败
     */
    void validateOrThrow(const JoinExecutionStats& stats) const;

    /**
     * @brief 获取当前规则数量
     */
    [[nodiscard]] size_t ruleCount() const { return rules_.size(); }

    /**
     * @brief 清空所有规则
     */
    void clearRules() { rules_.clear(); }

    // ==================== 预定义规则工厂 ====================

    /**
     * @brief 创建召回率阈值规则
     * @param min_recall 最小召回率 (0.0 ~ 1.0)
     * @param is_warning 是否为警告级别（默认为错误）
     */
    static ValidationRule recallThreshold(double min_recall, bool is_warning = false);

    /**
     * @brief 创建精确率阈值规则
     * @param min_precision 最小精确率 (0.0 ~ 1.0)
     * @param is_warning 是否为警告级别
     */
    static ValidationRule precisionThreshold(double min_precision, bool is_warning = false);

    /**
     * @brief 创建 F1 阈值规则
     * @param min_f1 最小 F1 分数 (0.0 ~ 1.0)
     * @param is_warning 是否为警告级别
     */
    static ValidationRule f1Threshold(double min_f1, bool is_warning = false);

    /**
     * @brief 创建吞吐量阈值规则
     * @param min_throughput 最小吞吐量（记录/秒）
     * @param is_warning 是否为警告级别
     */
    static ValidationRule throughputThreshold(double min_throughput, bool is_warning = false);

    /**
     * @brief 创建平均查询延迟规则
     * @param max_latency_us 最大平均查询延迟（微秒）
     * @param is_warning 是否为警告级别
     */
    static ValidationRule avgQueryLatencyThreshold(double max_latency_us, bool is_warning = false);

    /**
     * @brief 创建数据处理数量检查规则
     * @param min_records 最小处理记录数
     * @param is_warning 是否为警告级别
     */
    static ValidationRule minRecordsProcessed(int64_t min_records, bool is_warning = false);

    /**
     * @brief 创建输出匹配数检查规则
     * @param min_matches 最小输出匹配数
     * @param is_warning 是否为警告级别
     */
    static ValidationRule minOutputMatches(int64_t min_matches, bool is_warning = false);

  private:
    std::vector<ValidationRule> rules_;
};

/**
 * @brief 指标阈值配置
 *
 * 便于从配置文件加载阈值设置。
 */
struct MetricsThresholds {
    double min_recall = 0.0;            ///< 最小召回率
    double min_precision = 0.0;         ///< 最小精确率
    double min_f1 = 0.0;                ///< 最小 F1 分数
    double min_throughput = 0.0;        ///< 最小吞吐量（记录/秒）
    double max_avg_query_latency_us = 0.0;  ///< 最大平均查询延迟（微秒）
    int64_t min_records_processed = 0;  ///< 最小处理记录数
    int64_t min_output_matches = 0;     ///< 最小输出匹配数

    // 是否启用各项检查（0 或负值视为禁用）
    bool recall_enabled = false;
    bool precision_enabled = false;
    bool f1_enabled = false;
    bool throughput_enabled = false;
    bool latency_enabled = false;
    bool records_enabled = false;
    bool matches_enabled = false;

    /**
     * @brief 创建验证器
     * @return 根据阈值配置的验证器
     */
    [[nodiscard]] MetricsValidator createValidator() const;

    /**
     * @brief 从 TOML 配置文件加载
     * @param config_path 配置文件路径
     * @return 加载的阈值配置
     */
    static MetricsThresholds fromToml(const std::string& config_path);

    /**
     * @brief 使用默认测试阈值
     * @return 适合单元测试的默认阈值
     */
    static MetricsThresholds defaultForTesting();
};

}  // namespace metrics
}  // namespace sageFlow

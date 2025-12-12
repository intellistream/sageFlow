#include "metrics/metrics_validator.h"

#include <fmt/format.h>

#include <filesystem>
#include <sstream>

#include "toml++/toml.hpp"
#include "utils/logger.h"

namespace sageFlow {
namespace metrics {

// ============================================================================
// ValidationResult Implementation
// ============================================================================

std::string ValidationResult::summary() const {
    std::ostringstream oss;

    if (passed && warnings.empty()) {
        oss << "Validation PASSED (no errors or warnings)";
        return oss.str();
    }

    if (!errors.empty()) {
        oss << "Validation FAILED with " << errors.size() << " error(s):\n";
        for (size_t i = 0; i < errors.size(); ++i) {
            oss << "  [ERROR " << (i + 1) << "] " << errors[i] << "\n";
        }
    }

    if (!warnings.empty()) {
        if (!errors.empty()) {
            oss << "\n";
        }
        oss << warnings.size() << " warning(s):\n";
        for (size_t i = 0; i < warnings.size(); ++i) {
            oss << "  [WARN " << (i + 1) << "] " << warnings[i] << "\n";
        }
    }

    return oss.str();
}

// ============================================================================
// MetricsValidator Implementation
// ============================================================================

MetricsValidator MetricsValidator::createDefault() {
    MetricsValidator validator;

    // 添加基本的数据完整性检查
    validator.addRule(ValidationRule{
        .name = "data_processed",
        .description = "At least one record should be processed",
        .check =
            [](const JoinExecutionStats& stats) {
                return stats.left_records_processed > 0 || stats.right_records_processed > 0;
            },
        .errorMessage =
            [](const JoinExecutionStats& stats) {
                return fmt::format("No records processed (left={}, right={})", stats.left_records_processed,
                                   stats.right_records_processed);
            },
        .is_warning = true  // 作为警告，因为某些测试场景可能没有数据
    });

    return validator;
}

void MetricsValidator::addRule(ValidationRule rule) { rules_.push_back(std::move(rule)); }

void MetricsValidator::addRules(std::vector<ValidationRule> rules) {
    rules_.reserve(rules_.size() + rules.size());
    for (auto& rule : rules) {
        rules_.push_back(std::move(rule));
    }
}

ValidationResult MetricsValidator::validate(const JoinExecutionStats& stats) const {
    ValidationResult result;

    for (const auto& rule : rules_) {
        if (!rule.check(stats)) {
            std::string msg = rule.errorMessage ? rule.errorMessage(stats) : rule.description;

            if (rule.is_warning) {
                result.warnings.push_back(fmt::format("[{}] {}", rule.name, msg));
            } else {
                result.passed = false;
                result.errors.push_back(fmt::format("[{}] {}", rule.name, msg));
            }
        }
    }

    return result;
}

void MetricsValidator::validateOrThrow(const JoinExecutionStats& stats) const {
    auto result = validate(stats);
    if (!result.passed) {
        throw ValidationException(result);
    }
}

// ============================================================================
// Predefined Rule Factories
// ============================================================================

ValidationRule MetricsValidator::recallThreshold(double min_recall, bool is_warning) {
    return ValidationRule{
        .name = "recall_threshold",
        .description = fmt::format("Recall should be at least {:.2f}%", min_recall * 100),
        .check =
            [min_recall](const JoinExecutionStats& stats) {
                // 如果没有相关数据，跳过检查
                if (stats.true_positives + stats.false_negatives == 0) {
                    return true;
                }
                return stats.recall() >= min_recall;
            },
        .errorMessage =
            [min_recall](const JoinExecutionStats& stats) {
                return fmt::format("Recall {:.2f}% is below threshold {:.2f}% (TP={}, FN={})", stats.recall() * 100,
                                   min_recall * 100, stats.true_positives, stats.false_negatives);
            },
        .is_warning = is_warning};
}

ValidationRule MetricsValidator::precisionThreshold(double min_precision, bool is_warning) {
    return ValidationRule{
        .name = "precision_threshold",
        .description = fmt::format("Precision should be at least {:.2f}%", min_precision * 100),
        .check =
            [min_precision](const JoinExecutionStats& stats) {
                // 如果没有输出数据，跳过检查
                if (stats.true_positives + stats.false_positives == 0) {
                    return true;
                }
                return stats.precision() >= min_precision;
            },
        .errorMessage =
            [min_precision](const JoinExecutionStats& stats) {
                return fmt::format("Precision {:.2f}% is below threshold {:.2f}% (TP={}, FP={})",
                                   stats.precision() * 100, min_precision * 100, stats.true_positives,
                                   stats.false_positives);
            },
        .is_warning = is_warning};
}

ValidationRule MetricsValidator::f1Threshold(double min_f1, bool is_warning) {
    return ValidationRule{
        .name = "f1_threshold",
        .description = fmt::format("F1 score should be at least {:.2f}%", min_f1 * 100),
        .check =
            [min_f1](const JoinExecutionStats& stats) {
                // 如果没有数据，跳过检查
                if (stats.true_positives + stats.false_positives + stats.false_negatives == 0) {
                    return true;
                }
                return stats.f1Score() >= min_f1;
            },
        .errorMessage =
            [min_f1](const JoinExecutionStats& stats) {
                return fmt::format("F1 score {:.2f}% is below threshold {:.2f}%", stats.f1Score() * 100, min_f1 * 100);
            },
        .is_warning = is_warning};
}

ValidationRule MetricsValidator::throughputThreshold(double min_throughput, bool is_warning) {
    return ValidationRule{
        .name = "throughput_threshold",
        .description = fmt::format("Throughput should be at least {:.0f} records/sec", min_throughput),
        .check =
            [min_throughput](const JoinExecutionStats& stats) {
                // 如果没有时间数据，跳过检查
                if (stats.total_time.count() == 0) {
                    return true;
                }
                return stats.throughputRecordsPerSec() >= min_throughput;
            },
        .errorMessage =
            [min_throughput](const JoinExecutionStats& stats) {
                return fmt::format("Throughput {:.2f} records/sec is below threshold {:.0f} records/sec",
                                   stats.throughputRecordsPerSec(), min_throughput);
            },
        .is_warning = is_warning};
}

ValidationRule MetricsValidator::avgQueryLatencyThreshold(double max_latency_us, bool is_warning) {
    return ValidationRule{
        .name = "avg_query_latency_threshold",
        .description = fmt::format("Average query latency should be at most {:.2f} us", max_latency_us),
        .check =
            [max_latency_us](const JoinExecutionStats& stats) {
                // 如果没有查询，跳过检查
                if (stats.index_queries == 0) {
                    return true;
                }
                return stats.avgQueryTimeUs() <= max_latency_us;
            },
        .errorMessage =
            [max_latency_us](const JoinExecutionStats& stats) {
                return fmt::format("Average query latency {:.2f} us exceeds threshold {:.2f} us",
                                   stats.avgQueryTimeUs(), max_latency_us);
            },
        .is_warning = is_warning};
}

ValidationRule MetricsValidator::minRecordsProcessed(int64_t min_records, bool is_warning) {
    return ValidationRule{.name = "min_records_processed",
                          .description = fmt::format("Should process at least {} records", min_records),
                          .check =
                              [min_records](const JoinExecutionStats& stats) {
                                  return (stats.left_records_processed + stats.right_records_processed) >= min_records;
                              },
                          .errorMessage =
                              [min_records](const JoinExecutionStats& stats) {
                                  return fmt::format(
                                      "Only processed {} records (left={}, right={}), expected at least {}",
                                      stats.left_records_processed + stats.right_records_processed,
                                      stats.left_records_processed, stats.right_records_processed, min_records);
                              },
                          .is_warning = is_warning};
}

ValidationRule MetricsValidator::minOutputMatches(int64_t min_matches, bool is_warning) {
    return ValidationRule{.name = "min_output_matches",
                          .description = fmt::format("Should output at least {} matches", min_matches),
                          .check =
                              [min_matches](const JoinExecutionStats& stats) {
                                  return stats.output_matches >= min_matches;
                              },
                          .errorMessage =
                              [min_matches](const JoinExecutionStats& stats) {
                                  return fmt::format("Only output {} matches, expected at least {}",
                                                     stats.output_matches, min_matches);
                              },
                          .is_warning = is_warning};
}

// ============================================================================
// MetricsThresholds Implementation
// ============================================================================

MetricsValidator MetricsThresholds::createValidator() const {
    MetricsValidator validator;

    if (recall_enabled && min_recall > 0) {
        validator.addRule(MetricsValidator::recallThreshold(min_recall));
    }

    if (precision_enabled && min_precision > 0) {
        validator.addRule(MetricsValidator::precisionThreshold(min_precision));
    }

    if (f1_enabled && min_f1 > 0) {
        validator.addRule(MetricsValidator::f1Threshold(min_f1));
    }

    if (throughput_enabled && min_throughput > 0) {
        validator.addRule(MetricsValidator::throughputThreshold(min_throughput));
    }

    if (latency_enabled && max_avg_query_latency_us > 0) {
        validator.addRule(MetricsValidator::avgQueryLatencyThreshold(max_avg_query_latency_us));
    }

    if (records_enabled && min_records_processed > 0) {
        validator.addRule(MetricsValidator::minRecordsProcessed(min_records_processed));
    }

    if (matches_enabled && min_output_matches > 0) {
        validator.addRule(MetricsValidator::minOutputMatches(min_output_matches));
    }

    return validator;
}

MetricsThresholds MetricsThresholds::fromToml(const std::string& config_path) {
    MetricsThresholds thresholds;

    if (!std::filesystem::exists(config_path)) {
        SAGEFLOW_LOG_WARN("MetricsValidator", "Config file not found: {}", config_path);
        return thresholds;
    }

    try {
        auto config = toml::parse_file(config_path);

        // 尝试从 [metrics] 或 [metrics.thresholds] 节读取
        const toml::table* metrics_table = nullptr;
        if (config.contains("metrics")) {
            auto* metrics_node = config.get("metrics");
            if (metrics_node && metrics_node->is_table()) {
                metrics_table = metrics_node->as_table();
                // 检查是否有 thresholds 子节
                if (metrics_table->contains("thresholds")) {
                    auto* thresholds_node = metrics_table->get("thresholds");
                    if (thresholds_node && thresholds_node->is_table()) {
                        metrics_table = thresholds_node->as_table();
                    }
                }
            }
        }

        if (!metrics_table) {
            SAGEFLOW_LOG_DEBUG("MetricsValidator", "No [metrics] section found in {}", config_path);
            return thresholds;
        }

        auto read_double = [metrics_table](const std::string& key, double& value, bool& enabled) {
            if (metrics_table->contains(key)) {
                auto* node = metrics_table->get(key);
                if (node && node->is_floating_point()) {
                    value = node->as_floating_point()->get();
                    enabled = value > 0;
                } else if (node && node->is_integer()) {
                    value = static_cast<double>(node->as_integer()->get());
                    enabled = value > 0;
                }
            }
        };

        auto read_int64 = [metrics_table](const std::string& key, int64_t& value, bool& enabled) {
            if (metrics_table->contains(key)) {
                auto* node = metrics_table->get(key);
                if (node && node->is_integer()) {
                    value = node->as_integer()->get();
                    enabled = value > 0;
                }
            }
        };

        read_double("min_recall", thresholds.min_recall, thresholds.recall_enabled);
        read_double("min_precision", thresholds.min_precision, thresholds.precision_enabled);
        read_double("min_f1", thresholds.min_f1, thresholds.f1_enabled);
        read_double("min_throughput", thresholds.min_throughput, thresholds.throughput_enabled);
        read_double("max_avg_query_latency_us", thresholds.max_avg_query_latency_us, thresholds.latency_enabled);
        read_int64("min_records_processed", thresholds.min_records_processed, thresholds.records_enabled);
        read_int64("min_output_matches", thresholds.min_output_matches, thresholds.matches_enabled);

    } catch (const toml::parse_error& e) {
        SAGEFLOW_LOG_ERROR("MetricsValidator", "Failed to parse config file {}: {}", config_path, e.what());
    }

    return thresholds;
}

MetricsThresholds MetricsThresholds::defaultForTesting() {
    MetricsThresholds thresholds;

    // 为测试设置较宽松的阈值
    thresholds.min_recall = 0.8;
    thresholds.recall_enabled = true;

    thresholds.min_precision = 0.8;
    thresholds.precision_enabled = true;

    thresholds.min_f1 = 0.0;  // 不检查 F1
    thresholds.f1_enabled = false;

    thresholds.min_throughput = 0.0;  // 不检查吞吐量
    thresholds.throughput_enabled = false;

    thresholds.max_avg_query_latency_us = 0.0;  // 不检查延迟
    thresholds.latency_enabled = false;

    thresholds.min_records_processed = 1;
    thresholds.records_enabled = true;

    thresholds.min_output_matches = 0;  // 不强制要求有输出
    thresholds.matches_enabled = false;

    return thresholds;
}

}  // namespace metrics
}  // namespace sageFlow

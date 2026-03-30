#pragma once

#include "metrics/join_metrics_collector.h"
#include "test_utils/integration_test_config.h"

#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace sageFlow {
namespace test {

/**
 * @brief Join 算子内部阶段耗时明细 (Breakdown Analysis)
 *
 * 从 JoinMetrics::instance() 单例中采集，单位为纳秒。
 */
struct JoinBreakdown {
    uint64_t window_insert_ns = 0;    ///< 窗口插入/过期操作耗时
    uint64_t index_insert_ns = 0;     ///< 索引插入/删除操作耗时
    uint64_t expire_ns = 0;           ///< 过期逻辑耗时
    uint64_t candidate_fetch_ns = 0;  ///< 获取候选对耗时
    uint64_t similarity_ns = 0;       ///< 相似度计算耗时
    uint64_t join_function_ns = 0;    ///< Join 函数执行耗时
    uint64_t emit_ns = 0;             ///< 发射结果耗时
    uint64_t lock_wait_ns = 0;        ///< 等待锁耗时
    uint64_t apply_processing_ns = 0; ///< apply() 方法总耗时（实际测量值）

    // 阶段计数
    uint64_t window_insert_count = 0;   ///< 窗口插入次数
    uint64_t index_op_count = 0;        ///< 索引插入/删除次数
    uint64_t expire_count = 0;          ///< 过期记录数量
    uint64_t candidate_fetch_count = 0; ///< 候选获取次数
    uint64_t similarity_count = 0;      ///< 相似度比较次数
    uint64_t join_function_count = 0;   ///< Join 函数执行次数
    uint64_t emit_count = 0;            ///< emit 次数
    uint64_t lock_wait_count = 0;       ///< 锁等待次数

    // 计数指标
    uint64_t total_records_left = 0;     ///< 左侧处理的记录数
    uint64_t total_records_right = 0;    ///< 右侧处理的记录数
    uint64_t total_emits = 0;            ///< 发射的结果数
    uint64_t apply_processing_count = 0; ///< apply() 调用次数
    uint64_t e2e_latency_ns = 0;         ///< 累计端到端延迟
    uint64_t e2e_latency_count = 0;      ///< 延迟测量次数
    double e2e_latency_p95_us = 0.0;     ///< 端到端延迟 P95（微秒）
    double e2e_latency_p99_us = 0.0;     ///< 端到端延迟 P99（微秒）

    /**
     * @brief 计算各阶段耗时之和（用于对比验证）
     * 
     * 各指标关系说明：
     * - window_insert_ns: 窗口插入耗时
     * - index_insert_ns: 索引插入/删除耗时  
     * - expire_ns: 窗口过期处理耗时
     * - candidate_fetch_ns: 候选项获取耗时
     * - similarity_ns: 相似度筛选耗时（不含 join_function_ns）
     * - join_function_ns: join 函数调用耗时
     * - emit_ns: 结果发射耗时
     * - lock_wait_ns: 锁等待耗时
     * - apply_processing_ns: apply() 方法实际总耗时（包含所有阶段+锁等待）
     * 
     * 注意：各阶段之和 + lock_wait_ns 应约等于 apply_processing_ns
     */
    [[nodiscard]] uint64_t sumOfStagesNs() const {
        return window_insert_ns + index_insert_ns + expire_ns +
               candidate_fetch_ns + similarity_ns + join_function_ns + emit_ns;
    }
    
    /**
     * @brief 获取实际总处理耗时（直接测量值）
     * 
     * 这是 apply() 方法从进入到退出的实际耗时，
     * 包含所有处理阶段和锁等待时间。
     */
    [[nodiscard]] uint64_t totalProcessingNs() const {
        return apply_processing_ns;
    }
    
    /**
     * @brief 计算各阶段之和 + 锁等待（用于对比验证）
     */
    [[nodiscard]] uint64_t totalWithLockWaitNs() const {
        return sumOfStagesNs() + lock_wait_ns;
    }

    /**
     * @brief 计算平均端到端延迟（微秒）
     */
    [[nodiscard]] double avgE2ELatencyUs() const {
        return e2e_latency_count > 0
                   ? static_cast<double>(e2e_latency_ns) / static_cast<double>(e2e_latency_count) / 1000.0
                   : 0.0;
    }

    /**
     * @brief 检查是否有有效的 breakdown 数据
     */
    [[nodiscard]] bool hasData() const {
        return window_insert_ns > 0 || index_insert_ns > 0 || candidate_fetch_ns > 0 ||
               similarity_ns > 0 || join_function_ns > 0 || total_records_left > 0 ||
               total_records_right > 0;
    }
};

/**
 * @brief 单个测试结果
 */
struct TestResult {
    std::string name;       ///< 测试名称
    std::string algorithm;  ///< 算法名称
    int data_size = 0;      ///< 数据规模
    int parallelism = 0;    ///< 并行度

    // ==================== 准确性指标 ====================
    double recall = 0.0;
    double precision = 0.0;
    double f1_score = 0.0;

    // ==================== 性能指标 ====================
    double throughput_records_per_sec = 0.0;
    double execution_time_ms = 0.0;
    // 额外时间口径：用于区分“算法计算完成”与“Sink 追赶等待”
    double join_time_ms = 0.0;      ///< Join emits stable 时间（并行 makespan）
    double sink_wait_ms = 0.0;      ///< Sink catch-up 等待耗时
    double avg_query_latency_us = 0.0;

    // ==================== 计数 ====================
    int64_t expected_matches = 0;
    int64_t actual_matches = 0;
    int64_t true_positives = 0;
    int64_t false_positives = 0;
    int64_t false_negatives = 0;

    // ==================== Breakdown Analysis ====================
    JoinBreakdown breakdown;  ///< Join 算子内部阶段耗时明细

    // ==================== 状态 ====================
    bool passed = false;
    bool skipped = false;
    std::string failure_reason;
    std::string skip_reason;
};

/**
 * @brief 算法汇总统计
 */
struct AlgorithmSummary {
    std::string algorithm;
    int test_count = 0;
    int passed_count = 0;
    int failed_count = 0;
    int skipped_count = 0;

    double avg_recall = 0.0;
    double avg_precision = 0.0;
    double avg_f1_score = 0.0;
    double avg_throughput = 0.0;
    double max_throughput = 0.0;
    double min_throughput = std::numeric_limits<double>::max();

    [[nodiscard]] double passRate() const {
        return test_count > 0 ? static_cast<double>(passed_count) / static_cast<double>(test_count) : 0.0;
    }
};

/**
 * @brief 完整测试报告
 */
struct TestReport {
    // ==================== 元数据 ====================
    std::string version = "1.0";
    std::string report_name;
    std::string generated_at;
    std::string git_commit;
    std::string os_info;
    int cpu_cores = 0;
    double memory_gb = 0.0;

    // ==================== 汇总 ====================
    int total_tests = 0;
    int passed_tests = 0;
    int failed_tests = 0;
    int skipped_tests = 0;
    double total_duration_ms = 0.0;

    // ==================== 按算法汇总 ====================
    std::map<std::string, AlgorithmSummary> algorithm_summaries;

    // ==================== 详细结果 ====================
    std::vector<TestResult> detailed_results;

    // ==================== 失败测试列表 ====================
    std::vector<TestResult> failed_results;
};

/**
 * @brief 测试报告生成器
 *
 * 收集测试结果并生成多种格式的报告（JSON、Markdown）。
 * 支持从 JoinExecutionStats 创建 TestResult，并提供统计汇总功能。
 */
class TestReportGenerator {
  public:
    /**
     * @brief 构造函数
     * @param report_name 报告名称
     */
    explicit TestReportGenerator(std::string report_name);

    /**
     * @brief 获取报告名称
     */
    [[nodiscard]] const std::string& reportName() const { return report_name_; }

    /**
     * @brief 添加测试结果
     * @param result 测试结果
     */
    void addResult(TestResult result);

    /**
     * @brief 添加多个测试结果
     * @param results 测试结果列表
     */
    void addResults(const std::vector<TestResult>& results);

    /**
     * @brief 从 JoinExecutionStats 创建 TestResult
     * @param name 测试名称
     * @param algorithm 算法名称
     * @param stats 执行统计
     * @param data_size 数据规模
     * @param parallelism 并行度
     * @return TestResult 实例
     */
    static TestResult fromExecutionStats(const std::string& name, const std::string& algorithm,
                                         const metrics::JoinExecutionStats& stats, int data_size, int parallelism);

    /**
     * @brief 生成报告
     * @return 完整测试报告
     */
    [[nodiscard]] TestReport generateReport() const;

    /**
     * @brief 输出为 JSON
     * @param output_path 输出路径
     */
    void writeJson(const std::filesystem::path& output_path) const;

    /**
     * @brief 输出为 Markdown
     * @param output_path 输出路径
     */
    void writeMarkdown(const std::filesystem::path& output_path) const;

    /**
     * @brief 输出摘要到控制台
     */
    void printSummary() const;

    /**
     * @brief 设置 Git Commit
     * @param commit Git commit hash
     */
    void setGitCommit(const std::string& commit);

    /**
     * @brief 自动检测 Git Commit
     */
    void detectGitCommit();

    /**
     * @brief 检测系统环境信息
     */
    void detectEnvironment();

    /**
     * @brief 获取已收集的结果数量
     */
    [[nodiscard]] size_t resultCount() const { return results_.size(); }

    /**
     * @brief 清空所有结果
     */
    void clear() { results_.clear(); }

    /**
     * @brief 获取所有结果（只读）
     */
    [[nodiscard]] const std::vector<TestResult>& results() const { return results_; }

  private:
    std::string report_name_;
    std::string git_commit_;
    std::string os_info_;
    int cpu_cores_ = 0;
    double memory_gb_ = 0.0;
    std::vector<TestResult> results_;

    [[nodiscard]] std::string getTimestamp() const;
    [[nodiscard]] std::string getOsInfo() const;
    [[nodiscard]] int getCpuCores() const;
    [[nodiscard]] double getMemoryGb() const;

    void computeAlgorithmSummaries(TestReport& report) const;

    // JSON 辅助函数
    [[nodiscard]] std::string escapeJsonString(const std::string& str) const;
    void writeJsonEnvironment(std::ostream& os, const TestReport& report) const;
    void writeJsonSummary(std::ostream& os, const TestReport& report) const;
    void writeJsonAlgorithmResults(std::ostream& os, const TestReport& report) const;
    void writeJsonDetailedResults(std::ostream& os, const TestReport& report) const;

    // Markdown 辅助函数
    void writeMarkdownHeader(std::ostream& os, const TestReport& report) const;
    void writeMarkdownSummary(std::ostream& os, const TestReport& report) const;
    void writeMarkdownAlgorithmTable(std::ostream& os, const TestReport& report) const;
    void writeMarkdownFailedTests(std::ostream& os, const TestReport& report) const;
    void writeMarkdownDetailedResults(std::ostream& os, const TestReport& report) const;
};

/**
 * @brief 格式化吞吐量为人类可读字符串
 * @param throughput 吞吐量（记录/秒）
 * @return 格式化字符串（如 "50K/s", "1.2M/s"）
 */
[[nodiscard]] std::string formatThroughput(double throughput);

/**
 * @brief 格式化时间为人类可读字符串
 * @param ms 毫秒数
 * @return 格式化字符串（如 "12.35s", "1m 23s"）
 */
[[nodiscard]] std::string formatDuration(double ms);

}  // namespace test
}  // namespace sageFlow

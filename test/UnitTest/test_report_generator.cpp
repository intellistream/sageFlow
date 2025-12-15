#include <gtest/gtest.h>

#include "test_utils/test_report_generator.h"
#include "metrics/join_metrics_collector.h"

#include <filesystem>
#include <fstream>
#include <sstream>

namespace sageFlow {
namespace test {
namespace {

// ============================================================================
// Helper Functions
// ============================================================================

TestResult createTestResult(const std::string& name, const std::string& algorithm, bool passed, double recall = 1.0,
                            double precision = 1.0, int data_size = 1000, int parallelism = 4) {
    TestResult r;
    r.name = name;
    r.algorithm = algorithm;
    r.passed = passed;
    r.recall = recall;
    r.precision = precision;
    r.f1_score = (recall + precision > 0) ? 2 * recall * precision / (recall + precision) : 0;
    r.data_size = data_size;
    r.parallelism = parallelism;
    r.throughput_records_per_sec = 50000;
    r.execution_time_ms = 100;
    r.true_positives = 100;
    r.false_positives = passed ? 0 : 10;
    r.false_negatives = passed ? 0 : 5;
    r.expected_matches = 105;
    r.actual_matches = r.true_positives + r.false_positives;
    if (!passed) {
        r.failure_reason = "Recall below threshold";
    }
    return r;
}

// ============================================================================
// TestReportGenerator Tests
// ============================================================================

TEST(TestReportGeneratorTest, ConstructorInitializesCorrectly) {
    TestReportGenerator generator("TestReport");

    EXPECT_EQ(generator.reportName(), "TestReport");
    EXPECT_EQ(generator.resultCount(), 0u);
}

TEST(TestReportGeneratorTest, AddResultIncreasesCount) {
    TestReportGenerator generator("TestReport");

    generator.addResult(createTestResult("test1", "BRUTEFORCE", true));
    EXPECT_EQ(generator.resultCount(), 1u);

    generator.addResult(createTestResult("test2", "BRUTEFORCE", true));
    EXPECT_EQ(generator.resultCount(), 2u);
}

TEST(TestReportGeneratorTest, AddResultsBatch) {
    TestReportGenerator generator("TestReport");

    std::vector<TestResult> results;
    results.push_back(createTestResult("test1", "BRUTEFORCE", true));
    results.push_back(createTestResult("test2", "IVF", true));
    results.push_back(createTestResult("test3", "HNSW", false, 0.8, 0.9));

    generator.addResults(results);
    EXPECT_EQ(generator.resultCount(), 3u);
}

TEST(TestReportGeneratorTest, ClearRemovesAllResults) {
    TestReportGenerator generator("TestReport");

    generator.addResult(createTestResult("test1", "BRUTEFORCE", true));
    generator.addResult(createTestResult("test2", "IVF", true));
    EXPECT_EQ(generator.resultCount(), 2u);

    generator.clear();
    EXPECT_EQ(generator.resultCount(), 0u);
}

TEST(TestReportGeneratorTest, GenerateReportWithNoResults) {
    TestReportGenerator generator("EmptyReport");

    auto report = generator.generateReport();

    EXPECT_EQ(report.report_name, "EmptyReport");
    EXPECT_EQ(report.total_tests, 0);
    EXPECT_EQ(report.passed_tests, 0);
    EXPECT_EQ(report.failed_tests, 0);
    EXPECT_TRUE(report.detailed_results.empty());
    EXPECT_TRUE(report.algorithm_summaries.empty());
}

TEST(TestReportGeneratorTest, GenerateReportCountsCorrectly) {
    TestReportGenerator generator("TestReport");

    generator.addResult(createTestResult("test1", "BRUTEFORCE", true));
    generator.addResult(createTestResult("test2", "BRUTEFORCE", true));
    generator.addResult(createTestResult("test3", "BRUTEFORCE", false, 0.7, 0.8));

    auto report = generator.generateReport();

    EXPECT_EQ(report.total_tests, 3);
    EXPECT_EQ(report.passed_tests, 2);
    EXPECT_EQ(report.failed_tests, 1);
    EXPECT_EQ(report.skipped_tests, 0);
}

TEST(TestReportGeneratorTest, GenerateReportSkippedTests) {
    TestReportGenerator generator("TestReport");

    auto passed = createTestResult("test1", "BRUTEFORCE", true);
    auto skipped = createTestResult("test2", "BRUTEFORCE", false);
    skipped.skipped = true;
    skipped.skip_reason = "Not implemented";
    skipped.passed = false;

    generator.addResult(passed);
    generator.addResult(skipped);

    auto report = generator.generateReport();

    EXPECT_EQ(report.total_tests, 2);
    EXPECT_EQ(report.passed_tests, 1);
    EXPECT_EQ(report.failed_tests, 0);
    EXPECT_EQ(report.skipped_tests, 1);
}

TEST(TestReportGeneratorTest, AlgorithmSummariesComputed) {
    TestReportGenerator generator("TestReport");

    // BRUTEFORCE: 2 passed, 1 failed
    generator.addResult(createTestResult("bf_test1", "BRUTEFORCE", true, 1.0, 1.0));
    generator.addResult(createTestResult("bf_test2", "BRUTEFORCE", true, 0.95, 0.98));
    generator.addResult(createTestResult("bf_test3", "BRUTEFORCE", false, 0.7, 0.8));

    // IVF: 1 passed
    generator.addResult(createTestResult("ivf_test1", "IVF", true, 0.9, 0.92));

    auto report = generator.generateReport();

    ASSERT_EQ(report.algorithm_summaries.count("BRUTEFORCE"), 1u);
    ASSERT_EQ(report.algorithm_summaries.count("IVF"), 1u);

    const auto& bf_summary = report.algorithm_summaries.at("BRUTEFORCE");
    EXPECT_EQ(bf_summary.test_count, 3);
    EXPECT_EQ(bf_summary.passed_count, 2);
    EXPECT_EQ(bf_summary.failed_count, 1);
    EXPECT_NEAR(bf_summary.passRate(), 2.0 / 3.0, 0.01);

    const auto& ivf_summary = report.algorithm_summaries.at("IVF");
    EXPECT_EQ(ivf_summary.test_count, 1);
    EXPECT_EQ(ivf_summary.passed_count, 1);
    EXPECT_EQ(ivf_summary.failed_count, 0);
    EXPECT_DOUBLE_EQ(ivf_summary.passRate(), 1.0);
}

TEST(TestReportGeneratorTest, AlgorithmSummaryAverages) {
    TestReportGenerator generator("TestReport");

    generator.addResult(createTestResult("test1", "BRUTEFORCE", true, 1.0, 1.0));
    generator.addResult(createTestResult("test2", "BRUTEFORCE", true, 0.9, 0.95));

    auto report = generator.generateReport();

    const auto& summary = report.algorithm_summaries.at("BRUTEFORCE");
    EXPECT_NEAR(summary.avg_recall, 0.95, 0.01);
    EXPECT_NEAR(summary.avg_precision, 0.975, 0.01);
}

TEST(TestReportGeneratorTest, FailedResultsCollected) {
    TestReportGenerator generator("TestReport");

    generator.addResult(createTestResult("test1", "BRUTEFORCE", true));
    generator.addResult(createTestResult("test2", "BRUTEFORCE", false, 0.7, 0.8));
    generator.addResult(createTestResult("test3", "IVF", false, 0.6, 0.7));

    auto report = generator.generateReport();

    EXPECT_EQ(report.failed_results.size(), 2u);
    EXPECT_EQ(report.failed_results[0].name, "test2");
    EXPECT_EQ(report.failed_results[1].name, "test3");
}

TEST(TestReportGeneratorTest, FromExecutionStats) {
    metrics::JoinExecutionStats stats;
    stats.total_time = std::chrono::milliseconds(500);
    stats.left_records_processed = 1000;
    stats.right_records_processed = 1000;
    stats.true_positives = 90;
    stats.false_positives = 10;
    stats.false_negatives = 10;
    stats.output_matches = 100;
    stats.index_queries = 1000;
    stats.query_time = std::chrono::microseconds(100000);

    auto result = TestReportGenerator::fromExecutionStats("test_case", "IVF", stats, 2000, 4);

    EXPECT_EQ(result.name, "test_case");
    EXPECT_EQ(result.algorithm, "IVF");
    EXPECT_EQ(result.data_size, 2000);
    EXPECT_EQ(result.parallelism, 4);
    EXPECT_NEAR(result.recall, 0.9, 0.01);
    EXPECT_NEAR(result.precision, 0.9, 0.01);
    EXPECT_EQ(result.true_positives, 90);
    EXPECT_EQ(result.false_positives, 10);
    EXPECT_EQ(result.false_negatives, 10);
}

// ============================================================================
// JSON Output Tests
// ============================================================================

TEST(TestReportGeneratorTest, WriteJsonCreatesValidFile) {
    TestReportGenerator generator("JSONTest");

    generator.addResult(createTestResult("test1", "BRUTEFORCE", true));
    generator.addResult(createTestResult("test2", "IVF", false, 0.8, 0.9));

    auto temp_path = std::filesystem::temp_directory_path() / "test_report_json.json";

    // 确保文件不存在
    std::filesystem::remove(temp_path);

    generator.writeJson(temp_path);

    ASSERT_TRUE(std::filesystem::exists(temp_path));

    // 读取并验证内容
    std::ifstream ifs(temp_path);
    std::string content((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());

    // 验证 JSON 结构关键元素
    EXPECT_NE(content.find("\"report_version\""), std::string::npos);
    EXPECT_NE(content.find("\"report_name\": \"JSONTest\""), std::string::npos);
    EXPECT_NE(content.find("\"environment\""), std::string::npos);
    EXPECT_NE(content.find("\"summary\""), std::string::npos);
    EXPECT_NE(content.find("\"algorithm_results\""), std::string::npos);
    EXPECT_NE(content.find("\"detailed_results\""), std::string::npos);
    EXPECT_NE(content.find("\"BRUTEFORCE\""), std::string::npos);
    EXPECT_NE(content.find("\"IVF\""), std::string::npos);

    // 清理
    std::filesystem::remove(temp_path);
}

TEST(TestReportGeneratorTest, WriteJsonEscapesSpecialCharacters) {
    TestReportGenerator generator("Test \"with\" special\nchars");

    auto result = createTestResult("test\\path", "BRUTEFORCE", false);
    result.failure_reason = "Error: \"Invalid\" value\ttab";
    generator.addResult(result);

    auto temp_path = std::filesystem::temp_directory_path() / "test_escape.json";
    generator.writeJson(temp_path);

    std::ifstream ifs(temp_path);
    std::string content((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());

    // 验证特殊字符被转义
    EXPECT_NE(content.find("\\\""), std::string::npos);
    EXPECT_NE(content.find("\\n"), std::string::npos);
    EXPECT_NE(content.find("\\t"), std::string::npos);

    std::filesystem::remove(temp_path);
}

TEST(TestReportGeneratorTest, WriteJsonCreatesDirectory) {
    TestReportGenerator generator("DirTest");
    generator.addResult(createTestResult("test1", "BRUTEFORCE", true));

    auto temp_dir = std::filesystem::temp_directory_path() / "test_report_subdir";
    auto temp_path = temp_dir / "report.json";

    // 确保目录不存在
    std::filesystem::remove_all(temp_dir);

    generator.writeJson(temp_path);

    ASSERT_TRUE(std::filesystem::exists(temp_path));

    // 清理
    std::filesystem::remove_all(temp_dir);
}

// ============================================================================
// Markdown Output Tests
// ============================================================================

TEST(TestReportGeneratorTest, WriteMarkdownCreatesValidFile) {
    TestReportGenerator generator("MarkdownTest");

    generator.addResult(createTestResult("test1", "BRUTEFORCE", true));
    generator.addResult(createTestResult("test2", "IVF", false, 0.8, 0.9));

    auto temp_path = std::filesystem::temp_directory_path() / "test_report.md";

    generator.writeMarkdown(temp_path);

    ASSERT_TRUE(std::filesystem::exists(temp_path));

    std::ifstream ifs(temp_path);
    std::string content((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());

    // 验证 Markdown 结构
    EXPECT_NE(content.find("# SageFlow Join Integration Test Report"), std::string::npos);
    EXPECT_NE(content.find("## Summary"), std::string::npos);
    EXPECT_NE(content.find("## Algorithm Performance"), std::string::npos);
    EXPECT_NE(content.find("## Failed Tests"), std::string::npos);
    EXPECT_NE(content.find("| BRUTEFORCE |"), std::string::npos);
    EXPECT_NE(content.find("| IVF |"), std::string::npos);
    EXPECT_NE(content.find("✅"), std::string::npos);
    EXPECT_NE(content.find("❌"), std::string::npos);

    std::filesystem::remove(temp_path);
}

TEST(TestReportGeneratorTest, WriteMarkdownNoFailedSection) {
    TestReportGenerator generator("AllPassTest");

    generator.addResult(createTestResult("test1", "BRUTEFORCE", true));
    generator.addResult(createTestResult("test2", "IVF", true));

    auto temp_path = std::filesystem::temp_directory_path() / "test_report_pass.md";

    generator.writeMarkdown(temp_path);

    std::ifstream ifs(temp_path);
    std::string content((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());

    // 没有失败测试时不应有 Failed Tests 节
    EXPECT_EQ(content.find("## Failed Tests"), std::string::npos);

    std::filesystem::remove(temp_path);
}

// ============================================================================
// Helper Function Tests
// ============================================================================

TEST(FormatThroughputTest, FormatsMillions) {
    EXPECT_EQ(formatThroughput(1500000), "1.5M/s");
    EXPECT_EQ(formatThroughput(2000000), "2.0M/s");
}

TEST(FormatThroughputTest, FormatsThousands) {
    EXPECT_EQ(formatThroughput(50000), "50.0K/s");
    EXPECT_EQ(formatThroughput(1500), "1.5K/s");
}

TEST(FormatThroughputTest, FormatsSmallNumbers) {
    EXPECT_EQ(formatThroughput(500), "500/s");
    EXPECT_EQ(formatThroughput(99), "99/s");
}

TEST(FormatDurationTest, FormatsMinutes) {
    EXPECT_EQ(formatDuration(120000), "2m 0s");
    EXPECT_EQ(formatDuration(90000), "1m 30s");
}

TEST(FormatDurationTest, FormatsSeconds) {
    EXPECT_EQ(formatDuration(5000), "5.00s");
    EXPECT_EQ(formatDuration(12345), "12.35s");
}

TEST(FormatDurationTest, FormatsMilliseconds) {
    EXPECT_EQ(formatDuration(500), "500.0ms");
    EXPECT_EQ(formatDuration(99.5), "99.5ms");
}

// ============================================================================
// Environment Detection Tests
// ============================================================================

TEST(TestReportGeneratorTest, DetectsEnvironment) {
    TestReportGenerator generator("EnvTest");

    auto report = generator.generateReport();

    // 应该检测到某些环境信息
    EXPECT_FALSE(report.os_info.empty());
    EXPECT_GT(report.cpu_cores, 0);
    EXPECT_GT(report.memory_gb, 0);
}

TEST(TestReportGeneratorTest, DetectsGitCommit) {
    TestReportGenerator generator("GitTest");

    auto report = generator.generateReport();

    // git commit 可能是 "unknown" 或实际的 hash
    EXPECT_FALSE(report.git_commit.empty());
}

TEST(TestReportGeneratorTest, SetGitCommitOverridesDetection) {
    TestReportGenerator generator("GitOverrideTest");

    generator.setGitCommit("abc1234");

    auto report = generator.generateReport();
    EXPECT_EQ(report.git_commit, "abc1234");
}

// ============================================================================
// Print Summary Test (basic verification)
// ============================================================================

TEST(TestReportGeneratorTest, PrintSummaryDoesNotCrash) {
    TestReportGenerator generator("PrintTest");

    generator.addResult(createTestResult("test1", "BRUTEFORCE", true));
    generator.addResult(createTestResult("test2", "IVF", false, 0.8, 0.9));

    // 只验证不崩溃
    EXPECT_NO_THROW(generator.printSummary());
}

// ============================================================================
// AlgorithmSummary Tests
// ============================================================================

TEST(AlgorithmSummaryTest, PassRateCalculation) {
    AlgorithmSummary summary;
    summary.test_count = 10;
    summary.passed_count = 8;

    EXPECT_DOUBLE_EQ(summary.passRate(), 0.8);
}

TEST(AlgorithmSummaryTest, PassRateWithZeroTests) {
    AlgorithmSummary summary;
    summary.test_count = 0;
    summary.passed_count = 0;

    EXPECT_DOUBLE_EQ(summary.passRate(), 0.0);
}

}  // namespace
}  // namespace test
}  // namespace sageFlow

#include "test_utils/test_report_generator.h"

#include "utils/logger.h"

#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

#ifdef __linux__
#include <sys/sysinfo.h>
#include <unistd.h>
#endif

#ifdef _WIN32
#include <windows.h>
#endif

#ifdef __APPLE__
#include <sys/sysctl.h>
#include <sys/types.h>
#endif

namespace sageFlow {
namespace test {

// ============================================================================
// Helper Functions
// ============================================================================

std::string formatThroughput(double throughput) {
    if (throughput >= 1'000'000) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(1) << throughput / 1'000'000 << "M/s";
        return oss.str();
    }
    if (throughput >= 1'000) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(1) << throughput / 1'000 << "K/s";
        return oss.str();
    }
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(0) << throughput << "/s";
    return oss.str();
}

std::string formatDuration(double ms) {
    if (ms >= 60000) {
        int minutes = static_cast<int>(ms / 60000);
        double seconds = std::fmod(ms, 60000) / 1000.0;
        std::ostringstream oss;
        oss << minutes << "m " << std::fixed << std::setprecision(0) << seconds << "s";
        return oss.str();
    }
    if (ms >= 1000) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << ms / 1000.0 << "s";
        return oss.str();
    }
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1) << ms << "ms";
    return oss.str();
}

// ============================================================================
// TestReportGenerator Implementation
// ============================================================================

TestReportGenerator::TestReportGenerator(std::string report_name) : report_name_(std::move(report_name)) {
    detectEnvironment();
    detectGitCommit();
}

void TestReportGenerator::addResult(TestResult result) {
    results_.push_back(std::move(result));
}

void TestReportGenerator::addResults(const std::vector<TestResult>& results) {
    results_.insert(results_.end(), results.begin(), results.end());
}

TestResult TestReportGenerator::fromExecutionStats(const std::string& name, const std::string& algorithm,
                                                   const metrics::JoinExecutionStats& stats, int data_size,
                                                   int parallelism) {
    TestResult result;
    result.name = name;
    result.algorithm = algorithm;
    result.data_size = data_size;
    result.parallelism = parallelism;

    // 准确性指标
    result.recall = stats.recall();
    result.precision = stats.precision();
    result.f1_score = stats.f1Score();

    // 性能指标
    result.throughput_records_per_sec = stats.throughputRecordsPerSec();
    result.execution_time_ms =
        static_cast<double>(std::chrono::duration_cast<std::chrono::milliseconds>(stats.total_time).count());
    result.avg_query_latency_us = stats.avgQueryTimeUs();

    // 计数
    result.expected_matches = stats.true_positives + stats.false_negatives;
    result.actual_matches = stats.output_matches;
    result.true_positives = stats.true_positives;
    result.false_positives = stats.false_positives;
    result.false_negatives = stats.false_negatives;

    return result;
}

TestReport TestReportGenerator::generateReport() const {
    TestReport report;

    // 元数据
    report.report_name = report_name_;
    report.generated_at = getTimestamp();
    report.git_commit = git_commit_;
    report.os_info = os_info_;
    report.cpu_cores = cpu_cores_;
    report.memory_gb = memory_gb_;

    // 汇总统计
    for (const auto& result : results_) {
        report.total_tests++;
        report.total_duration_ms += result.execution_time_ms;

        if (result.skipped) {
            report.skipped_tests++;
        } else if (result.passed) {
            report.passed_tests++;
        } else {
            report.failed_tests++;
            report.failed_results.push_back(result);
        }

        report.detailed_results.push_back(result);
    }

    // 按算法汇总
    computeAlgorithmSummaries(report);

    return report;
}

void TestReportGenerator::computeAlgorithmSummaries(TestReport& report) const {
    // 按算法分组计算
    std::map<std::string, std::vector<const TestResult*>> by_algorithm;
    for (const auto& result : results_) {
        by_algorithm[result.algorithm].push_back(&result);
    }

    for (const auto& [algo, algo_results] : by_algorithm) {
        AlgorithmSummary summary;
        summary.algorithm = algo;
        summary.test_count = static_cast<int>(algo_results.size());

        double total_recall = 0.0;
        double total_precision = 0.0;
        double total_f1 = 0.0;
        double total_throughput = 0.0;
        int valid_metric_count = 0;

        for (const auto* r : algo_results) {
            if (r->skipped) {
                summary.skipped_count++;
                continue;
            }
            if (r->passed) {
                summary.passed_count++;
            } else {
                summary.failed_count++;
            }

            // 只统计非跳过的测试
            total_recall += r->recall;
            total_precision += r->precision;
            total_f1 += r->f1_score;
            total_throughput += r->throughput_records_per_sec;
            valid_metric_count++;

            if (r->throughput_records_per_sec > summary.max_throughput) {
                summary.max_throughput = r->throughput_records_per_sec;
            }
            if (r->throughput_records_per_sec < summary.min_throughput) {
                summary.min_throughput = r->throughput_records_per_sec;
            }
        }

        if (valid_metric_count > 0) {
            summary.avg_recall = total_recall / valid_metric_count;
            summary.avg_precision = total_precision / valid_metric_count;
            summary.avg_f1_score = total_f1 / valid_metric_count;
            summary.avg_throughput = total_throughput / valid_metric_count;
        }

        // 如果没有有效的吞吐量记录，重置 min_throughput
        if (summary.min_throughput == std::numeric_limits<double>::max()) {
            summary.min_throughput = 0.0;
        }

        report.algorithm_summaries[algo] = summary;
    }
}

void TestReportGenerator::writeJson(const std::filesystem::path& output_path) const {
    auto report = generateReport();

    // 确保目录存在
    if (output_path.has_parent_path()) {
        std::filesystem::create_directories(output_path.parent_path());
    }

    std::ofstream ofs(output_path);
    if (!ofs) {
        throw std::runtime_error("Cannot open file for writing: " + output_path.string());
    }

    ofs << "{\n";
    ofs << "  \"report_version\": \"" << report.version << "\",\n";
    ofs << "  \"report_name\": \"" << escapeJsonString(report.report_name) << "\",\n";
    ofs << "  \"generated_at\": \"" << report.generated_at << "\",\n";
    ofs << "  \"git_commit\": \"" << escapeJsonString(report.git_commit) << "\",\n";

    writeJsonEnvironment(ofs, report);
    writeJsonSummary(ofs, report);
    writeJsonAlgorithmResults(ofs, report);
    writeJsonDetailedResults(ofs, report);

    ofs << "}\n";

    SAGEFLOW_LOG_INFO("TestReport", "JSON report written to {}", output_path.string());
}

void TestReportGenerator::writeJsonEnvironment(std::ostream& os, const TestReport& report) const {
    os << "  \"environment\": {\n";
    os << "    \"os\": \"" << escapeJsonString(report.os_info) << "\",\n";
    os << "    \"cpu_cores\": " << report.cpu_cores << ",\n";
    os << "    \"memory_gb\": " << std::fixed << std::setprecision(1) << report.memory_gb << "\n";
    os << "  },\n";
}

void TestReportGenerator::writeJsonSummary(std::ostream& os, const TestReport& report) const {
    os << "  \"summary\": {\n";
    os << "    \"total_tests\": " << report.total_tests << ",\n";
    os << "    \"passed\": " << report.passed_tests << ",\n";
    os << "    \"failed\": " << report.failed_tests << ",\n";
    os << "    \"skipped\": " << report.skipped_tests << ",\n";
    os << "    \"total_duration_ms\": " << std::fixed << std::setprecision(2) << report.total_duration_ms << "\n";
    os << "  },\n";
}

void TestReportGenerator::writeJsonAlgorithmResults(std::ostream& os, const TestReport& report) const {
    os << "  \"algorithm_results\": {\n";
    size_t algo_idx = 0;
    for (const auto& [algo, summary] : report.algorithm_summaries) {
        os << "    \"" << escapeJsonString(algo) << "\": {\n";
        os << "      \"test_count\": " << summary.test_count << ",\n";
        os << "      \"passed\": " << summary.passed_count << ",\n";
        os << "      \"failed\": " << summary.failed_count << ",\n";
        os << "      \"skipped\": " << summary.skipped_count << ",\n";
        os << "      \"avg_recall\": " << std::fixed << std::setprecision(4) << summary.avg_recall << ",\n";
        os << "      \"avg_precision\": " << std::setprecision(4) << summary.avg_precision << ",\n";
        os << "      \"avg_f1_score\": " << std::setprecision(4) << summary.avg_f1_score << ",\n";
        os << "      \"avg_throughput\": " << std::setprecision(1) << summary.avg_throughput << ",\n";
        os << "      \"max_throughput\": " << std::setprecision(1) << summary.max_throughput << ",\n";
        os << "      \"min_throughput\": " << std::setprecision(1) << summary.min_throughput << "\n";
        os << "    }" << (++algo_idx < report.algorithm_summaries.size() ? "," : "") << "\n";
    }
    os << "  },\n";
}

void TestReportGenerator::writeJsonDetailedResults(std::ostream& os, const TestReport& report) const {
    os << "  \"detailed_results\": [\n";
    for (size_t i = 0; i < report.detailed_results.size(); ++i) {
        const auto& r = report.detailed_results[i];
        os << "    {\n";
        os << "      \"name\": \"" << escapeJsonString(r.name) << "\",\n";
        os << "      \"algorithm\": \"" << escapeJsonString(r.algorithm) << "\",\n";
        os << "      \"data_size\": " << r.data_size << ",\n";
        os << "      \"parallelism\": " << r.parallelism << ",\n";
        os << "      \"recall\": " << std::fixed << std::setprecision(4) << r.recall << ",\n";
        os << "      \"precision\": " << std::setprecision(4) << r.precision << ",\n";
        os << "      \"f1_score\": " << std::setprecision(4) << r.f1_score << ",\n";
        os << "      \"throughput_records_per_sec\": " << std::setprecision(1) << r.throughput_records_per_sec << ",\n";
        os << "      \"execution_time_ms\": " << std::setprecision(2) << r.execution_time_ms << ",\n";
        os << "      \"avg_query_latency_us\": " << std::setprecision(2) << r.avg_query_latency_us << ",\n";
        os << "      \"expected_matches\": " << r.expected_matches << ",\n";
        os << "      \"actual_matches\": " << r.actual_matches << ",\n";
        os << "      \"true_positives\": " << r.true_positives << ",\n";
        os << "      \"false_positives\": " << r.false_positives << ",\n";
        os << "      \"false_negatives\": " << r.false_negatives << ",\n";
        os << "      \"passed\": " << (r.passed ? "true" : "false") << ",\n";
        os << "      \"skipped\": " << (r.skipped ? "true" : "false");
        
        // 输出 breakdown 数据（如果有）
        if (r.breakdown.hasData()) {
            os << ",\n      \"breakdown\": {\n";
            os << "        \"window_insert_ns\": " << r.breakdown.window_insert_ns << ",\n";
            os << "        \"index_insert_ns\": " << r.breakdown.index_insert_ns << ",\n";
            os << "        \"expire_ns\": " << r.breakdown.expire_ns << ",\n";
            os << "        \"candidate_fetch_ns\": " << r.breakdown.candidate_fetch_ns << ",\n";
            os << "        \"similarity_ns\": " << r.breakdown.similarity_ns << ",\n";
            os << "        \"join_function_ns\": " << r.breakdown.join_function_ns << ",\n";
            os << "        \"emit_ns\": " << r.breakdown.emit_ns << ",\n";
            os << "        \"lock_wait_ns\": " << r.breakdown.lock_wait_ns << ",\n";
            os << "        \"total_processing_ns\": " << r.breakdown.totalProcessingNs() << ",\n";
            os << "        \"total_records_left\": " << r.breakdown.total_records_left << ",\n";
            os << "        \"total_records_right\": " << r.breakdown.total_records_right << ",\n";
            os << "        \"total_emits\": " << r.breakdown.total_emits << ",\n";
            os << "        \"apply_processing_count\": " << r.breakdown.apply_processing_count << ",\n";
            os << "        \"avg_e2e_latency_us\": " << std::fixed << std::setprecision(2) << r.breakdown.avgE2ELatencyUs() << "\n";
            os << "      }";
        }
        
        if (!r.passed && !r.failure_reason.empty()) {
            os << ",\n      \"failure_reason\": \"" << escapeJsonString(r.failure_reason) << "\"";
        }
        if (r.skipped && !r.skip_reason.empty()) {
            os << ",\n      \"skip_reason\": \"" << escapeJsonString(r.skip_reason) << "\"";
        }
        os << "\n    }" << (i + 1 < report.detailed_results.size() ? "," : "") << "\n";
    }
    os << "  ]\n";
}

void TestReportGenerator::writeMarkdown(const std::filesystem::path& output_path) const {
    auto report = generateReport();

    // 确保目录存在
    if (output_path.has_parent_path()) {
        std::filesystem::create_directories(output_path.parent_path());
    }

    std::ofstream ofs(output_path);
    if (!ofs) {
        throw std::runtime_error("Cannot open file for writing: " + output_path.string());
    }

    writeMarkdownHeader(ofs, report);
    writeMarkdownSummary(ofs, report);
    writeMarkdownAlgorithmTable(ofs, report);
    writeMarkdownFailedTests(ofs, report);
    writeMarkdownDetailedResults(ofs, report);

    SAGEFLOW_LOG_INFO("TestReport", "Markdown report written to {}", output_path.string());
}

void TestReportGenerator::writeMarkdownHeader(std::ostream& os, const TestReport& report) const {
    os << "# SageFlow Join Integration Test Report\n\n";
    os << "**Report Name**: " << report.report_name << "  \n";
    os << "**Generated**: " << report.generated_at << "  \n";
    os << "**Git Commit**: " << report.git_commit << "  \n";
    os << "**Environment**: " << report.os_info << ", " << report.cpu_cores << " cores, " << std::fixed
       << std::setprecision(0) << report.memory_gb << "GB RAM\n\n";
}

void TestReportGenerator::writeMarkdownSummary(std::ostream& os, const TestReport& report) const {
    os << "## Summary\n\n";
    os << "| Metric | Value |\n";
    os << "|--------|-------|\n";
    os << "| Total Tests | " << report.total_tests << " |\n";
    os << "| Passed | " << report.passed_tests << " ✅ |\n";
    os << "| Failed | " << report.failed_tests;
    if (report.failed_tests > 0) {
        os << " ❌";
    }
    os << " |\n";
    os << "| Skipped | " << report.skipped_tests << " |\n";
    os << "| Duration | " << formatDuration(report.total_duration_ms) << " |\n\n";
}

void TestReportGenerator::writeMarkdownAlgorithmTable(std::ostream& os, const TestReport& report) const {
    os << "## Algorithm Performance\n\n";
    os << "| Algorithm | Tests | Pass Rate | Avg Recall | Avg Precision | Avg F1 | Throughput |\n";
    os << "|-----------|-------|-----------|------------|---------------|--------|------------|\n";

    for (const auto& [algo, summary] : report.algorithm_summaries) {
        double pass_rate = summary.passRate() * 100;
        std::string status;
        if (pass_rate >= 100) {
            status = " ✅";
        } else if (pass_rate >= 80) {
            status = " ⚠️";
        } else {
            status = " ❌";
        }

        os << "| " << algo << " | " << summary.test_count << " | " << std::fixed << std::setprecision(0) << pass_rate
           << "%" << status << " | " << std::setprecision(3) << summary.avg_recall << " | " << summary.avg_precision
           << " | " << summary.avg_f1_score << " | " << formatThroughput(summary.avg_throughput) << " |\n";
    }
    os << "\n";
}

void TestReportGenerator::writeMarkdownFailedTests(std::ostream& os, const TestReport& report) const {
    if (report.failed_results.empty()) {
        return;
    }

    os << "## Failed Tests\n\n";
    for (const auto& r : report.failed_results) {
        os << "### " << r.algorithm << ": " << r.name << "\n\n";
        os << "- **Data Size**: " << r.data_size << "\n";
        os << "- **Parallelism**: " << r.parallelism << "\n";
        os << "- **Recall**: " << std::fixed << std::setprecision(4) << r.recall << "\n";
        os << "- **Precision**: " << r.precision << "\n";
        if (!r.failure_reason.empty()) {
            os << "- **Reason**: " << r.failure_reason << "\n";
        }
        os << "- **Details**: TP=" << r.true_positives << ", FP=" << r.false_positives << ", FN=" << r.false_negatives
           << "\n\n";
    }
}

void TestReportGenerator::writeMarkdownDetailedResults(std::ostream& os, const TestReport& report) const {
    os << "## Detailed Results\n\n";
    os << "<details>\n";
    os << "<summary>Click to expand all test results</summary>\n\n";
    os << "| Test Name | Algorithm | Size | Para | Recall | Precision | F1 | Throughput | Status |\n";
    os << "|-----------|-----------|------|------|--------|-----------|-----|------------|--------|\n";

    for (const auto& r : report.detailed_results) {
        std::string status = r.skipped ? "⏭️ Skip" : (r.passed ? "✅ Pass" : "❌ Fail");
        os << "| " << r.name << " | " << r.algorithm << " | " << r.data_size << " | " << r.parallelism << " | "
           << std::fixed << std::setprecision(3) << r.recall << " | " << r.precision << " | " << r.f1_score << " | "
           << formatThroughput(r.throughput_records_per_sec) << " | " << status << " |\n";
    }
    os << "\n</details>\n\n";
    
    // 检查是否有 breakdown 数据
    bool has_breakdown = false;
    for (const auto& r : report.detailed_results) {
        if (r.breakdown.hasData()) {
            has_breakdown = true;
            break;
        }
    }
    
    if (has_breakdown) {
        os << "## Join Operator Breakdown Analysis\n\n";
        os << "<details>\n";
        os << "<summary>Click to expand breakdown timing details</summary>\n\n";
        os << "*Note: Columns marked with `*` are included in the Sum of Stages calculation.*\n\n";
        os << "| Test Name | Para | Window Insert* | Index Insert* | Expire* | Candidate Fetch* | Similarity* | Join Func* | Emit* | Lock Wait* | Sum of Stages | Measured Total |\n";
        os << "|-----------|------|----------------|---------------|---------|------------------|-------------|------------|-------|------------|---------------|----------------|"
           << "\n";
        
        for (const auto& r : report.detailed_results) {
            if (!r.breakdown.hasData()) continue;
            
            auto formatNs = [](uint64_t ns) -> std::string {
                if (ns >= 1'000'000'000) {
                    return std::to_string(ns / 1'000'000'000) + "." + std::to_string((ns / 100'000'000) % 10) + "s";
                }
                if (ns >= 1'000'000) {
                    return std::to_string(ns / 1'000'000) + "." + std::to_string((ns / 100'000) % 10) + "ms";
                }
                if (ns >= 1'000) {
                    return std::to_string(ns / 1'000) + "." + std::to_string((ns / 100) % 10) + "µs";
                }
                return std::to_string(ns) + "ns";
            };
            
            os << "| " << r.name << " | " << r.parallelism << " | "
               << formatNs(r.breakdown.window_insert_ns) << " | "
               << formatNs(r.breakdown.index_insert_ns) << " | "
               << formatNs(r.breakdown.expire_ns) << " | "
               << formatNs(r.breakdown.candidate_fetch_ns) << " | "
               << formatNs(r.breakdown.similarity_ns) << " | "
               << formatNs(r.breakdown.join_function_ns) << " | "
               << formatNs(r.breakdown.emit_ns) << " | "
               << formatNs(r.breakdown.lock_wait_ns) << " | "
               << formatNs(r.breakdown.totalWithLockWaitNs()) << " | "
               << formatNs(r.breakdown.totalProcessingNs()) << " |\n";
        }
        os << "\n</details>\n";
    }
}

void TestReportGenerator::printSummary() const {
    auto report = generateReport();

    std::cout << "\n";
    std::cout << "========================================\n";
    std::cout << "    Integration Test Summary\n";
    std::cout << "========================================\n\n";

    std::cout << "Report: " << report.report_name << "\n";
    std::cout << "Total: " << report.total_tests << " | Passed: " << report.passed_tests
              << " | Failed: " << report.failed_tests << " | Skipped: " << report.skipped_tests << "\n";
    std::cout << "Duration: " << formatDuration(report.total_duration_ms) << "\n\n";

    std::cout << "By Algorithm:\n";
    for (const auto& [algo, summary] : report.algorithm_summaries) {
        std::cout << "  " << std::left << std::setw(15) << algo << " Pass: " << summary.passed_count << "/"
                  << summary.test_count << std::fixed << std::setprecision(3) << "  Recall: " << summary.avg_recall
                  << "  Precision: " << summary.avg_precision << "  Throughput: " << formatThroughput(summary.avg_throughput)
                  << "\n";
    }

    if (!report.failed_results.empty()) {
        std::cout << "\nFailed Tests:\n";
        for (const auto& r : report.failed_results) {
            std::cout << "  ❌ " << r.name;
            if (!r.failure_reason.empty()) {
                std::cout << ": " << r.failure_reason;
            }
            std::cout << "\n";
        }
    }

    std::cout << "\n========================================\n";
}

void TestReportGenerator::setGitCommit(const std::string& commit) {
    git_commit_ = commit;
}

void TestReportGenerator::detectGitCommit() {
    std::array<char, 128> buffer{};
    std::string result;

    FILE* pipe = popen("git rev-parse --short HEAD 2>/dev/null", "r");
    if (pipe != nullptr) {
        if (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
            result = buffer.data();
            // 移除末尾换行符
            if (!result.empty() && result.back() == '\n') {
                result.pop_back();
            }
            if (!result.empty() && result.back() == '\r') {
                result.pop_back();
            }
        }
        pclose(pipe);
    }

    git_commit_ = result.empty() ? "unknown" : result;
}

void TestReportGenerator::detectEnvironment() {
    os_info_ = getOsInfo();
    cpu_cores_ = getCpuCores();
    memory_gb_ = getMemoryGb();
}

std::string TestReportGenerator::getTimestamp() const {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    std::tm tm_now{};

#ifdef _WIN32
    localtime_s(&tm_now, &time_t_now);
#else
    localtime_r(&time_t_now, &tm_now);
#endif

    std::ostringstream oss;
    oss << std::put_time(&tm_now, "%Y-%m-%dT%H:%M:%S");

    // 添加时区偏移（简化版本）
    auto offset_seconds = tm_now.tm_gmtoff;
    int offset_hours = static_cast<int>(offset_seconds / 3600);
    int offset_minutes = static_cast<int>(std::abs(offset_seconds % 3600) / 60);
    oss << (offset_hours >= 0 ? "+" : "-") << std::setfill('0') << std::setw(2) << std::abs(offset_hours) << ":"
        << std::setw(2) << offset_minutes;

    return oss.str();
}

std::string TestReportGenerator::getOsInfo() const {
#ifdef __linux__
    std::array<char, 128> buffer{};
    FILE* pipe = popen("uname -s -r 2>/dev/null", "r");
    if (pipe != nullptr) {
        if (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
            std::string result = buffer.data();
            // 移除末尾换行符
            if (!result.empty() && result.back() == '\n') {
                result.pop_back();
            }
            pclose(pipe);
            return result;
        }
        pclose(pipe);
    }
    return "Linux";
#elif defined(_WIN32)
    return "Windows";
#elif defined(__APPLE__)
    return "macOS";
#else
    return "Unknown";
#endif
}

int TestReportGenerator::getCpuCores() const {
#ifdef __linux__
    return static_cast<int>(sysconf(_SC_NPROCESSORS_ONLN));
#elif defined(_WIN32)
    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    return static_cast<int>(sysinfo.dwNumberOfProcessors);
#elif defined(__APPLE__)
    int cores = 0;
    size_t len = sizeof(cores);
    sysctlbyname("hw.ncpu", &cores, &len, nullptr, 0);
    return cores;
#else
    return 1;
#endif
}

double TestReportGenerator::getMemoryGb() const {
#ifdef __linux__
    struct sysinfo info {};
    if (sysinfo(&info) == 0) {
        return static_cast<double>(info.totalram) * static_cast<double>(info.mem_unit) / (1024.0 * 1024.0 * 1024.0);
    }
    return 0.0;
#elif defined(_WIN32)
    MEMORYSTATUSEX status;
    status.dwLength = sizeof(status);
    if (GlobalMemoryStatusEx(&status)) {
        return static_cast<double>(status.ullTotalPhys) / (1024.0 * 1024.0 * 1024.0);
    }
    return 0.0;
#elif defined(__APPLE__)
    int64_t mem = 0;
    size_t len = sizeof(mem);
    sysctlbyname("hw.memsize", &mem, &len, nullptr, 0);
    return static_cast<double>(mem) / (1024.0 * 1024.0 * 1024.0);
#else
    return 0.0;
#endif
}

std::string TestReportGenerator::escapeJsonString(const std::string& str) const {
    std::string result;
    result.reserve(str.size());
    for (char c : str) {
        switch (c) {
            case '"':
                result += "\\\"";
                break;
            case '\\':
                result += "\\\\";
                break;
            case '\b':
                result += "\\b";
                break;
            case '\f':
                result += "\\f";
                break;
            case '\n':
                result += "\\n";
                break;
            case '\r':
                result += "\\r";
                break;
            case '\t':
                result += "\\t";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    // 控制字符使用 \u00xx 格式
                    std::ostringstream oss;
                    oss << "\\u" << std::hex << std::setfill('0') << std::setw(4) << static_cast<int>(c);
                    result += oss.str();
                } else {
                    result += c;
                }
        }
    }
    return result;
}

}  // namespace test
}  // namespace sageFlow

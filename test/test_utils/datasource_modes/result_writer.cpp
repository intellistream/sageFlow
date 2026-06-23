#include "test_utils/datasource_modes/result_writer.h"

#include <cctype>
#include <filesystem>
#include <fstream>
#include <iomanip>

#include "operator/join_metrics.h"
#include "test_utils/datasource_modes/dataset_sampling.h"
#include "test_utils/dynamic_config.h"
#include "utils/logger.h"

namespace sageFlow {
namespace test {

std::string sanitizeFilename(std::string name) {
  for (char& ch : name) {
    if (!(std::isalnum(static_cast<unsigned char>(ch)) || ch == '_' || ch == '-')) {
      ch = '_';
    }
  }
  return name;
}

std::filesystem::path datasourceResultDir() {
  auto result_dir = std::filesystem::path(
      DynamicConfigManager::resolveProjectRelativePath("test/result/datasource_modes"));
  std::filesystem::create_directories(result_dir);
  return result_dir;
}

void dumpSinkResults(
    const DataSourceModeConfig& config,
    const std::string& method,
    int data_size,
    int parallelism,
    uint64_t win_ms,
    const DatasourceCaseMetrics& metrics,
    const std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>& actual_pairs,
    const std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>& expected_pairs) {
  const auto file_path = datasourceResultDir() /
      (sanitizeFilename(config.name + "_" + method + "_" + std::to_string(data_size) +
                        "_p" + std::to_string(parallelism) + "_w" +
                        std::to_string(win_ms)) + ".json");
  std::ofstream ofs(file_path);
  if (!ofs.is_open()) {
    SAGEFLOW_LOG_WARN("TEST", "[SinkDump] Unable to open {} for writing", file_path.string());
    return;
  }

  ofs << std::fixed << std::setprecision(6);
  ofs << "{\n";
  ofs << "  \"test_name\": \"" << config.name << "\",\n";
  ofs << "  \"method\": \"" << method << "\",\n";
  ofs << "  \"size\": " << data_size << ",\n";
  ofs << "  \"parallelism\": " << parallelism << ",\n";
  ofs << "  \"window_ms\": " << win_ms << ",\n";
  ofs << "  \"duration_ms\": " << metrics.duration_ms << ",\n";
  ofs << "  \"recall\": " << metrics.recall << ",\n";
  ofs << "  \"precision\": " << metrics.precision << ",\n";
  ofs << "  \"f1\": " << metrics.f1 << ",\n";
  ofs << "  \"actual_pair_count\": " << actual_pairs.size() << ",\n";
  ofs << "  \"expected_pair_count\": " << expected_pairs.size() << ",\n";
  ofs << "  \"actual_pairs\": [\n";
  size_t idx = 0;
  for (const auto& pair : actual_pairs) {
    ofs << "    [" << pair.first << ", " << pair.second << "]";
    if (++idx < actual_pairs.size()) {
      ofs << ",";
    }
    ofs << "\n";
  }
  ofs << "  ],\n";
  ofs << "  \"expected_pairs\": [\n";
  idx = 0;
  for (const auto& pair : expected_pairs) {
    ofs << "    [" << pair.first << ", " << pair.second << "]";
    if (++idx < expected_pairs.size()) {
      ofs << ",";
    }
    ofs << "\n";
  }
  ofs << "  ]\n";
  ofs << "}\n";
  SAGEFLOW_LOG_INFO("TEST", "[SinkDump] Results saved to {}", file_path.string());
}

void dumpCaseSummary(
    const DataSourceModeConfig& config,
    const std::string& method,
    int data_size,
    int parallelism,
    uint64_t win_ms,
    const DatasourceCaseMetrics& metrics) {
  const auto file_path = datasourceResultDir() /
      (sanitizeFilename(config.name + "_" + method + "_" + std::to_string(data_size) +
                        "_p" + std::to_string(parallelism) + "_w" +
                        std::to_string(win_ms) + "_summary") + ".json");
  const uint64_t duplicate_count =
      metrics.total_emits > metrics.actual_count ? metrics.total_emits - metrics.actual_count : 0;

  std::ofstream ofs(file_path);
  if (!ofs.is_open()) {
    SAGEFLOW_LOG_WARN("TEST", "[Summary] Unable to open {} for writing", file_path.string());
    return;
  }
  ofs << std::fixed << std::setprecision(6);
  ofs << "{\n";
  ofs << "  \"test_name\": \"" << config.name << "\",\n";
  ofs << "  \"method\": \"" << method << "\",\n";
  ofs << "  \"mode\": \"" << config.mode << "\",\n";
  ofs << "  \"data_source_type\": \"" << config.data_source_type << "\",\n";
  ofs << "  \"sample_mode\": \"" << normalizeSampleMode(config.data_source_sample_mode) << "\",\n";
  ofs << "  \"split_mode\": \"" << config.split_mode << "\",\n";
  ofs << "  \"size\": " << data_size << ",\n";
  ofs << "  \"parallelism\": " << parallelism << ",\n";
  ofs << "  \"window_ms\": " << win_ms << ",\n";
  ofs << "  \"duration_ms\": " << metrics.duration_ms << ",\n";
  ofs << "  \"recall\": " << metrics.recall << ",\n";
  ofs << "  \"precision\": " << metrics.precision << ",\n";
  ofs << "  \"f1\": " << metrics.f1 << ",\n";
  ofs << "  \"actual_count\": " << metrics.actual_count << ",\n";
  ofs << "  \"expected_count\": " << metrics.expected_count << ",\n";
  ofs << "  \"total_emits\": " << metrics.total_emits << ",\n";
  ofs << "  \"duplicate_count\": " << duplicate_count << ",\n";
  ofs << "  \"breakdown\": {\n";
  ofs << "    \"window_insert_ns\": " << JoinMetrics::instance().window_insert_ns.load() << ",\n";
  ofs << "    \"index_insert_ns\": " << JoinMetrics::instance().index_insert_ns.load() << ",\n";
  ofs << "    \"candidate_fetch_ns\": " << JoinMetrics::instance().candidate_fetch_ns.load() << ",\n";
  ofs << "    \"similarity_ns\": " << JoinMetrics::instance().similarity_ns.load() << ",\n";
  ofs << "    \"join_function_ns\": " << JoinMetrics::instance().join_function_ns.load() << ",\n";
  ofs << "    \"emit_ns\": " << JoinMetrics::instance().emit_ns.load() << ",\n";
  ofs << "    \"lock_wait_ns\": " << JoinMetrics::instance().lock_wait_ns.load() << "\n";
  ofs << "  }\n";
  ofs << "}\n";
  SAGEFLOW_LOG_INFO("TEST", "[Summary] Result summary saved to {}", file_path.string());
}

void appendDatasourceTsvReport(
    const DataSourceModeConfig& config,
    const std::string& method,
    int data_size,
    int parallelism,
    uint64_t win_ms,
    size_t match_count,
    const DatasourceCaseMetrics& metrics) {
  try {
    const auto report_dir =
#ifdef PROJECT_DIR
        std::filesystem::path(PROJECT_DIR) / "test" / "result";
#else
        std::filesystem::current_path() / "test" / "result";
#endif
    std::filesystem::create_directories(report_dir);
    const auto report_path = report_dir / "datasource_modes_report.tsv";
    const bool new_file = !std::filesystem::exists(report_path);
    std::ofstream ofs(report_path, std::ios::app);
    if (!ofs.is_open()) {
      return;
    }
    if (new_file) {
      ofs << "test_name\tmode\tmethod\tsize\tparallelism\twin_ms\ttime_ms\tmatches\texpected\trecall\tprecision\tf1\n";
    }
    ofs << config.name << '\t' << config.mode << '\t' << method << '\t' << data_size
        << '\t' << parallelism << '\t' << win_ms << '\t' << metrics.duration_ms
        << '\t' << match_count << '\t' << metrics.expected_count
        << '\t' << metrics.recall << '\t' << metrics.precision << '\t' << metrics.f1 << '\n';
    SAGEFLOW_LOG_INFO("TEST", "Report written to {}", report_path.string());
  } catch (const std::exception& e) {
    SAGEFLOW_LOG_WARN("TEST", "Failed to write report: {}", e.what());
  }
}

}  // namespace test
}  // namespace sageFlow

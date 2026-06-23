#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <memory>
#include <vector>
#include <filesystem>
#include <atomic>
#include "operator/join_metrics.h"
#include "utils/logger.h"
#include "test_utils/dynamic_config.h"
#include "utils/log_config.h"
#include "test_utils/join_test_helper.h"
#include "test_utils/data_source/dataset_data_source.h"
#include "test_utils/datasource_modes/config.h"
#include "test_utils/datasource_modes/dataset_sampling.h"
#include "test_utils/datasource_modes/ground_truth.h"
#include "test_utils/datasource_modes/record_loader.h"
#include "test_utils/datasource_modes/pipeline_runner.h"
#include "test_utils/datasource_modes/result_writer.h"
#include "test_utils/datasource_modes/splitter.h"
#include <algorithm>

namespace sageFlow {
namespace test {

// Test class for data source modes
// -----------------------------------------------------------------------------
// 3. 测试夹具 (Test Fixture)
// -----------------------------------------------------------------------------

/**
 * @brief Google Test 参数化测试类。
 * 
 * 继承自 TestWithParam，参数类型为 tuple，包含：
 * - DataSourceModeConfig: 测试配置
 * - string: Join 方法名 (e.g., "bruteforce_eager")
 * - int: 数据规模
 * - int: 并行度
 * - uint64_t: 窗口大小
 */
class JoinDataSourceModesTest : public ::testing::TestWithParam<std::tuple<DataSourceModeConfig, std::string, int, int, uint64_t>> {
 protected:
  void SetUp() override {
    JoinMetrics::instance().reset();
  }

  void TearDown() override {
    // 输出 QIQ 三阶段统计
    auto& m = JoinMetrics::instance();
    uint64_t q1_count = m.qiq_q1_count.load();
    uint64_t insert_count = m.qiq_insert_count.load();
    uint64_t q2_count = m.qiq_q2_count.load();
    
    if (q1_count > 0 || insert_count > 0 || q2_count > 0) {
      double q1_avg_us = q1_count > 0 ? static_cast<double>(m.qiq_q1_ns.load()) / q1_count / 1000.0 : 0.0;
      double insert_avg_us = insert_count > 0 ? static_cast<double>(m.qiq_insert_ns.load()) / insert_count / 1000.0 : 0.0;
      double q2_avg_us = q2_count > 0 ? static_cast<double>(m.qiq_q2_ns.load()) / q2_count / 1000.0 : 0.0;
      
      SAGEFLOW_LOG_INFO("QIQ_STATS", 
          "Per-vector avg (incl lock): Q1={:.1f}us ({} calls), I={:.1f}us ({} calls), Q2={:.1f}us ({} calls), Total={:.1f}us",
          q1_avg_us, q1_count, insert_avg_us, insert_count, q2_avg_us, q2_count,
          q1_avg_us + insert_avg_us + q2_avg_us);
    }
    
    std::filesystem::create_directories("build/metrics");
    std::string metrics_path = "build/metrics/join_datasource_modes_" +
                               std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".tsv";
    JoinMetrics::instance().dump_tsv(DynamicConfigManager::resolveProjectRelativePath(metrics_path));
    SAGEFLOW_LOG_INFO("TEST", "Performance metrics saved to {}", metrics_path);
  }
};

// -----------------------------------------------------------------------------
// 4. 主测试逻辑
// -----------------------------------------------------------------------------

/**
 * @brief 核心测试用例：验证不同数据源模式和参数下的 Join 性能与准确性。
 * 
 * 流程：
 * 1. 准备数据：根据 mode (生成/加载/直接使用) 准备 base_records。
 * 2. 构建流：将数据分为左右两路流 (Left/Right Stream)。
 * 3. 计算预期结果：使用 computeExpectedPairsByTraversal 计算 Ground Truth。
 * 4. 构建管线：Source -> Join -> Sink。
 * 5. 执行：启动 StreamEnvironment 并等待完成。
 * 6. 验证：对比实际输出与预期结果，计算 Recall/Precision/F1。
 * 7. 报告：将性能指标写入 TSV 报告文件。
 */
TEST_P(JoinDataSourceModesTest, DataSourceModePerformance) {
  auto [mode_config, method, data_size, parallelism, win_ms] = GetParam();

  SAGEFLOW_LOG_INFO("TEST", "===== Running test: {} mode={} method={} size={} parallelism={} win_ms={} =====",
                    mode_config.name, mode_config.mode, method, data_size, parallelism, win_ms);

  auto loaded_records = loadDatasourceModeRecords(mode_config, data_size);

  auto split_records =
      splitDatasourceRecords(std::move(loaded_records.records), mode_config.split_mode);
  auto& left_records = split_records.left;
  auto& right_records = split_records.right;

  const size_t expected_left = left_records.size();
  const size_t expected_right = right_records.size();

  // Compute expected matches - use consistent UID mapping
  // Use alpha from config (default 0.1, use 0.001 for SIFT-like large-norm data)
  const double kAlpha = mode_config.alpha;
  SAGEFLOW_LOG_INFO("TEST", "[GT] Computing ground truth with alpha={}, threshold={}", 
                    kAlpha, mode_config.threshold);
  std::unordered_set<std::pair<uint64_t,uint64_t>, PairHash> expected_matches;
  bool used_cached_ground_truth = false;
  if (loaded_records.dataset_source_for_cache &&
      loaded_records.enable_dataset_ground_truth_cache) {
    auto cached = loadCachedGroundTruth(*loaded_records.dataset_source_for_cache, expected_left, win_ms, mode_config.threshold, kDatasourceModuloBase);
    if (cached) {
      expected_matches = std::move(*cached);
      used_cached_ground_truth = true;
      SAGEFLOW_LOG_INFO("TEST", "[GT] Loaded cached ground truth pairs ({}) for {}", expected_matches.size(), loaded_records.dataset_source_for_cache->getFilePath());
    }
  }
  if (!used_cached_ground_truth) {
    expected_matches =
      computeExpectedPairsByTraversal(left_records, right_records, mode_config.threshold, win_ms, 
                                       mode_config.similarity_mode, kAlpha, kDatasourceModuloBase);
    if (loaded_records.dataset_source_for_cache &&
        loaded_records.enable_dataset_ground_truth_cache) {
      persistGroundTruth(*loaded_records.dataset_source_for_cache, mode_config, method, expected_left, win_ms, mode_config.threshold, kAlpha, kDatasourceModuloBase, expected_matches);
    }
  }
  const uint64_t expected_emit_count = static_cast<uint64_t>(expected_matches.size());

  DatasourcePipelineInput pipeline_input;
  pipeline_input.left_records = std::move(left_records);
  pipeline_input.right_records = std::move(right_records);
  pipeline_input.config = mode_config;
  pipeline_input.method = method;
  pipeline_input.parallelism = parallelism;
  pipeline_input.window_ms = win_ms;
  pipeline_input.expected_emit_count = expected_emit_count;
  auto pipeline_result = runDatasourceJoinPipeline(std::move(pipeline_input));

  EXPECT_FALSE(pipeline_result.timed_out)
      << "Join pipeline did not drain within 30s: processed left="
      << pipeline_result.final_left << "/" << expected_left
      << " right=" << pipeline_result.final_right << "/" << expected_right
      << " completed_left=" << pipeline_result.final_completed_left << "/" << expected_left
      << " completed_right=" << pipeline_result.final_completed_right << "/" << expected_right
      << " emitted=" << pipeline_result.final_emitted << "/" << expected_emit_count;

  // Calculate metrics
  size_t match_count = 0;
  size_t false_positive_count = 0;
  std::vector<std::pair<uint64_t, uint64_t>> false_positives;
  for (auto ap : pipeline_result.actual_pairs) {
    if (expected_matches.count(ap)) {
      match_count++;
    } else {
      false_positive_count++;
      if (false_positives.size() < 20) {  // 只记录前 20 个
        false_positives.push_back(ap);
      }
    }
  }
  
  // 打印分析信息
  SAGEFLOW_LOG_INFO("TEST", "Analysis: actual_pairs={} expected_matches={} match_count={} false_positives={}",
                    pipeline_result.actual_pairs.size(), expected_matches.size(), match_count, false_positive_count);
  
  // 打印一些 false positive 样例
  for (const auto& fp : false_positives) {
    SAGEFLOW_LOG_INFO("TEST", "  False positive: lid={} rid={}", fp.first, fp.second);
  }

  double recall =
      expected_matches.empty() ? 1.0 : static_cast<double>(match_count) / static_cast<double>(expected_matches.size());
  double precision =
      pipeline_result.actual_pairs.empty() ? 0.0 : static_cast<double>(match_count) / static_cast<double>(pipeline_result.actual_pairs.size());
  double f1 = (precision + recall) > 0 ? 2 * precision * recall / (precision + recall) : 0.0;

  SAGEFLOW_LOG_INFO(
      "TEST",
      "Result: name={} mode={} method={} size={} parallelism={} time_ms={} matches={}/{} recall={:.3f} precision={:.3f} "
      "f1={:.3f}",
      mode_config.name, mode_config.mode, method, data_size, parallelism, pipeline_result.duration_ms, match_count,
      expected_matches.size(), recall, precision, f1);
  DatasourceCaseMetrics case_metrics;
  case_metrics.recall = recall;
  case_metrics.precision = precision;
  case_metrics.f1 = f1;
  case_metrics.duration_ms = pipeline_result.duration_ms;
  case_metrics.total_emits = JoinMetrics::instance().total_emits.load();
  case_metrics.actual_count = pipeline_result.actual_pairs.size();
  case_metrics.expected_count = expected_matches.size();
  dumpSinkResults(mode_config, method, data_size, parallelism, win_ms,
                  case_metrics, pipeline_result.actual_pairs, expected_matches);
  dumpCaseSummary(mode_config, method, data_size, parallelism, win_ms, case_metrics);
  appendDatasourceTsvReport(mode_config, method, data_size, parallelism, win_ms,
                            match_count, case_metrics);

  // Assertions
  // 注意：使用 SharedWindowState 时，高并行度（>8）会导致召回率下降
  // 这是由于锁竞争和快照复制时间导致的已知限制
  // 在生产环境中，高并行度场景应使用 PartitionedWindowState + 适当的分区策略
  double recall_threshold = (parallelism > 8) ? 0.50 : 0.85;
  EXPECT_GE(recall, recall_threshold) << "Recall too low for " << mode_config.name 
                                       << " (parallelism=" << parallelism << ")";
  EXPECT_GE(precision, 0.85) << "Precision too low for " << mode_config.name;
}

// Generate test parameters
// -----------------------------------------------------------------------------
// 5. 参数生成与实例化
// -----------------------------------------------------------------------------

/**
 * @brief 构建测试参数组合。
 * 
 * 遍历所有配置项，生成 (Config, Method, Size, Parallelism, Window) 的笛卡尔积组合。
 * 每一个组合都会生成一个独立的测试用例。
 */
static std::vector<std::tuple<DataSourceModeConfig, std::string, int, int, uint64_t>> buildTestParams() {
  std::vector<std::tuple<DataSourceModeConfig, std::string, int, int, uint64_t>> params;

  auto configs = loadDataSourceModeConfigs();
  for (const auto& config : configs) {
    for (const auto& method : config.methods) {
      for (int size : config.sizes) {
        for (int par : config.parallelism) {
          for (uint64_t win : config.win_ms_list) {
            params.push_back({config, method, size, par, win});
            SAGEFLOW_LOG_INFO("TEST", "[PARAM] Generated test case: {} mode={} method={} size={} par={} win={}",
                              config.name, config.mode, method, size, par, win);
          }
        }
      }
    }
  }

  return params;
}

INSTANTIATE_TEST_SUITE_P(
    DataSourceModes,
    JoinDataSourceModesTest,
    ::testing::ValuesIn(buildTestParams()),
    [](const ::testing::TestParamInfo<std::tuple<DataSourceModeConfig, std::string, int, int, uint64_t>>& info) {
      const DataSourceModeConfig& config = std::get<0>(info.param);
      const std::string& method = std::get<1>(info.param);
      int size = std::get<2>(info.param);
      int parallelism = std::get<3>(info.param);
      uint64_t win_ms = std::get<4>(info.param);
      
      std::string name = config.name + "_" + method + "_" + std::to_string(size) + "_p" + 
                        std::to_string(parallelism) + "_w" + std::to_string(win_ms);
      // Replace invalid characters
      std::replace(name.begin(), name.end(), '/', '_');
      std::replace(name.begin(), name.end(), '.', '_');
      return name;
    }
);

} // namespace test
} // namespace sageFlow

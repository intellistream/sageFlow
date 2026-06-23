#pragma once

#include <cstdint>
#include <string>
#include <unordered_set>

#include "test_utils/datasource_modes/config.h"
#include "test_utils/test_data_generator.h"

namespace sageFlow {
namespace test {

struct DatasourceCaseMetrics {
  double recall{0.0};
  double precision{0.0};
  double f1{0.0};
  uint64_t duration_ms{0};
  uint64_t total_emits{0};
  size_t actual_count{0};
  size_t expected_count{0};
};

void dumpSinkResults(
    const DataSourceModeConfig& config,
    const std::string& method,
    int data_size,
    int parallelism,
    uint64_t win_ms,
    const DatasourceCaseMetrics& metrics,
    const std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>& actual_pairs,
    const std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>& expected_pairs);

void dumpCaseSummary(
    const DataSourceModeConfig& config,
    const std::string& method,
    int data_size,
    int parallelism,
    uint64_t win_ms,
    const DatasourceCaseMetrics& metrics);

void appendDatasourceTsvReport(
    const DataSourceModeConfig& config,
    const std::string& method,
    int data_size,
    int parallelism,
    uint64_t win_ms,
    size_t match_count,
    const DatasourceCaseMetrics& metrics);

}  // namespace test
}  // namespace sageFlow

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/data_types.h"
#include "test_utils/datasource_modes/config.h"
#include "test_utils/test_data_generator.h"

namespace sageFlow {
namespace test {

struct DatasourcePipelineInput {
  std::vector<std::unique_ptr<VectorRecord>> left_records;
  std::vector<std::unique_ptr<VectorRecord>> right_records;
  DataSourceModeConfig config;
  std::string method;
  int parallelism{1};
  uint64_t window_ms{0};
  uint64_t expected_emit_count{0};
};

struct DatasourcePipelineResult {
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> actual_pairs;
  uint64_t duration_ms{0};
  bool timed_out{false};
  uint64_t final_left{0};
  uint64_t final_right{0};
  uint64_t final_completed_left{0};
  uint64_t final_completed_right{0};
  uint64_t final_emitted{0};
};

DatasourcePipelineResult runDatasourceJoinPipeline(DatasourcePipelineInput input);

}  // namespace test
}  // namespace sageFlow

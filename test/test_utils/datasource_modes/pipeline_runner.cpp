#include "test_utils/datasource_modes/pipeline_runner.h"

#include <algorithm>
#include <chrono>
#include <mutex>
#include <thread>

#include "concurrency/concurrency_manager.h"
#include "function/join_function.h"
#include "function/sink_function.h"
#include "operator/join_metrics.h"
#include "operator/utils/join_strategy_config.h"
#include "storage/storage_manager.h"
#include "stream/data_stream_source/data_stream_source.h"
#include "stream/stream_environment.h"
#include "test_utils/test_data_adapter.h"
#include "utils/logger.h"

namespace sageFlow {
namespace test {

class TestVectorStreamSource : public DataStreamSource {
 public:
  explicit TestVectorStreamSource(
      std::string name,
      std::vector<std::unique_ptr<VectorRecord>> records)
      : DataStreamSource(std::move(name), DataStreamSourceType::None),
        records_(std::move(records)) {}

  void Init() override { idx_ = 0; }

  auto Next() -> std::unique_ptr<VectorRecord> override {
    if (idx_ >= records_.size()) {
      return nullptr;
    }
    return std::move(records_[idx_++]);
  }

 private:
  std::vector<std::unique_ptr<VectorRecord>> records_;
  size_t idx_{0};
};

JoinStrategyConfig buildJoinStrategyConfigForDatasource(
    const std::string& method,
    const DataSourceModeConfig& config,
    uint64_t window_ms,
    uint64_t trigger_interval,
    int parallelism) {
  JoinStrategyConfig strategy_config;
  strategy_config.algorithm = parseJoinAlgorithm(method);
  strategy_config.similarity_threshold = config.threshold;
  strategy_config.similarity_alpha = config.alpha;
  strategy_config.similarity_mode = parseSimilarityMode(config.similarity_mode);
  strategy_config.dimension = config.vector_dim;
  strategy_config.window_size_ms = static_cast<int64_t>(window_ms);
  strategy_config.step_size_ms = static_cast<int64_t>(trigger_interval);
  strategy_config.inferDefaults();

  if (strategy_config.algorithm == JoinAlgorithm::CLUSTERED_JOIN) {
    strategy_config.num_partitions = parallelism;
    strategy_config.partition_strategy = PartitionStrategy::CENTROID;
    strategy_config.window_state_type = WindowStateType::PARTITIONED;
    strategy_config.index_strategy = IndexStrategy::SHARED;
    strategy_config.clustered_index_type =
        parseClusteredIndexType(config.clustered_index_type);
    strategy_config.clustered_overlap_ratio = config.clustered_overlap_ratio;
    strategy_config.clustered_training_samples = config.clustered_training_samples;
    strategy_config.clustered_multicast_enabled = config.clustered_multicast_enabled;
  }

  return strategy_config;
}

std::unique_ptr<JoinFunction> createDatasourceJoinFunction(
    const DataSourceModeConfig& config,
    uint64_t window_ms,
    uint64_t trigger_interval) {
  auto join_func = std::make_unique<JoinFunction>(
      "DataModeJoin",
      [](std::unique_ptr<VectorRecord>& left,
         std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
        auto left_vector = extractFloatVector(*left);
        auto right_vector = extractFloatVector(*right);
        std::vector<float> output;
        output.reserve(left_vector.size() + right_vector.size());
        output.insert(output.end(), left_vector.begin(), left_vector.end());
        output.insert(output.end(), right_vector.begin(), right_vector.end());
        const uint64_t id = left->uid_ * 1000000 + right->uid_ % 1000000;
        const int64_t timestamp = std::max(left->timestamp_, right->timestamp_);
        return createVectorRecord(id, timestamp, output);
      },
      config.vector_dim);
  join_func->setWindow(window_ms, trigger_interval);
  return join_func;
}

void waitForDatasourcePipeline(
    size_t expected_left,
    size_t expected_right,
    uint64_t expected_emit_count,
    DatasourcePipelineResult& result) {
  using namespace std::chrono_literals;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
  for (;;) {
    const uint64_t left = JoinMetrics::instance().total_records_left.load();
    const uint64_t right = JoinMetrics::instance().total_records_right.load();
    const uint64_t emitted = JoinMetrics::instance().total_emits.load();
    const uint64_t completed_left =
        JoinMetrics::instance().window_records_left_completed.load();
    const uint64_t completed_right =
        JoinMetrics::instance().window_records_right_completed.load();
    if (left >= expected_left && right >= expected_right) {
      break;
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      result.timed_out = true;
      SAGEFLOW_LOG_WARN("TEST",
                        "Timeout waiting for processing: left={}/{} right={}/{} completed={}/{}|{}/{} emitted={}/{}",
                        left, expected_left, right, expected_right,
                        completed_left, expected_left, completed_right,
                        expected_right, emitted, expected_emit_count);
      break;
    }
    std::this_thread::sleep_for(5ms);
  }

  if (!result.timed_out) {
    const auto stable_window = 50ms;
    const auto max_wait = std::chrono::seconds(5);
    uint64_t last = JoinMetrics::instance().total_emits.load();
    auto stable_since = std::chrono::steady_clock::now();
    const auto end_by = std::chrono::steady_clock::now() + max_wait;
    while (std::chrono::steady_clock::now() < end_by) {
      std::this_thread::sleep_for(5ms);
      const uint64_t current = JoinMetrics::instance().total_emits.load();
      if (current != last) {
        last = current;
        stable_since = std::chrono::steady_clock::now();
      }
      if (std::chrono::steady_clock::now() - stable_since >= stable_window) {
        break;
      }
    }
  }

  result.final_left = JoinMetrics::instance().total_records_left.load();
  result.final_right = JoinMetrics::instance().total_records_right.load();
  result.final_completed_left =
      JoinMetrics::instance().window_records_left_completed.load();
  result.final_completed_right =
      JoinMetrics::instance().window_records_right_completed.load();
  result.final_emitted = JoinMetrics::instance().total_emits.load();
}

DatasourcePipelineResult runDatasourceJoinPipeline(DatasourcePipelineInput input) {
  DatasourcePipelineResult result;
  const size_t expected_left = input.left_records.size();
  const size_t expected_right = input.right_records.size();
  const uint64_t trigger_interval =
      static_cast<uint64_t>(std::max<int64_t>(input.config.time_interval_ms, 1));

  auto left_source = std::make_shared<TestVectorStreamSource>(
      "DataModeLeft", std::move(input.left_records));
  auto right_source = std::make_shared<TestVectorStreamSource>(
      "DataModeRight", std::move(input.right_records));
  auto join_func = createDatasourceJoinFunction(
      input.config, input.window_ms, trigger_interval);

  std::mutex match_mutex;
  auto sink_func = std::make_unique<SinkFunction>(
      "DataModeSink", [&](std::unique_ptr<VectorRecord>& record) {
        if (!record) {
          return;
        }
        const uint64_t candidate_id = record->uid_;
        const uint64_t left_id = candidate_id / 1000000;
        const uint64_t right_id = candidate_id % 1000000;
        std::lock_guard<std::mutex> lock(match_mutex);
        result.actual_pairs.insert({left_id, right_id});
      });

  auto join_stream = left_source->join(
      right_source,
      std::move(join_func),
      input.method,
      input.config.threshold,
      static_cast<size_t>(input.parallelism));
  auto strategy_config = buildJoinStrategyConfigForDatasource(
      input.method, input.config, input.window_ms, trigger_interval, input.parallelism);
  join_stream->setJoinStrategyConfig(strategy_config);
  join_stream->writeSink(std::move(sink_func), 1);

  StreamEnvironment env;
  JoinMetrics::instance().reset();
  env.addStream(left_source);
  env.addStream(right_source);

  const auto start = std::chrono::high_resolution_clock::now();
  env.execute();
  waitForDatasourcePipeline(
      expected_left, expected_right, input.expected_emit_count, result);
  env.stop();
  env.awaitTermination();
  const auto end = std::chrono::high_resolution_clock::now();
  result.duration_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
  return result;
}

}  // namespace test
}  // namespace sageFlow

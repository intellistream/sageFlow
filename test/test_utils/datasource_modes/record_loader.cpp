#include "test_utils/datasource_modes/record_loader.h"

#include <algorithm>
#include <filesystem>

#include "test_utils/data_writer/data_writer_base.h"
#include "test_utils/data_writer/fvecs_writer.h"
#include "test_utils/data_writer/json_writer.h"
#include "test_utils/datasource_modes/dataset_sampling.h"
#include "test_utils/test_data_adapter.h"
#include "test_utils/test_data_generator.h"
#include "utils/logger.h"

namespace sageFlow {
namespace test {

TestDataGenerator::Config buildGeneratorConfig(
    const DataSourceModeConfig& mode_config,
    int data_size) {
  TestDataGenerator::Config gen_config;
  gen_config.vector_dim = mode_config.vector_dim;
  gen_config.similarity_threshold = mode_config.threshold;
  gen_config.seed = mode_config.seed;
  gen_config.base_timestamp = 1000000;
  gen_config.time_interval = mode_config.time_interval_ms;

  const int target_pos = static_cast<int>(data_size * 0.10);
  const int target_neg = static_cast<int>(data_size * 0.60);
  const int pos_pairs = target_pos / 2;
  const int neg_pairs = target_neg / 2;
  const int used = 2 * pos_pairs + 2 * neg_pairs;
  gen_config.positive_pairs = pos_pairs;
  gen_config.near_threshold_pairs = 0;
  gen_config.negative_pairs = neg_pairs;
  gen_config.random_tail = std::max(0, data_size - used);
  return gen_config;
}

std::vector<std::unique_ptr<VectorRecord>> loadSequentialDatasetRecords(
    DatasetDataSource& data_source,
    int data_size,
    int64_t time_interval_ms) {
  std::vector<std::unique_ptr<VectorRecord>> records;
  records.reserve(data_size);
  int64_t timestamp = 1000000;
  uint64_t uid = 1;
  while (data_source.hasMore() && records.size() < static_cast<size_t>(data_size)) {
    auto vector = data_source.getNextVector();
    records.push_back(createVectorRecord(uid++, timestamp, vector));
    timestamp += time_interval_ms;
  }
  return records;
}

DatasourceRecordLoadResult loadDirectDatasetRecords(
    const DataSourceModeConfig& mode_config,
    int data_size) {
  DatasourceRecordLoadResult result;
  SAGEFLOW_LOG_INFO("TEST", "[MODE2] Direct-Load from: {}", mode_config.data_source_file_path);

  DatasetDataSource::Config ds_config;
  ds_config.file_path = mode_config.data_source_file_path;
  ds_config.expected_dim = mode_config.data_source_expected_dim;
  ds_config.loop = mode_config.data_source_loop;

  result.dataset_source_for_cache = std::make_shared<DatasetDataSource>(ds_config);
  auto& data_source = *result.dataset_source_for_cache;
  result.records.reserve(data_size);
  int64_t timestamp = 1000000;
  uint64_t uid = 1;
  const auto sample_indices = buildDatasetSampleIndices(
      data_source.getAllVectors().size(), static_cast<size_t>(data_size), mode_config);
  for (size_t sample_index : sample_indices) {
    const auto& vector = data_source.getAllVectors()[sample_index];
    result.records.push_back(createVectorRecord(uid++, timestamp, vector));
    timestamp += mode_config.time_interval_ms;
  }

  const std::string sample_mode = normalizeSampleMode(mode_config.data_source_sample_mode);
  result.enable_dataset_ground_truth_cache =
      (sample_mode == "sequential" &&
       mode_config.data_source_sample_offset == 0 &&
       mode_config.data_source_sample_stride == 1);
  SAGEFLOW_LOG_INFO(
      "TEST",
      "[MODE2] Loaded {} records directly from dataset sample_mode={} offset={} stride={} seed={}",
      result.records.size(), sample_mode, mode_config.data_source_sample_offset,
      mode_config.data_source_sample_stride, mode_config.data_source_sample_seed);
  return result;
}

DatasourceRecordLoadResult loadGeneratedPersistedRecords(
    const DataSourceModeConfig& mode_config,
    int data_size) {
  DatasourceRecordLoadResult result;
  SAGEFLOW_LOG_INFO("TEST", "[MODE1] Generate-Save-Load: format={} path={}",
                    mode_config.storage_format, mode_config.storage_file_path);

  if (!std::filesystem::exists(mode_config.storage_file_path)) {
    SAGEFLOW_LOG_INFO("TEST", "[MODE1] File doesn't exist, generating data");
    TestDataGenerator generator(buildGeneratorConfig(mode_config, data_size));
    auto [records, _] = generator.generateData();

    std::filesystem::create_directories(
        std::filesystem::path(mode_config.storage_file_path).parent_path());
    std::shared_ptr<DataWriterBase> writer;
    if (mode_config.storage_format == "fvecs") {
      writer = std::make_shared<FvecsWriter>();
    } else {
      writer = std::make_shared<JsonWriter>();
    }
    generator.saveGeneratedVectors(mode_config.storage_file_path, writer);
    SAGEFLOW_LOG_INFO("TEST", "[MODE1] Saved {} records to {}",
                      records.size(), mode_config.storage_file_path);
  } else {
    SAGEFLOW_LOG_INFO("TEST", "[MODE1] File exists, skipping generation");
  }

  DatasetDataSource::Config ds_config;
  ds_config.file_path = mode_config.storage_file_path;
  ds_config.expected_dim = mode_config.vector_dim;
  ds_config.loop = true;
  result.dataset_source_for_cache = std::make_shared<DatasetDataSource>(ds_config);
  result.records = loadSequentialDatasetRecords(
      *result.dataset_source_for_cache, data_size, mode_config.time_interval_ms);
  SAGEFLOW_LOG_INFO("TEST", "[MODE1] Loaded {} records from file", result.records.size());
  return result;
}

DatasourceRecordLoadResult loadGeneratedDirectRecords(
    const DataSourceModeConfig& mode_config,
    int data_size) {
  DatasourceRecordLoadResult result;
  SAGEFLOW_LOG_INFO("TEST", "[MODE3] Generate-Direct-Use (no file I/O)");

  TestDataGenerator generator(buildGeneratorConfig(mode_config, data_size));
  auto [records, _] = generator.generateData();
  result.records = std::move(records);
  SAGEFLOW_LOG_INFO("TEST", "[MODE3] Generated {} records directly", result.records.size());
  return result;
}

DatasourceRecordLoadResult loadDatasourceModeRecords(
    const DataSourceModeConfig& mode_config,
    int data_size) {
  if (mode_config.mode == "generate_save_load") {
    return loadGeneratedPersistedRecords(mode_config, data_size);
  }
  if (mode_config.mode == "direct_load") {
    return loadDirectDatasetRecords(mode_config, data_size);
  }
  return loadGeneratedDirectRecords(mode_config, data_size);
}

}  // namespace test
}  // namespace sageFlow

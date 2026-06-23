#include "test_utils/datasource_modes/config.h"

#include <algorithm>
#include <cstdlib>

#include "test_utils/dynamic_config.h"
#include "utils/log_config.h"
#include "utils/logger.h"

namespace sageFlow {
namespace test {

std::string resolveDataSourceModeConfigPath() {
  const char* env_path = std::getenv("SAGEFLOW_TEST_CONFIG_PATH");
  if (env_path != nullptr && env_path[0] != '\0') {
    return DynamicConfigManager::resolveProjectRelativePath(env_path);
  }
  return DynamicConfigManager::resolveProjectRelativePath(
      "config/perf_join_datasource_modes.toml");
}

std::vector<DataSourceModeConfig> loadDataSourceModeConfigs() {
  std::vector<DataSourceModeConfig> configs;
  std::vector<DynamicConfig> perf_configs;
  const std::string config_path = resolveDataSourceModeConfigPath();

  if (!DynamicConfigManager::loadConfigs(config_path, "performance_test", perf_configs)) {
    SAGEFLOW_LOG_WARN("TEST", "Failed to load datasource mode config from {}", config_path);
    return configs;
  }

  DynamicConfig global_config;
  if (DynamicConfigManager::loadConfig(config_path, "", global_config)) {
    auto log_level = global_config.get<std::string>("log.level", "info");
    SAGEFLOW_LOG_INFO("TEST", "Setting log level to: {}", log_level);
    sageFlow::init_log_level(log_level);
  }

  for (const auto& config : perf_configs) {
    DataSourceModeConfig mode_config;
    mode_config.name = config.get<std::string>("name", "unnamed_test");
    mode_config.mode = config.get<std::string>("mode", "generate_direct_use");
    mode_config.methods = config.get<std::vector<std::string>>(
        "methods", std::vector<std::string>{"bruteforce_eager"});

    auto sizes = config.get<std::vector<int>>("sizes", std::vector<int>{});
    mode_config.sizes =
        sizes.empty() ? std::vector<int>{config.get<int>("records_count", 1000)}
                      : std::move(sizes);

    mode_config.parallelism = config.get<std::vector<int>>("parallelism", {1});
    mode_config.threshold = config.get<double>("similarity_threshold", 0.8);

    auto win_list = config.get<std::vector<int>>("window_time_ms", {});
    mode_config.win_ms_list.clear();
    if (win_list.empty()) {
      mode_config.win_ms_list.push_back(
          static_cast<uint64_t>(config.get<int>("window_time_ms", 10000)));
    } else {
      for (int win_ms : win_list) {
        mode_config.win_ms_list.push_back(static_cast<uint64_t>(win_ms));
      }
    }

    mode_config.trig_ms = static_cast<uint64_t>(config.get<int>("window_trigger_ms", 50));
    mode_config.vector_dim = config.get<int>("vector_dim", 128);
    mode_config.time_interval_ms = config.get<int>("time_interval", 10);
    mode_config.seed = static_cast<uint32_t>(config.get<int>("seed", 42));

    mode_config.data_source_type = config.get<std::string>("data_source.type", "random");
    mode_config.data_source_file_path = DynamicConfigManager::resolveProjectRelativePath(
        config.get<std::string>("data_source.file_path", ""));
    mode_config.data_source_expected_dim =
        config.get<int>("data_source.expected_dim", 128);
    mode_config.data_source_loop = (config.get<int>("data_source.loop", 1) != 0);
    mode_config.data_source_sample_mode =
        config.get<std::string>("data_source.sample_mode", "sequential");
    mode_config.data_source_sample_seed = static_cast<uint32_t>(
        config.get<int>("data_source.sample_seed", static_cast<int>(mode_config.seed)));
    mode_config.data_source_sample_offset =
        static_cast<size_t>(std::max(0, config.get<int>("data_source.sample_offset", 0)));
    mode_config.data_source_sample_stride =
        static_cast<size_t>(std::max(1, config.get<int>("data_source.sample_stride", 1)));

    mode_config.storage_format = config.get<std::string>("storage.format", "fvecs");
    mode_config.storage_file_path = DynamicConfigManager::resolveProjectRelativePath(
        config.get<std::string>("storage.file_path", "test/data/temp_generated.fvecs"));

    mode_config.split_mode = config.get<std::string>("split_mode", "duplicate");
    mode_config.similarity_mode = config.get<std::string>("similarity_mode", "fixed_alpha");
    mode_config.alpha = config.get<double>("similarity_alpha",
                                           config.get<double>("alpha", 0.1));

    mode_config.clustered_index_type =
        config.get<std::string>("clustered_join_params.index_type", "ivf");
    mode_config.clustered_overlap_ratio =
        config.get<double>("clustered_join_params.overlap_ratio", 0.1);
    mode_config.clustered_training_samples =
        config.get<int>("clustered_join_params.training_samples", 500);
    mode_config.clustered_multicast_enabled =
        (config.get<int>("clustered_join_params.multicast_enabled", 1) != 0);

    SAGEFLOW_LOG_INFO("TEST", "[CONFIG] Loaded test: name={} mode={} split_mode={} methods={} sizes={} vector_dim={}",
                      mode_config.name, mode_config.mode, mode_config.split_mode,
                      mode_config.methods.size(), mode_config.sizes.size(), mode_config.vector_dim);
    configs.push_back(std::move(mode_config));
  }

  return configs;
}

}  // namespace test
}  // namespace sageFlow

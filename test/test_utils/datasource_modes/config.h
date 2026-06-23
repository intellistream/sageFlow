#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace sageFlow {
namespace test {

struct DataSourceModeConfig {
  std::string name;
  std::string mode;
  std::vector<std::string> methods;
  std::vector<int> sizes;
  std::vector<int> parallelism;
  double threshold{0.8};
  std::vector<uint64_t> win_ms_list{10000};
  uint64_t trig_ms{50};
  int vector_dim{128};
  int64_t time_interval_ms{10};
  uint32_t seed{42};

  std::string data_source_type;
  std::string data_source_file_path;
  int data_source_expected_dim{128};
  bool data_source_loop{true};
  std::string data_source_sample_mode{"sequential"};
  uint32_t data_source_sample_seed{42};
  size_t data_source_sample_offset{0};
  size_t data_source_sample_stride{1};

  std::string storage_format;
  std::string storage_file_path;

  std::string split_mode{"duplicate"};
  std::string similarity_mode{"fixed_alpha"};
  double alpha{0.1};

  std::string clustered_index_type{"ivf"};
  bool clustered_multicast_enabled{true};
  double clustered_overlap_ratio{0.1};
  int clustered_training_samples{500};
};

std::string resolveDataSourceModeConfigPath();

std::vector<DataSourceModeConfig> loadDataSourceModeConfigs();

}  // namespace test
}  // namespace sageFlow

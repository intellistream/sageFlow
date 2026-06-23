#include "test_utils/datasource_modes/dataset_sampling.h"

#include <algorithm>
#include <cctype>
#include <numeric>
#include <random>
#include <stdexcept>

namespace sageFlow {
namespace test {

std::string normalizeSampleMode(std::string mode) {
  std::transform(mode.begin(), mode.end(), mode.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return mode;
}

std::vector<size_t> buildDatasetSampleIndices(
    size_t total_count,
    size_t requested_count,
    const DataSourceModeConfig& mode_config) {
  std::vector<size_t> result;
  if (total_count == 0 || requested_count == 0) {
    return result;
  }

  const std::string sample_mode = normalizeSampleMode(mode_config.data_source_sample_mode);
  size_t offset = mode_config.data_source_sample_offset;
  if (offset >= total_count) {
    if (!mode_config.data_source_loop) {
      return result;
    }
    offset %= total_count;
  }

  result.reserve(requested_count);
  if (sample_mode == "sequential" || sample_mode == "stride") {
    const size_t stride =
        (sample_mode == "stride") ? mode_config.data_source_sample_stride : 1;
    size_t index = offset;
    while (result.size() < requested_count) {
      if (index >= total_count) {
        if (!mode_config.data_source_loop) {
          break;
        }
        index %= total_count;
      }
      result.push_back(index);
      index += stride;
    }
    return result;
  }

  if (sample_mode == "random") {
    std::vector<size_t> indices(total_count);
    std::iota(indices.begin(), indices.end(), 0);
    std::mt19937 rng(mode_config.data_source_sample_seed);
    std::shuffle(indices.begin(), indices.end(), rng);

    size_t index = offset;
    while (result.size() < requested_count) {
      if (index >= indices.size()) {
        if (!mode_config.data_source_loop) {
          break;
        }
        index = 0;
      }
      result.push_back(indices[index++]);
    }
    return result;
  }

  throw std::runtime_error("Unknown dataset sample mode: " +
                           mode_config.data_source_sample_mode);
}

}  // namespace test
}  // namespace sageFlow

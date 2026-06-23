#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "test_utils/datasource_modes/config.h"

namespace sageFlow {
namespace test {

std::string normalizeSampleMode(std::string mode);

std::vector<size_t> buildDatasetSampleIndices(
    size_t total_count,
    size_t requested_count,
    const DataSourceModeConfig& mode_config);

}  // namespace test
}  // namespace sageFlow

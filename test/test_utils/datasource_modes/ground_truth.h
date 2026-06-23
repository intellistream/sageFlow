#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_set>
#include <vector>

#include "common/data_types.h"
#include "test_utils/data_source/dataset_data_source.h"
#include "test_utils/datasource_modes/config.h"
#include "test_utils/test_data_generator.h"

namespace sageFlow {
namespace test {

double computeDatasourceSimilarity(
    const std::vector<float>& left,
    const std::vector<float>& right,
    const std::string& similarity_mode,
    double alpha);

std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>
computeExpectedPairsByTraversal(
    const std::vector<std::unique_ptr<VectorRecord>>& left_records,
    const std::vector<std::unique_ptr<VectorRecord>>& right_records,
    double similarity_threshold,
    uint64_t window_ms,
    const std::string& similarity_mode,
    double alpha,
    uint64_t modulo_base);

std::optional<std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>>
loadCachedGroundTruth(DatasetDataSource& data_source,
                      size_t record_count,
                      uint64_t window_ms,
                      double similarity_threshold,
                      uint64_t modulo_base);

void persistGroundTruth(
    DatasetDataSource& data_source,
    const DataSourceModeConfig& config,
    const std::string& method,
    size_t record_count,
    uint64_t window_ms,
    double similarity_threshold,
    double alpha,
    uint64_t modulo_base,
    const std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>& expected_matches);

}  // namespace test
}  // namespace sageFlow

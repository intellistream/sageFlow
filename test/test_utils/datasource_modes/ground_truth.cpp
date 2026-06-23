#include "test_utils/datasource_modes/ground_truth.h"

#include <algorithm>
#include <cmath>

#include "test_utils/test_data_adapter.h"
#include "utils/logger.h"

namespace sageFlow {
namespace test {

double l2Distance(const std::vector<float>& left, const std::vector<float>& right) {
  double acc = 0.0;
  const size_t n = std::min(left.size(), right.size());
  for (size_t i = 0; i < n; ++i) {
    const double diff = static_cast<double>(left[i]) - static_cast<double>(right[i]);
    acc += diff * diff;
  }
  return std::sqrt(acc);
}

double vectorNorm(const std::vector<float>& values) {
  double sum = 0.0;
  for (float value : values) {
    sum += static_cast<double>(value) * static_cast<double>(value);
  }
  return std::sqrt(sum);
}

double computeDatasourceSimilarity(
    const std::vector<float>& left,
    const std::vector<float>& right,
    const std::string& similarity_mode,
    double alpha) {
  if (similarity_mode == "normalized") {
    const double left_norm = vectorNorm(left);
    const double right_norm = vectorNorm(right);
    if (left_norm < 1e-10 || right_norm < 1e-10) {
      return 0.0;
    }
    double dist_sq = 0.0;
    for (size_t i = 0; i < left.size(); ++i) {
      const double diff =
          static_cast<double>(left[i]) / left_norm -
          static_cast<double>(right[i]) / right_norm;
      dist_sq += diff * diff;
    }
    return std::exp(-alpha * std::sqrt(dist_sq));
  }

  return std::exp(-alpha * l2Distance(left, right));
}

std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>
computeExpectedPairsByTraversal(
    const std::vector<std::unique_ptr<VectorRecord>>& left_records,
    const std::vector<std::unique_ptr<VectorRecord>>& right_records,
    double similarity_threshold,
    uint64_t window_ms,
    const std::string& similarity_mode,
    double alpha,
    uint64_t modulo_base) {
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> expected;
  expected.reserve(left_records.size());

  const int64_t window = static_cast<int64_t>(window_ms);
  size_t j_low = 0;
  size_t j_high = 0;
  const size_t right_size = right_records.size();

  for (const auto& left : left_records) {
    if (!left) {
      continue;
    }
    const int64_t left_ts = left->timestamp_;
    while (j_low < right_size) {
      const auto& right = right_records[j_low];
      if (!right) {
        ++j_low;
        continue;
      }
      if (right->timestamp_ >= left_ts - window) {
        break;
      }
      ++j_low;
    }

    if (j_high < j_low) {
      j_high = j_low;
    }
    while (j_high < right_size) {
      const auto& right = right_records[j_high];
      if (!right) {
        ++j_high;
        continue;
      }
      if (right->timestamp_ > left_ts + window) {
        break;
      }
      ++j_high;
    }

    const auto left_vector = extractFloatVector(*left);
    for (size_t j = j_low; j < j_high; ++j) {
      const auto& right = right_records[j];
      if (!right) {
        continue;
      }
      const auto right_vector = extractFloatVector(*right);
      const double similarity = computeDatasourceSimilarity(
          left_vector, right_vector, similarity_mode, alpha);
      if (similarity >= similarity_threshold) {
        expected.insert({left->uid_, right->uid_ % modulo_base});
      }
    }
  }

  return expected;
}

std::optional<std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>>
loadCachedGroundTruth(DatasetDataSource& data_source,
                      size_t record_count,
                      uint64_t window_ms,
                      double similarity_threshold,
                      uint64_t modulo_base) {
  auto cached = data_source.findGroundTruthEntry(
      window_ms, similarity_threshold, modulo_base, record_count);
  if (!cached) {
    return std::nullopt;
  }
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> restored;
  restored.reserve(cached->pairs.size());
  for (const auto& pair : cached->pairs) {
    restored.insert(pair);
  }
  return restored;
}

void persistGroundTruth(
    DatasetDataSource& data_source,
    const DataSourceModeConfig& config,
    const std::string& method,
    size_t record_count,
    uint64_t window_ms,
    double similarity_threshold,
    double alpha,
    uint64_t modulo_base,
    const std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>& expected_matches) {
  DatasetDataSource::GroundTruthEntry entry;
  entry.window_ms = window_ms;
  entry.similarity_threshold = similarity_threshold;
  entry.alpha = alpha;
  entry.modulo_base = modulo_base;
  entry.record_count = record_count;
  entry.label = config.name + "_" + method + "_p" + std::to_string(record_count);
  entry.pairs.reserve(expected_matches.size());
  for (const auto& pair : expected_matches) {
    entry.pairs.push_back(pair);
  }
  if (data_source.persistGroundTruthEntry(entry)) {
    SAGEFLOW_LOG_INFO("TEST", "[GT] Persisted {} ground truth pairs for {} window={} threshold={}",
                      entry.pairs.size(), data_source.getFilePath(), window_ms, similarity_threshold);
  } else {
    SAGEFLOW_LOG_WARN("TEST", "[GT] Failed to persist ground truth for {}", data_source.getFilePath());
  }
}

}  // namespace test
}  // namespace sageFlow

#include <core/compute_engine/compute_engine.h>

#include <memory>

#include "core/common/data_types.h"

namespace candy {

void ComputeEngine::validateEqualSize(std::unique_ptr<VectorRecord> &record1, std::unique_ptr<VectorRecord> &record2) {
  if (record1->data_->size() != record2->data_->size()) {
    throw std::invalid_argument("VectorRecords must have data of the same size.");
  }
}

auto ComputeEngine::calculateSimilarity(std::unique_ptr<VectorRecord> &record1,
                                        std::unique_ptr<VectorRecord> &record2) -> double {
  validateEqualSize(record1, record2);

  const auto &vec1 = *record1->data_;
  const auto &vec2 = *record2->data_;

  const double dot_product = std::inner_product(vec1.begin(), vec1.end(), vec2.begin(), 0.0);
  const double magnitude1 = std::sqrt(std::inner_product(vec1.begin(), vec1.end(), vec1.begin(), 0.0));
  const double magnitude2 = std::sqrt(std::inner_product(vec2.begin(), vec2.end(), vec2.begin(), 0.0));

  if (magnitude1 == 0.0 || magnitude2 == 0.0) {
    throw std::runtime_error("One or both VectorRecords have zero magnitude.");
  }

  return dot_product / (magnitude1 * magnitude2);
}

auto ComputeEngine::computeEuclideanDistance(std::unique_ptr<VectorRecord> &record1,
                                             std::unique_ptr<VectorRecord> &record2) -> double {
  validateEqualSize(record1, record2);

  const auto &vec1 = *record1->data_;
  const auto &vec2 = *record2->data_;

  double sum_of_squares = 0.0;
  for (size_t i = 0; i < vec1.size(); ++i) {
    double diff = vec1[i] - vec2[i];
    sum_of_squares += diff * diff;
  }

  return std::sqrt(sum_of_squares);
}

auto ComputeEngine::normalizeVector(std::unique_ptr<VectorRecord> &record) -> std::unique_ptr<VectorRecord> {
  const auto &vec = *record->data_;
  double magnitude = std::sqrt(std::inner_product(vec.begin(), vec.end(), vec.begin(), 0.0));

  if (magnitude == 0.0) {
    throw std::runtime_error("Cannot normalize a VectorRecord with zero-magnitude data.");
  }

  VectorData normalized(vec.size());
  std::ranges::transform(vec, normalized.begin(), [&](const double val) { return val / magnitude; });

  return std::make_unique<VectorRecord>(record->id_, std::move(normalized), record->timestamp_);
}

auto ComputeEngine::findTopK(std::vector<std::unique_ptr<VectorRecord>> &records, const size_t k,
                             const std::function<double(std::unique_ptr<VectorRecord> &)> &scorer)
    -> std::vector<std::unique_ptr<VectorRecord>> {
  if (k > records.size()) {
    throw std::invalid_argument("k cannot be greater than the number of records.");
  }

  std::vector<std::pair<double, std::unique_ptr<VectorRecord>>> scored_records;
  scored_records.reserve(records.size());

  for (auto &record : records) {
    scored_records.emplace_back(scorer(record), std::move(record));
  }

  std::ranges::nth_element(scored_records, scored_records.begin() + k,
                           [](const auto &a, const auto &b) { return a.first > b.first; });

  std::vector<std::unique_ptr<VectorRecord>> top_k(k);
  std::ranges::transform(scored_records.begin(), scored_records.begin() + k, top_k.begin(),
                         [](auto &pair) { return std::move(pair.second); });

  return top_k;
}
}  // namespace candy
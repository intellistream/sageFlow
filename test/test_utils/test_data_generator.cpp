#include "test_utils/test_data_generator.h"
#include <algorithm>
#include <cmath>

namespace candy { namespace test {

TestDataGenerator::TestDataGenerator(const Config& config) : config_(config), rng_(config.seed) {}

std::pair<std::vector<std::unique_ptr<VectorRecord>>, std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>>
TestDataGenerator::generateData() {
  std::vector<std::unique_ptr<VectorRecord>> records;
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> expected_matches;
  uint64_t uid_counter = next_uid_; int64_t timestamp = config_.base_timestamp;
  for (int i = 0; i < config_.positive_pairs; ++i) {
    auto base_vector = generateRandomVector(); auto perturbed_vector = perturbVector(base_vector, config_.similarity_threshold + 0.05);
    uint64_t uid1 = uid_counter++; uint64_t uid2 = uid_counter++;
    records.push_back(createRecord(uid1, base_vector, timestamp));
    records.push_back(createRecord(uid2, perturbed_vector, timestamp + config_.time_interval));
    expected_matches.insert({uid1, uid2}); timestamp += config_.time_interval * 2;
  }
  for (int i = 0; i < config_.near_threshold_pairs; ++i) {
    auto base_vector = generateRandomVector(); double target_sim = config_.similarity_threshold + (i % 2 == 0 ? 0.001 : -0.001);
    auto perturbed_vector = perturbVector(base_vector, target_sim); uint64_t uid1 = uid_counter++; uint64_t uid2 = uid_counter++;
    records.push_back(createRecord(uid1, base_vector, timestamp));
    records.push_back(createRecord(uid2, perturbed_vector, timestamp + config_.time_interval));
    if (target_sim >= config_.similarity_threshold) expected_matches.insert({uid1, uid2}); timestamp += config_.time_interval * 2;
  }
  for (int i = 0; i < config_.negative_pairs; ++i) {
    auto vec1 = generateRandomVector(); auto vec2 = generateRandomVector(); uint64_t uid1 = uid_counter++; uint64_t uid2 = uid_counter++;
    records.push_back(createRecord(uid1, vec1, timestamp));
    records.push_back(createRecord(uid2, vec2, timestamp + config_.time_interval));
    if (calculateSimilarity(vec1, vec2) >= config_.similarity_threshold) expected_matches.insert({uid1, uid2}); timestamp += config_.time_interval * 2;
  }
  for (int i = 0; i < config_.random_tail; ++i) { auto vec = generateRandomVector(); records.push_back(createRecord(uid_counter++, vec, timestamp)); timestamp += config_.time_interval; }
  next_uid_ = uid_counter; return {std::move(records), std::move(expected_matches)};
}

std::unique_ptr<VectorRecord> TestDataGenerator::createRecord(uint64_t uid, const std::vector<float>& data, int64_t timestamp) {
  auto record = createVectorRecord(uid, timestamp, data);
  TestRecordSideManager::instance().setSide(uid, (uid % 2 == 0) ? Side::LEFT : Side::RIGHT);
  return record;
}

std::vector<float> TestDataGenerator::generateRandomVector() {
  std::vector<float> vec(config_.vector_dim); std::normal_distribution<float> dist(0.0f, 1.0f);
  for (int i = 0; i < config_.vector_dim; ++i) vec[i] = dist(rng_);
  float norm = 0.0f; for (float v : vec) norm += v*v; norm = std::sqrt(norm);
  if (norm > 1e-6f) for (float &v : vec) v /= norm; return vec;
}

std::vector<float> TestDataGenerator::perturbVector(const std::vector<float>& base, double target_similarity) {
  auto result = base; std::normal_distribution<float> noise_dist(0.0f, 0.1f);
  for (int iter=0; iter<100; ++iter) {
    for (int i=0;i<config_.vector_dim;++i) result[i] += noise_dist(rng_) * 0.1f;
    float norm = 0.0f; for (float v:result) norm += v*v; norm = std::sqrt(norm);
    if (norm>1e-6f) for(float &v:result) v/=norm;
    double current_sim = calculateSimilarity(base, result);
    if (std::abs(current_sim - target_similarity) < 0.01) break;
    if (current_sim < target_similarity) { float alpha=0.9f; for(int i=0;i<config_.vector_dim;++i) result[i]=alpha*base[i]+(1-alpha)*result[i]; }
  }
  return result;
}

double TestDataGenerator::calculateSimilarity(const std::vector<float>& a, const std::vector<float>& b) { double dot=0.0; for (size_t i=0;i<a.size();++i) dot += a[i]*b[i]; return dot; }

std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>
BaselineJoinChecker::computeExpectedMatches(const std::vector<std::unique_ptr<VectorRecord>>& records, double threshold, int64_t window_size_ms) {
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> matches;
  for (size_t i=0;i<records.size();++i) for (size_t j=i+1;j<records.size();++j) {
    const auto& rec1 = records[i]; const auto& rec2 = records[j];
    if (TestRecordSideManager::instance().getSide(rec1->uid_) != TestRecordSideManager::instance().getSide(rec2->uid_) && areInSameWindow(rec1->timestamp_, rec2->timestamp_, window_size_ms)) {
      auto v1 = extractFloatVector(*rec1); auto v2 = extractFloatVector(*rec2); double sim = computeCosineSimilarity(v1, v2); if (sim >= threshold) matches.insert({rec1->uid_, rec2->uid_});
    }
  }
  return matches;
}

double BaselineJoinChecker::computeCosineSimilarity(const std::vector<float>& a, const std::vector<float>& b) { double dot=0.0, na=0.0, nb=0.0; for(size_t i=0;i<a.size();++i){ dot+=a[i]*b[i]; na+=a[i]*a[i]; nb+=b[i]*b[i]; } double np = std::sqrt(na*nb); return np>1e-9 ? dot/np : 0.0; }

bool BaselineJoinChecker::areInSameWindow(int64_t ts1, int64_t ts2, int64_t window_size) { return std::abs(ts1-ts2) <= window_size; }

}} // namespace

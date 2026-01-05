#include "test_utils/test_data_generator.h"
#include "test_utils/data_source/random_data_source.h"
#include "test_utils/data_source/dataset_data_source.h"
#include "test_utils/data_source/data_source_factory.h"
#include "utils/logger.h"
#include <algorithm>
#include <cmath>
#include <fstream>
#include <mutex>
#include <chrono>

namespace sageFlow { namespace test {

TestDataGenerator::TestDataGenerator(const Config& config) : config_(config), rng_(config.seed) {
  // Create default random data source
  RandomDataSource::Config ds_config;
  ds_config.vector_dim = config_.vector_dim;
  ds_config.seed = config_.seed;
  ds_config.max_vectors = -1;  // Unlimited
  data_source_ = std::make_shared<RandomDataSource>(ds_config);
}

TestDataGenerator::TestDataGenerator(const Config& config, std::shared_ptr<DataSourceBase> data_source) 
    : config_(config), rng_(config.seed), data_source_(std::move(data_source)) {
  if (!data_source_) {
    throw std::runtime_error("Data source cannot be null");
  }
  // Update config dimension to match data source
  config_.vector_dim = data_source_->getDimension();
}

TestDataGenerator TestDataGenerator::createFromConfig(const Config& config, const DynamicConfig* data_source_config) {
  if (!data_source_config) {
    // No data source config provided, use default random
    return TestDataGenerator(config);
  }
  
  // Create data source from config
  auto data_source = DataSourceFactory::createFromConfig(*data_source_config, config.vector_dim, config.seed);
  return TestDataGenerator(config, data_source);
}

std::pair<std::vector<std::unique_ptr<VectorRecord>>, std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>>
TestDataGenerator::generateData() {
  std::vector<std::unique_ptr<VectorRecord>> records;
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> expected_matches;
  last_generated_vectors_.clear();  // Clear cache for new generation
  
  uint64_t uid_counter = next_uid_; int64_t timestamp = config_.base_timestamp;
  for (int i = 0; i < config_.positive_pairs; ++i) {
    auto base_vector = getNextVector(); auto perturbed_vector = perturbVector(base_vector, config_.similarity_threshold + 0.05);
    uint64_t uid1 = uid_counter++; uint64_t uid2 = uid_counter++;
    records.push_back(createRecord(uid1, base_vector, timestamp));
    records.push_back(createRecord(uid2, perturbed_vector, timestamp + config_.time_interval));
    last_generated_vectors_.push_back(base_vector);
    last_generated_vectors_.push_back(perturbed_vector);
    expected_matches.insert({uid1, uid2}); timestamp += config_.time_interval * 2;
  }
  for (int i = 0; i < config_.near_threshold_pairs; ++i) {
    auto base_vector = getNextVector(); double target_sim = config_.similarity_threshold + (i % 2 == 0 ? 0.001 : -0.001);
    auto perturbed_vector = perturbVector(base_vector, target_sim); uint64_t uid1 = uid_counter++; uint64_t uid2 = uid_counter++;
    records.push_back(createRecord(uid1, base_vector, timestamp));
    records.push_back(createRecord(uid2, perturbed_vector, timestamp + config_.time_interval));
    last_generated_vectors_.push_back(base_vector);
    last_generated_vectors_.push_back(perturbed_vector);
    if (target_sim >= config_.similarity_threshold) expected_matches.insert({uid1, uid2}); timestamp += config_.time_interval * 2;
  }
  for (int i = 0; i < config_.negative_pairs; ++i) {
    auto vec1 = getNextVector(); auto vec2 = getNextVector(); uint64_t uid1 = uid_counter++; uint64_t uid2 = uid_counter++;
    records.push_back(createRecord(uid1, vec1, timestamp));
    records.push_back(createRecord(uid2, vec2, timestamp + config_.time_interval));
    last_generated_vectors_.push_back(vec1);
    last_generated_vectors_.push_back(vec2);
    if (calculateSimilarity(vec1, vec2) >= config_.similarity_threshold) expected_matches.insert({uid1, uid2}); timestamp += config_.time_interval * 2;
  }
  for (int i = 0; i < config_.random_tail; ++i) { 
    auto vec = getNextVector(); 
    records.push_back(createRecord(uid_counter++, vec, timestamp)); 
    last_generated_vectors_.push_back(vec);
    timestamp += config_.time_interval; 
  }
  next_uid_ = uid_counter; return {std::move(records), std::move(expected_matches)};
}

std::unique_ptr<VectorRecord> TestDataGenerator::createRecord(uint64_t uid, const std::vector<float>& data, int64_t timestamp) {
  auto record = createVectorRecord(uid, timestamp, data);
  TestRecordSideManager::instance().setSide(uid, (uid % 2 == 0) ? Side::LEFT : Side::RIGHT);
  return record;
}

std::vector<float> TestDataGenerator::getNextVector() {
  if (!data_source_->hasMore()) {
    // If data source is exhausted, reset it to allow reuse
    data_source_->reset();
  }
  return data_source_->getNextVector();
}

std::vector<float> TestDataGenerator::perturbVector(const std::vector<float>& base, double target_similarity) {
  // 目标：使 exp(-alpha * ||result - base||_2) ≈ target_similarity
  // 推出所需欧氏距离 d = -ln(target_similarity) / alpha
  double eps = 1e-6;
  double alpha = (config_.alpha > eps ? config_.alpha : 0.1);
  double clipped = std::min(std::max(target_similarity, eps), 1.0 - 1e-9);
  double d = -std::log(clipped) / alpha;

  // 生成一个随机方向向量 u，归一化后沿该方向偏移长度 d
  std::vector<float> dir(config_.vector_dim);
  std::normal_distribution<float> noise(0.0f, 1.0f);
  for (int i = 0; i < config_.vector_dim; ++i) dir[i] = noise(rng_);
  // 轻微去除与 base 的相关性，避免极端重合；不过 ComputeEngine::Similarity 使用绝对坐标的 L2，和方向无关
  // 因为我们在原坐标系中工作，不做单位化 base；仅确保 dir 为单位方向
  double norm = 0.0; for (float v : dir) norm += v * v; norm = std::sqrt(std::max(norm, 1e-12));
  for (float &v : dir) v = static_cast<float>(v / norm);

  std::vector<float> result(base);
  for (int i = 0; i < config_.vector_dim; ++i) {
    result[i] = static_cast<float>(base[i] + d * dir[i]);
  }
  return result;
}

double TestDataGenerator::calculateSimilarity(const std::vector<float>& a, const std::vector<float>& b) {
  // 与项目内 ComputeEngine::Similarity 等价：exp(-alpha * L2)
  double sum = 0.0; for (size_t i = 0; i < a.size(); ++i) { double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]); sum += diff * diff; }
  double dist = std::sqrt(sum);
  double alpha = (config_.alpha > 1e-6 ? config_.alpha : 0.1);
  return std::exp(-alpha * dist);
}

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

bool TestDataGenerator::saveGeneratedVectors(const std::string& file_path, std::shared_ptr<DataWriterBase> writer) {
  if (!writer) {
    SAGEFLOW_LOG_ERROR("TEST", "[TestDataGenerator] Error: Writer cannot be null");
    return false;
  }

  if (last_generated_vectors_.empty()) {
    SAGEFLOW_LOG_ERROR("TEST", "[TestDataGenerator] Error: No data to save. Call generateData() first.");
    return false;
  }

  return writer->writeVectors(file_path, last_generated_vectors_, config_.vector_dim);
}

}} // namespace

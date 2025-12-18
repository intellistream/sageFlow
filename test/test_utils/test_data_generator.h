#pragma once

#include <vector>
#include <random>
#include <memory>
#include <unordered_set>
#include <cmath>
#include "common/data_types.h"
#include "test_utils/test_data_adapter.h"
#include "test_utils/data_source/data_source_base.h"
#include "test_utils/data_writer/data_writer_base.h"
#include "test_utils/dynamic_config.h"

namespace sageFlow { namespace test {

struct PairHash { size_t operator()(const std::pair<uint64_t, uint64_t>& p) const noexcept { uint64_t a = std::min(p.first, p.second); uint64_t b = std::max(p.first, p.second); uint64_t mix = a * 1315423911u ^ ((b << 13) | (b >> 7)); return std::hash<uint64_t>{}(mix); } };

class TestDataGenerator {
public:
  struct Config {
    int vector_dim = 128;
    int positive_pairs = 500;
    int near_threshold_pairs = 50;
    int negative_pairs = 500;
    int random_tail = 2000;
    double similarity_threshold = 0.8;
    // 与项目内 ComputeEngine::Similarity 保持一致的 alpha（默认 0.1）
    double alpha = 0.1;
    uint32_t seed = 42;
    int64_t base_timestamp = 1000000;
    int64_t time_interval = 100;
  };
  explicit TestDataGenerator(const Config& config);
  explicit TestDataGenerator(const Config& config, std::shared_ptr<DataSourceBase> data_source);
  
  /**
   * @brief Create TestDataGenerator from configuration with optional data source config
   * @param config Test data generator configuration
   * @param data_source_config Optional data source configuration (if empty, uses default random)
   */
  static TestDataGenerator createFromConfig(const Config& config, const DynamicConfig* data_source_config = nullptr);
  
  std::pair<std::vector<std::unique_ptr<VectorRecord>>, std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>> generateData();
  
  /**
   * @brief Save generated vectors to a file using the specified writer
   * @param file_path Path to the output file
   * @param writer DataWriter implementation (FvecsWriter, JsonWriter, etc.)
   * @return true if save was successful
   */
  bool saveGeneratedVectors(const std::string& file_path, std::shared_ptr<DataWriterBase> writer);

  /**
   * @brief Get the last generated vectors (for saving after generation)
   */
  std::vector<std::vector<float>> getLastGeneratedVectors() const { return last_generated_vectors_; }

  /**
   * @brief Get the configuration used by this generator
   */
  const Config& getConfig() const { return config_; }

private:
  Config config_; 
  std::mt19937 rng_; 
  uint64_t next_uid_ = 1;
  std::shared_ptr<DataSourceBase> data_source_;
  std::vector<std::vector<float>> last_generated_vectors_;  // Cache for saving
  
  std::unique_ptr<VectorRecord> createRecord(uint64_t uid, const std::vector<float>& data, int64_t timestamp);
  std::vector<float> getNextVector();
  std::vector<float> perturbVector(const std::vector<float>& base, double target_similarity);
  double calculateSimilarity(const std::vector<float>& a, const std::vector<float>& b);
};

class BaselineJoinChecker {
public:
  static std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> computeExpectedMatches(const std::vector<std::unique_ptr<VectorRecord>>& records, double threshold, int64_t window_size_ms);
private:
  static double computeCosineSimilarity(const std::vector<float>& a, const std::vector<float>& b);
  static bool areInSameWindow(int64_t ts1, int64_t ts2, int64_t window_size);
};

}} // namespace

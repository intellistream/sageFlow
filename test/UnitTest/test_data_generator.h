#pragma once

#include <vector>
#include <random>
#include <memory>
#include <unordered_set>
#include <cmath>
#include "common/data_types.h"
#include "test_data_adapter.h"

namespace candy {
namespace test {

struct PairHash {
  size_t operator()(const std::pair<uint64_t, uint64_t>& p) const noexcept {
    uint64_t a = std::min(p.first, p.second);
    uint64_t b = std::max(p.first, p.second);
    uint64_t mix = a * 1315423911u ^ ((b << 13) | (b >> 7));
    return std::hash<uint64_t>{}(mix);
  }
};

class TestDataGenerator {
public:
  struct Config {
    int vector_dim = 128;
    int positive_pairs = 500;
    int near_threshold_pairs = 50;
    int negative_pairs = 500;
    int random_tail = 2000;
    double similarity_threshold = 0.8;
    uint32_t seed = 42;
    int64_t base_timestamp = 1000000;
    int64_t time_interval = 1000; // ms between records
  };

  explicit TestDataGenerator(const Config& config);
  
  // 生成数据并返回预期的匹配对
  std::pair<std::vector<std::unique_ptr<VectorRecord>>, 
           std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>> 
  generateData();

private:
  Config config_;
  std::mt19937 rng_;
  
  std::unique_ptr<VectorRecord> createRecord(uint64_t uid, const std::vector<float>& data, int64_t timestamp);
  std::vector<float> generateRandomVector();
  std::vector<float> perturbVector(const std::vector<float>& base, double target_similarity);
  double calculateSimilarity(const std::vector<float>& a, const std::vector<float>& b);
};

// Baseline算法实现，用于对比验证
class BaselineJoinChecker {
public:
  static std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> 
  computeExpectedMatches(
    const std::vector<std::unique_ptr<VectorRecord>>& records,
    double threshold,
    int64_t window_size_ms);

private:
  static double computeCosineSimilarity(const std::vector<float>& a, const std::vector<float>& b);
  static bool areInSameWindow(int64_t ts1, int64_t ts2, int64_t window_size);
};

} // namespace test
} // namespace candy
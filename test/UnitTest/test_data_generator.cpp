#include "test_data_generator.h"
#include <algorithm>
#include <cmath>

namespace candy {
namespace test {

TestDataGenerator::TestDataGenerator(const Config& config) 
    : config_(config), rng_(config.seed) {}

std::pair<std::vector<std::unique_ptr<VectorRecord>>, 
         std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>>
TestDataGenerator::generateData() {
  std::vector<std::unique_ptr<VectorRecord>> records;
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> expected_matches;
  
  uint64_t uid_counter = 1;
  int64_t timestamp = config_.base_timestamp;
  
  // 生成正样本对
  for (int i = 0; i < config_.positive_pairs; ++i) {
    auto base_vector = generateRandomVector();
    auto perturbed_vector = perturbVector(base_vector, config_.similarity_threshold + 0.05);
    
    uint64_t uid1 = uid_counter++;
    uint64_t uid2 = uid_counter++;
    
    records.push_back(createRecord(uid1, base_vector, timestamp));
    records.push_back(createRecord(uid2, perturbed_vector, timestamp + config_.time_interval));
    
    expected_matches.insert({uid1, uid2});
    timestamp += config_.time_interval * 2;
  }
  
  // 生成阈值边界样本对
  for (int i = 0; i < config_.near_threshold_pairs; ++i) {
    auto base_vector = generateRandomVector();
    // 一半略高于阈值，一半略低于阈值
    double target_sim = config_.similarity_threshold + 
        (i % 2 == 0 ? 0.001 : -0.001);
    auto perturbed_vector = perturbVector(base_vector, target_sim);
    
    uint64_t uid1 = uid_counter++;
    uint64_t uid2 = uid_counter++;
    
    records.push_back(createRecord(uid1, base_vector, timestamp));
    records.push_back(createRecord(uid2, perturbed_vector, timestamp + config_.time_interval));
    
    if (target_sim >= config_.similarity_threshold) {
      expected_matches.insert({uid1, uid2});
    }
    timestamp += config_.time_interval * 2;
  }
  
  // 生成负样本对
  for (int i = 0; i < config_.negative_pairs; ++i) {
    auto vec1 = generateRandomVector();
    auto vec2 = generateRandomVector();
    
    uint64_t uid1 = uid_counter++;
    uint64_t uid2 = uid_counter++;
    
    records.push_back(createRecord(uid1, vec1, timestamp));
    records.push_back(createRecord(uid2, vec2, timestamp + config_.time_interval));
    
    // 负样本通常不会匹配，但如果偶然相似度高于阈值也要记录
    if (calculateSimilarity(vec1, vec2) >= config_.similarity_threshold) {
      expected_matches.insert({uid1, uid2});
    }
    timestamp += config_.time_interval * 2;
  }
  
  // 生成随机尾部数据
  for (int i = 0; i < config_.random_tail; ++i) {
    auto vec = generateRandomVector();
    records.push_back(createRecord(uid_counter++, vec, timestamp));
    timestamp += config_.time_interval;
  }
  
  // 打乱记录顺序以模拟实际场景
  std::shuffle(records.begin(), records.end(), rng_);
  
  return {std::move(records), std::move(expected_matches)};
}

std::unique_ptr<VectorRecord> TestDataGenerator::createRecord(
    uint64_t uid, const std::vector<float>& data, int64_t timestamp) {
  auto record = createVectorRecord(uid, timestamp, data);
  // 记录侧信息到测试辅助管理器（VectorRecord 不包含 side 字段）
  TestRecordSideManager::instance().setSide(uid, (uid % 2 == 0) ? Side::LEFT : Side::RIGHT);
  return record;
}

std::vector<float> TestDataGenerator::generateRandomVector() {
  std::vector<float> vec(config_.vector_dim);
  std::normal_distribution<float> dist(0.0f, 1.0f);
  
  for (int i = 0; i < config_.vector_dim; ++i) {
    vec[i] = dist(rng_);
  }
  
  // 归一化
  float norm = 0.0f;
  for (float v : vec) {
    norm += v * v;
  }
  norm = std::sqrt(norm);
  if (norm > 1e-6f) {
    for (float& v : vec) {
      v /= norm;
    }
  }
  
  return vec;
}

std::vector<float> TestDataGenerator::perturbVector(
    const std::vector<float>& base, double target_similarity) {
  auto result = base;
  std::normal_distribution<float> noise_dist(0.0f, 0.1f);
  
  // 通过添加噪声并调整来达到目标相似度
  for (int iter = 0; iter < 100; ++iter) {
    // 添加噪声
    for (int i = 0; i < config_.vector_dim; ++i) {
      result[i] += noise_dist(rng_) * 0.1f;
    }
    
    // 归一化
    float norm = 0.0f;
    for (float v : result) {
      norm += v * v;
    }
    norm = std::sqrt(norm);
    if (norm > 1e-6f) {
      for (float& v : result) {
        v /= norm;
      }
    }
    
    double current_sim = calculateSimilarity(base, result);
    if (std::abs(current_sim - target_similarity) < 0.01) {
      break;
    }
    
    // 如果相似度差距太大，重新调整
    if (current_sim < target_similarity) {
      float alpha = 0.9f;
      for (int i = 0; i < config_.vector_dim; ++i) {
        result[i] = alpha * base[i] + (1 - alpha) * result[i];
      }
    }
  }
  
  return result;
}

double TestDataGenerator::calculateSimilarity(
    const std::vector<float>& a, const std::vector<float>& b) {
  double dot_product = 0.0;
  for (size_t i = 0; i < a.size(); ++i) {
    dot_product += a[i] * b[i];
  }
  return dot_product; // 假设向量已归一化，余弦相似度即为点积
}

// Baseline算法实现
std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>
BaselineJoinChecker::computeExpectedMatches(
    const std::vector<std::unique_ptr<VectorRecord>>& records,
    double threshold,
    int64_t window_size_ms) {
  
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> matches;
  
  for (size_t i = 0; i < records.size(); ++i) {
    for (size_t j = i + 1; j < records.size(); ++j) {
      const auto& rec1 = records[i];
      const auto& rec2 = records[j];
      
      // 检查是否在同一窗口内且来自不同侧
      if (TestRecordSideManager::instance().getSide(rec1->uid_) !=
              TestRecordSideManager::instance().getSide(rec2->uid_) &&
          areInSameWindow(rec1->timestamp_, rec2->timestamp_, window_size_ms)) {
        auto v1 = extractFloatVector(*rec1);
        auto v2 = extractFloatVector(*rec2);
        double similarity = computeCosineSimilarity(v1, v2);
        if (similarity >= threshold) {
          matches.insert({rec1->uid_, rec2->uid_});
        }
      }
    }
  }
  
  return matches;
}

double BaselineJoinChecker::computeCosineSimilarity(
    const std::vector<float>& a, const std::vector<float>& b) {
  double dot_product = 0.0, norm_a = 0.0, norm_b = 0.0;
  
  for (size_t i = 0; i < a.size(); ++i) {
    dot_product += a[i] * b[i];
    norm_a += a[i] * a[i];
    norm_b += b[i] * b[i];
  }
  
  double norm_product = std::sqrt(norm_a * norm_b);
  return norm_product > 1e-9 ? dot_product / norm_product : 0.0;
}

bool BaselineJoinChecker::areInSameWindow(int64_t ts1, int64_t ts2, int64_t window_size) {
  return std::abs(ts1 - ts2) <= window_size;
}

} // namespace test
} // namespace candy
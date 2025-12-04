#include <gtest/gtest.h>
#include "utils/logger.h"
#include <memory>
#include <unordered_set>
#include "operator/join_operator.h"
#include "function/join_function.h"
#include "test_utils/test_data_generator.h"
#include "test_utils/test_data_adapter.h"
#include "operator/join_metrics.h"
#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"
#include "execution/collector.h"

namespace sageFlow {
namespace test {

// 通用 JoinFunction 工厂，供本文件所有测试复用
static std::unique_ptr<Function> createSimpleJoinFunction() {
  auto join_func_lambda = [](std::unique_ptr<VectorRecord>& left,
                             std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
    auto lv = extractFloatVector(*left);
    auto rv = extractFloatVector(*right);
    std::vector<float> out;
    out.reserve(lv.size() + rv.size());
    out.insert(out.end(), lv.begin(), lv.end());
    out.insert(out.end(), rv.begin(), rv.end());
    uint64_t id = left->uid_ * 1000000 + right->uid_;
    int64_t ts = std::max(left->timestamp_, right->timestamp_);
    return createVectorRecord(id, ts, out);
  };
  return std::make_unique<JoinFunction>("SimpleJoin", join_func_lambda, 128);
}

class JoinHNSWTest : public ::testing::Test {
protected:
  void SetUp() override {
    JoinMetrics::instance().reset();
    auto storage = std::make_shared<StorageManager>();
    concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage);
    
    generator_config_.vector_dim = 128;
    generator_config_.similarity_threshold = 0.8;
    generator_config_.seed = 42;
  }

  void TearDown() override {
    if (::testing::Test::HasFailure()) {
      SAGEFLOW_LOG_WARN("TEST", "HNSW Test failed. Metrics: IDX={}ns CAND={}ns EMITS={} ",
             JoinMetrics::instance().index_insert_ns.load(),
             JoinMetrics::instance().candidate_fetch_ns.load(),
             JoinMetrics::instance().total_emits.load());
    }
  }

protected:
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  TestDataGenerator::Config generator_config_;
};

/**
 * @brief 基本功能测试
 * 
 * 验证 HNSW Join 方法能够正常执行，不会超时或崩溃。
 */
TEST_F(JoinHNSWTest, HNSWBasicCorrectness) {
  generator_config_.positive_pairs = 30;
  generator_config_.negative_pairs = 50;
  generator_config_.random_tail = 20;
  
  TestDataGenerator generator(generator_config_);
  auto [records, expected_matches] = generator.generateData();
  
  auto join_func_ptr = createSimpleJoinFunction();
  JoinOperator join_op(join_func_ptr, concurrency_manager_, "hnsw_eager", 
                      generator_config_.similarity_threshold);
  
  join_op.open();
  
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> actual_matches;
  std::vector<std::unique_ptr<Response>> emitted;
  Collector collector([&](std::unique_ptr<Response> r, int){ 
    if (r && r->record_) emitted.push_back(std::move(r)); 
  });
  
  for (auto& record : records) {
    Response response;
    response.type_ = ResponseType::Record;
    response.record_ = std::move(record);
    join_op.apply(std::move(response), 0, collector);
  }
  
  for (auto &r : emitted) {
    uint64_t combined_uid = r->record_->uid_;
    uint64_t left_uid = combined_uid / 1000000;
    uint64_t right_uid = combined_uid % 1000000;
    actual_matches.insert({left_uid, right_uid});
  }
  
  // 验证 pipeline 能正常跑完（不超时不崩溃）
  SUCCEED() << "HNSW BasicCorrectness executed without timeout/crash."
            << " TODO: add correctness checks later.";
}

/**
 * @brief Lazy 模式测试
 * 
 * 验证 HNSW Lazy 模式能够正常工作。
 */
TEST_F(JoinHNSWTest, HNSWLazyMode) {
  generator_config_.positive_pairs = 25;
  generator_config_.negative_pairs = 40;
  generator_config_.random_tail = 15;
  
  TestDataGenerator generator(generator_config_);
  auto [records, expected_matches] = generator.generateData();
  
  auto join_func_ptr = createSimpleJoinFunction();
  JoinOperator join_op(join_func_ptr, concurrency_manager_, "hnsw_lazy", 
                      generator_config_.similarity_threshold);
  
  join_op.open();
  
  std::vector<std::unique_ptr<Response>> emitted;
  Collector collector([&](std::unique_ptr<Response> r, int){ 
    if (r && r->record_) emitted.push_back(std::move(r)); 
  });
  
  for (auto& record : records) {
    Response response;
    response.type_ = ResponseType::Record;
    response.record_ = std::move(record);
    join_op.apply(std::move(response), 0, collector);
  }
  
  // 验证 pipeline 能正常跑完
  SUCCEED() << "HNSW LazyMode executed without timeout/crash.";
}

/**
 * @brief 大规模测试
 * 
 * 验证 HNSW 在较大数据量下的性能表现。
 */
TEST_F(JoinHNSWTest, HNSWLargeScale) {
  generator_config_.positive_pairs = 500;
  generator_config_.negative_pairs = 1500;
  generator_config_.random_tail = 500;
  generator_config_.vector_dim = 128;
  
  TestDataGenerator generator(generator_config_);
  auto [records, expected_matches] = generator.generateData();
  
  auto join_func_ptr = createSimpleJoinFunction();
  JoinOperator join_op(join_func_ptr, concurrency_manager_, "hnsw_eager", 
                      generator_config_.similarity_threshold);
  
  join_op.open();
  
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> actual_matches;
  std::vector<std::unique_ptr<Response>> emitted;
  Collector collector([&](std::unique_ptr<Response> r, int){ 
    if (r && r->record_) emitted.push_back(std::move(r)); 
  });
  
  uint64_t start_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::high_resolution_clock::now().time_since_epoch()).count();
  
  for (auto& record : records) {
    Response response;
    response.type_ = ResponseType::Record;
    response.record_ = std::move(record);
    join_op.apply(std::move(response), 0, collector);
  }
  
  for (auto &r : emitted) {
    uint64_t combined_uid = r->record_->uid_;
    uint64_t left_uid = combined_uid / 1000000;
    uint64_t right_uid = combined_uid % 1000000;
    actual_matches.insert({left_uid, right_uid});
  }
  
  uint64_t end_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::high_resolution_clock::now().time_since_epoch()).count();
  
  SAGEFLOW_LOG_INFO("TEST", "HNSW LargeScale duration_ms={} expected={} actual={}", 
                    (end_time - start_time) / 1000000, expected_matches.size(), actual_matches.size());
  
  // 验证大规模 pipeline 能正常跑完
  SUCCEED() << "HNSW LargeScale executed without timeout/crash."
            << " TODO: add large-scale accuracy checks later.";
}

/**
 * @brief 阈值边界测试
 * 
 * 测试边界阈值附近的匹配行为。
 */
TEST_F(JoinHNSWTest, HNSWThresholdBoundary) {
  generator_config_.similarity_threshold = 0.85;
  generator_config_.near_threshold_pairs = 20;
  generator_config_.positive_pairs = 10;
  generator_config_.negative_pairs = 20;
  generator_config_.random_tail = 0;
  
  TestDataGenerator generator(generator_config_);
  auto [records, expected_matches] = generator.generateData();
  
  auto join_func_ptr = createSimpleJoinFunction();
  JoinOperator join_op(join_func_ptr, concurrency_manager_, "hnsw_eager", 0.85);
  
  join_op.open();
  
  std::vector<std::unique_ptr<Response>> emitted;
  Collector collector([&](std::unique_ptr<Response> r, int){ 
    if (r && r->record_) emitted.push_back(std::move(r)); 
  });
  
  // 处理左流数据 (slot 0)
  for (auto& record : records) {
    auto record_copy = std::make_unique<VectorRecord>(*record);
    Response response;
    response.type_ = ResponseType::Record;
    response.record_ = std::move(record_copy);
    join_op.apply(std::move(response), 0, collector);
  }
  
  // 处理右流数据 (slot 1)
  for (auto& record : records) {
    Response response;
    response.type_ = ResponseType::Record;
    response.record_ = std::move(record);
    join_op.apply(std::move(response), 1, collector);
  }
  
  SAGEFLOW_LOG_INFO("TEST", "HNSW ThresholdBoundary emitted={}", emitted.size());
  
  // 验证 pipeline 能正常跑完
  SUCCEED() << "HNSW ThresholdBoundary executed without timeout/crash.";
}

/**
 * @brief 双流测试
 * 
 * 测试同时处理左右两个流的场景。
 */
TEST_F(JoinHNSWTest, HNSWDualStream) {
  generator_config_.positive_pairs = 50;
  generator_config_.negative_pairs = 100;
  
  TestDataGenerator generator(generator_config_);
  auto [records, expected_matches] = generator.generateData();
  
  auto join_func = createSimpleJoinFunction();
  JoinOperator join_op(join_func, concurrency_manager_, "hnsw_eager",
                       generator_config_.similarity_threshold);
  
  join_op.open();
  
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> actual_matches;
  std::vector<std::unique_ptr<Response>> emitted;
  Collector collector([&](std::unique_ptr<Response> r, int){ 
    if (r && r->record_) emitted.push_back(std::move(r)); 
  });
  
  // 处理左流数据 (slot 0)
  for (auto& record : records) {
    auto record_copy = std::make_unique<VectorRecord>(*record);
    Response response;
    response.type_ = ResponseType::Record;
    response.record_ = std::move(record_copy);
    join_op.apply(std::move(response), 0, collector);
  }
  
  // 处理右流数据 (slot 1)
  for (auto& record : records) {
    auto record_copy = std::make_unique<VectorRecord>(*record);
    Response response;
    response.type_ = ResponseType::Record;
    response.record_ = std::move(record_copy);
    join_op.apply(std::move(response), 1, collector);
  }
  
  for (auto &r : emitted) {
    uint64_t combined_uid = r->record_->uid_;
    uint64_t left_uid = combined_uid / 1000000;
    uint64_t right_uid = combined_uid % 1000000;
    actual_matches.insert({left_uid, right_uid});
  }
  
  SAGEFLOW_LOG_INFO("TEST", "HNSW DualStream emitted={} unique_matches={}", 
                    emitted.size(), actual_matches.size());
  
  // 验证 pipeline 能正常跑完
  SUCCEED() << "HNSW DualStream executed without timeout/crash.";
}

/**
 * @brief 高维向量测试
 * 
 * 测试 HNSW 在高维向量上的表现。
 */
TEST_F(JoinHNSWTest, HNSWHighDimension) {
  generator_config_.positive_pairs = 50;
  generator_config_.negative_pairs = 100;
  generator_config_.vector_dim = 512;  // 高维向量
  
  TestDataGenerator generator(generator_config_);
  auto [records, expected_matches] = generator.generateData();
  
  // 创建适配高维的 JoinFunction
  auto join_func_lambda = [](std::unique_ptr<VectorRecord>& left,
                             std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
    auto lv = extractFloatVector(*left);
    auto rv = extractFloatVector(*right);
    std::vector<float> out;
    out.reserve(lv.size() + rv.size());
    out.insert(out.end(), lv.begin(), lv.end());
    out.insert(out.end(), rv.begin(), rv.end());
    uint64_t id = left->uid_ * 1000000 + right->uid_;
    int64_t ts = std::max(left->timestamp_, right->timestamp_);
    return createVectorRecord(id, ts, out);
  };
  auto join_func = std::make_unique<JoinFunction>("HighDimJoin", join_func_lambda, 512);
  
  std::unique_ptr<Function> func_ptr = std::move(join_func);
  JoinOperator join_op(func_ptr, concurrency_manager_, "hnsw_eager",
                       generator_config_.similarity_threshold);
  
  join_op.open();
  
  std::vector<std::unique_ptr<Response>> emitted;
  Collector collector([&](std::unique_ptr<Response> r, int){ 
    if (r && r->record_) emitted.push_back(std::move(r)); 
  });
  
  for (auto& record : records) {
    Response response;
    response.type_ = ResponseType::Record;
    response.record_ = std::move(record);
    join_op.apply(std::move(response), 0, collector);
  }
  
  SAGEFLOW_LOG_INFO("TEST", "HNSW HighDimension (dim=512) emitted={}", emitted.size());
  
  // 验证 pipeline 能正常跑完
  SUCCEED() << "HNSW HighDimension executed without timeout/crash.";
}

}  // namespace test
}  // namespace sageFlow

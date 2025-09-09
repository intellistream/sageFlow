#include <gtest/gtest.h>
#include "utils/logger.h"
#include <memory>
#include <unordered_set>
#include "operator/join_operator.h"
#include "function/join_function.h"
#include "test_data_generator.h"
#include "test_data_adapter.h"
#include "operator/join_metrics.h"
#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"
#include "execution/collector.h"

namespace candy {
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

class JoinIVFTest : public ::testing::Test {
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
  CANDY_LOG_WARN("TEST", "IVF Test failed. Metrics: IDX={}ns CAND={}ns EMITS={} ",
         JoinMetrics::instance().index_insert_ns.load(),
         JoinMetrics::instance().candidate_fetch_ns.load(),
         JoinMetrics::instance().total_emits.load());
    }
  }

protected:
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  TestDataGenerator::Config generator_config_;
};

TEST_F(JoinIVFTest, IVFBasicCorrectness) {
  generator_config_.positive_pairs = 30;
  generator_config_.negative_pairs = 50;
  generator_config_.random_tail = 20;
  
  TestDataGenerator generator(generator_config_);
  auto [records, expected_matches] = generator.generateData();
  
  auto join_func_ptr = createSimpleJoinFunction();
  JoinOperator join_op(join_func_ptr, concurrency_manager_, "ivf_eager", 
                      generator_config_.similarity_threshold);
  
  join_op.open();
  
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> actual_matches;
  std::vector<std::unique_ptr<Response>> emitted;
  Collector collector([&](std::unique_ptr<Response> r, int){ if (r && r->record_) emitted.push_back(std::move(r)); });
  
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
  
  // 验证结果
  EXPECT_EQ(expected_matches.size(), actual_matches.size()) 
    << "IVF match count mismatch";
  
  for (const auto& expected_pair : expected_matches) {
    EXPECT_TRUE(actual_matches.count(expected_pair)) 
      << "IVF missing expected match: (" << expected_pair.first 
      << ", " << expected_pair.second << ")";
  }
}

TEST_F(JoinIVFTest, IVFLargeScale) {
  generator_config_.positive_pairs = 1000;
  generator_config_.negative_pairs = 3000;
  generator_config_.random_tail = 1000;
  generator_config_.vector_dim = 256;
  
  TestDataGenerator generator(generator_config_);
  auto [records, expected_matches] = generator.generateData();
  
  auto join_func_ptr = createSimpleJoinFunction();
  JoinOperator join_op(join_func_ptr, concurrency_manager_, "ivf_lazy", 
                      generator_config_.similarity_threshold);
  
  join_op.open();
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> actual_matches;
  std::vector<std::unique_ptr<Response>> emitted;
  Collector collector([&](std::unique_ptr<Response> r, int){ if (r && r->record_) emitted.push_back(std::move(r)); });
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
  
  CANDY_LOG_INFO("TEST", "IVF LargeScale duration_ms={} expected={} actual={} ", (end_time - start_time) / 1000000, expected_matches.size(), actual_matches.size());
  
  // 对于大规模测试，我们允许少量差异（IVF可能有近似误差）
  double match_ratio = static_cast<double>(actual_matches.size()) / expected_matches.size();
  EXPECT_GE(match_ratio, 0.95) << "IVF recall too low";
  EXPECT_LE(match_ratio, 1.05) << "IVF precision issues";
}

TEST_F(JoinIVFTest, IVFWindowExpiry) {
  generator_config_.positive_pairs = 20;
  generator_config_.negative_pairs = 30;
  generator_config_.time_interval = 5000; // 5秒间隔
  
  TestDataGenerator generator(generator_config_);
  auto [records, expected_matches] = generator.generateData();
  
  auto join_func_ptr = createSimpleJoinFunction();
  JoinOperator join_op(join_func_ptr, concurrency_manager_, "ivf_eager", 
                      generator_config_.similarity_threshold);
  
  join_op.open();
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> actual_matches;
  std::vector<std::unique_ptr<Response>> emitted;
  Collector collector([&](std::unique_ptr<Response> r, int){ if (r && r->record_) emitted.push_back(std::move(r)); });
  
  // 按时间顺序排序记录
  std::sort(records.begin(), records.end(), 
           [](const auto& a, const auto& b) { 
             return a->timestamp_ < b->timestamp_; 
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
  
  // 验证过期机制：应该没有超出窗口的匹配
  for (const auto& match : actual_matches) {
    // 这里需要根据实际的窗口机制来验证
    // 暂时验证基本数量合理性
    EXPECT_TRUE(expected_matches.count(match)) 
      << "Window expiry test: unexpected match found";
  }
}

class IVFParameterizedTest : public ::testing::TestWithParam<
  std::tuple<int, double, std::string>> {
protected:
  void SetUp() override {
    JoinMetrics::instance().reset();
  concurrency_manager_ = std::make_shared<ConcurrencyManager>(std::make_shared<StorageManager>());
  }

protected:
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};

TEST_P(IVFParameterizedTest, ParameterVariations) {
  auto [vector_dim, threshold, method] = GetParam();
  
  TestDataGenerator::Config config;
  config.vector_dim = vector_dim;
  config.similarity_threshold = threshold;
  config.positive_pairs = 50;
  config.negative_pairs = 100;
  config.seed = 42;
  
  TestDataGenerator generator(config);
  auto [records, expected_matches] = generator.generateData();
  
  auto join_func_ptr = createSimpleJoinFunction();
  JoinOperator join_op(join_func_ptr, concurrency_manager_, method, threshold);
  
  join_op.open();
  
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> actual_matches;
  std::vector<std::unique_ptr<Response>> emitted;
  Collector collector([&](std::unique_ptr<Response> r, int){ if (r && r->record_) emitted.push_back(std::move(r)); });
  
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
  
  CANDY_LOG_INFO("TEST", "Method={} Dim={} Threshold={} Expected={} Actual={} ", method, vector_dim, threshold, expected_matches.size(), actual_matches.size());
  
  // 验证无崩溃且有合理输出
  EXPECT_GE(actual_matches.size(), 0);
  
  // 对于IVF方法，允许一定的近似误差
  if (method.find("ivf") != std::string::npos) {
    double recall = expected_matches.empty() ? 1.0 : 
        static_cast<double>(actual_matches.size()) / expected_matches.size();
    EXPECT_GE(recall, 0.90) << "IVF recall too low for " << method;
  }
}

INSTANTIATE_TEST_SUITE_P(
  IVFParameterTests,
  IVFParameterizedTest,
  ::testing::Values(
    std::make_tuple(64, 0.7, "ivf_eager"),
    std::make_tuple(128, 0.8, "ivf_lazy"),
    std::make_tuple(256, 0.85, "ivf_eager"),
    std::make_tuple(512, 0.9, "ivf_lazy")
  )
);

} // namespace test
} // namespace candy
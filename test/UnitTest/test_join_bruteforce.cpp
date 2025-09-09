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

class JoinBruteForceTest : public ::testing::Test {
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
  CANDY_LOG_WARN("TEST", "BF Test failed. Metrics: WIN={}ns IDX={}ns SIM={}ns ",
         JoinMetrics::instance().window_insert_ns.load(),
         JoinMetrics::instance().index_insert_ns.load(),
         JoinMetrics::instance().similarity_ns.load());
    }
  }

  // 创建JoinFunction
  std::unique_ptr<Function> createSimpleJoinFunction() {
    auto join_func_lambda = [](std::unique_ptr<VectorRecord>& left,
                              std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
      std::vector<float> result_data;

      // 提取左右向量数据
      auto left_vec = extractFloatVector(*left);
      auto right_vec = extractFloatVector(*right);

      // 拼接向量
      result_data.reserve(left_vec.size() + right_vec.size());
      result_data.insert(result_data.end(), left_vec.begin(), left_vec.end());
      result_data.insert(result_data.end(), right_vec.begin(), right_vec.end());

      // 创建结果记录，组合UID以便后续验证
      uint64_t combined_uid = left->uid_ * 1000000 + right->uid_;
      int64_t result_timestamp = std::max(left->timestamp_, right->timestamp_);

      return createVectorRecord(combined_uid, result_timestamp, result_data);
    };

    return std::make_unique<JoinFunction>("SimpleJoin", join_func_lambda, 128);
  }

protected:
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  TestDataGenerator::Config generator_config_;
};

TEST_F(JoinBruteForceTest, BasicCorrectness) {
  generator_config_.positive_pairs = 50;
  generator_config_.negative_pairs = 100;

  TestDataGenerator generator(generator_config_);
    auto [records, expected_matches] = generator.generateData();

  auto join_func = createSimpleJoinFunction();
  JoinOperator join_op(join_func, concurrency_manager_, "bruteforce_lazy",
                      generator_config_.similarity_threshold);

  join_op.open();
  
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> actual_matches;
  std::vector<std::unique_ptr<Response>> emitted;
  Collector collector([&](std::unique_ptr<Response> r, int){ if (r && r->record_) emitted.push_back(std::move(r)); });

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

    EXPECT_EQ(expected_matches.size(), actual_matches.size())
    << "Match count mismatch";

    for (const auto& expected_pair : expected_matches) {
    EXPECT_TRUE(actual_matches.count(expected_pair))
      << "Missing expected match: (" << expected_pair.first
      << ", " << expected_pair.second << ")";
  }

  for (const auto& actual_pair : actual_matches) {
      EXPECT_TRUE(expected_matches.count(actual_pair))
      << "Unexpected match: (" << actual_pair.first
      << ", " << actual_pair.second << ")";
  }
}

TEST_F(JoinBruteForceTest, ThresholdBoundary) {
  generator_config_.similarity_threshold = 0.85;
  generator_config_.near_threshold_pairs = 20;
  generator_config_.positive_pairs = 0;
  generator_config_.negative_pairs = 0;
  generator_config_.random_tail = 0;

  TestDataGenerator generator(generator_config_);
    auto [records, expected_matches] = generator.generateData();

  auto join_func = createSimpleJoinFunction();
  JoinOperator join_op(join_func, concurrency_manager_, "bruteforce_eager", 0.85);

  join_op.open();
  
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> actual_matches;
  std::vector<std::unique_ptr<Response>> emitted;
  Collector collector([&](std::unique_ptr<Response> r, int){ if (r && r->record_) emitted.push_back(std::move(r)); });

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

  for (auto &r : emitted) {
    uint64_t combined_uid = r->record_->uid_;
    uint64_t left_uid = combined_uid / 1000000;
    uint64_t right_uid = combined_uid % 1000000;
    actual_matches.insert({left_uid, right_uid});
  }

  // 验证阈值边界行为
    EXPECT_EQ(expected_matches.size(), actual_matches.size());

  for (const auto& match : actual_matches) {
      EXPECT_TRUE(expected_matches.count(match))
      << "Boundary test failed for pair: (" << match.first << ", " << match.second << ")";
  }
}

class JoinMethodComparison : public ::testing::TestWithParam<std::tuple<std::string, std::string>> {
protected:
  void SetUp() override {
    JoinMetrics::instance().reset();
    auto storage = std::make_shared<StorageManager>();
    concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage);

    generator_config_.vector_dim = 128;
    generator_config_.similarity_threshold = 0.8;
    generator_config_.positive_pairs = 100;
    generator_config_.negative_pairs = 200;
    generator_config_.random_tail = 100;
    generator_config_.seed = 42;
  }

  std::unique_ptr<Function> createSimpleJoinFunction() {
    auto join_func_lambda = [](std::unique_ptr<VectorRecord>& left,
                              std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
      auto result_data = std::vector<float>();

      auto left_vec = extractFloatVector(*left);
      auto right_vec = extractFloatVector(*right);

      result_data.reserve(left_vec.size() + right_vec.size());
      result_data.insert(result_data.end(), left_vec.begin(), left_vec.end());
      result_data.insert(result_data.end(), right_vec.begin(), right_vec.end());

      uint64_t combined_uid = left->uid_ * 1000000 + right->uid_;
      int64_t result_timestamp = std::max(left->timestamp_, right->timestamp_);

      return createVectorRecord(combined_uid, result_timestamp, result_data);
    };

    return std::make_unique<JoinFunction>("SimpleJoin", join_func_lambda, 128);
  }

protected:
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  TestDataGenerator::Config generator_config_;
};

TEST_P(JoinMethodComparison, MethodConsistency) {
  auto [method1, method2] = GetParam();

  TestDataGenerator generator(generator_config_);
  auto generated_data = generator.generateData();

  auto testMethod = [&](const std::string& method) {
    auto join_func = createSimpleJoinFunction();
    JoinOperator join_op(join_func, concurrency_manager_, method,
                        generator_config_.similarity_threshold);
    join_op.open();
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> matches;
  std::vector<std::unique_ptr<Response>> emitted;
  Collector collector([&](std::unique_ptr<Response> r, int){ if (r && r->record_) emitted.push_back(std::move(r)); });

    // 处理左流（使用生成的同一批记录）
    const auto& records = generated_data.first;
    for (const auto& record : records) {
      auto record_copy = std::make_unique<VectorRecord>(record->uid_, record->timestamp_, record->data_);
      Response response;
      response.type_ = ResponseType::Record;
      response.record_ = std::move(record_copy);

      join_op.apply(std::move(response), 0, collector);
    }

    // 处理右流（再次使用相同记录集）
    for (const auto& record : records) {
      auto record_copy = std::make_unique<VectorRecord>(record->uid_, record->timestamp_, record->data_);
      Response response;
      response.type_ = ResponseType::Record;
      response.record_ = std::move(record_copy);
      join_op.apply(std::move(response), 1, collector);
    }

  for (auto &r : emitted) {
      uint64_t combined_uid = r->record_->uid_;
      uint64_t left_uid = combined_uid / 1000000;
      uint64_t right_uid = combined_uid % 1000000;
      matches.insert({left_uid, right_uid});
    }

    return matches;
  };

  auto matches1 = testMethod(method1);
  auto matches2 = testMethod(method2);

  // 验证两种方法产生相同结果
  EXPECT_EQ(matches1.size(), matches2.size())
    << "Method " << method1 << " vs " << method2 << " size mismatch";

  for (const auto& match : matches1) {
    EXPECT_TRUE(matches2.count(match))
      << "Method consistency failed for pair: (" << match.first << ", " << match.second << ")";
  }
}

INSTANTIATE_TEST_SUITE_P(
  MethodConsistencyTests,
  JoinMethodComparison,
  ::testing::Values(
    std::make_tuple("bruteforce_eager", "bruteforce_lazy"),
    std::make_tuple("ivf_eager", "ivf_lazy"),
    std::make_tuple("bruteforce_eager", "ivf_eager"),
    std::make_tuple("bruteforce_lazy", "ivf_lazy")
  )
);

} // namespace test
} // namespace candy
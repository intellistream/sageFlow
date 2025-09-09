#include <gtest/gtest.h>
#include <memory>
#include <unordered_set>
#include "test_config_manager.h"
#include "test_data_generator.h"
#include "test_data_adapter.h"
#include "operator/join_operator.h"
#include "function/join_function.h"
#include "operator/join_metrics.h"
#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"
#include <filesystem>
#include "utils/logger.h"

namespace candy {
namespace test {

class ConfigDrivenJoinTest : public ::testing::Test {
protected:
  void SetUp() override {
    JoinMetrics::instance().reset();
    auto storage = std::make_shared<StorageManager>();
    concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage);
  }

  std::unique_ptr<Function> createSimpleJoinFunction() {
    auto join_func_lambda = [](std::unique_ptr<VectorRecord>& left,
                              std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
      // 拼接左右向量作为输出数据，UID 合并编码
      auto left_vec = extractFloatVector(*left);
      auto right_vec = extractFloatVector(*right);
      std::vector<float> result_data;
      result_data.reserve(left_vec.size() + right_vec.size());
      result_data.insert(result_data.end(), left_vec.begin(), left_vec.end());
      result_data.insert(result_data.end(), right_vec.begin(), right_vec.end());

      uint64_t combined_uid = left->uid_ * 1000000 + right->uid_;
      int64_t ts = std::max(left->timestamp_, right->timestamp_);
      return createVectorRecord(combined_uid, ts, result_data);
    };
    return std::make_unique<JoinFunction>("SimpleJoin", join_func_lambda, 128);
  }

protected:
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};

TEST_F(ConfigDrivenJoinTest, RunConfiguredTestCases) {
  // 加载配置文件中的测试用例
  std::vector<TestCaseConfig> test_cases;
  bool loaded = TestConfigManager::loadTestCases("config/join_method_cases.toml", test_cases);
  
  if (!loaded || test_cases.empty()) {
    GTEST_SKIP() << "No test cases loaded from config file";
    return;
  }
  
  CANDY_LOG_INFO("TEST", "Loaded {} test cases from config", test_cases.size());
  
  for (const auto& test_config : test_cases) {
    SCOPED_TRACE("Running configured test case: " + test_config.name);
    
    // 根据配置生成测试数据
    TestDataGenerator::Config gen_config;
    gen_config.vector_dim = test_config.vector_dim;
    gen_config.similarity_threshold = test_config.similarity_threshold;
    gen_config.positive_pairs = test_config.positive_pairs;
    gen_config.near_threshold_pairs = test_config.near_threshold_pairs;
    gen_config.negative_pairs = test_config.negative_pairs;
    gen_config.random_tail = test_config.random_tail;
    gen_config.seed = test_config.seed;
    
    TestDataGenerator generator(gen_config);
    auto [records, expected_matches] = generator.generateData();
    
    // 创建Join算子
    auto join_func = createSimpleJoinFunction();
    JoinOperator join_op(join_func, concurrency_manager_, test_config.method, 
                        test_config.similarity_threshold);
    
    join_op.open();
    
    std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> actual_matches;
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // 处理所有记录，使用 apply + Collector 收集
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
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    CANDY_LOG_INFO("TEST", "Test case '{}' ({}): {}ms, Expected: {}, Actual: {}", 
                   test_config.name, test_config.method, duration.count(), expected_matches.size(), actual_matches.size());
    
    // 验证结果
    if (test_config.method.find("ivf") != std::string::npos) {
      // IVF方法允许一定的近似误差
      double recall = expected_matches.empty() ? 1.0 : 
          static_cast<double>(actual_matches.size()) / expected_matches.size();
      EXPECT_GE(recall, 0.95) << "Test case " << test_config.name << " recall too low";
    } else {
      // BruteForce方法期望精确匹配
      EXPECT_EQ(expected_matches.size(), actual_matches.size()) 
        << "Test case " << test_config.name << " match count mismatch";
      
      for (const auto& expected_pair : expected_matches) {
        EXPECT_TRUE(actual_matches.count(expected_pair)) 
          << "Test case " << test_config.name << " missing match: (" 
          << expected_pair.first << ", " << expected_pair.second << ")";
      }
    }
    
    // 输出metrics
  std::filesystem::create_directories("build/metrics");
  std::string metrics_file = "build/metrics/" + test_config.name + "_metrics.tsv";
    JoinMetrics::instance().dump_tsv(metrics_file);
    JoinMetrics::instance().reset();
  }
}

TEST_F(ConfigDrivenJoinTest, RunConfiguredPerformanceTests) {
  // 加载性能测试配置
  std::vector<PerformanceTestConfig> perf_tests;
  bool loaded = TestConfigManager::loadPerformanceTests("config/join_method_cases.toml", perf_tests);
  
  if (!loaded || perf_tests.empty()) {
    GTEST_SKIP() << "No performance tests loaded from config file";
    return;
  }
  
  CANDY_LOG_INFO("TEST", "Loaded {} performance tests from config", perf_tests.size());
  
  for (const auto& perf_config : perf_tests) {
    SCOPED_TRACE("Running performance test: " + perf_config.name);
    
    CANDY_LOG_INFO("TEST", "=== Performance Test: {} ===", perf_config.name);
    
    // 生成测试数据
    TestDataGenerator::Config gen_config;
    gen_config.vector_dim = perf_config.vector_dim;
    gen_config.similarity_threshold = perf_config.similarity_threshold;
    gen_config.positive_pairs = perf_config.records_count / 10;
    gen_config.negative_pairs = perf_config.records_count * 6 / 10;
    gen_config.random_tail = perf_config.records_count * 3 / 10;
    gen_config.seed = perf_config.seed;
    
    TestDataGenerator generator(gen_config);
    auto [base_records, expected_matches] = generator.generateData();
    
    // 测试所有方法和并行度组合
    for (const auto& method : perf_config.methods) {
      for (int parallelism : perf_config.parallelism) {
        SCOPED_TRACE("Method: " + method + ", Parallelism: " + std::to_string(parallelism));
        
        // 复制记录
        std::vector<std::unique_ptr<VectorRecord>> records;
        for (const auto& record : base_records) {
          records.push_back(std::make_unique<VectorRecord>(*record));
        }
        
        JoinMetrics::instance().reset();
        auto start_time = std::chrono::high_resolution_clock::now();
        
  if (parallelism == 1) {
          // 单线程测试
          auto join_func = createSimpleJoinFunction();
          JoinOperator join_op(join_func, concurrency_manager_, method, 
                              perf_config.similarity_threshold);
          join_op.open();
          
          int match_count = 0;
          Collector collector([&](std::unique_ptr<Response> r, int){ if (r && r->record_) match_count++; });
          for (auto& record : records) {
            Response response;
            response.type_ = ResponseType::Record;
            response.record_ = std::move(record);
            join_op.apply(std::move(response), 0, collector);
          }
          
        } else {
          // 多线程测试
          std::vector<std::thread> workers;
          std::atomic<int> record_index{0};
          std::atomic<int> total_matches{0};
          
          std::vector<std::unique_ptr<JoinOperator>> join_ops;
          for (int i = 0; i < parallelism; ++i) {
            auto join_func = createSimpleJoinFunction();
            auto join_op = std::make_unique<JoinOperator>(join_func, concurrency_manager_, 
                                                          method, perf_config.similarity_threshold);
            join_op->open();
            join_ops.push_back(std::move(join_op));
          }
          
          for (int worker_id = 0; worker_id < parallelism; ++worker_id) {
            workers.emplace_back([&, worker_id]() {
              int local_matches = 0;
              int idx;
              while ((idx = record_index.fetch_add(1)) < records.size()) {
                auto record_copy = std::make_unique<VectorRecord>(*records[idx]);
                Response response;
                response.type_ = ResponseType::Record;
                response.record_ = std::move(record_copy);
                
                Collector collector_local([&](std::unique_ptr<Response> r, int){ if (r && r->record_) local_matches++; });
                join_ops[worker_id]->apply(std::move(response), worker_id, collector_local);
              }
              total_matches.fetch_add(local_matches);
            });
          }
          
          for (auto& worker : workers) {
            worker.join();
          }
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        CANDY_LOG_INFO("TEST", "{} (P={}): {}ms, Metrics: WI={}ms II={}ms SIM={}ms", 
                       method, parallelism, duration.count(),
                       JoinMetrics::instance().window_insert_ns.load() / 1000000,
                       JoinMetrics::instance().index_insert_ns.load() / 1000000,
                       JoinMetrics::instance().similarity_ns.load() / 1000000);
        
        // 性能断言
        EXPECT_LT(duration.count(), perf_config.records_count * 2) 
          << "Performance too slow for " << method << " with parallelism " << parallelism;
        
        // 输出详细metrics
  std::filesystem::create_directories("build/metrics");
  std::string metrics_file = "build/metrics/" + perf_config.name + "_" + method + 
                                  "_p" + std::to_string(parallelism) + "_metrics.tsv";
        JoinMetrics::instance().dump_tsv(metrics_file);
      }
    }
  }
}

} // namespace test
} // namespace candy
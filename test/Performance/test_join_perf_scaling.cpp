#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <memory>
#include <vector>
#include <filesystem>
#include <atomic>
#include "operator/join_operator.h"
#include "function/join_function.h"
#include "test_data_generator.h"
#include "operator/join_metrics.h"
#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"
#include "test_data_adapter.h"
#include "execution/collector.h"
#include "utils/logger.h"

namespace candy {
namespace test {

class JoinPerformanceTest : public ::testing::Test {
protected:
  void SetUp() override {
    JoinMetrics::instance().reset();
    auto storage = std::make_shared<StorageManager>();
    concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage);
  }

  void TearDown() override {
    // 输出性能指标到文件
  std::filesystem::create_directories("build/metrics");
  std::string metrics_path = "build/metrics/join_perf_" + 
        std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".tsv";
    JoinMetrics::instance().dump_tsv(metrics_path);
  CANDY_LOG_INFO("TEST", "Performance metrics saved path={} ", metrics_path);
  }

  // 创建JoinFunction（使用 test_data_adapter 助手）
  std::unique_ptr<Function> createSimpleJoinFunction() {
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

protected:
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};

class JoinScalingTest : public ::testing::TestWithParam<
  std::tuple<std::string, int, int>> {
protected:
  void SetUp() override {
    JoinMetrics::instance().reset();
    concurrency_manager_ = std::make_shared<ConcurrencyManager>(std::make_shared<StorageManager>());
  }

  std::unique_ptr<Function> createSimpleJoinFunction() {
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

protected:
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};

TEST_P(JoinScalingTest, PerformanceScaling) {
  auto [method, data_size, parallelism] = GetParam();
  
  TestDataGenerator::Config config;
  config.vector_dim = 128;
  config.similarity_threshold = 0.8;
  config.positive_pairs = data_size / 10;
  config.negative_pairs = data_size * 6 / 10;
  config.random_tail = data_size * 3 / 10;
  config.seed = 42;
  
  TestDataGenerator generator(config);
  auto [records, expected_matches] = generator.generateData();
  
  auto start_time = std::chrono::high_resolution_clock::now();
  
  if (parallelism == 1) {
    // 单线程测试
    auto join_func = createSimpleJoinFunction();
    JoinOperator join_op(join_func, concurrency_manager_, method, 
                        config.similarity_threshold);
    join_op.open();
    
    int match_count = 0;
    std::vector<std::unique_ptr<Response>> emitted;
    Collector collector([&](std::unique_ptr<Response> r, int){ if (r && r->record_) { match_count++; emitted.push_back(std::move(r)); }});
    for (auto& record : records) {
      Response response;
      response.type_ = ResponseType::Record;
      response.record_ = std::move(record);
      join_op.apply(std::move(response), 0, collector);
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
  CANDY_LOG_INFO("TEST", "Method={} Size={} Parallelism=1 time_ms={} matches={} ",
           method, data_size, duration.count(), match_count);
    
  } else {
    // 多线程测试
    std::vector<std::thread> workers;
    std::atomic<int> total_matches{0};
    std::atomic<int> record_index{0};
    
    // 创建多个JoinOperator实例
  std::vector<std::unique_ptr<JoinOperator>> join_ops;
    for (int i = 0; i < parallelism; ++i) {
      auto join_func = createSimpleJoinFunction();
      auto join_op = std::make_unique<JoinOperator>(join_func, concurrency_manager_, 
                                                    method, config.similarity_threshold);
      join_op->open();
      join_ops.push_back(std::move(join_op));
    }
    
    // 启动工作线程
    for (int i = 0; i < parallelism; ++i) {
      workers.emplace_back([&, i]() {
        int local_matches = 0;
        int idx;
        while ((idx = record_index.fetch_add(1)) < records.size()) {
          if (idx >= records.size()) break;
          
          // 复制记录（因为原记录可能被其他线程使用）
          auto record_copy = std::make_unique<VectorRecord>(*records[idx]);
          Response response;
          response.type_ = ResponseType::Record;
          response.record_ = std::move(record_copy);
          
          std::vector<std::unique_ptr<Response>> emitted_local;
          Collector collector_local([&](std::unique_ptr<Response> r, int){ if (r && r->record_) { local_matches++; emitted_local.push_back(std::move(r)); }});
          join_ops[i]->apply(std::move(response), i, collector_local);
        }
        total_matches.fetch_add(local_matches);
      });
    }
    
    // 等待所有线程完成
    for (auto& worker : workers) {
      worker.join();
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
  CANDY_LOG_INFO("TEST", "Method={} Size={} Parallelism={} time_ms={} matches={} ",
           method, data_size, parallelism, duration.count(), total_matches.load());
  }
  
  auto end_time = std::chrono::high_resolution_clock::now();
  auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
  
  // 性能断言：确保没有异常慢的情况
  EXPECT_LT(total_duration.count(), data_size * 10) 
    << "Performance too slow for method " << method;
  
  // 验证锁等待时间不超过总时间的30%
  uint64_t total_time_ns = JoinMetrics::instance().window_insert_ns.load() +
                          JoinMetrics::instance().index_insert_ns.load() +
                          JoinMetrics::instance().similarity_ns.load() +
                          JoinMetrics::instance().join_function_ns.load();
  
  if (total_time_ns > 0) {
    double lock_ratio = static_cast<double>(JoinMetrics::instance().lock_wait_ns.load()) / total_time_ns;
    EXPECT_LE(lock_ratio, 0.3) << "Lock contention too high: " << lock_ratio * 100 << "%";
  }
}

INSTANTIATE_TEST_SUITE_P(
  JoinPerformanceTests,
  JoinScalingTest,
  ::testing::Values(
    // 小规模测试
    std::make_tuple("bruteforce_eager", 1000, 1),
    std::make_tuple("bruteforce_lazy", 1000, 2),
    std::make_tuple("ivf_eager", 1000, 4),
    std::make_tuple("ivf_lazy", 1000, 4),
    
    // 中规模测试
    std::make_tuple("bruteforce_eager", 10000, 2),
    std::make_tuple("bruteforce_lazy", 10000, 4),
    std::make_tuple("ivf_eager", 10000, 4),
    std::make_tuple("ivf_lazy", 10000, 8),
    
    // 大规模测试（仅IVF）
    std::make_tuple("ivf_eager", 100000, 4),
    std::make_tuple("ivf_lazy", 100000, 8)
  )
);

TEST_F(JoinPerformanceTest, MethodSpeedComparison) {
  TestDataGenerator::Config config;
  config.vector_dim = 128;
  config.similarity_threshold = 0.8;
  config.positive_pairs = 1000;
  config.negative_pairs = 3000;
  config.random_tail = 1000;
  config.seed = 42;
  
  TestDataGenerator generator(config);
  auto [records, expected_matches] = generator.generateData();
  
  std::vector<std::string> methods = {"bruteforce_eager", "bruteforce_lazy", "ivf_eager", "ivf_lazy"};
  std::vector<std::pair<std::string, int64_t>> method_times;
  
  for (const auto& method : methods) {
    // 复制记录
    std::vector<std::unique_ptr<VectorRecord>> test_records;
    for (const auto& record : records) {
      test_records.push_back(std::make_unique<VectorRecord>(*record));
    }
    
    JoinMetrics::instance().reset();
    
    auto start = std::chrono::high_resolution_clock::now();
    
    auto join_func = createSimpleJoinFunction();
    JoinOperator join_op(join_func, concurrency_manager_, method, config.similarity_threshold);
    join_op.open();
    
    int match_count = 0;
    Collector collector([&](std::unique_ptr<Response> r, int){ if (r && r->record_) match_count++; });
    for (auto& record : test_records) {
      Response response;
      response.type_ = ResponseType::Record;
      response.record_ = std::move(record);
      join_op.apply(std::move(response), 0, collector);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    method_times.emplace_back(method, duration.count());
  CANDY_LOG_INFO("TEST", "Method={} time_ms={} matches={} ", method, duration.count(), match_count);
  }
  
  // 验证IVF方法确实比BruteForce快（在大规模数据下）
  auto bruteforce_time = std::find_if(method_times.begin(), method_times.end(),
      [](const auto& p) { return p.first == "bruteforce_eager"; });
  auto ivf_time = std::find_if(method_times.begin(), method_times.end(),
      [](const auto& p) { return p.first == "ivf_eager"; });
  
  if (bruteforce_time != method_times.end() && ivf_time != method_times.end()) {
    // 对于5000条记录，IVF应该比BruteForce快
    EXPECT_LT(ivf_time->second * 2, bruteforce_time->second) 
      << "IVF should be faster than BruteForce for large datasets";
  }
}

} // namespace test
} // namespace candy

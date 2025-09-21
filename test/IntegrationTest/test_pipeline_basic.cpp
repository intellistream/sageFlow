#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <memory>
#include <vector>
#include <atomic>
#include <queue>
#include <mutex>
#include "utils/logger.h"
#include "stream/stream_environment.h"
#include "stream/stream.h"
#include "stream/data_stream_source/data_stream_source.h"
#include "stream/data_stream_source/simple_stream_source.h"
#include "function/join_function.h"
#include "function/sink_function.h"
#include "operator/join_operator.h"
#include "execution/collector.h"
#include "operator/join_metrics.h"
#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"
#include "test_utils/test_data_generator.h"
#include "test_utils/test_config_manager.h"
#include "test_utils/test_data_adapter.h"

namespace candy {
namespace test {

// 简单的内存数据源，便于在测试中直接注入 VectorRecord
class TestVectorStreamSource : public DataStreamSource {
 public:
  explicit TestVectorStreamSource(std::string name, std::vector<std::unique_ptr<VectorRecord>> records)
      : DataStreamSource(std::move(name), DataStreamSourceType::None),
        records_(std::move(records)), current_index_(0) {}

  void Init() override { current_index_ = 0; }

  auto Next() -> std::unique_ptr<VectorRecord> override {
    if (current_index_ >= records_.size()) {
      return nullptr;
    }
    return std::move(records_[current_index_++]);
  }

 private:
  std::vector<std::unique_ptr<VectorRecord>> records_;
  size_t current_index_;
};

class MultiThreadPipelineTest : public ::testing::Test {
protected:
  void SetUp() override {
    JoinMetrics::instance().reset();
  auto storage = std::make_shared<StorageManager>();
  concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage);
  env_ = std::make_shared<StreamEnvironment>();
  }

  void TearDown() override {
    // 清理资源
    if (env_) {
      env_->stop();
      env_->awaitTermination();
    }
  }

  // 创建JoinFunction
  std::unique_ptr<Function> createSimpleJoinFunction() {
    auto join_func_lambda = [](std::unique_ptr<VectorRecord>& left, 
                              std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
      auto lv = extractFloatVector(*left);
      auto rv = extractFloatVector(*right);
      std::vector<float> out;
      out.reserve(lv.size()+rv.size());
      out.insert(out.end(), lv.begin(), lv.end());
      out.insert(out.end(), rv.begin(), rv.end());
      uint64_t id = left->uid_ * 1000000 + right->uid_;
      int64_t ts = std::max(left->timestamp_, right->timestamp_);
      return createVectorRecord(id, ts, out);
    };
    
    return std::make_unique<JoinFunction>("SimpleJoin", join_func_lambda, 128);
  }

  // 线程安全的结果收集器
  class ThreadSafeResultCollector {
  public:
    void addResult(std::unique_ptr<VectorRecord> result) {
      std::lock_guard<std::mutex> lock(mutex_);
      results_.push_back(std::move(result));
    }
    
    std::vector<std::unique_ptr<VectorRecord>> getResults() {
      std::lock_guard<std::mutex> lock(mutex_);
      return std::move(results_);
    }
    
    size_t size() const {
      std::lock_guard<std::mutex> lock(mutex_);
      return results_.size();
    }
    
  private:
    mutable std::mutex mutex_;
    std::vector<std::unique_ptr<VectorRecord>> results_;
  };

protected:
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  std::shared_ptr<StreamEnvironment> env_;
  std::shared_ptr<ThreadSafeResultCollector> result_collector_;
};

TEST_F(MultiThreadPipelineTest, BasicPipelineConstruction) {
  // 测试使用 StreamEnvironment 与链式 Stream API 的 Pipeline 构建与启动/关闭流程
  result_collector_ = std::make_shared<ThreadSafeResultCollector>();

  // 创建两个简单的数据源（此处不依赖文件，仅验证构建流程）
  auto left_source = std::make_shared<SimpleStreamSource>("left_source");
  auto right_source = std::make_shared<SimpleStreamSource>("right_source");

  // 创建 JoinFunction 与 SinkFunction（使用最小逻辑以验证链路）
  auto join_func_direct = std::make_unique<JoinFunction>(
      "SimpleJoin",
      [](std::unique_ptr<VectorRecord>& left,
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
      },
      128);
  auto sink_func = std::make_unique<SinkFunction>(
      "BasicSink",
      [&](std::unique_ptr<VectorRecord>& rec) {
        // 收集结果到线程安全容器
        result_collector_->addResult(std::make_unique<VectorRecord>(*rec));
      });

  // 从配置读取 Join 配置
  candy::test::PipelineConfig pipeline_cfg{};
  if (candy::test::TestConfigManager::loadPipelineConfig("config/join_pipeline_basic.toml", pipeline_cfg)) {
    // 应用窗口配置
    join_func_direct->setWindow(pipeline_cfg.window.time_ms, pipeline_cfg.window.trigger_interval_ms);
    // 通过 Stream API 构建链式算子链，并设置并行度（使用配置的 join 方法与阈值）
    auto joined = left_source->join(right_source, std::move(join_func_direct),
                                    pipeline_cfg.join_method, pipeline_cfg.similarity_threshold, 1);
    // 添加到环境并执行（由 Planner 自动构建 ExecutionGraph）
    env_->addStream(left_source);
    env_->addStream(right_source);
    EXPECT_NO_THROW(env_->execute()) << "StreamEnvironment execute should not throw";
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_NO_THROW(env_->stop()) << "StreamEnvironment stop should not throw";
    EXPECT_NO_THROW(env_->awaitTermination()) << "StreamEnvironment awaitTermination should not throw";
  CANDY_LOG_INFO("TEST", "pipeline construction success");
    return;
  }
  // 回退：若配置加载失败，使用默认 join 参数
  auto joined = left_source->join(right_source, std::move(join_func_direct),
                                  std::string("bruteforce_lazy"), 0.8, 1);
  joined->writeSink(std::move(sink_func), 1);

  // 添加到环境并执行（由 Planner 自动构建 ExecutionGraph）
  env_->addStream(left_source);
  env_->addStream(right_source);

  EXPECT_NO_THROW(env_->execute()) << "StreamEnvironment execute should not throw";

  // 等待一段时间确保Pipeline正常运行
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // 停止Pipeline
  EXPECT_NO_THROW(env_->stop()) << "StreamEnvironment stop should not throw";
  EXPECT_NO_THROW(env_->awaitTermination()) << "StreamEnvironment awaitTermination should not throw";

  CANDY_LOG_INFO("TEST", "pipeline construction success");
}

TEST_F(MultiThreadPipelineTest, ParallelJoinConsistency) {
  // 测试不同并行度下Join结果的一致性
  std::vector<int> parallelism_levels = {1, 2, 4};
  
  TestDataGenerator::Config config;
  // 调小数据规模与维度，便于排查
  config.vector_dim = 10;
  // 这三项合计大约产生 ~10 条记录（具体以生成器实现为准）
  config.positive_pairs = 3;
  config.negative_pairs = 4;
  config.random_tail = 3;
  config.seed = 42;
  
  TestDataGenerator generator(config);
  auto [base_records, expected_matches] = generator.generateData();

  // 打印所有记录信息，便于排查
  CANDY_LOG_INFO("TEST", "ParallelJoinConsistency dataset: records={} expected_matches_size={} dim={} ",
                 base_records.size(), expected_matches.size(), config.vector_dim);
  for (size_t i = 0; i < base_records.size(); ++i) {
    const auto& r = base_records[i];
    auto vec = extractFloatVector(*r);
    std::string vals;
    vals.reserve(vec.size() * 8);
    for (size_t d = 0; d < vec.size(); ++d) {
      vals += std::to_string(vec[d]);
      if (d + 1 < vec.size()) vals += ",";
    }
    CANDY_LOG_INFO("TEST", "rec#{} uid={} ts={} values=[{}] ", i, r->uid_, r->timestamp_, vals);
  }
  
  std::vector<std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash>> results_by_parallelism;
  
  for (int parallelism : parallelism_levels) {
    SCOPED_TRACE("Testing parallelism: " + std::to_string(parallelism));

  // 确保每次循环使用全新执行环境，避免上一次执行残留的队列/线程/索引状态
  if (env_) { env_->reset(); } else { env_ = std::make_shared<StreamEnvironment>(); }

    // 为左右两侧分别复制一份数据
    std::vector<std::unique_ptr<VectorRecord>> left_records;
    left_records.reserve(base_records.size());
    for (const auto& rec : base_records) {
      left_records.push_back(std::make_unique<VectorRecord>(*rec));
    }
    std::vector<std::unique_ptr<VectorRecord>> right_records;
    right_records.reserve(base_records.size());
    // 给右侧流的 UID 加偏移，确保左右两侧不共享相同 UID；
    // 偏移量保持在 <1e6 内，保证 left*1e6 + right 的编码/解码逻辑仍然成立。
    constexpr uint64_t kRightUidOffset = 500000;
    for (const auto& rec : base_records) {
      uint64_t new_uid = rec->uid_ + kRightUidOffset;
      // 复制数据与时间戳，但使用新的 UID
      right_records.push_back(std::make_unique<VectorRecord>(new_uid, rec->timestamp_, rec->data_));
    }

    JoinMetrics::instance().reset();

    // 构建基于 StreamEnvironment 的流水线
    auto left_source = std::make_shared<TestVectorStreamSource>("JoinLeft", std::move(left_records));
    auto right_source = std::make_shared<TestVectorStreamSource>("JoinRight", std::move(right_records));

    auto join_func_direct = std::make_unique<JoinFunction>(
        "SimpleJoin",
        [](std::unique_ptr<VectorRecord>& left,
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
        },
  10);

    std::mutex result_mutex;
    std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> result_set;
    auto sink_func = std::make_unique<SinkFunction>(
        "ConsistencySink",
        [&](std::unique_ptr<VectorRecord>& rec) {
          if (!rec) return;
          uint64_t combined_uid = rec->uid_;
          uint64_t left_uid = combined_uid / 1000000;
          uint64_t right_uid = combined_uid % 1000000;
          std::lock_guard<std::mutex> lock(result_mutex);
          result_set.insert({left_uid, right_uid});
        });

    // 从配置读取 Join 配置
    candy::test::PipelineConfig pipeline_cfg{};
    std::string method = "bruteforce_lazy";
    double threshold = 0.8;
    if (candy::test::TestConfigManager::loadPipelineConfig("config/join_pipeline_basic.toml", pipeline_cfg)) {
      method = pipeline_cfg.join_method;
      threshold = pipeline_cfg.similarity_threshold;
      join_func_direct->setWindow(pipeline_cfg.window.time_ms, pipeline_cfg.window.trigger_interval_ms);
    }
    // 链式构建，设置 Join 并行度与方法/阈值
    left_source->join(right_source, std::move(join_func_direct), method, threshold, static_cast<size_t>(parallelism))
               ->writeSink(std::move(sink_func), 1);

    env_->addStream(left_source);
    env_->addStream(right_source);
    env_->execute();

    // 给予时间处理
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    env_->stop();
    env_->awaitTermination();

    results_by_parallelism.push_back(std::move(result_set));
    CANDY_LOG_INFO("TEST", "Parallelism {} matches={} ", parallelism, results_by_parallelism.back().size());
  }
  
  // 验证不同并行度下结果一致性
  for (size_t i = 1; i < results_by_parallelism.size(); ++i) {
    EXPECT_EQ(results_by_parallelism[0].size(), results_by_parallelism[i].size())
      << "Parallelism consistency failed between level 0 and " << i;
    
    for (const auto& match : results_by_parallelism[0]) {
      EXPECT_TRUE(results_by_parallelism[i].count(match))
        << "Missing match in parallelism level " << parallelism_levels[i] 
        << ": (" << match.first << ", " << match.second << ")";
    }
  }
}

TEST_F(MultiThreadPipelineTest, StressTestMultipleRestarts) {
  // 压力测试：多次启动和停止Pipeline
  TestDataGenerator::Config config;
  config.vector_dim = 64;
  config.positive_pairs = 20;
  config.negative_pairs = 40;
  config.seed = 42;
  
  for (int restart = 0; restart < 5; ++restart) {
    SCOPED_TRACE("Restart iteration: " + std::to_string(restart));
  // 使用 StreamEnvironment 构建并执行最小可运行的流水线
  env_ = std::make_shared<StreamEnvironment>();
  JoinMetrics::instance().reset();

  // 两个简单数据源（无数据，仅用于验证启动/停止流程）
  auto left_source = std::make_shared<SimpleStreamSource>("restart_left");
  auto right_source = std::make_shared<SimpleStreamSource>("restart_right");

  // Join 与 Sink（sink收集计数以验证链路畅通）
    auto join_func_direct = std::make_unique<JoinFunction>(
        "SimpleJoin",
        [](std::unique_ptr<VectorRecord>& left,
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
        },
        128);
  std::atomic<int> sink_count{0};
  auto sink_func = std::make_unique<SinkFunction>(
    "RestartSink",
    [&](std::unique_ptr<VectorRecord>& rec) { sink_count.fetch_add(1, std::memory_order_relaxed); });

  // 配置驱动 Join 方法
  candy::test::PipelineConfig pipeline_cfg_rs{};
  std::string method_rs = "bruteforce_lazy";
  double threshold_rs = 0.8;
  if (candy::test::TestConfigManager::loadPipelineConfig("config/join_pipeline_basic.toml", pipeline_cfg_rs)) {
    method_rs = pipeline_cfg_rs.join_method;
    threshold_rs = pipeline_cfg_rs.similarity_threshold;
    join_func_direct->setWindow(pipeline_cfg_rs.window.time_ms, pipeline_cfg_rs.window.trigger_interval_ms);
  }
  auto joined = left_source->join(right_source, std::move(join_func_direct), method_rs, threshold_rs, 1);
  joined->writeSink(std::move(sink_func), 1);

  env_->addStream(left_source);
  env_->addStream(right_source);
  env_->execute();

  // 运行一段时间
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // 停止和清理
  env_->stop();
  env_->awaitTermination();
    
  CANDY_LOG_INFO("TEST", "Restart {} success", restart);
  }
  
  CANDY_LOG_INFO("TEST", "Stress test restarts completed");
}

TEST_F(MultiThreadPipelineTest, HighConcurrencyDeadlockTest) {
  // 高并发死锁测试
  TestDataGenerator::Config config;
  config.vector_dim = 128;
  config.positive_pairs = 500;
  config.negative_pairs = 1000;
  config.random_tail = 500;
  config.seed = 42;
  
  TestDataGenerator generator(config);
  auto [records, expected_matches] = generator.generateData();
  
  const int high_parallelism = 8;
  JoinMetrics::instance().reset();

  // 为左右两侧分别复制一份数据
  std::vector<std::unique_ptr<VectorRecord>> left_records;
  left_records.reserve(records.size());
  for (const auto& rec : records) {
    left_records.push_back(std::make_unique<VectorRecord>(*rec));
  }
  std::vector<std::unique_ptr<VectorRecord>> right_records;
  right_records.reserve(records.size());
  constexpr uint64_t kRightUidOffsetHC = 500000;
  for (const auto& rec : records) {
    uint64_t new_uid = rec->uid_ + kRightUidOffsetHC;
    right_records.push_back(std::make_unique<VectorRecord>(new_uid, rec->timestamp_, rec->data_));
  }

  auto left_source = std::make_shared<TestVectorStreamSource>("HC_Left", std::move(left_records));
  auto right_source = std::make_shared<TestVectorStreamSource>("HC_Right", std::move(right_records));

  auto join_func_direct = std::make_unique<JoinFunction>(
      "SimpleJoin",
      [](std::unique_ptr<VectorRecord>& left,
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
      },
      128);

  std::atomic<int> sink_count{0};
  auto sink_func = std::make_unique<SinkFunction>(
      "HC_Sink",
      [&](std::unique_ptr<VectorRecord>&) { sink_count.fetch_add(1, std::memory_order_relaxed); });

  // 构建并行 Join
  // 配置驱动 Join 方法
  candy::test::PipelineConfig pipeline_cfg_hc{};
  std::string method_hc = "bruteforce_lazy";
  double threshold_hc = 0.8;
  if (candy::test::TestConfigManager::loadPipelineConfig("config/join_pipeline_basic.toml", pipeline_cfg_hc)) {
    method_hc = pipeline_cfg_hc.join_method;
    threshold_hc = pipeline_cfg_hc.similarity_threshold;
    join_func_direct->setWindow(pipeline_cfg_hc.window.time_ms, pipeline_cfg_hc.window.trigger_interval_ms);
  }
  left_source->join(right_source, std::move(join_func_direct), method_hc, threshold_hc, static_cast<size_t>(high_parallelism))
             ->writeSink(std::move(sink_func), 1);

  env_->addStream(left_source);
  env_->addStream(right_source);
  env_->execute();

  std::this_thread::sleep_for(std::chrono::seconds(2));
  env_->stop();
  env_->awaitTermination();

  CANDY_LOG_INFO("TEST", "High concurrency test completed matches={} lock_wait_ms={} ",
                 sink_count.load(), JoinMetrics::instance().lock_wait_ns.load() / 1000000);
  
  // 验证无死锁且有合理处理量（以产生结果为标志）
  EXPECT_GT(sink_count.load(), 0) << "No results produced, possible deadlock";
  
  // 验证锁竞争不过于严重
  uint64_t total_work_time = JoinMetrics::instance().window_insert_ns.load() +
                            JoinMetrics::instance().index_insert_ns.load() +
                            JoinMetrics::instance().similarity_ns.load();
  if (total_work_time > 0) {
    double lock_ratio = static_cast<double>(JoinMetrics::instance().lock_wait_ns.load()) / total_work_time;
    EXPECT_LE(lock_ratio, 0.5) << "Lock contention too high in high concurrency: " << lock_ratio * 100 << "%";
  }
}

class PipelineParameterTest : public ::testing::TestWithParam<
  std::tuple<std::string, int, int, int>> {
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
  out.reserve(lv.size()+rv.size());
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

TEST_P(PipelineParameterTest, VariousConfigurationTest) {
  auto [method, source_parallelism, join_parallelism, data_size] = GetParam();
  
  TestDataGenerator::Config config;
  config.vector_dim = 128;
  config.positive_pairs = data_size / 10;
  config.negative_pairs = data_size * 6 / 10;
  config.random_tail = data_size * 3 / 10;
  config.seed = 42;
  
  TestDataGenerator generator(config);
  auto [records, expected_matches] = generator.generateData();
  
  CANDY_LOG_INFO("TEST", "Testing configuration method={} Source={} Join={} Data={} ",
                 method, source_parallelism, join_parallelism, data_size);
  
  // 使用 StreamEnvironment 构建并行流水线
  // 为左右两侧分别复制一份数据
  std::vector<std::unique_ptr<VectorRecord>> left_records;
  left_records.reserve(records.size());
  for (const auto& rec : records) {
    left_records.push_back(std::make_unique<VectorRecord>(*rec));
  }
  std::vector<std::unique_ptr<VectorRecord>> right_records;
  right_records.reserve(records.size());
  constexpr uint64_t kRightUidOffsetParam = 500000;
  for (const auto& rec : records) {
    uint64_t new_uid = rec->uid_ + kRightUidOffsetParam;
    right_records.push_back(std::make_unique<VectorRecord>(new_uid, rec->timestamp_, rec->data_));
  }

  auto env = std::make_shared<StreamEnvironment>();
  auto left_source = std::make_shared<TestVectorStreamSource>("ParamLeft", std::move(left_records));
  auto right_source = std::make_shared<TestVectorStreamSource>("ParamRight", std::move(right_records));
  left_source->setParallelism(static_cast<size_t>(source_parallelism));
  right_source->setParallelism(static_cast<size_t>(source_parallelism));

  auto join_func_direct = std::make_unique<JoinFunction>(
      "SimpleJoin",
      [](std::unique_ptr<VectorRecord>& left,
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
      },
      128);

  std::atomic<int> total_matches{0};
  auto sink_func = std::make_unique<SinkFunction>(
      "ParamSink",
      [&](std::unique_ptr<VectorRecord>&) { total_matches.fetch_add(1, std::memory_order_relaxed); });

  // 从配置读取默认阈值；方法由参数提供
  candy::test::PipelineConfig pipeline_cfg_param{};
  double threshold_param = 0.8;
  if (candy::test::TestConfigManager::loadPipelineConfig("config/join_pipeline_basic.toml", pipeline_cfg_param)) {
    threshold_param = pipeline_cfg_param.similarity_threshold;
    join_func_direct->setWindow(pipeline_cfg_param.window.time_ms, pipeline_cfg_param.window.trigger_interval_ms);
  }
  left_source->join(right_source, std::move(join_func_direct), method, threshold_param, static_cast<size_t>(join_parallelism))
             ->writeSink(std::move(sink_func), 1);

  env->addStream(left_source);
  env->addStream(right_source);

  auto start_time = std::chrono::high_resolution_clock::now();
  env->execute();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  env->stop();
  env->awaitTermination();
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
  
  CANDY_LOG_INFO("TEST", "Configuration test completed duration_ms={} matches={} ",
                 duration.count(), total_matches.load());
  
  // 验证处理完成且无崩溃
  EXPECT_GT(total_matches.load(), 0) << "Should have some matches";
  EXPECT_LT(duration.count(), data_size * 5) << "Performance should be reasonable";
}

INSTANTIATE_TEST_SUITE_P(
  PipelineConfigurationTests,
  PipelineParameterTest,
  ::testing::Values(
    std::make_tuple("bruteforce_eager", 1, 1, 500),
    std::make_tuple("bruteforce_lazy", 2, 2, 1000),
    std::make_tuple("ivf_eager", 2, 3, 2000),
    std::make_tuple("ivf_lazy", 3, 4, 3000),
    std::make_tuple("bruteforce_eager", 1, 8, 1000),
    std::make_tuple("ivf_eager", 4, 8, 5000)
  )
);

} // namespace test
} // namespace candy
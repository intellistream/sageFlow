//
// Created by Administrator on 25-1-20.
//

#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <chrono>
#include <thread>
#include <atomic>
#include <functional>
#include <mutex>
#include <iostream>

// 项目核心头文件
#include "common/data_types.h"
#include "stream/stream_environment.h"
#include "stream/stream.h"
#include "stream/data_stream_source/data_stream_source.h"
#include "function/filter_function.h"
#include "function/map_function.h"
#include "function/sink_function.h"
#include "function/join_function.h"

namespace candy {
namespace test {

/**
 * @brief 用于单元测试的内存数据源
 * 提供可控的数据输入，避免对文件系统的依赖
 */
class TestVectorStreamSource : public DataStreamSource {
 public:
  explicit TestVectorStreamSource(std::string name, std::vector<std::unique_ptr<VectorRecord>> records)
      : DataStreamSource(std::move(name), DataStreamSourceType::None),
        records_(std::move(records)),
        current_index_(0) {}

  void Init() override {
    current_index_ = 0;
  }

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

/**
 * @brief 用于测试的线程安全结果收集器
 * 使用组合而不是继承，因为SinkFunction是final类
 */
class TestResultCollector {
 public:
  TestResultCollector() = default;

  // 创建一个SinkFunction来收集结果
  auto createSinkFunction() -> std::unique_ptr<SinkFunction> {
    return std::make_unique<SinkFunction>(
        "TestResultCollector",
        [this](std::unique_ptr<VectorRecord>& record) {
          std::lock_guard<std::mutex> lock(mutex_);
          results_.push_back(std::make_unique<VectorRecord>(*record));
        }
    );
  }

  auto getResults() -> std::vector<std::unique_ptr<VectorRecord>> {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::unique_ptr<VectorRecord>> result;
    result.reserve(results_.size());
    for (const auto& record : results_) {
      result.push_back(std::make_unique<VectorRecord>(*record));
    }
    return result;
  }

  size_t getResultCount() {
    std::lock_guard<std::mutex> lock(mutex_);
    return results_.size();
  }

  void waitForResults(size_t expected_count, std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    auto start_time = std::chrono::steady_clock::now();
    while (getResultCount() < expected_count) {
      if (std::chrono::steady_clock::now() - start_time > timeout) {
        throw std::runtime_error("Timeout waiting for test results.");
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }

  void clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    results_.clear();
  }

 private:
  std::mutex mutex_;
  std::vector<std::unique_ptr<VectorRecord>> results_;
};

/**
 * @brief 多线程流水线测试基类
 */
class MultiThreadPipelineTest : public ::testing::Test {
 protected:
  void SetUp() override {
    env_ = std::make_shared<StreamEnvironment>();
    result_collector_ = std::make_shared<TestResultCollector>();
  }

  void TearDown() override {
    if (env_) {
      env_->stop();
      env_->awaitTermination();
    }
  }

  // 创建测试数据
  static auto createTestData(size_t count) -> std::vector<std::unique_ptr<VectorRecord>> {
    std::vector<std::unique_ptr<VectorRecord>> records;
    records.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      // 创建简单的测试向量数据
      const int32_t dim = 4;
      auto data = std::make_unique<char[]>(dim * sizeof(float));
      auto* float_data = reinterpret_cast<float*>(data.get());
      for (int32_t j = 0; j < dim; ++j) {
        float_data[j] = static_cast<float>(i + j * 0.1);
      }

      VectorData vector_data(dim, DataType::Float32, data.release());
      auto record = std::make_unique<VectorRecord>(
          i,
          std::chrono::system_clock::now().time_since_epoch().count(),
          std::move(vector_data)
      );
      records.push_back(std::move(record));
    }
    return records;
  }

  std::shared_ptr<StreamEnvironment> env_;
  std::shared_ptr<TestResultCollector> result_collector_;
};

/**
 * @brief 测试算子链的构建和执行
 */
class PipelineConstructionTest : public MultiThreadPipelineTest {
 protected:
  std::mutex result_mutex_;
  std::vector<std::unique_ptr<VectorRecord>> test_results_;

  // 创建线程安全的结果收集Lambda
  auto createResultCollectorLambda() -> std::function<void(std::unique_ptr<VectorRecord>&)> {
    return [this](std::unique_ptr<VectorRecord>& record) {
      std::lock_guard<std::mutex> lock(result_mutex_);
      test_results_.push_back(std::make_unique<VectorRecord>(*record));
    };
  }
};

// 测试简单的Filter->Map->Sink流水线
TEST_F(PipelineConstructionTest, SimpleFilterMapSinkPipeline) {
  const size_t data_count = 100;
  const size_t parallelism = 2;
  env_->setParallelism(parallelism);

  // 1. 创建测试数据源
  auto test_data = createTestData(data_count);
  auto source = std::make_shared<TestVectorStreamSource>("TestSource", std::move(test_data));

  // 2. 定义算子逻辑
  auto filter_func = std::make_unique<FilterFunction>(
      "FilterEvens",
      [](std::unique_ptr<VectorRecord>& record) -> bool {
        return record->uid_ % 2 == 0;  // 只保留偶数ID的记录
      }
  );

  auto map_func = std::make_unique<MapFunction>(
      "MapMultiply",
      [](std::unique_ptr<VectorRecord>& record) {
        const_cast<uint64_t&>(record->uid_) *= 10;  // 将ID乘以10
      }
  );

  // 3. 构建流处理管道
  source->filter(std::move(filter_func))
        ->map(std::move(map_func))
        ->writeSink(std::make_unique<SinkFunction>(
            "TestSink",
            createResultCollectorLambda()
        ));

  // 4. 添加到环境并执行
  env_->addStream(source);

  // 验证流处理管道已正确构建
  EXPECT_NO_THROW(env_->execute());

  // 等待处理完成
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  env_->stop();
  env_->awaitTermination();

  // 5. 验证结果
  std::lock_guard<std::mutex> lock(result_mutex_);
  const size_t expected_count = data_count / 2;  // 过滤掉一半的数据
  EXPECT_EQ(test_results_.size(), expected_count);

  // 验证每个结果都是正确的
  for (const auto& record : test_results_) {
    EXPECT_EQ(record->uid_ % 10, 0);  // 乘以10后应该是10的倍数
    uint64_t original_uid = record->uid_ / 10;
    EXPECT_EQ(original_uid % 2, 0);  // 原始ID应该是偶数
  }
}

// 测试高并行度的流水线
TEST_F(PipelineConstructionTest, HighParallelismPipeline) {
  const size_t data_count = 200;
  const size_t high_parallelism = 4;
  env_->setParallelism(high_parallelism);

  // 1. 创建测试数据源
  auto test_data = createTestData(data_count);
  auto source = std::make_shared<TestVectorStreamSource>("TestSource", std::move(test_data));

  // 2. 定义复杂的处理逻辑
  auto filter_func = std::make_unique<FilterFunction>(
      "FilterPositive",
      [](std::unique_ptr<VectorRecord>& record) -> bool {
        // 访问向量数据的第一个元素
        auto* float_data = reinterpret_cast<float*>(record->data_.data_.get());
        return float_data[0] > 0.5f;
      }
  );

  auto map_func = std::make_unique<MapFunction>(
      "MapSquare",
      [](std::unique_ptr<VectorRecord>& record) {
        // 对向量的每个元素求平方
        auto* float_data = reinterpret_cast<float*>(record->data_.data_.get());
        for (int32_t i = 0; i < record->data_.dim_; ++i) {
          float_data[i] = float_data[i] * float_data[i];
        }
      }
  );

  // 3. 构建流处理管道
  source->filter(std::move(filter_func))
        ->map(std::move(map_func))
        ->writeSink(std::make_unique<SinkFunction>(
            "TestSink",
            createResultCollectorLambda()
        ));

  // 4. 验证管道构建
  EXPECT_NO_THROW(env_->addStream(source));
  EXPECT_NO_THROW(env_->execute());

  // 等待处理完成
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  env_->stop();
  env_->awaitTermination();

  // 5. 验证并行度效果（至少有一些数据被处理）
  std::lock_guard<std::mutex> lock(result_mutex_);
  EXPECT_GT(test_results_.size(), 0);

  // 验证数据变换是否正确
  for (const auto& record : test_results_) {
    auto* float_data = reinterpret_cast<float*>(record->data_.data_.get());
    // 验证所有元素都是正数（经过平方变换）
    for (int32_t i = 0; i < record->data_.dim_; ++i) {
      EXPECT_GE(float_data[i], 0.0f);
    }
  }
}

// 测试Join算子的流水线构建
TEST_F(PipelineConstructionTest, JoinPipelineConstruction) {
  const size_t data_count = 50;
  const size_t parallelism = 2;
  env_->setParallelism(parallelism);

  // 1. 创建两个数据源
  auto test_data1 = createTestData(data_count);
  auto test_data2 = createTestData(data_count);

  auto source1 = std::make_shared<TestVectorStreamSource>("TestSource1", std::move(test_data1));
  auto source2 = std::make_shared<TestVectorStreamSource>("TestSource2", std::move(test_data2));

  // 2. 定义Join逻辑
  const int dim = 4;
  auto join_func = std::make_unique<JoinFunction>(
      "TestJoin",
      [](std::unique_ptr<VectorRecord>& left, std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
        // 简单的Join：返回左侧记录
        return std::make_unique<VectorRecord>(left->uid_, left->timestamp_, left->data_);
      },
      dim
  );

  // 3. 构建Join流水线
  EXPECT_NO_THROW(
    source1->join(source2, std::move(join_func))
           ->writeSink(std::make_unique<SinkFunction>(
               "JoinSink",
               createResultCollectorLambda()
           ))
  );

  // 4. 验证Join管道可以添加到环境
  EXPECT_NO_THROW(env_->addStream(source1));

  // 验证构建过程没有异常
  EXPECT_NO_THROW(env_->execute());

  // 清理
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  env_->stop();
  env_->awaitTermination();
}

// 测试复杂的多阶段流水线
TEST_F(PipelineConstructionTest, ComplexMultiStagePipeline) {
  const size_t data_count = 80;
  const size_t parallelism = 3;
  env_->setParallelism(parallelism);

  // 1. 创建测试数据源
  auto test_data = createTestData(data_count);
  auto source = std::make_shared<TestVectorStreamSource>("ComplexSource", std::move(test_data));

  // 2. 构建多阶段流水线：Filter -> Map -> Filter -> Map -> Sink
  source->filter(std::make_unique<FilterFunction>(
            "FirstFilter",
            [](std::unique_ptr<VectorRecord>& record) -> bool {
              return record->uid_ % 3 == 0;  // 只保留3的倍数
            }
        ))
        ->map(std::make_unique<MapFunction>(
            "FirstMap",
            [](std::unique_ptr<VectorRecord>& record) {
              const_cast<uint64_t&>(record->uid_) += 1000;  // 加上1000
            }
        ))
        ->filter(std::make_unique<FilterFunction>(
            "SecondFilter",
            [](std::unique_ptr<VectorRecord>& record) -> bool {
              return record->uid_ < 1050;  // 只保留小于1050的
            }
        ))
        ->map(std::make_unique<MapFunction>(
            "SecondMap",
            [](std::unique_ptr<VectorRecord>& record) {
              const_cast<uint64_t&>(record->uid_) *= 2;  // 乘以2
            }
        ))
        ->writeSink(std::make_unique<SinkFunction>(
            "ComplexSink",
            createResultCollectorLambda()
        ));

  // 3. 验证复杂管道构建
  EXPECT_NO_THROW(env_->addStream(source));
  EXPECT_NO_THROW(env_->execute());

  // 等待处理完成
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  env_->stop();
  env_->awaitTermination();

  // 4. 验证复杂变换的结果
  std::lock_guard<std::mutex> lock(result_mutex_);
  EXPECT_GT(test_results_.size(), 0);

  // 验证每个结果都经过了正确的变换
  for (const auto& record : test_results_) {
    // 反推原始值：除以2，减去1000，应该是3的倍数且小于50
    uint64_t processed_uid = record->uid_;
    uint64_t before_second_map = processed_uid / 2;
    uint64_t original_uid = before_second_map - 1000;

    EXPECT_EQ(original_uid % 3, 0);  // 原始值应该是3的倍数
    EXPECT_LT(before_second_map, 1050);  // 第二次过滤前应该小于1050
  }
}

// 测试使用TestResultCollector的简化流水线
TEST_F(PipelineConstructionTest, SimplePipelineWithCollector) {
  const size_t data_count = 50;
  const size_t parallelism = 2;
  env_->setParallelism(parallelism);

  // 1. 创建测试数据源
  auto test_data = createTestData(data_count);
  auto source = std::make_shared<TestVectorStreamSource>("TestSource", std::move(test_data));

  // 2. 使用result_collector_创建Sink
  source->writeSink(result_collector_->createSinkFunction());

  // 3. 执行测试
  env_->addStream(source);
  EXPECT_NO_THROW(env_->execute());

  // 等待处理完成
  result_collector_->waitForResults(data_count, std::chrono::seconds(5));
  env_->stop();
  env_->awaitTermination();

  // 4. 验证结果
  auto results = result_collector_->getResults();
  EXPECT_EQ(results.size(), data_count);

  // 验证数据完整性
  for (size_t i = 0; i < results.size(); ++i) {
    EXPECT_EQ(results[i]->uid_, i);
  }
}

// 测试算子链构建的基本功能
TEST_F(PipelineConstructionTest, BasicPipelineConstruction) {
  const size_t data_count = 10;
  env_->setParallelism(1);

  // 1. 创建最简单的测试数据源
  auto test_data = createTestData(data_count);
  auto source = std::make_shared<TestVectorStreamSource>("BasicSource", std::move(test_data));

  // 2. 只测试基本的管道构建，不执行复杂逻辑
  EXPECT_NO_THROW(
    source->writeSink(std::make_unique<SinkFunction>(
      "BasicSink",
      [](std::unique_ptr<VectorRecord>& record) {
        // 简单的打印输出
        std::cout << "Processing record with uid: " << record->uid_ << std::endl;
      }
    ))
  );

  // 3. 验证可以添加到环境
  EXPECT_NO_THROW(env_->addStream(source));

  // 4. 验证可以构建执行图（但不一定要完全执行）
  EXPECT_NO_THROW(env_->execute());

  // 短暂等待
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  env_->stop();
  env_->awaitTermination();
}

} // namespace test
} // namespace candy

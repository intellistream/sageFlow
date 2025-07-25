#include "../UnitTest/execution/test_common.h"

namespace candy {
namespace test {

/**
 * @brief 测试多线程流水线的端到端执行。
 * 这个测试套件模拟了真实的用户场景，从数据源开始，
 * 经过一系列的算子转换，最终将结果汇集到 Sink。
 * 主要验证：
 * 1. StreamEnvironment 是否能正确构建和执行多线程计算图。
 * 2. 不同并行度设置下的数据处理是否正确。
 * 3. 整个流程的数据完整性，确保没有数据丢失或重复。
 */
class MultiThreadPipelineExecutionTest : public MultiThreadPipelineTest {
};

TEST_F(MultiThreadPipelineExecutionTest, SimplePipeline) {
  const size_t data_count = 100;
  const size_t filter_parallelism = 2;
  env_->setParallelism(filter_parallelism);

  // 1. 创建数据源
  auto test_data = createTestData(data_count);
  auto source = std::make_shared<TestVectorStreamSource>("TestSource", std::move(test_data));

  // 2. 定义处理逻辑
  auto filter_func = std::make_unique<FilterFunction>(
      "FilterEvens", [](const std::unique_ptr<VectorRecord>& record) {
        return record->uid_ % 2 == 0;
      });

  auto map_func = std::make_unique<MapFunction>(
      "MapMultiply", [](std::unique_ptr<VectorRecord>& record) {
        record->uid_ *= 10;
      });

  // 3. 构建流处理管道
  source->filter(std::move(filter_func))
      ->map(std::move(map_func))
      ->writeSink(result_collector_);

  // 4. 执行
  env_->addStream(source);
  env_->execute();

  // 5. 验证结果
  const size_t expected_count = data_count / 2;
  result_collector_->waitForResults(expected_count);

  auto results = result_collector_->getResults();
  ASSERT_EQ(results.size(), expected_count);

  for (const auto& record : results) {
    EXPECT_EQ(record->uid_ % 10, 0);
    uint64_t original_uid = record->uid_ / 10;
    EXPECT_EQ(original_uid % 2, 0);
  }
}

TEST_F(MultiThreadPipelineExecutionTest, HighParallelism) {
  const size_t data_count = 1000;
  const size_t map_parallelism = 8;
  env_->setParallelism(map_parallelism);

  // 1. 创建数据源
  auto test_data = createTestData(data_count);
  auto source = std::make_shared<TestVectorStreamSource>("TestSource", std::move(test_data));

  // 2. 定义处理逻辑
  auto map_func = std::make_unique<MapFunction>(
      "MapComplex", [](std::unique_ptr<VectorRecord>& record) {
        // 模拟一些计算
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        record->uid_ += 1;
      });

  // 3. 构建流处理管道
  source->map(std::move(map_func))
      ->writeSink(result_collector_);

  // 4. 执行
  env_->addStream(source);
  env_->execute();

  // 5. 验证结果
  result_collector_->waitForResults(data_count);
  ASSERT_EQ(result_collector_->getResultCount(), data_count);
}

} // namespace test
} // namespace candy

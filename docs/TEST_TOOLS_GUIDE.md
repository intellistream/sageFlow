# SageFlow 测试工具使用指南

本文档详细介绍 SageFlow 项目中用于 Join 操作测试的各种工具类，包括配置加载、数据生成、流水线构建、指标收集和报告生成等功能。

## 目录

- [概述](#概述)
- [工具组件总览](#工具组件总览)
- [配置加载工具](#配置加载工具)
  - [JoinConfigLoader](#joinconfigloader)
  - [IntegrationTestConfigLoader](#integrationtestconfigloader)
- [测试流水线构建](#测试流水线构建)
  - [JoinIntegrationPipelineHelper](#joinintegrationpipelinehelper)
  - [JoinTestHelper](#jointesthelper)
- [数据生成工具](#数据生成工具)
  - [TestDataGenerator](#testdatagenerator)
  - [DataSource 系统](#datasource-系统)
- [指标收集与报告](#指标收集与报告)
  - [JoinMetricsCollector](#joinmetricscollector)
  - [TestReportGenerator](#testreportgenerator)
- [配置文件格式](#配置文件格式)
- [完整示例](#完整示例)
- [常见问题](#常见问题)

---

## 概述

SageFlow 的测试工具体系分为以下几个层次：

```
┌────────────────────────────────────────────────────────────────┐
│                        测试用例 (Test Cases)                    │
├────────────────────────────────────────────────────────────────┤
│  IntegrationTestConfigLoader  │  JoinConfigLoader              │
│       (加载测试配置)           │    (加载策略配置)               │
├────────────────────────────────────────────────────────────────┤
│              JoinIntegrationPipelineHelper                      │
│                  (构建测试流水线)                                │
├─────────────────────────┬──────────────────────────────────────┤
│   TestDataGenerator     │     DataSource System                 │
│   (生成测试数据)         │     (数据源抽象)                      │
├─────────────────────────┴──────────────────────────────────────┤
│              JoinMetricsCollector  (指标收集)                   │
├────────────────────────────────────────────────────────────────┤
│              TestReportGenerator   (报告生成)                   │
└────────────────────────────────────────────────────────────────┘
```

---

## 工具组件总览

| 组件 | 位置 | 功能 |
|------|------|------|
| `JoinConfigLoader` | `test/test_utils/join_config_loader.h` | 从 TOML 加载 JoinStrategyConfig |
| `IntegrationTestConfigLoader` | `test/test_utils/integration_test_config.h` | 加载完整的集成测试用例配置 |
| `JoinIntegrationPipelineHelper` | `test/test_utils/join_integration_pipeline_helper.h` | 构建和运行 Join 测试流水线 |
| `JoinTestHelper` | `test/test_utils/join_test_helper.h` | 创建 Join 测试所需的输入流 |
| `TestDataGenerator` | `test/test_utils/test_data_generator.h` | 生成具有可控相似度的测试向量 |
| `JoinMetricsCollector` | `include/metrics/join_metrics_collector.h` | 收集 Join 执行过程中的各项指标 |
| `TestReportGenerator` | `test/test_utils/test_report_generator.h` | 生成 JSON/Markdown 测试报告 |

---

## 配置加载工具

### JoinConfigLoader

**用途**：从 TOML 配置文件加载 `JoinStrategyConfig`。

**头文件**：`test/test_utils/join_config_loader.h`

**主要方法**：

```cpp
namespace sageFlow::test {

class JoinConfigLoader {
public:
    // 从文件加载单个配置（根级别配置）
    static JoinStrategyConfig loadFromFile(const std::string& config_path);
    
    // 按策略名称加载（如 "bruteforce_baseline"）
    static JoinStrategyConfig loadByName(const std::string& config_path,
                                         const std::string& strategy_name);
    
    // 加载文件中所有策略配置
    static std::vector<JoinStrategyConfig> loadAllFromFile(const std::string& config_path);
    
    // 按算法类型过滤加载
    static std::vector<JoinStrategyConfig> loadByAlgorithm(
        const std::string& config_path,
        JoinAlgorithm algorithm);
    
    // 合并两个配置（override 覆盖 base）
    static JoinStrategyConfig merge(const JoinStrategyConfig& base,
                                    const JoinStrategyConfig& override_config);
    
    // 列出所有策略名称
    static std::vector<std::string> listStrategyNames(const std::string& config_path);
    
    // 获取默认配置文件路径
    static std::string getDefaultConfigPath();
};

} // namespace sageFlow::test
```

**使用示例**：

```cpp
#include "test/test_utils/join_config_loader.h"

// 1. 加载单个指定策略
auto config = JoinConfigLoader::loadByName(
    "config/join_strategies.toml",
    "ivf_standard"
);

// 2. 加载所有策略（用于参数化测试）
auto all_configs = JoinConfigLoader::loadAllFromFile("config/join_strategies.toml");
for (const auto& cfg : all_configs) {
    std::cout << "Strategy: " << cfg.name << std::endl;
}

// 3. 按算法类型筛选
auto ivf_configs = JoinConfigLoader::loadByAlgorithm(
    "config/join_strategies.toml",
    JoinAlgorithm::IVF
);

// 4. 配置合并
auto base = JoinConfigLoader::loadByName(path, "bruteforce_baseline");
JoinStrategyConfig override_cfg;
override_cfg.parallelism = 8;
auto merged = JoinConfigLoader::merge(base, override_cfg);
```

---

### IntegrationTestConfigLoader

**用途**：加载完整的集成测试用例，包括策略配置、数据生成参数和验证配置。

**头文件**：`test/test_utils/integration_test_config.h`

**核心结构**：

```cpp
struct IntegrationTestCase {
    // 基本信息
    std::string name;
    std::string description;
    bool enabled = true;
    
    // Join 策略配置
    JoinStrategyConfig strategy;
    
    // 数据配置
    int vector_dim = 128;
    std::vector<int> data_sizes;
    std::vector<int> parallelism;
    
    // 数据生成配置
    int positive_pairs = 500;
    int near_threshold_pairs = 50;
    int negative_pairs = 500;
    int random_tail = 2000;
    uint32_t seed = 42;
    
    // 验证配置
    double expected_min_recall = 0.0;
    double expected_min_precision = 0.0;
    bool compare_with_ground_truth = true;
    
    // 辅助方法
    std::string summary() const;
    std::vector<std::string> validate() const;
};
```

**主要方法**：

```cpp
class IntegrationTestConfigLoader {
public:
    // 加载所有测试用例
    static std::vector<IntegrationTestCase> loadFromFile(const std::string& config_path);
    
    // 按名称加载单个用例
    static std::optional<IntegrationTestCase> loadByName(
        const std::string& config_path,
        const std::string& name);
    
    // 按算法类型筛选
    static std::vector<IntegrationTestCase> loadByAlgorithm(
        const std::string& config_path,
        JoinAlgorithm algorithm);
    
    // 加载已启用的测试
    static std::vector<IntegrationTestCase> loadEnabledTests(
        const std::string& config_path);
    
    // 按数据规模过滤
    static std::vector<IntegrationTestCase> filterByDataSize(
        const std::vector<IntegrationTestCase>& cases,
        int min_size, int max_size);
    
    // 获取默认配置文件路径
    static std::string getDefaultConfigPath();
};
```

**使用示例**：

```cpp
#include "test/test_utils/integration_test_config.h"

// 1. 加载所有启用的测试
auto test_cases = IntegrationTestConfigLoader::loadEnabledTests(
    "config/integration_test_cases.toml"
);

// 2. 按算法筛选
auto bruteforce_cases = IntegrationTestConfigLoader::loadByAlgorithm(
    "config/integration_test_cases.toml",
    JoinAlgorithm::BruteForce
);

// 3. 遍历测试用例
for (const auto& tc : test_cases) {
    // 验证配置
    auto errors = tc.validate();
    if (!errors.empty()) {
        std::cerr << "Config error in " << tc.name << std::endl;
        continue;
    }
    
    // 打印摘要
    std::cout << tc.summary() << std::endl;
    
    // 执行测试...
}
```

---

## 测试流水线构建

### JoinIntegrationPipelineHelper

**用途**：根据配置构建完整的 Join 测试流水线，支持执行和结果验证。

**头文件**：`test/test_utils/join_integration_pipeline_helper.h`

**主要功能**：

```cpp
class JoinIntegrationPipelineHelper {
public:
    // 构造函数 - 从 IntegrationTestCase 初始化
    explicit JoinIntegrationPipelineHelper(const IntegrationTestCase& test_case);
    
    // 设置数据源
    void setLeftSource(std::shared_ptr<DataStreamSource> source);
    void setRightSource(std::shared_ptr<DataStreamSource> source);
    
    // 构建流水线
    bool build();
    
    // 执行流水线
    PipelineExecutionResult execute();
    
    // 获取结果
    std::vector<std::unique_ptr<VectorRecord>> getResults() const;
    
    // 验证结果
    ValidationResult validateResults(
        const std::vector<ExpectedMatch>& ground_truth) const;
    
    // 静态工具方法
    static double computeRecall(
        const std::vector<std::unique_ptr<VectorRecord>>& actual,
        const std::vector<ExpectedMatch>& expected);
    
    static double computePrecision(
        const std::vector<std::unique_ptr<VectorRecord>>& actual,
        const std::vector<ExpectedMatch>& expected);
};
```

**使用示例**：

```cpp
#include "test/test_utils/join_integration_pipeline_helper.h"

// 1. 从测试用例创建 helper
auto test_case = IntegrationTestConfigLoader::loadByName(path, "bruteforce_baseline").value();
JoinIntegrationPipelineHelper helper(test_case);

// 2. 生成数据并设置数据源
TestDataGenerator::Config gen_config;
gen_config.vector_dim = test_case.vector_dim;
gen_config.positive_pairs = test_case.positive_pairs;
gen_config.seed = test_case.seed;

TestDataGenerator generator(gen_config);
auto [left_stream, right_stream, ground_truth] = generator.generateJoinData();

helper.setLeftSource(std::make_shared<TestVectorStreamSource>("left", std::move(left_stream)));
helper.setRightSource(std::make_shared<TestVectorStreamSource>("right", std::move(right_stream)));

// 3. 构建并执行
ASSERT_TRUE(helper.build());
auto exec_result = helper.execute();

// 4. 验证结果
auto results = helper.getResults();
double recall = JoinIntegrationPipelineHelper::computeRecall(results, ground_truth);
double precision = JoinIntegrationPipelineHelper::computePrecision(results, ground_truth);

EXPECT_GE(recall, test_case.expected_min_recall);
EXPECT_GE(precision, test_case.expected_min_precision);
```

---

### JoinTestHelper

**用途**：简化创建 Join 测试的左右输入流。

**头文件**：`test/test_utils/join_test_helper.h`

**主要方法**：

```cpp
class JoinTestHelper {
public:
    // 从 TestDataGenerator 创建流
    static std::pair<
        std::vector<std::unique_ptr<VectorRecord>>,
        std::vector<std::unique_ptr<VectorRecord>>>
    generateJoinStreamsFromGenerator(TestDataGenerator& generator);
    
    // 从 DataSource 创建流
    static std::pair<
        std::shared_ptr<DataStreamSource>,
        std::shared_ptr<DataStreamSource>>
    generateJoinStreamsFromSource(
        std::shared_ptr<DataSource> source,
        int left_count,
        int right_count);
    
    // 从不同的数据源创建左右流
    static std::pair<
        std::shared_ptr<DataStreamSource>,
        std::shared_ptr<DataStreamSource>>
    generateJoinStreamsFromSeparateSources(
        std::shared_ptr<DataSource> left_source,
        std::shared_ptr<DataSource> right_source);
};
```

---

## 数据生成工具

### TestDataGenerator

**用途**：生成具有可控相似度分布的测试向量数据。

**头文件**：`test/test_utils/test_data_generator.h`

**配置结构**：

```cpp
class TestDataGenerator {
public:
    struct Config {
        int vector_dim = 128;
        int positive_pairs = 100;      // 相似度 > threshold 的向量对
        int near_threshold_pairs = 20; // 相似度接近 threshold 的向量对
        int negative_pairs = 100;      // 相似度 < threshold - margin 的向量对
        int random_tail = 500;         // 随机填充向量
        double similarity_threshold = 0.8;
        double alpha = 0.1;            // 正样本相似度控制参数
        uint32_t seed = 42;
        int64_t base_timestamp = 1000000;
        int64_t time_interval_ms = 10;
    };
    
    explicit TestDataGenerator(const Config& config);
    
    // 生成数据
    std::tuple<
        std::vector<std::unique_ptr<VectorRecord>>,  // left stream
        std::vector<std::unique_ptr<VectorRecord>>,  // right stream
        std::vector<ExpectedMatch>>                   // ground truth
    generateJoinData();
    
    // 获取预期匹配数
    [[nodiscard]] int getExpectedMatchCount() const;
};
```

**使用示例**：

```cpp
#include "test/test_utils/test_data_generator.h"

TestDataGenerator::Config config;
config.vector_dim = 128;
config.positive_pairs = 500;
config.negative_pairs = 500;
config.similarity_threshold = 0.75;
config.seed = 12345;

TestDataGenerator generator(config);
auto [left_data, right_data, ground_truth] = generator.generateJoinData();

std::cout << "Generated " << left_data.size() << " left records" << std::endl;
std::cout << "Generated " << right_data.size() << " right records" << std::endl;
std::cout << "Expected matches: " << ground_truth.size() << std::endl;
```

---

### DataSource 系统

**位置**：`test/test_utils/data_source/`

**类型**：

| DataSource | 用途 |
|------------|------|
| `RandomDataSource` | 实时生成随机向量 |
| `DatasetDataSource` | 从 .fvecs/.bvecs 文件读取 |
| `JsonDataSource` | 从 JSON 文件读取 |

**使用工厂创建**：

```cpp
#include "test/test_utils/data_source/data_source_factory.h"

// 创建随机数据源
DataSourceConfig config;
config.type = DataSourceType::Random;
config.vector_dim = 128;
config.count = 10000;

auto source = DataSourceFactory::createFromConfig(config);

// 读取数据
while (auto record = source->next()) {
    // 处理 record
}
```

---

## 指标收集与报告

### JoinMetricsCollector

**用途**：收集 Join 执行过程中的各项指标，用于性能分析。

**头文件**：`include/metrics/join_metrics_collector.h`

**核心结构**：

```cpp
struct JoinExecutionStats {
    // 时间指标
    std::chrono::nanoseconds total_time{0};
    std::chrono::nanoseconds index_build_time{0};
    std::chrono::nanoseconds query_time{0};
    std::chrono::nanoseconds window_eviction_time{0};
    
    // 数据规模
    int64_t left_records_processed = 0;
    int64_t right_records_processed = 0;
    int64_t total_records_in_window = 0;
    
    // 匹配统计
    int64_t total_comparisons = 0;
    int64_t candidate_pairs = 0;
    int64_t output_matches = 0;
    
    // 准确性指标
    int64_t true_positives = 0;
    int64_t false_positives = 0;
    int64_t false_negatives = 0;
    
    // 计算方法
    [[nodiscard]] double recall() const;
    [[nodiscard]] double precision() const;
    [[nodiscard]] double f1Score() const;
    [[nodiscard]] double throughputRecordsPerSec() const;
};
```

**JoinMetricsCollector 使用**：

```cpp
class JoinMetricsCollector {
public:
    explicit JoinMetricsCollector(const std::string& test_name);
    
    // 记录时间
    void recordIndexBuildTime(std::chrono::nanoseconds duration);
    void recordQueryTime(std::chrono::nanoseconds duration);
    void recordTotalTime(std::chrono::nanoseconds duration);
    
    // 记录数据量
    void recordLeftProcessed(int64_t count);
    void recordRightProcessed(int64_t count);
    void recordOutputMatches(int64_t count);
    
    // 设置准确性指标
    void setTruePositives(int64_t tp);
    void setFalsePositives(int64_t fp);
    void setFalseNegatives(int64_t fn);
    
    // 获取统计快照
    [[nodiscard]] JoinExecutionStats getStats() const;
    
    // 打印摘要
    void printSummary(std::ostream& os) const;
};
```

**使用示例**：

```cpp
#include "metrics/join_metrics_collector.h"

JoinMetricsCollector collector("ivf_join_test");

auto start = std::chrono::high_resolution_clock::now();

// 执行 Join 操作...
auto index_build_end = std::chrono::high_resolution_clock::now();
collector.recordIndexBuildTime(index_build_end - start);

// 更多操作...
auto query_end = std::chrono::high_resolution_clock::now();
collector.recordQueryTime(query_end - index_build_end);

collector.recordLeftProcessed(10000);
collector.recordRightProcessed(10000);
collector.recordOutputMatches(results.size());

// 设置准确性
collector.setTruePositives(correct_matches);
collector.setFalsePositives(incorrect_matches);
collector.setFalseNegatives(missed_matches);

// 获取统计
auto stats = collector.getStats();
std::cout << "Recall: " << stats.recall() << std::endl;
std::cout << "Precision: " << stats.precision() << std::endl;
std::cout << "Throughput: " << stats.throughputRecordsPerSec() << " records/sec" << std::endl;
```

---

### TestReportGenerator

**用途**：生成 JSON 和 Markdown 格式的测试报告。

**头文件**：`test/test_utils/test_report_generator.h`

**核心结构**：

```cpp
struct TestResult {
    std::string name;
    std::string algorithm;
    bool passed = false;
    bool skipped = false;
    std::string skip_reason;
    
    // 指标
    double recall = 0.0;
    double precision = 0.0;
    double throughput = 0.0;
    int64_t duration_ms = 0;
    
    // 配置摘要
    int data_size = 0;
    int parallelism = 0;
    int vector_dim = 0;
    double threshold = 0.0;
};

struct TestReport {
    std::string title;
    std::string timestamp;
    int total_tests = 0;
    int passed_tests = 0;
    int failed_tests = 0;
    int skipped_tests = 0;
    
    std::vector<TestResult> results;
    std::map<std::string, AlgorithmSummary> algorithm_summaries;
};
```

**主要方法**：

```cpp
class TestReportGenerator {
public:
    explicit TestReportGenerator(const std::string& report_title);
    
    // 添加测试结果
    void addResult(const TestResult& result);
    void addResult(TestResult&& result);
    
    // 添加跳过的测试
    void addSkipped(const std::string& name, const std::string& reason);
    
    // 生成报告
    [[nodiscard]] TestReport generateReport() const;
    
    // 输出方法
    void writeJsonReport(const std::string& file_path) const;
    void writeMarkdownReport(const std::string& file_path) const;
    void writeToStream(std::ostream& os, const std::string& format = "markdown") const;
    
    // 打印控制台摘要
    void printConsoleSummary(std::ostream& os = std::cout) const;
};
```

**使用示例**：

```cpp
#include "test/test_utils/test_report_generator.h"

TestReportGenerator reporter("Join Algorithm Integration Tests");

// 添加测试结果
for (const auto& test_case : test_cases) {
    TestResult result;
    result.name = test_case.name;
    result.algorithm = getAlgorithmName(test_case.strategy.join_algorithm);
    
    // 执行测试并收集指标...
    result.passed = (recall >= expected_recall);
    result.recall = recall;
    result.precision = precision;
    result.throughput = throughput;
    result.duration_ms = duration;
    result.data_size = test_case.data_sizes[0];
    result.parallelism = test_case.parallelism[0];
    
    reporter.addResult(std::move(result));
}

// 生成报告
reporter.writeJsonReport("test/result/integration_report.json");
reporter.writeMarkdownReport("test/result/integration_report.md");

// 打印控制台摘要
reporter.printConsoleSummary();
```

---

## 配置文件格式

### join_strategies.toml

位置：`config/join_strategies.toml`

```toml
# 通用默认配置
[defaults]
similarity_threshold = 0.75
window_time_ms = 60000
vector_dim = 128

# 策略定义
[strategies.bruteforce_baseline]
name = "bruteforce_baseline"
join_algorithm = "BruteForce"
partition_strategy = "RoundRobin"
window_state_type = "Shared"
similarity_threshold = 0.75
window_time_ms = 60000

[strategies.ivf_standard]
name = "ivf_standard"
join_algorithm = "IVF"
partition_strategy = "VectorHash"
window_state_type = "Partitioned"
similarity_threshold = 0.75
window_time_ms = 60000

[strategies.ivf_standard.ivf_config]
nlist = 100
nprobes = 10
rebuild_threshold = 0.2
```

### integration_test_cases.toml

位置：`config/integration_test_cases.toml`

```toml
[common]
vector_dim = 128
data_sizes = [1000, 5000, 10000]
parallelism = [1, 2, 4]
time_interval_ms = 10
base_timestamp = 1000000

[test_cases.bruteforce_small]
name = "bruteforce_small"
description = "BruteForce join with small dataset"
enabled = true

# 策略配置
[test_cases.bruteforce_small.strategy]
join_algorithm = "BruteForce"
partition_strategy = "RoundRobin"
window_state_type = "Shared"
similarity_threshold = 0.75
window_time_ms = 60000

# 数据生成配置
[test_cases.bruteforce_small.data_generation]
positive_pairs = 500
near_threshold_pairs = 50
negative_pairs = 500
random_tail = 2000
seed = 42

# 验证配置
[test_cases.bruteforce_small.validation]
expected_min_recall = 0.85
expected_min_precision = 0.80
compare_with_ground_truth = true
```

---

## 完整示例

### 示例 1：简单的集成测试

```cpp
#include <gtest/gtest.h>
#include "test/test_utils/integration_test_config.h"
#include "test/test_utils/join_integration_pipeline_helper.h"
#include "test/test_utils/test_data_generator.h"
#include "test/test_utils/test_report_generator.h"

class JoinIntegrationTest : public ::testing::Test {
protected:
    TestReportGenerator reporter_{"Join Integration Test"};
};

TEST_F(JoinIntegrationTest, BruteForceBaseline) {
    // 1. 加载配置
    auto config_path = IntegrationTestConfigLoader::getDefaultConfigPath();
    auto test_case = IntegrationTestConfigLoader::loadByName(config_path, "bruteforce_small");
    ASSERT_TRUE(test_case.has_value());
    
    // 2. 生成数据
    TestDataGenerator::Config gen_config;
    gen_config.vector_dim = test_case->vector_dim;
    gen_config.positive_pairs = test_case->positive_pairs;
    gen_config.seed = test_case->seed;
    
    TestDataGenerator generator(gen_config);
    auto [left, right, ground_truth] = generator.generateJoinData();
    
    // 3. 构建和执行流水线
    JoinIntegrationPipelineHelper helper(*test_case);
    helper.setLeftSource(makeSource("left", std::move(left)));
    helper.setRightSource(makeSource("right", std::move(right)));
    
    ASSERT_TRUE(helper.build());
    auto exec_result = helper.execute();
    
    // 4. 验证结果
    auto results = helper.getResults();
    double recall = JoinIntegrationPipelineHelper::computeRecall(results, ground_truth);
    double precision = JoinIntegrationPipelineHelper::computePrecision(results, ground_truth);
    
    EXPECT_GE(recall, test_case->expected_min_recall);
    EXPECT_GE(precision, test_case->expected_min_precision);
    
    // 5. 记录结果
    TestResult result;
    result.name = test_case->name;
    result.algorithm = "BruteForce";
    result.passed = (recall >= test_case->expected_min_recall);
    result.recall = recall;
    result.precision = precision;
    reporter_.addResult(std::move(result));
}
```

### 示例 2：参数化测试

```cpp
#include <gtest/gtest.h>
#include "test/test_utils/integration_test_config.h"

class ParameterizedJoinTest : public ::testing::TestWithParam<IntegrationTestCase> {};

TEST_P(ParameterizedJoinTest, ExecuteTestCase) {
    const auto& tc = GetParam();
    
    // 跳过禁用的测试
    if (!tc.enabled) {
        GTEST_SKIP() << "Test case disabled: " << tc.name;
    }
    
    // 验证配置
    auto errors = tc.validate();
    ASSERT_TRUE(errors.empty()) << "Config errors: " << errors[0];
    
    // 执行测试逻辑...
}

INSTANTIATE_TEST_SUITE_P(
    AllAlgorithms,
    ParameterizedJoinTest,
    ::testing::ValuesIn(
        IntegrationTestConfigLoader::loadEnabledTests(
            IntegrationTestConfigLoader::getDefaultConfigPath()
        )
    ),
    [](const ::testing::TestParamInfo<IntegrationTestCase>& info) {
        return info.param.name;
    }
);
```

---

## 常见问题

### Q1: 如何选择合适的数据生成配置？

**A**: 根据测试目的选择：
- **功能测试**：使用较多的 `positive_pairs`，较少的 `negative_pairs`
- **性能测试**：使用较大的 `random_tail`，覆盖各种数据规模
- **边界测试**：增加 `near_threshold_pairs`，测试阈值边界行为

### Q2: 测试报告在哪里？

**A**: 默认输出到 `test/result/` 目录：
- `integration_report.json` - JSON 格式
- `integration_report.md` - Markdown 格式

也可以使用 Python 脚本聚合多次运行的结果：
```bash
python scripts/generate_test_summary.py test/result/ --output test/result/summary.md
```

### Q3: 如何调试 Join 算法的召回率问题？

**A**: 
1. 启用详细日志：设置 `SAGEFLOW_LOG_LEVEL=DEBUG`
2. 使用 `JoinMetricsCollector` 收集详细指标
3. 检查 `ground_truth` 与实际结果的差异
4. 确保时间窗口配置正确（窗口边界可能导致匹配丢失）

### Q4: TestConfigManager vs IntegrationTestConfigLoader？

**A**: 
- `TestConfigManager`：旧的配置加载器，主要用于性能测试
- `IntegrationTestConfigLoader`：新的配置加载器，功能更完整，推荐使用

---

## 相关文档

- [Join 流水线指南](./JOIN_PIPELINE_GUIDE.md)
- [系统架构](./SYSTEM_ARCHITECTURE.md)
- [添加新 Join 方法指南](./ADDING_NEW_JOIN_METHOD.md)


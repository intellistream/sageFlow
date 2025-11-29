# Group E: 测试与验证

本文档包含 VSJoin 实现的测试、性能验证和实验任务。

---

## E-01: 性能基准测试框架

**优先级**: 🟡 中  
**预估工时**: 2-3 天  
**依赖**: C-01 (VSJoin 集成)  
**输出文件**:
- `test/Performance/benchmark_framework.h`
- `test/Performance/benchmark_framework.cpp`
- `test/Performance/benchmark_main.cpp`
- `config/benchmark_config.toml`

### 任务描述

搭建统一的性能基准测试框架，用于对比 VSJoin 与各 Baseline 的性能。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现性能基准测试框架。

## 背景
需要一个统一的框架来：
1. 公平对比 VSJoin 与 Baselines
2. 收集各项性能指标
3. 生成可复现的实验结果

## 任务目标
实现 BenchmarkFramework 类，支持：
- 统一的数据加载
- 统一的计时和指标收集
- 结果导出

## 文件位置
- 头文件: test/Performance/benchmark_framework.h
- 实现文件: test/Performance/benchmark_framework.cpp
- 主程序: test/Performance/benchmark_main.cpp

## 接口要求

```cpp
#pragma once

#include "common/vector_record.h"
#include <vector>
#include <memory>
#include <string>
#include <chrono>
#include <functional>
#include <fstream>

namespace sageFlow::benchmark {

/**
 * @brief 数据集描述
 */
struct DatasetInfo {
    std::string name;
    int dimension;
    size_t size;
    std::string path;
};

/**
 * @brief 性能指标
 */
struct Metrics {
    // 时间指标 (毫秒)
    double total_time_ms;
    double avg_latency_ms;
    double p50_latency_ms;
    double p99_latency_ms;
    double max_latency_ms;
    
    // 吞吐量
    double throughput_qps;  // queries per second
    
    // 准确性指标
    double recall;          // 召回率
    double precision;       // 精确率
    
    // 资源指标
    size_t peak_memory_mb;
    double avg_cpu_usage;
    
    // 统计指标
    size_t total_queries;
    size_t total_matches;
};

/**
 * @brief 基准测试配置
 */
struct BenchmarkConfig {
    // 测试参数
    size_t warmup_iterations = 100;
    size_t test_iterations = 1000;
    bool enable_ground_truth = true;  // 是否计算准确性
    
    // Join 参数
    double similarity_threshold = 0.8;
    int k = 10;
    
    // 数据配置
    DatasetInfo left_dataset;
    DatasetInfo right_dataset;
    
    // 输出配置
    std::string output_path = "benchmark_results.json";
};

/**
 * @brief 待测试的方法接口
 */
class BenchmarkableMethod {
public:
    virtual ~BenchmarkableMethod() = default;
    
    virtual std::string getName() const = 0;
    virtual void setup(const BenchmarkConfig& config) = 0;
    virtual void teardown() = 0;
    
    virtual void insert(std::unique_ptr<VectorRecord> record, int slot) = 0;
    virtual std::vector<std::unique_ptr<VectorRecord>> 
        query(const VectorRecord& query, int slot) = 0;
    
    virtual size_t getMemoryUsage() const = 0;
};

/**
 * @brief 基准测试框架
 */
class BenchmarkFramework {
public:
    /**
     * @brief 构造函数
     * @param config 测试配置
     */
    explicit BenchmarkFramework(BenchmarkConfig config);
    
    /**
     * @brief 注册待测试方法
     */
    void registerMethod(std::shared_ptr<BenchmarkableMethod> method);
    
    /**
     * @brief 加载数据集
     */
    void loadDatasets();
    
    /**
     * @brief 运行所有测试
     */
    void runAll();
    
    /**
     * @brief 运行单个方法测试
     */
    Metrics runSingle(const std::string& method_name);
    
    /**
     * @brief 导出结果
     */
    void exportResults(const std::string& path = "");
    
    /**
     * @brief 获取所有结果
     */
    const std::unordered_map<std::string, Metrics>& getResults() const;
    
    /**
     * @brief 生成对比报告
     */
    std::string generateReport() const;

private:
    BenchmarkConfig config_;
    std::vector<std::shared_ptr<BenchmarkableMethod>> methods_;
    std::unordered_map<std::string, Metrics> results_;
    
    // 数据集
    std::vector<std::unique_ptr<VectorRecord>> left_data_;
    std::vector<std::unique_ptr<VectorRecord>> right_data_;
    
    // Ground truth (用于计算召回率)
    std::unordered_map<uint64_t, std::vector<uint64_t>> ground_truth_;
    
    /**
     * @brief 计算 ground truth
     */
    void computeGroundTruth();
    
    /**
     * @brief 测量延迟分位数
     */
    void measureLatencyPercentiles(
        const std::vector<double>& latencies,
        Metrics& metrics);
    
    /**
     * @brief 计算召回率
     */
    double computeRecall(
        const std::vector<uint64_t>& results,
        const std::vector<uint64_t>& ground_truth);
};

/**
 * @brief 结果导出器
 */
class ResultExporter {
public:
    static void toJSON(const std::unordered_map<std::string, Metrics>& results,
                       const std::string& path);
    static void toCSV(const std::unordered_map<std::string, Metrics>& results,
                      const std::string& path);
    static void toMarkdown(const std::unordered_map<std::string, Metrics>& results,
                           const std::string& path);
};

} // namespace sageFlow::benchmark
```

## 实现要点

1. **loadDatasets()**:
   - 支持多种格式（fvecs, bvecs, txt）
   - 使用 FileStreamSource 或自定义加载器

2. **computeGroundTruth()**:
   - 使用暴力搜索计算精确结果
   - 可以采样减少计算量

3. **runSingle()**:
   ```cpp
   Metrics runSingle(const std::string& method_name) {
       auto method = findMethod(method_name);
       method->setup(config_);
       
       // Warmup
       for (size_t i = 0; i < config_.warmup_iterations; ++i) {
           // ...
       }
       
       // 插入阶段
       auto insert_start = std::chrono::high_resolution_clock::now();
       for (auto& record : left_data_) {
           method->insert(record->clone(), 0);
       }
       for (auto& record : right_data_) {
           method->insert(record->clone(), 1);
       }
       auto insert_end = std::chrono::high_resolution_clock::now();
       
       // 查询阶段
       std::vector<double> latencies;
       for (const auto& query : test_queries_) {
           auto start = std::chrono::high_resolution_clock::now();
           auto results = method->query(*query, 0);
           auto end = std::chrono::high_resolution_clock::now();
           
           latencies.push_back(/* duration */);
       }
       
       // 计算指标
       Metrics metrics;
       measureLatencyPercentiles(latencies, metrics);
       metrics.peak_memory_mb = method->getMemoryUsage();
       
       if (config_.enable_ground_truth) {
           // 计算召回率...
       }
       
       method->teardown();
       return metrics;
   }
   ```

## 配置文件

```toml
# config/benchmark_config.toml
[benchmark]
warmup_iterations = 100
test_iterations = 1000
enable_ground_truth = true

[benchmark.join]
similarity_threshold = 0.8
k = 10

[benchmark.datasets.sift]
name = "siftsmall"
dimension = 128
left_path = "data/siftsmall/siftsmall_base.fvecs"
right_path = "data/siftsmall/siftsmall_query.fvecs"

[benchmark.output]
path = "test/result/benchmark_results.json"
format = ["json", "csv", "markdown"]
```

## 测试要求

```cpp
TEST(BenchmarkFrameworkTest, LoadDataset) {
    // 测试数据加载
}

TEST(BenchmarkFrameworkTest, MetricsCollection) {
    // 测试指标收集正确性
}

TEST(BenchmarkFrameworkTest, ResultExport) {
    // 测试结果导出
}
```

## 验收标准
1. 框架可正确运行
2. 指标计算准确
3. 结果可复现
```

---

## E-02: 集成测试套件

**优先级**: 🟡 中  
**预估工时**: 2-3 天  
**依赖**: C-01 (VSJoin 集成), D-01~D-05 (Baselines)  
**输出文件**:
- `test/IntegrationTest/test_vsjoin_complete.cpp`
- `test/IntegrationTest/test_baselines_correctness.cpp`
- `test/IntegrationTest/test_streaming_scenario.cpp`

### 任务描述

实现完整的集成测试套件，验证 VSJoin 和 Baselines 的功能正确性。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现集成测试套件。

## 背景
需要验证：
1. VSJoin 完整流程正确性
2. 各 Baseline 实现正确性
3. 流式场景下的行为

## 任务目标
实现三组集成测试。

## 文件位置
- test/IntegrationTest/test_vsjoin_complete.cpp
- test/IntegrationTest/test_baselines_correctness.cpp
- test/IntegrationTest/test_streaming_scenario.cpp

## 测试内容

### test_vsjoin_complete.cpp

```cpp
#include <gtest/gtest.h>
#include "stream/stream_environment.h"
#include "stream/simple_stream_source.h"
#include "function/filter_function.h"
#include "function/map_function.h"
#include "function/join_function.h"
#include "function/sink_function.h"

namespace sageFlow::test {

class VSJoinCompleteTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 创建测试数据
        dimension_ = 128;
        num_left_ = 1000;
        num_right_ = 1000;
        
        // 生成随机向量
        generateTestData();
    }
    
    void generateTestData();
    
    int dimension_;
    size_t num_left_;
    size_t num_right_;
    std::vector<std::unique_ptr<VectorRecord>> left_data_;
    std::vector<std::unique_ptr<VectorRecord>> right_data_;
};

TEST_F(VSJoinCompleteTest, EndToEndExecution) {
    // 测试端到端执行
    // 1. 创建 StreamEnvironment
    // 2. 创建左右流
    // 3. 执行 VSJoin
    // 4. 验证结果数量和正确性
}

TEST_F(VSJoinCompleteTest, ParallelExecution) {
    // 测试并行执行
    // parallelism = 4
}

TEST_F(VSJoinCompleteTest, WindowExpiration) {
    // 测试窗口过期
    // 插入带时间戳的数据，验证过期清理
}

TEST_F(VSJoinCompleteTest, LateArrivalHandling) {
    // 测试延迟到达处理
}

TEST_F(VSJoinCompleteTest, CrossPartitionJoin) {
    // 测试跨分区 Join
    // 使用边界向量场景
}

TEST_F(VSJoinCompleteTest, ResultConsistency) {
    // 对比 VSJoin 与暴力搜索结果
    // 召回率应 > 95%
}

} // namespace sageFlow::test
```

### test_baselines_correctness.cpp

```cpp
namespace sageFlow::test {

class BaselinesCorrectnessTest : public ::testing::Test {
protected:
    void SetUp() override {
        generateTestData();
        computeGroundTruth();
    }
    
    void generateTestData();
    void computeGroundTruth();
    
    std::vector<std::unique_ptr<VectorRecord>> test_data_;
    std::unordered_map<uint64_t, std::vector<uint64_t>> ground_truth_;
};

TEST_F(BaselinesCorrectnessTest, S3JCorrectness) {
    // S3J 应该 100% 正确（暴力搜索）
}

TEST_F(BaselinesCorrectnessTest, HDRTreeCorrectness) {
    // HDR-Tree 召回率测试
}

TEST_F(BaselinesCorrectnessTest, HNSWCorrectness) {
    // HNSW 召回率测试
}

TEST_F(BaselinesCorrectnessTest, IVFCorrectness) {
    // IVF 召回率测试
}

TEST_F(BaselinesCorrectnessTest, ClusteredJoinCorrectness) {
    // ClusteredJoin 召回率测试
}

TEST_F(BaselinesCorrectnessTest, AllMethodsConsistency) {
    // 所有方法对相同数据的结果应该高度一致
}

} // namespace sageFlow::test
```

### test_streaming_scenario.cpp

```cpp
namespace sageFlow::test {

class StreamingScenarioTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 创建模拟流数据
    }
};

TEST_F(StreamingScenarioTest, ContinuousIngestion) {
    // 测试持续数据摄入
    // 模拟每秒 1000 条记录
}

TEST_F(StreamingScenarioTest, BurstTraffic) {
    // 测试突发流量
    // 模拟短时间内大量数据
}

TEST_F(StreamingScenarioTest, WindowSliding) {
    // 测试滑动窗口行为
    // 验证窗口正确滑动
}

TEST_F(StreamingScenarioTest, OrderedVsUnordered) {
    // 测试有序 vs 无序数据
}

TEST_F(StreamingScenarioTest, SkewedDistribution) {
    // 测试倾斜分布数据
    // 向量集中在某些区域
}

TEST_F(StreamingScenarioTest, GracefulShutdown) {
    // 测试优雅关闭
    // 确保处理完所有缓冲数据
}

} // namespace sageFlow::test
```

## 测试数据生成

```cpp
void generateTestData() {
    std::mt19937 gen(42);  // 固定种子保证可重复
    std::normal_distribution<float> dist(0.0f, 1.0f);
    
    for (size_t i = 0; i < num_left_; ++i) {
        std::vector<float> vec(dimension_);
        for (int j = 0; j < dimension_; ++j) {
            vec[j] = dist(gen);
        }
        // 归一化
        float norm = 0;
        for (float v : vec) norm += v * v;
        norm = std::sqrt(norm);
        for (float& v : vec) v /= norm;
        
        left_data_.push_back(std::make_unique<VectorRecord>(
            i, std::move(vec), static_cast<int64_t>(i)));
    }
    // 类似生成右流数据...
}
```

## 验收标准
1. 所有集成测试通过
2. 测试覆盖核心场景
3. 测试可在 CI 中运行
```

---

## E-03: 召回率验证工具

**优先级**: 🟡 中  
**预估工时**: 1-2 天  
**依赖**: E-01 (性能框架)  
**输出文件**:
- `test/utils/recall_verifier.h`
- `test/utils/recall_verifier.cpp`
- `test/UnitTest/test_recall_verifier.cpp`

### 任务描述

实现召回率验证工具，用于评估近似算法的准确性。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现召回率验证工具。

## 背景
近似算法需要验证召回率，确保在性能优化的同时保持结果质量。

## 任务目标
实现 RecallVerifier 类。

## 文件位置
- 头文件: test/utils/recall_verifier.h
- 实现文件: test/utils/recall_verifier.cpp

## 接口要求

```cpp
#pragma once

#include "common/vector_record.h"
#include <vector>
#include <unordered_set>
#include <functional>

namespace sageFlow::test {

/**
 * @brief 召回率验证器
 */
class RecallVerifier {
public:
    /**
     * @brief 查询函数类型
     */
    using QueryFunction = std::function<
        std::vector<uint64_t>(const VectorRecord& query, int k)>;
    
    /**
     * @brief 设置 ground truth 计算方法
     */
    void setGroundTruthFunction(QueryFunction func);
    
    /**
     * @brief 设置待验证方法
     */
    void setApproximateFunction(QueryFunction func);
    
    /**
     * @brief 验证单个查询的召回率
     * @param query 查询向量
     * @param k 返回数量
     * @return 召回率 (0.0 - 1.0)
     */
    double verifyQuery(const VectorRecord& query, int k);
    
    /**
     * @brief 批量验证召回率
     * @param queries 查询向量列表
     * @param k 返回数量
     * @return 平均召回率
     */
    double verifyBatch(const std::vector<const VectorRecord*>& queries, int k);
    
    /**
     * @brief 获取详细统计
     */
    struct Stats {
        double avg_recall;
        double min_recall;
        double max_recall;
        double std_dev;
        size_t num_queries;
        size_t num_below_threshold;  // 低于阈值的查询数
    };
    Stats getStats(double recall_threshold = 0.95) const;
    
    /**
     * @brief 重置统计
     */
    void reset();

private:
    QueryFunction ground_truth_func_;
    QueryFunction approximate_func_;
    std::vector<double> recall_history_;
    
    /**
     * @brief 计算召回率
     */
    double computeRecall(
        const std::vector<uint64_t>& approximate,
        const std::vector<uint64_t>& ground_truth);
};

/**
 * @brief 暴力搜索 ground truth 生成器
 */
class BruteForceGroundTruth {
public:
    /**
     * @brief 构造函数
     * @param dataset 数据集
     * @param threshold 相似度阈值
     */
    BruteForceGroundTruth(
        const std::vector<std::unique_ptr<VectorRecord>>& dataset,
        double threshold);
    
    /**
     * @brief 查询
     */
    std::vector<uint64_t> query(const VectorRecord& query, int k) const;
    
    /**
     * @brief 范围查询
     */
    std::vector<uint64_t> rangeQuery(const VectorRecord& query, double threshold) const;

private:
    const std::vector<std::unique_ptr<VectorRecord>>* dataset_;
    double threshold_;
};

} // namespace sageFlow::test
```

## 实现要点

1. **computeRecall()**:
   ```cpp
   double computeRecall(
       const std::vector<uint64_t>& approximate,
       const std::vector<uint64_t>& ground_truth) {
       
       if (ground_truth.empty()) return 1.0;
       
       std::unordered_set<uint64_t> gt_set(
           ground_truth.begin(), ground_truth.end());
       
       size_t hits = 0;
       for (uint64_t uid : approximate) {
           if (gt_set.count(uid)) {
               hits++;
           }
       }
       
       return static_cast<double>(hits) / ground_truth.size();
   }
   ```

2. **getStats()**:
   - 计算均值、标准差、最大最小值
   - 统计低于阈值的查询数

## 测试要求

```cpp
TEST(RecallVerifierTest, PerfectRecall) {
    // 完全相同的结果应该召回率 = 1.0
}

TEST(RecallVerifierTest, PartialRecall) {
    // 部分匹配的召回率计算
}

TEST(RecallVerifierTest, EmptyResults) {
    // 空结果处理
}

TEST(RecallVerifierTest, StatisticsAccuracy) {
    // 统计计算正确性
}
```

## 验收标准
1. 所有单元测试通过
2. 召回率计算准确
3. 可用于其他测试
```

---

## E-04: 实验报告生成

**优先级**: 🟢 低  
**预估工时**: 1 天  
**依赖**: E-01, E-02  
**输出文件**:
- `test/Performance/report_generator.h`
- `test/Performance/report_generator.cpp`
- `scripts/generate_report.py`

### 任务描述

实现实验报告生成工具，自动生成对比图表和分析报告。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现实验报告生成工具。

## 背景
需要自动化生成：
1. 性能对比图表
2. 召回率对比表格
3. 分析报告

## 任务目标
实现 ReportGenerator 和 Python 辅助脚本。

## C++ 部分

```cpp
#pragma once

#include "benchmark_framework.h"
#include <string>

namespace sageFlow::benchmark {

/**
 * @brief 报告生成器
 */
class ReportGenerator {
public:
    /**
     * @brief 构造函数
     * @param results 基准测试结果
     */
    explicit ReportGenerator(
        const std::unordered_map<std::string, Metrics>& results);
    
    /**
     * @brief 生成 Markdown 报告
     */
    std::string generateMarkdown() const;
    
    /**
     * @brief 生成 LaTeX 表格
     */
    std::string generateLatexTable() const;
    
    /**
     * @brief 导出用于 Python 绑定的 JSON
     */
    void exportForPython(const std::string& path) const;

private:
    std::unordered_map<std::string, Metrics> results_;
};

} // namespace sageFlow::benchmark
```

## Python 脚本

```python
#!/usr/bin/env python3
# scripts/generate_report.py

import json
import matplotlib.pyplot as plt
import pandas as pd
import argparse

def load_results(path):
    with open(path, 'r') as f:
        return json.load(f)

def plot_throughput(results, output_path):
    """生成吞吐量对比图"""
    methods = list(results.keys())
    throughputs = [results[m]['throughput_qps'] for m in methods]
    
    plt.figure(figsize=(10, 6))
    plt.bar(methods, throughputs)
    plt.ylabel('Throughput (QPS)')
    plt.title('Throughput Comparison')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

def plot_latency(results, output_path):
    """生成延迟对比图"""
    methods = list(results.keys())
    
    p50 = [results[m]['p50_latency_ms'] for m in methods]
    p99 = [results[m]['p99_latency_ms'] for m in methods]
    
    x = range(len(methods))
    width = 0.35
    
    plt.figure(figsize=(10, 6))
    plt.bar([i - width/2 for i in x], p50, width, label='P50')
    plt.bar([i + width/2 for i in x], p99, width, label='P99')
    plt.ylabel('Latency (ms)')
    plt.title('Latency Comparison')
    plt.xticks(x, methods, rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

def plot_recall(results, output_path):
    """生成召回率对比图"""
    methods = list(results.keys())
    recalls = [results[m]['recall'] for m in methods]
    
    plt.figure(figsize=(10, 6))
    plt.bar(methods, recalls)
    plt.ylabel('Recall')
    plt.title('Recall Comparison')
    plt.ylim(0, 1.1)
    plt.axhline(y=0.95, color='r', linestyle='--', label='95% threshold')
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

def generate_table(results, output_path):
    """生成对比表格"""
    df = pd.DataFrame(results).T
    df = df[['throughput_qps', 'avg_latency_ms', 'p99_latency_ms', 
             'recall', 'peak_memory_mb']]
    df.columns = ['Throughput (QPS)', 'Avg Latency (ms)', 
                  'P99 Latency (ms)', 'Recall', 'Memory (MB)']
    df.to_markdown(output_path)
    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help='Input JSON file')
    parser.add_argument('--output-dir', default='test/result/plots')
    args = parser.parse_args()
    
    results = load_results(args.input)
    
    plot_throughput(results, f'{args.output_dir}/throughput.png')
    plot_latency(results, f'{args.output_dir}/latency.png')
    plot_recall(results, f'{args.output_dir}/recall.png')
    table = generate_table(results, f'{args.output_dir}/comparison.md')
    
    print("Report generated successfully!")
    print("\nSummary:")
    print(table)

if __name__ == '__main__':
    main()
```

## 验收标准
1. 报告生成正确
2. 图表清晰可读
3. 可在 CI 中自动运行
```

---

## 任务检查清单

| 任务ID | 名称 | 状态 | 负责人 | 开始日期 | 完成日期 | 依赖完成 |
|--------|------|------|--------|----------|----------|----------|
| E-01 | 性能基准测试框架 | ⬜ | - | - | - | C-01 |
| E-02 | 集成测试套件 | ⬜ | - | - | - | C-01, D-01~D-05 |
| E-03 | 召回率验证工具 | ⬜ | - | - | - | E-01 |
| E-04 | 实验报告生成 | ⬜ | - | - | - | E-01, E-02 |

---

## 测试执行指南

### 运行单元测试

```bash
# 构建
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -j $(nproc)

# 运行所有单元测试
ctest --test-dir build -L UNIT --output-on-failure

# 运行特定测试
./build/bin/test_vsjoin_complete
```

### 运行集成测试

```bash
ctest --test-dir build -L INTEGRATION --output-on-failure
```

### 运行性能测试

```bash
./build/bin/benchmark_main --config config/benchmark_config.toml

# 生成报告
python3 scripts/generate_report.py \
    --input test/result/benchmark_results.json \
    --output-dir test/result/plots
```

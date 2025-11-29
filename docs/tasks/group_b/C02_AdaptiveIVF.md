# Task C-02: AdaptiveIVF 自适应召回控制

**优先级**: 🟢 低  
**预估工时**: 2-3 天  
**依赖**: C-01 ⏳  
**输出文件**:
- `include/index/adaptive_ivf.h`
- `src/index/adaptive_ivf.cpp`
- `test/UnitTest/test_adaptive_ivf.cpp`

---

## ⚠️ 注意

此任务依赖 C-01 (VSJoin 集成) 完成。
**请等待 C-01 完成后再开始此任务。**

---

## 任务描述

实现自适应 nprobes 调整机制，在运行时平衡召回率和性能。

---

## 提示词

```
你是 sageFlow 项目的开发者，需要实现 AdaptiveIVF 类。

## 项目背景
sageFlow 是一个 C++20 流式向量处理引擎，遵循以下规范：
- 类名: CamelCase (如 AdaptiveIVF)
- 方法名: camelBack (如 adjustNprobes, updateRecallEstimate)
- 成员变量: lower_case_ 带尾部下划线 (如 target_recall_, current_nprobes_)
- 使用 #pragma once 作为头文件保护
- 使用 spdlog 进行日志记录 (SAGEFLOW_LOG_* 宏)

## 背景
固定的 nprobes 可能导致：
- 太小：召回率不足
- 太大：性能下降

自适应调整可以在运行时平衡召回率和性能。

## 任务目标
实现 AdaptiveIVF：
1. 在线召回率估计（通过采样）
2. 自适应 nprobes 调整
3. 召回率目标配置

## 文件位置
- 头文件: include/index/adaptive_ivf.h
- 实现文件: src/index/adaptive_ivf.cpp

## 接口要求

```cpp
#pragma once

#include "index/ivf.h"
#include <atomic>
#include <deque>
#include <mutex>
#include <random>

namespace sageFlow {

/**
 * @brief 自适应 IVF 统计信息
 */
struct AdaptiveIVFStats {
    int current_nprobes;           ///< 当前 nprobes
    double estimated_recall;       ///< 估计的召回率
    uint64_t total_queries;        ///< 总查询数
    uint64_t sample_count;         ///< 采样数
    uint64_t adjustment_count;     ///< 调整次数
};

/**
 * @brief 自适应 IVF 索引
 * 
 * 通过采样估计召回率，自动调整 nprobes 以达到目标召回率。
 */
class AdaptiveIVF : public Ivf {
public:
    /**
     * @brief 构造函数
     * @param nlist 聚类数量
     * @param rebuild_threshold 重建阈值
     * @param initial_nprobes 初始 nprobes
     * @param target_recall 目标召回率 (0.0-1.0)
     * @param sample_rate 采样率 (用于召回率估计)
     * @param adjustment_interval 调整间隔（每多少次采样后调整一次）
     */
    AdaptiveIVF(int nlist, double rebuild_threshold, int initial_nprobes,
                double target_recall = 0.95, double sample_rate = 0.01,
                size_t adjustment_interval = 10);
    
    // 覆盖查询方法
    std::vector<std::shared_ptr<const VectorRecord>> 
        query(const VectorRecord& query, int k) override;
    
    std::vector<std::shared_ptr<const VectorRecord>>
        queryForJoin(const VectorRecord& query, double threshold) override;
    
    /**
     * @brief 获取当前 nprobes
     */
    int getCurrentNprobes() const { return current_nprobes_.load(); }
    
    /**
     * @brief 获取估计的召回率
     */
    double getEstimatedRecall() const { return estimated_recall_.load(); }
    
    /**
     * @brief 设置目标召回率
     */
    void setTargetRecall(double target);
    
    /**
     * @brief 获取目标召回率
     */
    double getTargetRecall() const { return target_recall_; }
    
    /**
     * @brief 设置 nprobes 范围
     */
    void setNprobesRange(int min_probes, int max_probes);
    
    /**
     * @brief 获取 nprobes 范围
     */
    std::pair<int, int> getNprobesRange() const { 
        return {min_nprobes_, max_nprobes_}; 
    }
    
    /**
     * @brief 设置采样率
     */
    void setSampleRate(double rate);
    
    /**
     * @brief 获取统计信息
     */
    AdaptiveIVFStats getStats() const;
    
    /**
     * @brief 重置统计信息
     */
    void resetStats();
    
    /**
     * @brief 强制触发一次调整
     */
    void forceAdjust();

private:
    double target_recall_;
    double sample_rate_;
    size_t adjustment_interval_;
    int min_nprobes_;
    int max_nprobes_;
    
    std::atomic<int> current_nprobes_;
    std::atomic<double> estimated_recall_{1.0};
    std::atomic<uint64_t> query_count_{0};
    std::atomic<uint64_t> sample_count_{0};
    std::atomic<uint64_t> adjustment_count_{0};
    
    // 召回率估计的滑动窗口
    std::deque<double> recall_samples_;
    mutable std::mutex samples_mutex_;
    static constexpr size_t MAX_SAMPLES = 100;
    
    // 随机数生成器
    mutable std::mt19937 rng_;
    mutable std::mutex rng_mutex_;
    
    /**
     * @brief 判断是否需要采样
     */
    bool shouldSample();
    
    /**
     * @brief 更新召回率估计
     * @param sample_recall 本次采样的召回率
     */
    void updateRecallEstimate(double sample_recall);
    
    /**
     * @brief 调整 nprobes
     */
    void adjustNprobes();
    
    /**
     * @brief 执行精确查询（用于采样验证）
     */
    std::vector<std::shared_ptr<const VectorRecord>>
        queryExact(const VectorRecord& query, int k);
    
    /**
     * @brief 计算两个结果集的召回率
     */
    double computeRecall(
        const std::vector<std::shared_ptr<const VectorRecord>>& approximate,
        const std::vector<std::shared_ptr<const VectorRecord>>& exact) const;
};

} // namespace sageFlow
```

## 实现要点

1. **shouldSample()**:
   ```cpp
   bool shouldSample() {
       std::lock_guard<std::mutex> lock(rng_mutex_);
       std::uniform_real_distribution<> dis(0.0, 1.0);
       return dis(rng_) < sample_rate_;
   }
   ```

2. **query() 覆盖**:
   ```cpp
   std::vector<std::shared_ptr<const VectorRecord>>
   query(const VectorRecord& query, int k) {
       query_count_++;
       
       // 使用当前 nprobes 执行近似查询
       int saved_nprobes = getNprobes();
       setNprobes(current_nprobes_.load());
       auto approximate_results = Ivf::query(query, k);
       setNprobes(saved_nprobes);
       
       // 采样检测召回率
       if (shouldSample()) {
           sample_count_++;
           
           // 执行精确查询
           auto exact_results = queryExact(query, k);
           
           // 计算召回率
           double recall = computeRecall(approximate_results, exact_results);
           
           // 更新估计
           updateRecallEstimate(recall);
           
           // 检查是否需要调整
           if (sample_count_ % adjustment_interval_ == 0) {
               adjustNprobes();
           }
       }
       
       return approximate_results;
   }
   ```

3. **updateRecallEstimate()**:
   ```cpp
   void updateRecallEstimate(double sample_recall) {
       std::lock_guard<std::mutex> lock(samples_mutex_);
       
       recall_samples_.push_back(sample_recall);
       if (recall_samples_.size() > MAX_SAMPLES) {
           recall_samples_.pop_front();
       }
       
       // 计算滑动窗口平均值
       double sum = 0.0;
       for (double r : recall_samples_) {
           sum += r;
       }
       estimated_recall_ = sum / recall_samples_.size();
   }
   ```

4. **adjustNprobes()**:
   ```cpp
   void adjustNprobes() {
       double current_recall = estimated_recall_.load();
       int current = current_nprobes_.load();
       int new_nprobes = current;
       
       // 容差范围
       const double tolerance = 0.02;
       
       if (current_recall < target_recall_ - tolerance) {
           // 召回率不足，增加 nprobes
           new_nprobes = std::min(current + 1, max_nprobes_);
           SAGEFLOW_LOG_DEBUG("AdaptiveIVF", 
               "Increasing nprobes: {} -> {} (recall={:.3f}, target={:.3f})",
               current, new_nprobes, current_recall, target_recall_);
       } else if (current_recall > target_recall_ + tolerance) {
           // 召回率充足，尝试减少 nprobes
           new_nprobes = std::max(current - 1, min_nprobes_);
           SAGEFLOW_LOG_DEBUG("AdaptiveIVF",
               "Decreasing nprobes: {} -> {} (recall={:.3f}, target={:.3f})",
               current, new_nprobes, current_recall, target_recall_);
       }
       
       if (new_nprobes != current) {
           current_nprobes_ = new_nprobes;
           adjustment_count_++;
       }
   }
   ```

5. **computeRecall()**:
   ```cpp
   double computeRecall(
       const std::vector<std::shared_ptr<const VectorRecord>>& approximate,
       const std::vector<std::shared_ptr<const VectorRecord>>& exact) const {
       
       if (exact.empty()) return 1.0;
       
       // 将 exact 结果的 uid 放入集合
       std::unordered_set<uint64_t> exact_uids;
       for (const auto& r : exact) {
           exact_uids.insert(r->getUid());
       }
       
       // 计算 approximate 中有多少在 exact 中
       size_t hits = 0;
       for (const auto& r : approximate) {
           if (exact_uids.count(r->getUid())) {
               hits++;
           }
       }
       
       return static_cast<double>(hits) / exact.size();
   }
   ```

6. **queryExact()**:
   - 使用 nprobes = nlist 执行精确查询
   - 确保结果是真正的 top-k

## 测试要求

```cpp
#include <gtest/gtest.h>
#include "index/adaptive_ivf.h"

class AdaptiveIVFTest : public ::testing::Test {
protected:
    void SetUp() override {
        index_ = std::make_unique<AdaptiveIVF>(
            /*nlist=*/10, /*rebuild_threshold=*/0.5, 
            /*initial_nprobes=*/2, /*target_recall=*/0.9,
            /*sample_rate=*/0.1, /*adjustment_interval=*/5);
        
        // 插入测试数据
        for (int i = 0; i < 1000; ++i) {
            index_->insert(createRandomRecord(i));
        }
    }
    
    std::unique_ptr<AdaptiveIVF> index_;
    
    std::unique_ptr<VectorRecord> createRandomRecord(uint64_t uid);
};

// 基础功能测试
TEST_F(AdaptiveIVFTest, QueryReturnsResults) {
    // 测试查询返回结果
}

TEST_F(AdaptiveIVFTest, InitialNprobes) {
    // 测试初始 nprobes 设置正确
}

// 自动调整测试
TEST_F(AdaptiveIVFTest, NprobesAutoIncrease) {
    // 设置较高的目标召回率
    // 执行多次查询
    // 验证 nprobes 增加
}

TEST_F(AdaptiveIVFTest, NprobesAutoDecrease) {
    // 设置较低的目标召回率
    // 从高 nprobes 开始
    // 执行多次查询
    // 验证 nprobes 减少
}

TEST_F(AdaptiveIVFTest, NprobesStaysInRange) {
    // 测试 nprobes 不超出范围
}

// 召回率估计测试
TEST_F(AdaptiveIVFTest, RecallEstimateAccuracy) {
    // 测试召回率估计准确性
}

TEST_F(AdaptiveIVFTest, RecallEstimateUpdates) {
    // 测试召回率估计更新
}

// 稳态行为测试
TEST_F(AdaptiveIVFTest, SteadyState) {
    // 测试达到目标后不频繁调整
}

TEST_F(AdaptiveIVFTest, QuickConvergence) {
    // 测试快速收敛到目标
}

// 配置测试
TEST_F(AdaptiveIVFTest, SetTargetRecall) {
    // 测试设置目标召回率
}

TEST_F(AdaptiveIVFTest, SetNprobesRange) {
    // 测试设置 nprobes 范围
}

TEST_F(AdaptiveIVFTest, SetSampleRate) {
    // 测试设置采样率
}

// 统计测试
TEST_F(AdaptiveIVFTest, StatsAccurate) {
    // 测试统计信息准确
}

TEST_F(AdaptiveIVFTest, ResetStats) {
    // 测试统计重置
}

// 强制调整测试
TEST_F(AdaptiveIVFTest, ForceAdjust) {
    // 测试强制触发调整
}
```

## 验收标准
1. 所有单元测试通过
2. 自适应调整正确
3. 召回率估计误差 < 5%
4. 稳态时调整频率低
5. 代码通过 clang-tidy 检查
```

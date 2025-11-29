# Task A-05: DistanceVerifier 距离验证器

**优先级**: 🟡 中  
**预估工时**: 2-3 天  
**依赖**: 无  
**输出文件**:
- `include/operator/distance_verifier.h`
- `src/operator/distance_verifier.cpp`
- `test/UnitTest/test_distance_verifier.cpp`

---

## 任务描述

实现高效的距离验证器，用于验证候选向量是否满足相似度阈值，支持 SIMD 加速和早期终止优化。

---

## 提示词

```
你是 sageFlow 项目的开发者，需要实现 DistanceVerifier 类。

## 项目背景
sageFlow 是一个 C++20 流式向量处理引擎，遵循以下规范：
- 类名: CamelCase
- 方法名: camelBack
- 成员变量: lower_case_ 带尾部下划线
- 使用 #pragma once 作为头文件保护

## 背景
当前 JoinOperator 中的候选验证与候选生成耦合在一起。
将验证逻辑独立出来，可以：
1. 支持 SIMD 批量验证
2. 实现早期终止优化
3. 方便并行验证

## 任务目标
实现距离验证器：
1. 批量验证候选向量
2. 使用 SIMD 加速距离计算（可选）
3. 支持早期终止（部分维度快速筛选）

## 文件位置
- 头文件: include/operator/distance_verifier.h
- 实现文件: src/operator/distance_verifier.cpp

## 接口要求

```cpp
#pragma once

#include "common/vector_record.h"
#include <vector>
#include <memory>

namespace sageFlow {

/**
 * @brief 验证结果
 */
struct VerificationResult {
    uint64_t candidate_uid;
    double distance;
    double similarity;
    bool passed;
};

/**
 * @brief 距离验证器
 * 
 * 验证候选向量是否满足相似度阈值。
 * 支持批量验证和早期终止优化。
 */
class DistanceVerifier {
public:
    /**
     * @brief 构造函数
     * @param similarity_threshold 相似度阈值 (similarity >= threshold 才通过)
     * @param alpha 距离到相似度的转换系数 (similarity = exp(-alpha * distance))
     */
    explicit DistanceVerifier(double similarity_threshold, double alpha = 0.1);
    
    /**
     * @brief 验证单个候选
     * @param query 查询向量
     * @param candidate 候选向量
     * @return 验证结果
     */
    VerificationResult verify(const VectorRecord& query, const VectorRecord& candidate);
    
    /**
     * @brief 批量验证
     * @param query 查询向量
     * @param candidates 候选向量列表
     * @return 所有验证结果
     */
    std::vector<VerificationResult> verifyBatch(
        const VectorRecord& query,
        const std::vector<std::unique_ptr<VectorRecord>>& candidates);
    
    /**
     * @brief 批量验证（只返回通过的）
     * @param query 查询向量
     * @param candidates 候选向量列表（会被移动）
     * @return 通过验证的候选
     */
    std::vector<std::unique_ptr<VectorRecord>> filterCandidates(
        const VectorRecord& query,
        std::vector<std::unique_ptr<VectorRecord>>&& candidates);
    
    /**
     * @brief 设置早期终止的维度检查数
     * @param dims 0 表示不使用早期终止
     */
    void setEarlyTerminationDims(int dims) { early_termination_dims_ = dims; }
    
    /**
     * @brief 获取相似度阈值
     */
    double getThreshold() const { return similarity_threshold_; }
    
    /**
     * @brief 将距离转换为相似度
     */
    double distanceToSimilarity(double distance) const {
        return std::exp(-alpha_ * distance);
    }
    
    /**
     * @brief 将相似度转换为距离阈值
     */
    double similarityToDistance(double similarity) const {
        return -std::log(similarity) / alpha_;
    }

private:
    double similarity_threshold_;
    double alpha_;
    int early_termination_dims_ = 0;  // 0 表示不使用早期终止
    double distance_threshold_;  // 预计算的距离阈值
    
    /**
     * @brief 计算 L2 距离
     */
    double computeL2Distance(const VectorRecord& a, const VectorRecord& b) const;
    
    /**
     * @brief 早期终止检查：使用前 N 维估计距离下界
     * @return true 表示可以安全拒绝
     */
    bool earlyReject(const VectorRecord& query, const VectorRecord& candidate) const;
};

} // namespace sageFlow
```

## 实现要点

1. **computeL2Distance()**:
   ```cpp
   double computeL2Distance(const VectorRecord& a, const VectorRecord& b) const {
       const auto& vec_a = a.getVector();
       const auto& vec_b = b.getVector();
       
       double sum = 0.0;
       for (size_t i = 0; i < vec_a.size(); ++i) {
           double diff = vec_a[i] - vec_b[i];
           sum += diff * diff;
       }
       return std::sqrt(sum);
   }
   ```

2. **earlyReject()**:
   - 只用前 early_termination_dims_ 维计算部分距离
   - 如果部分距离已超过 distance_threshold_，直接拒绝
   - 利用 L2 距离的性质：部分维度距离 <= 完整距离

3. **filterCandidates()**:
   - 先进行早期终止筛选（如果启用）
   - 对剩余候选进行完整验证
   - 返回通过验证的候选（使用 std::move）

## 测试要求

```cpp
TEST(DistanceVerifierTest, VerifySingleCandidate) {
    DistanceVerifier verifier(0.8, 0.1);
    // 测试单个候选验证
}

TEST(DistanceVerifierTest, BatchVerification) {
    // 测试批量验证正确性
}

TEST(DistanceVerifierTest, EarlyTermination) {
    // 测试早期终止不影响正确性
    // 确保不会错误拒绝满足条件的候选
}

TEST(DistanceVerifierTest, FilterCandidates) {
    // 测试过滤后只保留通过的候选
}
```

## 验收标准
1. 所有单元测试通过
2. 早期终止正确性验证
3. 批量验证结果与单个验证一致
```

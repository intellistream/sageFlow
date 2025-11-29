# Task A-06: PCA 工具类

**优先级**: 🟡 中  
**预估工时**: 2 天  
**依赖**: 无  
**输出文件**:
- `include/compute_engine/pca.h`
- `src/compute_engine/pca.cpp`
- `test/UnitTest/test_pca.cpp`

---

## 任务描述

实现 PCA（主成分分析）工具类，用于 HDR-Tree baseline 的降维操作。

---

## 提示词

```
你是 sageFlow 项目的开发者，需要实现 PCA 类。

## 项目背景
sageFlow 是一个 C++20 流式向量处理引擎，遵循以下规范：
- 类名: CamelCase
- 方法名: camelBack
- 成员变量: lower_case_ 带尾部下划线
- 使用 #pragma once 作为头文件保护

## 背景
HDR-Tree baseline 需要使用 PCA 将高维向量投影到低维空间，
利用 PCA 距离下界性质进行候选过滤。

## 任务目标
实现 PCA 工具类：
1. 使用样本数据拟合 PCA
2. 投影向量到低维空间
3. 支持批量投影

## 文件位置
- 头文件: include/compute_engine/pca.h
- 实现文件: src/compute_engine/pca.cpp

## 接口要求

```cpp
#pragma once

#include <vector>
#include <cstddef>

namespace sageFlow {

/**
 * @brief 主成分分析 (PCA) 工具类
 * 
 * 使用幂迭代法计算主成分，适用于中等规模数据。
 * 对于大规模数据建议使用增量 PCA 或随机化 PCA。
 */
class PCA {
public:
    /**
     * @brief 构造函数
     * @param original_dim 原始维度
     * @param target_dim 目标维度（主成分数量）
     */
    PCA(int original_dim, int target_dim);
    
    /**
     * @brief 使用样本数据拟合 PCA
     * @param samples 样本数据 (n_samples x original_dim)
     * @param max_iterations 最大迭代次数
     * @param tolerance 收敛阈值
     */
    void fit(const std::vector<std::vector<float>>& samples,
             int max_iterations = 100, double tolerance = 1e-6);
    
    /**
     * @brief 投影单个向量到低维空间
     * @param vector 原始向量
     * @return 低维向量
     */
    std::vector<float> transform(const std::vector<float>& vector) const;
    
    /**
     * @brief 批量投影
     * @param vectors 原始向量列表
     * @return 低维向量列表
     */
    std::vector<std::vector<float>> transformBatch(
        const std::vector<std::vector<float>>& vectors) const;
    
    /**
     * @brief 检查是否已拟合
     */
    bool isFitted() const { return fitted_; }
    
    /**
     * @brief 获取解释方差比例
     */
    const std::vector<float>& getExplainedVarianceRatio() const;
    
    /**
     * @brief 获取主成分矩阵 (target_dim x original_dim)
     */
    const std::vector<std::vector<float>>& getComponents() const { return components_; }
    
    /**
     * @brief 获取数据均值
     */
    const std::vector<float>& getMean() const { return mean_; }

private:
    int original_dim_;
    int target_dim_;
    bool fitted_ = false;
    
    std::vector<float> mean_;
    std::vector<std::vector<float>> components_;  // target_dim x original_dim
    std::vector<float> explained_variance_;
    std::vector<float> explained_variance_ratio_;
    
    /**
     * @brief 计算数据均值
     */
    std::vector<float> computeMean(const std::vector<std::vector<float>>& data) const;
    
    /**
     * @brief 中心化数据
     */
    std::vector<std::vector<float>> centerData(
        const std::vector<std::vector<float>>& data,
        const std::vector<float>& mean) const;
    
    /**
     * @brief 使用幂迭代法计算主成分
     */
    void powerIteration(const std::vector<std::vector<float>>& centered_data,
                        int max_iterations, double tolerance);
};

} // namespace sageFlow
```

## 实现要点

1. **fit()**:
   - 计算数据均值
   - 中心化数据
   - 使用幂迭代法或协方差矩阵特征分解计算主成分

2. **transform()**:
   - 减去均值
   - 与主成分矩阵相乘

3. **powerIteration()**:
   - 迭代计算每个主成分
   - 每次计算后需要去除已有主成分的影响（deflation）

## 测试要求

```cpp
TEST(PCATest, FitAndTransform) {
    PCA pca(128, 32);
    // 生成测试数据并拟合
    // 验证 transform 输出维度正确
}

TEST(PCATest, DistanceLowerBound) {
    // 验证 PCA 距离下界性质
    // ||P*x - P*y|| <= ||x - y||
}

TEST(PCATest, ExplainedVariance) {
    // 验证解释方差比例合理
}
```

## 验收标准
1. 所有单元测试通过
2. PCA 距离下界性质验证通过
3. 性能可接受（1000 样本 128 维 < 1s）
```

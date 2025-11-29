# Task A-07: ComputeEngine SIMD 优化

**优先级**: 🟢 低  
**预估工时**: 2 天  
**依赖**: 无  
**输出文件**:
- `include/compute_engine/simd_distance.h`
- `src/compute_engine/simd_distance.cpp`
- `test/UnitTest/test_simd_distance.cpp`

---

## 任务描述

为现有 ComputeEngine 添加 SIMD 优化的距离计算函数。

---

## 提示词

```
你是 sageFlow 项目的开发者，需要为 ComputeEngine 添加 SIMD 优化。

## 项目背景
sageFlow 是一个 C++20 流式向量处理引擎，遵循以下规范：
- 类名: CamelCase
- 方法名: camelBack
- 成员变量: lower_case_ 带尾部下划线
- 使用 #pragma once 作为头文件保护

## 背景
距离计算是 Join 操作的性能热点，使用 SIMD 指令可以显著提升性能。

## 任务目标
实现 SIMD 加速的距离计算：
1. L2 距离 (SSE/AVX)
2. 余弦相似度 (SSE/AVX)
3. 自动检测 CPU 支持的指令集

## 文件位置
- 头文件: include/compute_engine/simd_distance.h
- 实现文件: src/compute_engine/simd_distance.cpp

## 接口要求

```cpp
#pragma once

#include <vector>
#include <cstddef>

namespace sageFlow {

/**
 * @brief SIMD 加速的距离计算
 */
class SIMDDistance {
public:
    /**
     * @brief 检测支持的 SIMD 指令集
     */
    enum class SIMDLevel {
        NONE,   ///< 无 SIMD 支持
        SSE,    ///< SSE 支持
        AVX,    ///< AVX 支持
        AVX2,   ///< AVX2 支持
        AVX512  ///< AVX-512 支持
    };
    
    /**
     * @brief 获取当前 CPU 支持的 SIMD 级别
     */
    static SIMDLevel detectSIMDLevel();
    
    /**
     * @brief 计算 L2 距离（自动选择最优实现）
     */
    static float l2Distance(const float* a, const float* b, size_t dim);
    
    /**
     * @brief 计算 L2 距离平方（避免 sqrt）
     */
    static float l2DistanceSquared(const float* a, const float* b, size_t dim);
    
    /**
     * @brief 计算余弦相似度
     */
    static float cosineSimilarity(const float* a, const float* b, size_t dim);
    
    /**
     * @brief 批量计算 L2 距离
     * @param query 查询向量
     * @param candidates 候选向量数组
     * @param num_candidates 候选数量
     * @param dim 向量维度
     * @param results 输出距离数组
     */
    static void l2DistanceBatch(const float* query, 
                                const float* const* candidates,
                                size_t num_candidates, size_t dim,
                                float* results);

private:
    // 标量实现
    static float l2DistanceScalar(const float* a, const float* b, size_t dim);
    
    // SSE 实现
    static float l2DistanceSSE(const float* a, const float* b, size_t dim);
    
    // AVX 实现
    static float l2DistanceAVX(const float* a, const float* b, size_t dim);
};

} // namespace sageFlow
```

## 实现要点

1. **detectSIMDLevel()**:
   - 使用 __cpuid 检测 CPU 特性
   - 返回最高支持的 SIMD 级别

2. **l2DistanceAVX()**:
   ```cpp
   static float l2DistanceAVX(const float* a, const float* b, size_t dim) {
       __m256 sum = _mm256_setzero_ps();
       size_t i = 0;
       
       // 每次处理 8 个 float
       for (; i + 8 <= dim; i += 8) {
           __m256 va = _mm256_loadu_ps(a + i);
           __m256 vb = _mm256_loadu_ps(b + i);
           __m256 diff = _mm256_sub_ps(va, vb);
           sum = _mm256_fmadd_ps(diff, diff, sum);  // FMA
       }
       
       // 水平求和
       __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum, 0),
                                   _mm256_extractf128_ps(sum, 1));
       sum128 = _mm_hadd_ps(sum128, sum128);
       sum128 = _mm_hadd_ps(sum128, sum128);
       float result = _mm_cvtss_f32(sum128);
       
       // 处理剩余元素
       for (; i < dim; ++i) {
           float diff = a[i] - b[i];
           result += diff * diff;
       }
       
       return std::sqrt(result);
   }
   ```

3. **l2DistanceSSE()**:
   ```cpp
   static float l2DistanceSSE(const float* a, const float* b, size_t dim) {
       __m128 sum = _mm_setzero_ps();
       size_t i = 0;
       
       // 每次处理 4 个 float
       for (; i + 4 <= dim; i += 4) {
           __m128 va = _mm_loadu_ps(a + i);
           __m128 vb = _mm_loadu_ps(b + i);
           __m128 diff = _mm_sub_ps(va, vb);
           sum = _mm_add_ps(sum, _mm_mul_ps(diff, diff));
       }
       
       // 水平求和
       sum = _mm_hadd_ps(sum, sum);
       sum = _mm_hadd_ps(sum, sum);
       float result = _mm_cvtss_f32(sum);
       
       // 处理剩余元素
       for (; i < dim; ++i) {
           float diff = a[i] - b[i];
           result += diff * diff;
       }
       
       return std::sqrt(result);
   }
   ```

4. **l2Distance() 自动选择**:
   ```cpp
   static float l2Distance(const float* a, const float* b, size_t dim) {
       static SIMDLevel level = detectSIMDLevel();
       
       switch (level) {
           case SIMDLevel::AVX:
           case SIMDLevel::AVX2:
           case SIMDLevel::AVX512:
               return l2DistanceAVX(a, b, dim);
           case SIMDLevel::SSE:
               return l2DistanceSSE(a, b, dim);
           default:
               return l2DistanceScalar(a, b, dim);
       }
   }
   ```

## 测试要求

```cpp
TEST(SIMDDistanceTest, L2DistanceCorrectness) {
    // 验证 SIMD 结果与标量结果一致
    std::vector<float> a(128), b(128);
    // 随机初始化
    
    float scalar = SIMDDistance::l2DistanceScalar(a.data(), b.data(), 128);
    float simd = SIMDDistance::l2Distance(a.data(), b.data(), 128);
    
    EXPECT_NEAR(scalar, simd, 1e-5);
}

TEST(SIMDDistanceTest, CosineSimilarityCorrectness) {
    // 验证余弦相似度计算正确
}

TEST(SIMDDistanceTest, BatchDistance) {
    // 验证批量计算正确
}

TEST(SIMDDistanceTest, Performance) {
    // 性能对比测试
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 100000; ++i) {
        SIMDDistance::l2Distance(a.data(), b.data(), 128);
    }
    auto end = std::chrono::high_resolution_clock::now();
    // 输出性能数据
}

TEST(SIMDDistanceTest, EdgeCases) {
    // 测试非对齐维度 (如 dim=127)
    // 测试小维度 (如 dim=3)
}
```

## 验收标准
1. 所有单元测试通过
2. SIMD 结果与标量结果误差 < 1e-5
3. 性能提升 > 2x（在支持 AVX 的 CPU 上）
```

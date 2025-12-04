#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>

namespace sageFlow {

/**
 * @brief SIMD 加速的距离计算类
 *
 * 提供 L2 距离、余弦相似度等向量距离计算的 SIMD 优化实现。
 * 自动检测 CPU 支持的 SIMD 指令集并选择最优实现。
 */
class SIMDDistance {
 public:
  /**
   * @brief 支持的 SIMD 指令集级别
   */
  enum class SIMDLevel {
    NONE,    ///< 无 SIMD 支持（使用标量实现）
    SSE,     ///< SSE 支持（128位，4个float）
    AVX,     ///< AVX 支持（256位，8个float）
    AVX2,    ///< AVX2 支持（增强的整数运算）
    AVX512   ///< AVX-512 支持（512位，16个float）
  };

  /**
   * @brief 获取当前 CPU 支持的最高 SIMD 级别
   * @return 当前 CPU 支持的 SIMD 级别
   */
  static auto detectSIMDLevel() -> SIMDLevel;

  /**
   * @brief 获取 SIMD 级别的字符串描述
   * @param level SIMD 级别
   * @return 对应的字符串描述
   */
  static auto simdLevelToString(SIMDLevel level) -> const char*;

  /**
   * @brief 计算 L2 距离（欧氏距离）
   *
   * 自动选择最优的 SIMD 实现
   *
   * @param a 第一个向量
   * @param b 第二个向量
   * @param dim 向量维度
   * @return L2 距离值
   */
  static auto l2Distance(const float* a, const float* b, size_t dim) -> float;

  /**
   * @brief 计算 L2 距离的平方（避免开方运算）
   *
   * 在只需要比较距离大小时使用，可节省开方运算的开销
   *
   * @param a 第一个向量
   * @param b 第二个向量
   * @param dim 向量维度
   * @return L2 距离的平方
   */
  static auto l2DistanceSquared(const float* a, const float* b, size_t dim) -> float;

  /**
   * @brief 计算余弦相似度
   *
   * 值域为 [-1, 1]，1 表示完全相同，-1 表示完全相反
   *
   * @param a 第一个向量
   * @param b 第二个向量
   * @param dim 向量维度
   * @return 余弦相似度值
   */
  static auto cosineSimilarity(const float* a, const float* b, size_t dim) -> float;

  /**
   * @brief 计算向量的模长（L2 范数）
   *
   * @param vec 输入向量
   * @param dim 向量维度
   * @return 向量的模长
   */
  static auto vectorNorm(const float* vec, size_t dim) -> float;

  /**
   * @brief 计算两个向量的点积
   *
   * @param a 第一个向量
   * @param b 第二个向量
   * @param dim 向量维度
   * @return 点积值
   */
  static auto dotProduct(const float* a, const float* b, size_t dim) -> float;

  /**
   * @brief 批量计算 L2 距离
   *
   * 计算一个查询向量与多个候选向量之间的 L2 距离
   *
   * @param query 查询向量
   * @param candidates 候选向量数组（指针数组）
   * @param num_candidates 候选向量数量
   * @param dim 向量维度
   * @param results 输出距离数组（需预分配空间）
   */
  static void l2DistanceBatch(const float* query, const float* const* candidates, size_t num_candidates, size_t dim,
                              float* results);

  /**
   * @brief 批量计算余弦相似度
   *
   * 计算一个查询向量与多个候选向量之间的余弦相似度
   *
   * @param query 查询向量
   * @param candidates 候选向量数组（指针数组）
   * @param num_candidates 候选向量数量
   * @param dim 向量维度
   * @param results 输出相似度数组（需预分配空间）
   */
  static void cosineSimilarityBatch(const float* query, const float* const* candidates, size_t num_candidates,
                                    size_t dim, float* results);

 private:
  // 标量实现（备用方案）
  static auto l2DistanceSquaredScalar(const float* a, const float* b, size_t dim) -> float;
  static auto dotProductScalar(const float* a, const float* b, size_t dim) -> float;

#if defined(__SSE__) || defined(_M_X64) || (defined(_M_IX86_FP) && _M_IX86_FP >= 1)
  // SSE 实现
  static auto l2DistanceSquaredSSE(const float* a, const float* b, size_t dim) -> float;
  static auto dotProductSSE(const float* a, const float* b, size_t dim) -> float;
#endif

#if defined(__AVX__) || defined(__AVX2__)
  // AVX 实现
  static auto l2DistanceSquaredAVX(const float* a, const float* b, size_t dim) -> float;
  static auto dotProductAVX(const float* a, const float* b, size_t dim) -> float;
#endif

#if defined(__AVX512F__)
  // AVX-512 实现
  static auto l2DistanceSquaredAVX512(const float* a, const float* b, size_t dim) -> float;
  static auto dotProductAVX512(const float* a, const float* b, size_t dim) -> float;
#endif

  // 缓存检测到的 SIMD 级别
  static SIMDLevel cached_simd_level_;
  static bool simd_level_detected_;
};

}  // namespace sageFlow

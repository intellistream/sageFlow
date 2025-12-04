#include "compute_engine/simd_distance.h"

#include <algorithm>
#include <cmath>
#include <cstring>

// SIMD intrinsics headers
#if defined(__SSE__) || defined(_M_X64) || (defined(_M_IX86_FP) && _M_IX86_FP >= 1)
#include <xmmintrin.h>  // SSE
#include <emmintrin.h>  // SSE2
#endif

#if defined(__SSE3__)
#include <pmmintrin.h>  // SSE3
#endif

#if defined(__SSSE3__)
#include <tmmintrin.h>  // SSSE3
#endif

#if defined(__SSE4_1__)
#include <smmintrin.h>  // SSE4.1
#endif

#if defined(__AVX__) || defined(__AVX2__)
#include <immintrin.h>  // AVX, AVX2, FMA
#endif

#if defined(__AVX512F__)
#include <immintrin.h>  // AVX-512
#endif

// CPU feature detection
#if defined(_MSC_VER)
#include <intrin.h>
#define CPUID(info, x) __cpuidex(info, x, 0)
#elif defined(__GNUC__) || defined(__clang__)
#include <cpuid.h>
#define CPUID(info, x) __cpuid_count(x, 0, info[0], info[1], info[2], info[3])
#endif

namespace sageFlow {

// 静态成员初始化
SIMDDistance::SIMDLevel SIMDDistance::cached_simd_level_ = SIMDLevel::NONE;
bool SIMDDistance::simd_level_detected_ = false;

auto SIMDDistance::detectSIMDLevel() -> SIMDLevel {
  if (simd_level_detected_) {
    return cached_simd_level_;
  }

  SIMDLevel level = SIMDLevel::NONE;

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
  int info[4] = {0};

  // 获取基本 CPUID 信息
  CPUID(info, 0);
  int num_ids = info[0];

  if (num_ids >= 1) {
    CPUID(info, 1);
    bool sse = (info[3] & (1 << 25)) != 0;
    bool avx = (info[2] & (1 << 28)) != 0;

    if (sse) {
      level = SIMDLevel::SSE;
    }

    if (avx) {
      level = SIMDLevel::AVX;
    }
  }

  if (num_ids >= 7) {
    CPUID(info, 7);
    bool avx2 = (info[1] & (1 << 5)) != 0;
    bool avx512f = (info[1] & (1 << 16)) != 0;

    if (avx2 && level >= SIMDLevel::AVX) {
      level = SIMDLevel::AVX2;
    }

    if (avx512f && level >= SIMDLevel::AVX2) {
      level = SIMDLevel::AVX512;
    }
  }
#endif

  cached_simd_level_ = level;
  simd_level_detected_ = true;
  return level;
}

auto SIMDDistance::simdLevelToString(SIMDLevel level) -> const char* {
  switch (level) {
    case SIMDLevel::NONE:
      return "None (Scalar)";
    case SIMDLevel::SSE:
      return "SSE";
    case SIMDLevel::AVX:
      return "AVX";
    case SIMDLevel::AVX2:
      return "AVX2";
    case SIMDLevel::AVX512:
      return "AVX-512";
    default:
      return "Unknown";
  }
}

// ============================================================================
// 标量实现（备用方案）
// ============================================================================

auto SIMDDistance::l2DistanceSquaredScalar(const float* a, const float* b, size_t dim) -> float {
  float sum = 0.0f;
  for (size_t i = 0; i < dim; ++i) {
    float diff = a[i] - b[i];
    sum += diff * diff;
  }
  return sum;
}

auto SIMDDistance::dotProductScalar(const float* a, const float* b, size_t dim) -> float {
  float sum = 0.0f;
  for (size_t i = 0; i < dim; ++i) {
    sum += a[i] * b[i];
  }
  return sum;
}

// ============================================================================
// SSE 实现
// ============================================================================

#if defined(__SSE__) || defined(_M_X64) || (defined(_M_IX86_FP) && _M_IX86_FP >= 1)

auto SIMDDistance::l2DistanceSquaredSSE(const float* a, const float* b, size_t dim) -> float {
  __m128 sum = _mm_setzero_ps();
  size_t i = 0;

  // 每次处理 4 个 float
  for (; i + 4 <= dim; i += 4) {
    __m128 va = _mm_loadu_ps(a + i);
    __m128 vb = _mm_loadu_ps(b + i);
    __m128 diff = _mm_sub_ps(va, vb);
    __m128 sq = _mm_mul_ps(diff, diff);
    sum = _mm_add_ps(sum, sq);
  }

  // 水平求和：将 4 个 float 相加
  // sum = [s0, s1, s2, s3]
  __m128 shuf = _mm_shuffle_ps(sum, sum, _MM_SHUFFLE(2, 3, 0, 1));  // [s1, s0, s3, s2]
  sum = _mm_add_ps(sum, shuf);                                      // [s0+s1, s0+s1, s2+s3, s2+s3]
  shuf = _mm_movehl_ps(shuf, sum);                                  // [s2+s3, s2+s3, ...]
  sum = _mm_add_ss(sum, shuf);                                      // [s0+s1+s2+s3, ...]

  float result = _mm_cvtss_f32(sum);

  // 处理剩余元素
  for (; i < dim; ++i) {
    float diff = a[i] - b[i];
    result += diff * diff;
  }

  return result;
}

auto SIMDDistance::dotProductSSE(const float* a, const float* b, size_t dim) -> float {
  __m128 sum = _mm_setzero_ps();
  size_t i = 0;

  // 每次处理 4 个 float
  for (; i + 4 <= dim; i += 4) {
    __m128 va = _mm_loadu_ps(a + i);
    __m128 vb = _mm_loadu_ps(b + i);
    __m128 prod = _mm_mul_ps(va, vb);
    sum = _mm_add_ps(sum, prod);
  }

  // 水平求和
  __m128 shuf = _mm_shuffle_ps(sum, sum, _MM_SHUFFLE(2, 3, 0, 1));
  sum = _mm_add_ps(sum, shuf);
  shuf = _mm_movehl_ps(shuf, sum);
  sum = _mm_add_ss(sum, shuf);

  float result = _mm_cvtss_f32(sum);

  // 处理剩余元素
  for (; i < dim; ++i) {
    result += a[i] * b[i];
  }

  return result;
}

#endif  // SSE

// ============================================================================
// AVX 实现
// ============================================================================

#if defined(__AVX__) || defined(__AVX2__)

auto SIMDDistance::l2DistanceSquaredAVX(const float* a, const float* b, size_t dim) -> float {
  __m256 sum = _mm256_setzero_ps();
  size_t i = 0;

  // 每次处理 8 个 float
  for (; i + 8 <= dim; i += 8) {
    __m256 va = _mm256_loadu_ps(a + i);
    __m256 vb = _mm256_loadu_ps(b + i);
    __m256 diff = _mm256_sub_ps(va, vb);
#if defined(__FMA__)
    sum = _mm256_fmadd_ps(diff, diff, sum);  // FMA: sum += diff * diff
#else
    __m256 sq = _mm256_mul_ps(diff, diff);
    sum = _mm256_add_ps(sum, sq);
#endif
  }

  // 水平求和：将 256 位寄存器中的 8 个 float 相加
  // 首先将高 128 位和低 128 位相加
  __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum, 0), _mm256_extractf128_ps(sum, 1));

  // 然后对 128 位寄存器进行水平求和
  __m128 shuf = _mm_shuffle_ps(sum128, sum128, _MM_SHUFFLE(2, 3, 0, 1));
  sum128 = _mm_add_ps(sum128, shuf);
  shuf = _mm_movehl_ps(shuf, sum128);
  sum128 = _mm_add_ss(sum128, shuf);

  float result = _mm_cvtss_f32(sum128);

  // 处理剩余元素
  for (; i < dim; ++i) {
    float diff = a[i] - b[i];
    result += diff * diff;
  }

  return result;
}

auto SIMDDistance::dotProductAVX(const float* a, const float* b, size_t dim) -> float {
  __m256 sum = _mm256_setzero_ps();
  size_t i = 0;

  // 每次处理 8 个 float
  for (; i + 8 <= dim; i += 8) {
    __m256 va = _mm256_loadu_ps(a + i);
    __m256 vb = _mm256_loadu_ps(b + i);
#if defined(__FMA__)
    sum = _mm256_fmadd_ps(va, vb, sum);  // FMA: sum += va * vb
#else
    __m256 prod = _mm256_mul_ps(va, vb);
    sum = _mm256_add_ps(sum, prod);
#endif
  }

  // 水平求和
  __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum, 0), _mm256_extractf128_ps(sum, 1));

  __m128 shuf = _mm_shuffle_ps(sum128, sum128, _MM_SHUFFLE(2, 3, 0, 1));
  sum128 = _mm_add_ps(sum128, shuf);
  shuf = _mm_movehl_ps(shuf, sum128);
  sum128 = _mm_add_ss(sum128, shuf);

  float result = _mm_cvtss_f32(sum128);

  // 处理剩余元素
  for (; i < dim; ++i) {
    result += a[i] * b[i];
  }

  return result;
}

#endif  // AVX

// ============================================================================
// AVX-512 实现
// ============================================================================

#if defined(__AVX512F__)

auto SIMDDistance::l2DistanceSquaredAVX512(const float* a, const float* b, size_t dim) -> float {
  __m512 sum = _mm512_setzero_ps();
  size_t i = 0;

  // 每次处理 16 个 float
  for (; i + 16 <= dim; i += 16) {
    __m512 va = _mm512_loadu_ps(a + i);
    __m512 vb = _mm512_loadu_ps(b + i);
    __m512 diff = _mm512_sub_ps(va, vb);
    sum = _mm512_fmadd_ps(diff, diff, sum);  // AVX-512 always has FMA
  }

  // 使用 reduce_add 进行水平求和
  float result = _mm512_reduce_add_ps(sum);

  // 处理剩余元素
  for (; i < dim; ++i) {
    float diff = a[i] - b[i];
    result += diff * diff;
  }

  return result;
}

auto SIMDDistance::dotProductAVX512(const float* a, const float* b, size_t dim) -> float {
  __m512 sum = _mm512_setzero_ps();
  size_t i = 0;

  // 每次处理 16 个 float
  for (; i + 16 <= dim; i += 16) {
    __m512 va = _mm512_loadu_ps(a + i);
    __m512 vb = _mm512_loadu_ps(b + i);
    sum = _mm512_fmadd_ps(va, vb, sum);
  }

  // 使用 reduce_add 进行水平求和
  float result = _mm512_reduce_add_ps(sum);

  // 处理剩余元素
  for (; i < dim; ++i) {
    result += a[i] * b[i];
  }

  return result;
}

#endif  // AVX512

// ============================================================================
// 公共接口实现
// ============================================================================

auto SIMDDistance::l2DistanceSquared(const float* a, const float* b, size_t dim) -> float {
  if (dim == 0) {
    return 0.0f;
  }

  SIMDLevel level = detectSIMDLevel();

  // 根据检测到的 SIMD 级别选择最优实现
  // 注意：需要在编译时启用对应的指令集支持
#if defined(__AVX512F__)
  if (level >= SIMDLevel::AVX512) {
    return l2DistanceSquaredAVX512(a, b, dim);
  }
#endif

#if defined(__AVX__) || defined(__AVX2__)
  if (level >= SIMDLevel::AVX) {
    return l2DistanceSquaredAVX(a, b, dim);
  }
#endif

#if defined(__SSE__) || defined(_M_X64) || (defined(_M_IX86_FP) && _M_IX86_FP >= 1)
  if (level >= SIMDLevel::SSE) {
    return l2DistanceSquaredSSE(a, b, dim);
  }
#endif

  return l2DistanceSquaredScalar(a, b, dim);
}

auto SIMDDistance::l2Distance(const float* a, const float* b, size_t dim) -> float {
  return std::sqrt(l2DistanceSquared(a, b, dim));
}

auto SIMDDistance::dotProduct(const float* a, const float* b, size_t dim) -> float {
  if (dim == 0) {
    return 0.0f;
  }

  SIMDLevel level = detectSIMDLevel();

#if defined(__AVX512F__)
  if (level >= SIMDLevel::AVX512) {
    return dotProductAVX512(a, b, dim);
  }
#endif

#if defined(__AVX__) || defined(__AVX2__)
  if (level >= SIMDLevel::AVX) {
    return dotProductAVX(a, b, dim);
  }
#endif

#if defined(__SSE__) || defined(_M_X64) || (defined(_M_IX86_FP) && _M_IX86_FP >= 1)
  if (level >= SIMDLevel::SSE) {
    return dotProductSSE(a, b, dim);
  }
#endif

  return dotProductScalar(a, b, dim);
}

auto SIMDDistance::vectorNorm(const float* vec, size_t dim) -> float {
  return std::sqrt(dotProduct(vec, vec, dim));
}

auto SIMDDistance::cosineSimilarity(const float* a, const float* b, size_t dim) -> float {
  if (dim == 0) {
    return 0.0f;
  }

  float dot = dotProduct(a, b, dim);
  float norm_a = vectorNorm(a, dim);
  float norm_b = vectorNorm(b, dim);

  // 避免除以零
  if (norm_a < 1e-10f || norm_b < 1e-10f) {
    return 0.0f;
  }

  return dot / (norm_a * norm_b);
}

void SIMDDistance::l2DistanceBatch(const float* query, const float* const* candidates, size_t num_candidates,
                                   size_t dim, float* results) {
  for (size_t i = 0; i < num_candidates; ++i) {
    results[i] = l2Distance(query, candidates[i], dim);
  }
}

void SIMDDistance::cosineSimilarityBatch(const float* query, const float* const* candidates, size_t num_candidates,
                                         size_t dim, float* results) {
  // 预计算 query 的模长以避免重复计算
  float query_norm = vectorNorm(query, dim);

  if (query_norm < 1e-10f) {
    // 如果 query 的模长接近零，所有相似度都设为 0
    std::fill(results, results + num_candidates, 0.0f);
    return;
  }

  for (size_t i = 0; i < num_candidates; ++i) {
    float dot = dotProduct(query, candidates[i], dim);
    float cand_norm = vectorNorm(candidates[i], dim);

    if (cand_norm < 1e-10f) {
      results[i] = 0.0f;
    } else {
      results[i] = dot / (query_norm * cand_norm);
    }
  }
}

}  // namespace sageFlow

#pragma once

#include <cmath>
#include <memory>
#include <vector>

#include "common/data_types.h"
#include "compute_engine/compute_engine.h"

namespace sageFlow {

/**
 * @brief 验证结果
 */
struct VerificationResult {
  uint64_t candidate_uid;
  double distance;
  double similarity;
  bool passed;

  VerificationResult(uint64_t uid, double dist, double sim, bool pass)
      : candidate_uid(uid), distance(dist), similarity(sim), passed(pass) {}
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
  std::vector<VerificationResult> verifyBatch(const VectorRecord& query,
                                              const std::vector<std::unique_ptr<VectorRecord>>& candidates);

  /**
   * @brief 批量验证（只返回通过的）
   * @param query 查询向量
   * @param candidates 候选向量列表（会被移动）
   * @return 通过验证的候选
   */
  std::vector<std::unique_ptr<VectorRecord>> filterCandidates(const VectorRecord& query,
                                                               std::vector<std::unique_ptr<VectorRecord>>&& candidates);

  /**
   * @brief 设置早期终止的维度检查数
   * @param dims 0 表示不使用早期终止
   */
  void setEarlyTerminationDims(int dims) { early_termination_dims_ = dims; }

  /**
   * @brief 获取早期终止的维度检查数
   */
  int getEarlyTerminationDims() const { return early_termination_dims_; }

  /**
   * @brief 获取相似度阈值
   */
  double getThreshold() const { return similarity_threshold_; }

  /**
   * @brief 获取距离阈值
   */
  double getDistanceThreshold() const { return distance_threshold_; }

  /**
   * @brief 获取 alpha 参数
   */
  double getAlpha() const { return alpha_; }

  /**
   * @brief 将距离转换为相似度
   * @param distance 欧氏距离
   * @return 相似度 (0, 1]
   */
  double distanceToSimilarity(double distance) const { return std::exp(-alpha_ * distance); }

  /**
   * @brief 将相似度转换为距离阈值
   * @param similarity 相似度
   * @return 对应的距离阈值
   */
  double similarityToDistance(double similarity) const {
    if (similarity <= 0.0 || similarity > 1.0) {
      return std::numeric_limits<double>::max();
    }
    return -std::log(similarity) / alpha_;
  }

 private:
  double similarity_threshold_;     ///< 相似度阈值
  double alpha_;                    ///< 距离到相似度转换系数
  int early_termination_dims_ = 0;  ///< 早期终止维度数，0 表示不使用
  double distance_threshold_;       ///< 预计算的距离阈值
  ComputeEngine compute_engine_;    ///< 计算引擎

  /**
   * @brief 计算 L2 距离
   * @param a 向量 a
   * @param b 向量 b
   * @return L2 距离
   */
  double computeL2Distance(const VectorRecord& a, const VectorRecord& b);

  /**
   * @brief 早期终止检查：使用前 N 维估计距离下界
   * @param query 查询向量
   * @param candidate 候选向量
   * @return true 表示可以安全拒绝
   */
  bool earlyReject(const VectorRecord& query, const VectorRecord& candidate) const;

  /**
   * @brief 计算部分维度的 L2 距离平方（用于早期终止）
   * @param a 向量 a
   * @param b 向量 b
   * @param dims 要计算的维度数
   * @return 部分维度的 L2 距离平方
   */
  double computePartialL2DistanceSquared(const VectorRecord& a, const VectorRecord& b, int dims) const;
};

}  // namespace sageFlow

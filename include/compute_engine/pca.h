// PCA.h
#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace sageFlow {

/**
 * @brief 主成分分析 (PCA) 工具类
 *
 * 使用幂迭代法计算主成分，适用于中等规模数据。
 * 对于大规模数据建议使用增量 PCA 或随机化 PCA。
 *
 * PCA 具有距离下界性质：||P*x - P*y|| <= ||x - y||
 * 这一性质可用于 HDR-Tree baseline 的候选过滤。
 */
class PCA {
 public:
  /**
   * @brief 构造函数
   * @param original_dim 原始维度
   * @param target_dim 目标维度（主成分数量）
   * @throws std::invalid_argument 如果 target_dim > original_dim 或维度 <= 0
   */
  PCA(int original_dim, int target_dim);

  /**
   * @brief 使用样本数据拟合 PCA
   * @param samples 样本数据 (n_samples x original_dim)
   * @param max_iterations 最大迭代次数
   * @param tolerance 收敛阈值
   * @throws std::invalid_argument 如果样本数量不足或维度不匹配
   */
  void fit(const std::vector<std::vector<float>>& samples, int max_iterations = 100,
           double tolerance = 1e-6);

  /**
   * @brief 投影单个向量到低维空间
   * @param vector 原始向量
   * @return 低维向量
   * @throws std::runtime_error 如果 PCA 未拟合
   * @throws std::invalid_argument 如果向量维度不匹配
   */
  [[nodiscard]] auto transform(const std::vector<float>& vector) const -> std::vector<float>;

  /**
   * @brief 批量投影
   * @param vectors 原始向量列表
   * @return 低维向量列表
   * @throws std::runtime_error 如果 PCA 未拟合
   * @throws std::invalid_argument 如果任意向量维度不匹配
   */
  [[nodiscard]] auto transformBatch(const std::vector<std::vector<float>>& vectors) const
      -> std::vector<std::vector<float>>;

  /**
   * @brief 检查是否已拟合
   * @return 是否已完成拟合
   */
  [[nodiscard]] auto isFitted() const -> bool { return fitted_; }

  /**
   * @brief 获取解释方差比例
   * @return 各主成分的解释方差比例
   * @throws std::runtime_error 如果 PCA 未拟合
   */
  [[nodiscard]] auto getExplainedVarianceRatio() const -> const std::vector<float>&;

  /**
   * @brief 获取主成分矩阵 (target_dim x original_dim)
   * @return 主成分矩阵
   */
  [[nodiscard]] auto getComponents() const -> const std::vector<std::vector<float>>& {
    return components_;
  }

  /**
   * @brief 获取数据均值
   * @return 均值向量
   */
  [[nodiscard]] auto getMean() const -> const std::vector<float>& { return mean_; }

  /**
   * @brief 获取原始维度
   * @return 原始维度
   */
  [[nodiscard]] auto getOriginalDim() const -> int { return original_dim_; }

  /**
   * @brief 获取目标维度
   * @return 目标维度
   */
  [[nodiscard]] auto getTargetDim() const -> int { return target_dim_; }

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
   * @param data 输入数据
   * @return 均值向量
   */
  [[nodiscard]] auto computeMean(const std::vector<std::vector<float>>& data) const
      -> std::vector<float>;

  /**
   * @brief 中心化数据
   * @param data 输入数据
   * @param mean 均值向量
   * @return 中心化后的数据
   */
  [[nodiscard]] auto centerData(const std::vector<std::vector<float>>& data,
                                const std::vector<float>& mean) const
      -> std::vector<std::vector<float>>;

  /**
   * @brief 使用幂迭代法计算主成分
   * @param centered_data 中心化后的数据
   * @param max_iterations 最大迭代次数
   * @param tolerance 收敛阈值
   */
  void powerIteration(const std::vector<std::vector<float>>& centered_data, int max_iterations,
                      double tolerance);

  /**
   * @brief 计算向量的 L2 范数
   * @param vec 输入向量
   * @return L2 范数
   */
  [[nodiscard]] static auto vectorNorm(const std::vector<float>& vec) -> float;

  /**
   * @brief 计算两个向量的点积
   * @param vec1 向量1
   * @param vec2 向量2
   * @return 点积结果
   */
  [[nodiscard]] static auto dotProduct(const std::vector<float>& vec1,
                                       const std::vector<float>& vec2) -> float;

  /**
   * @brief 归一化向量
   * @param vec 输入向量
   * @return 归一化后的向量
   */
  [[nodiscard]] static auto normalizeVector(const std::vector<float>& vec) -> std::vector<float>;

  /**
   * @brief 计算协方差矩阵与向量的乘积 (用于幂迭代)
   * @param centered_data 中心化后的数据
   * @param vec 输入向量
   * @return 结果向量
   */
  [[nodiscard]] static auto covMatrixVectorProduct(
      const std::vector<std::vector<float>>& centered_data, const std::vector<float>& vec)
      -> std::vector<float>;

  /**
   * @brief 从向量中去除某个方向的分量 (deflation)
   * @param vec 输入向量
   * @param direction 要去除的方向
   * @return 去除后的向量
   */
  [[nodiscard]] static auto removeComponent(const std::vector<float>& vec,
                                            const std::vector<float>& direction)
      -> std::vector<float>;
};

}  // namespace sageFlow

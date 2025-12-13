// PCA.cpp
#include "compute_engine/pca.h"

#include <algorithm>
#include <cmath>
#include <numeric>
#include <random>
#include <stdexcept>
#include <iostream>

namespace sageFlow {

PCA::PCA(int original_dim, int target_dim) : original_dim_(original_dim), target_dim_(target_dim) {
  if (original_dim <= 0 || target_dim <= 0) {
    throw std::invalid_argument("Dimensions must be positive");
  }
  if (target_dim > original_dim) {
    throw std::invalid_argument("Target dimension cannot exceed original dimension");
  }
}

void PCA::fit(const std::vector<std::vector<float>>& samples, int max_iterations,
              double tolerance) {
  if (samples.empty()) {
    throw std::invalid_argument("Sample data cannot be empty");
  }

  // 验证样本维度
  for (const auto& sample : samples) {
    if (static_cast<int>(sample.size()) != original_dim_) {
      throw std::invalid_argument("Sample dimension does not match original_dim");
    }
  }

  // 需要足够的样本来计算主成分
  if (static_cast<int>(samples.size()) < target_dim_) {
    throw std::invalid_argument("Number of samples must be at least target_dim");
  }

  // 计算均值
  mean_ = computeMean(samples);

  // 中心化数据
  auto centered_data = centerData(samples, mean_);

  // 使用幂迭代法计算主成分
  powerIteration(centered_data, max_iterations, tolerance);

  fitted_ = true;
}

auto PCA::transform(const std::vector<float>& vector) const -> std::vector<float> {
  if (!fitted_) {
    throw std::runtime_error("PCA has not been fitted yet");
  }

  if (static_cast<int>(vector.size()) != original_dim_) {
    throw std::invalid_argument("Vector dimension does not match original_dim");
  }

  // 中心化向量
  std::vector<float> centered(original_dim_);
  for (int i = 0; i < original_dim_; ++i) {
    centered[i] = vector[i] - mean_[i];
  }

  // 投影到主成分空间
  std::vector<float> result(target_dim_);
  for (int i = 0; i < target_dim_; ++i) {
    result[i] = dotProduct(centered, components_[i]);
  }

  return result;
}

auto PCA::transformBatch(const std::vector<std::vector<float>>& vectors) const
    -> std::vector<std::vector<float>> {
  if (!fitted_) {
    throw std::runtime_error("PCA has not been fitted yet");
  }

  std::vector<std::vector<float>> results;
  results.reserve(vectors.size());

  for (const auto& vec : vectors) {
    results.push_back(transform(vec));
  }

  return results;
}

auto PCA::getExplainedVarianceRatio() const -> const std::vector<float>& {
  if (!fitted_) {
    throw std::runtime_error("PCA has not been fitted yet");
  }
  return explained_variance_ratio_;
}

auto PCA::computeMean(const std::vector<std::vector<float>>& data) const -> std::vector<float> {
  std::vector<float> mean(original_dim_, 0.0F);
  auto n = static_cast<float>(data.size());

  for (const auto& sample : data) {
    for (int i = 0; i < original_dim_; ++i) {
      mean[i] += sample[i];
    }
  }

  for (int i = 0; i < original_dim_; ++i) {
    mean[i] /= n;
  }

  return mean;
}

auto PCA::centerData(const std::vector<std::vector<float>>& data,
                     const std::vector<float>& mean) const -> std::vector<std::vector<float>> {
  std::vector<std::vector<float>> centered;
  centered.reserve(data.size());

  for (const auto& sample : data) {
    std::vector<float> centered_sample(original_dim_);
    for (int i = 0; i < original_dim_; ++i) {
      centered_sample[i] = sample[i] - mean[i];
    }
    centered.push_back(std::move(centered_sample));
  }

  return centered;
}

void PCA::powerIteration(const std::vector<std::vector<float>>& centered_data, int max_iterations,
                         double tolerance) {
  components_.clear();
  components_.reserve(target_dim_);
  explained_variance_.clear();
  explained_variance_.reserve(target_dim_);

  // 复制一份数据用于 deflation
  auto deflated_data = centered_data;

  // 使用随机数生成器初始化
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<float> dist(-1.0F, 1.0F);

  for (int comp = 0; comp < target_dim_; ++comp) {
    // 随机初始化向量
    std::vector<float> eigenvector(original_dim_);
    for (int i = 0; i < original_dim_; ++i) {
      eigenvector[i] = dist(gen);
    }
    eigenvector = normalizeVector(eigenvector);

    float eigenvalue = 0.0F;

    // 幂迭代
    for (int iter = 0; iter < max_iterations; ++iter) {
      // 计算 C * v，其中 C 是协方差矩阵
      auto new_vector = covMatrixVectorProduct(deflated_data, eigenvector);

      // 计算特征值 (Rayleigh quotient)
      float new_eigenvalue = dotProduct(eigenvector, new_vector);

      // 归一化
      new_vector = normalizeVector(new_vector);

      // 检查收敛
      float diff = 0.0F;
      for (int i = 0; i < original_dim_; ++i) {
        diff += (new_vector[i] - eigenvector[i]) * (new_vector[i] - eigenvector[i]);
      }
      diff = std::sqrt(diff);

      eigenvector = std::move(new_vector);
      eigenvalue = new_eigenvalue;

      if (diff < tolerance) {
        break;
      }
    }

    // 保存主成分和方差
    components_.push_back(eigenvector);
    explained_variance_.push_back(eigenvalue);

    // Deflation: 从数据中去除该主成分的影响
    for (auto& sample : deflated_data) {
      float proj = dotProduct(sample, eigenvector);
      for (int i = 0; i < original_dim_; ++i) {
        sample[i] -= proj * eigenvector[i];
      }
    }
  }

  // 计算解释方差比例
  float total_variance = 0.0F;
  for (float var : explained_variance_) {
    total_variance += var;
  }

  explained_variance_ratio_.clear();
  explained_variance_ratio_.reserve(target_dim_);

  if (total_variance > 0.0F) {
    for (float var : explained_variance_) {
      explained_variance_ratio_.push_back(var / total_variance);
    }
  } else {
    // 如果总方差为0，平均分配
    float avg = 1.0F / static_cast<float>(target_dim_);
    for (int i = 0; i < target_dim_; ++i) {
      explained_variance_ratio_.push_back(avg);
    }
  }
}

auto PCA::vectorNorm(const std::vector<float>& vec) -> float {
  float sum = 0.0F;
  for (float v : vec) {
    sum += v * v;
  }
  return std::sqrt(sum);
}

auto PCA::dotProduct(const std::vector<float>& vec1, const std::vector<float>& vec2) -> float {
  float sum = 0.0F;
  for (size_t i = 0; i < vec1.size(); ++i) {
    sum += vec1[i] * vec2[i];
  }
  return sum;
}

auto PCA::normalizeVector(const std::vector<float>& vec) -> std::vector<float> {
  float norm = vectorNorm(vec);
  if (norm < 1e-10F) {
    return vec;
  }

  std::vector<float> result(vec.size());
  for (size_t i = 0; i < vec.size(); ++i) {
    result[i] = vec[i] / norm;
  }
  return result;
}

auto PCA::covMatrixVectorProduct(const std::vector<std::vector<float>>& centered_data,
                                 const std::vector<float>& vec) -> std::vector<float> {
  // 计算 X^T * X * v / n，其中 X 是中心化数据矩阵
  // 等价于 X^T * (X * v) / n

  size_t n_samples = centered_data.size();
  size_t dim = vec.size();

  // 首先计算 X * v (得到 n_samples 维向量)
  std::vector<float> temp(n_samples, 0.0F);
  for (size_t i = 0; i < n_samples; ++i) {
    for (size_t j = 0; j < dim; ++j) {
      temp[i] += centered_data[i][j] * vec[j];
    }
  }

  // 然后计算 X^T * temp (得到 dim 维向量)
  std::vector<float> result(dim, 0.0F);
  for (size_t i = 0; i < n_samples; ++i) {
    for (size_t j = 0; j < dim; ++j) {
      result[j] += centered_data[i][j] * temp[i];
    }
  }

  // 除以样本数量
  auto n = static_cast<float>(n_samples);
  for (size_t j = 0; j < dim; ++j) {
    result[j] /= n;
  }

  return result;
}

auto PCA::removeComponent(const std::vector<float>& vec, const std::vector<float>& direction)
    -> std::vector<float> {
  float proj = dotProduct(vec, direction);
  std::vector<float> result(vec.size());
  for (size_t i = 0; i < vec.size(); ++i) {
    result[i] = vec[i] - proj * direction[i];
  }
  return result;
}

}  // namespace sageFlow

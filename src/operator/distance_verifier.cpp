#include "operator/distance_verifier.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace sageFlow {

DistanceVerifier::DistanceVerifier(double similarity_threshold, double alpha)
    : similarity_threshold_(similarity_threshold), alpha_(alpha), early_termination_dims_(0) {
  // 预计算距离阈值
  distance_threshold_ = similarityToDistance(similarity_threshold_);
}

VerificationResult DistanceVerifier::verify(const VectorRecord& query, const VectorRecord& candidate) {
  // 如果启用了早期终止，先进行快速检查
  if (early_termination_dims_ > 0 && earlyReject(query, candidate)) {
    return VerificationResult(candidate.uid_, std::numeric_limits<double>::max(), 0.0, false);
  }

  // 计算完整的 L2 距离
  double distance = computeL2Distance(query, candidate);
  double similarity = distanceToSimilarity(distance);
  bool passed = similarity >= similarity_threshold_;

  return VerificationResult(candidate.uid_, distance, similarity, passed);
}

std::vector<VerificationResult> DistanceVerifier::verifyBatch(
    const VectorRecord& query, const std::vector<std::unique_ptr<VectorRecord>>& candidates) {
  std::vector<VerificationResult> results;
  results.reserve(candidates.size());

  for (const auto& candidate : candidates) {
    if (candidate != nullptr) {
      results.push_back(verify(query, *candidate));
    }
  }

  return results;
}

std::vector<std::unique_ptr<VectorRecord>> DistanceVerifier::filterCandidates(
    const VectorRecord& query, std::vector<std::unique_ptr<VectorRecord>>&& candidates) {
  std::vector<std::unique_ptr<VectorRecord>> passed_candidates;
  passed_candidates.reserve(candidates.size());

  for (auto& candidate : candidates) {
    if (candidate == nullptr) {
      continue;
    }

    // 如果启用了早期终止，先进行快速检查
    if (early_termination_dims_ > 0 && earlyReject(query, *candidate)) {
      continue;  // 被早期拒绝
    }

    // 计算完整的 L2 距离并验证
    double distance = computeL2Distance(query, *candidate);
    double similarity = distanceToSimilarity(distance);

    if (similarity >= similarity_threshold_) {
      passed_candidates.push_back(std::move(candidate));
    }
  }

  return passed_candidates;
}

double DistanceVerifier::computeL2Distance(const VectorRecord& a, const VectorRecord& b) {
  return compute_engine_.EuclideanDistance(a.data_, b.data_);
}

bool DistanceVerifier::earlyReject(const VectorRecord& query, const VectorRecord& candidate) const {
  // 计算前 N 维的部分距离平方
  double partial_dist_sq = computePartialL2DistanceSquared(query, candidate, early_termination_dims_);

  // 距离阈值的平方
  double threshold_sq = distance_threshold_ * distance_threshold_;

  // 如果部分维度的距离平方已经超过阈值平方，可以安全拒绝
  // 因为 L2 距离满足：部分维度距离 <= 完整距离
  return partial_dist_sq > threshold_sq;
}

double DistanceVerifier::computePartialL2DistanceSquared(const VectorRecord& a, const VectorRecord& b,
                                                          int dims) const {
  const auto& data_a = a.data_;
  const auto& data_b = b.data_;

  if (data_a.dim_ != data_b.dim_) {
    throw std::invalid_argument("Vectors must be of the same dimension");
  }
  if (data_a.type_ != data_b.type_) {
    throw std::invalid_argument("Vectors must be of the same type");
  }

  // 限制维度数不超过实际维度
  int actual_dims = std::min(dims, static_cast<int>(data_a.dim_));
  if (actual_dims <= 0) {
    return 0.0;
  }

  double sum = 0.0;

  // 根据数据类型进行计算
  switch (data_a.type_) {
    case DataType::Float32: {
      auto ptr_a = reinterpret_cast<const float*>(data_a.data_.get());
      auto ptr_b = reinterpret_cast<const float*>(data_b.data_.get());
      for (int i = 0; i < actual_dims; ++i) {
        double diff = static_cast<double>(ptr_a[i]) - static_cast<double>(ptr_b[i]);
        sum += diff * diff;
      }
      break;
    }
    case DataType::Float64: {
      auto ptr_a = reinterpret_cast<const double*>(data_a.data_.get());
      auto ptr_b = reinterpret_cast<const double*>(data_b.data_.get());
      for (int i = 0; i < actual_dims; ++i) {
        double diff = ptr_a[i] - ptr_b[i];
        sum += diff * diff;
      }
      break;
    }
    case DataType::Int8: {
      auto ptr_a = reinterpret_cast<const int8_t*>(data_a.data_.get());
      auto ptr_b = reinterpret_cast<const int8_t*>(data_b.data_.get());
      for (int i = 0; i < actual_dims; ++i) {
        double diff = static_cast<double>(ptr_a[i]) - static_cast<double>(ptr_b[i]);
        sum += diff * diff;
      }
      break;
    }
    case DataType::Int16: {
      auto ptr_a = reinterpret_cast<const int16_t*>(data_a.data_.get());
      auto ptr_b = reinterpret_cast<const int16_t*>(data_b.data_.get());
      for (int i = 0; i < actual_dims; ++i) {
        double diff = static_cast<double>(ptr_a[i]) - static_cast<double>(ptr_b[i]);
        sum += diff * diff;
      }
      break;
    }
    case DataType::Int32: {
      auto ptr_a = reinterpret_cast<const int32_t*>(data_a.data_.get());
      auto ptr_b = reinterpret_cast<const int32_t*>(data_b.data_.get());
      for (int i = 0; i < actual_dims; ++i) {
        double diff = static_cast<double>(ptr_a[i]) - static_cast<double>(ptr_b[i]);
        sum += diff * diff;
      }
      break;
    }
    case DataType::Int64: {
      auto ptr_a = reinterpret_cast<const int64_t*>(data_a.data_.get());
      auto ptr_b = reinterpret_cast<const int64_t*>(data_b.data_.get());
      for (int i = 0; i < actual_dims; ++i) {
        double diff = static_cast<double>(ptr_a[i]) - static_cast<double>(ptr_b[i]);
        sum += diff * diff;
      }
      break;
    }
    default:
      throw std::invalid_argument("Unsupported data type for partial distance calculation");
  }

  return sum;
}

}  // namespace sageFlow

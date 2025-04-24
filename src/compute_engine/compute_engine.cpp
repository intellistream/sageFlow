#include "compute_engine/compute_engine.h"

auto candy::ComputeEngine::Similarity(const VectorData& vec1, const VectorData& vec2) -> double { return 0.0; }

auto candy::ComputeEngine::EuclideanDistance(const VectorData& vec1, const VectorData& vec2) -> double {
  if (vec1.dim_ != vec2.dim_) {
    throw std::invalid_argument("Vectors must be of the same size");
  }
  auto data_ptr1 = reinterpret_cast<float*>(vec1.data_.get());
  auto data_ptr2 = reinterpret_cast<float*>(vec2.data_.get());
  float distance = 0.0;
  for (int i = 0; i < vec1.dim_; ++i) {
    float diff = data_ptr1[i] - data_ptr2[i];
    distance += diff * diff;
  }
  return std::sqrt(distance);
}

auto candy::ComputeEngine::normalizeVector(const VectorData& vec) -> VectorData { return vec; }

auto candy::ComputeEngine::getVectorSquareLength(const VectorData& vec) -> double {
    return 0.0;
}

auto candy::ComputeEngine::dotmultiply(const VectorData& vec1, const VectorData& vec2) -> double {
    return 0.0;
}

candy::ComputeEngine::ComputeEngine() = default;
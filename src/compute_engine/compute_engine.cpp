#include "compute_engine/compute_engine.h"

auto candy::ComputeEngine::Similarity(const VectorData& vec1, const VectorData& vec2) -> double { return 0.0; }

// 私有模板辅助函数
template <typename T>
auto candy::ComputeEngine::EuclideanDistanceImpl(const VectorData& vec1, const VectorData& vec2) -> double {
  // 确保 T 是算术类型
  static_assert(std::is_arithmetic<T>::value, "Template parameter T must be an arithmetic type.");

  auto data_ptr1 = reinterpret_cast<const T*>(vec1.data_.get());
  auto data_ptr2 = reinterpret_cast<const T*>(vec2.data_.get());

  double distance_sq = 0.0;

  for (int i = 0; i < vec1.dim_; ++i) {
    // 将 T 转换为 double 进行计算
    double diff = static_cast<double>(data_ptr1[i]) - static_cast<double>(data_ptr2[i]);
    distance_sq += diff * diff;
  }
  return std::sqrt(distance_sq);
}

auto candy::ComputeEngine::EuclideanDistance(const VectorData& vec1, const VectorData& vec2) -> double {
  if (vec1.dim_ != vec2.dim_) {
    throw std::invalid_argument("Vectors must be of the same size");
  }
  if (vec1.type_ != vec2.type_) {
    throw std::invalid_argument("Vectors must be of the same type");
  }
  auto type = vec1.type_;
  double distance = 0.0;
  switch (type) {
    case DataType::Float32:
      distance = EuclideanDistanceImpl<float>(vec1, vec2);
      break;
    case DataType::Float64:
      distance = EuclideanDistanceImpl<double>(vec1, vec2);
      break;
    case DataType::Int8:
      distance = EuclideanDistanceImpl<int8_t>(vec1, vec2);
      break;
    case DataType::Int16:
      distance = EuclideanDistanceImpl<int16_t>(vec1, vec2);
      break;
    case DataType::Int32:
      distance = EuclideanDistanceImpl<int32_t>(vec1, vec2);
      break;
    case DataType::Int64:
      distance = EuclideanDistanceImpl<int64_t>(vec1, vec2);
      break;
    default:
      throw std::invalid_argument("Unsupported data type");
  }
  return std::sqrt(distance);
}

auto candy::ComputeEngine::normalizeVector(const VectorData& vec) -> VectorData { return vec; }

auto candy::ComputeEngine::getVectorSquareLength(const VectorData& vec) -> double {
    if (vec.dim_ == 0) {
        throw std::invalid_argument("Vector dimension cannot be zero");
    }
    if (vec.type_ != DataType::Float32 && vec.type_ != DataType::Float64) {
        throw std::invalid_argument("Unsupported data type for square length calculation");
    }
    auto data_ptr = reinterpret_cast<const float*>(vec.data_.get());
    long double square_length = 0.0;
    for (int i = 0; i < vec.dim_; ++i) {
        square_length += (double) data_ptr[i] * (double) data_ptr[i];
    }
    return square_length;
    return 0.0;
}

auto candy::ComputeEngine::dotmultiply(const VectorData& vec1, const VectorData& vec2) -> double {
    if (vec1.dim_ != vec2.dim_) {
        throw std::invalid_argument("Vectors must be of the same size");
    }
    if (vec1.type_ != vec2.type_) {
        throw std::invalid_argument("Vectors must be of the same type");
    }
    auto data_ptr1 = reinterpret_cast<const float*>(vec1.data_.get());
    auto data_ptr2 = reinterpret_cast<const float*>(vec2.data_.get());
    double dot_product = 0.0;
    for (int i = 0; i < vec1.dim_; ++i) {
        dot_product += (double) data_ptr1[i] * (double) data_ptr2[i];
    }
    return dot_product;
    return 0.0;
}

candy::ComputeEngine::ComputeEngine() = default;
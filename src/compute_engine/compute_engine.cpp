#include "compute_engine/compute_engine.h"
#include <cmath>
#include <stdexcept>

namespace {
// Helper templates for type-specific calculations
template<typename T>
double dotProduct(const T* vec1, const T* vec2, int32_t dim) {
  double result = 0.0;
  for (int i = 0; i < dim; ++i) {
    result += static_cast<double>(vec1[i]) * static_cast<double>(vec2[i]);
  }
  return result;
}

template<typename T>
double getSquareLength(const T* vec, int32_t dim) {
  double result = 0.0;
  for (int i = 0; i < dim; ++i) {
    double val = static_cast<double>(vec[i]);
    result += val * val;
  }
  return result;
}

// Renamed from normalizeVector to normalize to avoid name conflict
template<typename T>
void normalize(const T* src, T* dst, int32_t dim, double magnitude) {
  for (int i = 0; i < dim; ++i) {
    dst[i] = static_cast<T>(static_cast<double>(src[i]) / magnitude);
  }
}

template<typename T>
double euclideanDistance(const T* vec1, const T* vec2, int32_t dim) {
  double result = 0.0;
  for (int i = 0; i < dim; ++i) {
    double diff = static_cast<double>(vec1[i]) - static_cast<double>(vec2[i]);
    result += diff * diff;
  }
  return std::sqrt(result);
}
}  // namespace

auto candy::ComputeEngine::calcSimilarity(const VectorData& vec1, const VectorData& vec2) -> double {
  if (vec1.dim_ != vec2.dim_) {
    throw std::invalid_argument("Vectors must be of the same dimension for similarity calculation");
  }
  
  if (vec1.type_ != vec2.type_) {
    throw std::invalid_argument("Vectors must be of the same data type for similarity calculation");
  }
  
  // Cosine similarity: dot product of vectors divided by product of their magnitudes
  double dotProduct = DotMultiply(vec1, vec2);
  double magnitude1 = std::sqrt(getVectorSquareLength(vec1));
  double magnitude2 = std::sqrt(getVectorSquareLength(vec2));
  
  // Avoid division by zero
  if (magnitude1 == 0.0 || magnitude2 == 0.0) {
    return 0.0;
  }
  
  return dotProduct / (magnitude1 * magnitude2);
}

auto candy::ComputeEngine::calcEuclideanDistance(const VectorData& vec1, const VectorData& vec2) -> double {
  if (vec1.dim_ != vec2.dim_) {
    throw std::invalid_argument("Vectors must be of the same dimension");
  }
  
  if (vec1.type_ != vec2.type_) {
    throw std::invalid_argument("Vectors must be of the same data type");
  }

  switch (vec1.type_) {
    case DataType::Int8:
      return euclideanDistance(reinterpret_cast<const int8_t*>(vec1.data_.get()),
                               reinterpret_cast<const int8_t*>(vec2.data_.get()),
                               vec1.dim_);
    case DataType::Int16:
      return euclideanDistance(reinterpret_cast<const int16_t*>(vec1.data_.get()),
                               reinterpret_cast<const int16_t*>(vec2.data_.get()),
                               vec1.dim_);
    case DataType::Int32:
      return euclideanDistance(reinterpret_cast<const int32_t*>(vec1.data_.get()),
                               reinterpret_cast<const int32_t*>(vec2.data_.get()),
                               vec1.dim_);
    case DataType::Int64:
      return euclideanDistance(reinterpret_cast<const int64_t*>(vec1.data_.get()),
                               reinterpret_cast<const int64_t*>(vec2.data_.get()),
                               vec1.dim_);
    case DataType::Float32:
      return euclideanDistance(reinterpret_cast<const float*>(vec1.data_.get()),
                               reinterpret_cast<const float*>(vec2.data_.get()),
                               vec1.dim_);
    case DataType::Float64:
      return euclideanDistance(reinterpret_cast<const double*>(vec1.data_.get()),
                               reinterpret_cast<const double*>(vec2.data_.get()),
                               vec1.dim_);
    default:
      throw std::invalid_argument("Unsupported vector data type");
  }
}

auto candy::ComputeEngine::normalizeVector(const VectorData& vec) -> VectorData {
  // Create a new vector with the same dimensions and type
  auto result = VectorData(vec.dim_, vec.type_);
  
  // Calculate the magnitude of the vector
  double magnitude = std::sqrt(getVectorSquareLength(vec));
  
  // Avoid division by zero
  if (magnitude == 0.0) {
    return result; // Return zero vector
  }
  
  // Normalize based on the data type
  switch (vec.type_) {
    case DataType::Int8:
      normalize(reinterpret_cast<const int8_t*>(vec.data_.get()),
               reinterpret_cast<int8_t*>(result.data_.get()),
               vec.dim_, magnitude);
      break;
    case DataType::Int16:
      normalize(reinterpret_cast<const int16_t*>(vec.data_.get()),
               reinterpret_cast<int16_t*>(result.data_.get()),
               vec.dim_, magnitude);
      break;
    case DataType::Int32:
      normalize(reinterpret_cast<const int32_t*>(vec.data_.get()),
               reinterpret_cast<int32_t*>(result.data_.get()),
               vec.dim_, magnitude);
      break;
    case DataType::Int64:
      normalize(reinterpret_cast<const int64_t*>(vec.data_.get()),
               reinterpret_cast<int64_t*>(result.data_.get()),
               vec.dim_, magnitude);
      break;
    case DataType::Float32:
      normalize(reinterpret_cast<const float*>(vec.data_.get()),
               reinterpret_cast<float*>(result.data_.get()),
               vec.dim_, magnitude);
      break;
    case DataType::Float64:
      normalize(reinterpret_cast<const double*>(vec.data_.get()),
               reinterpret_cast<double*>(result.data_.get()),
               vec.dim_, magnitude);
      break;
    default:
      throw std::invalid_argument("Unsupported vector data type");
  }
  
  return result;
}

auto candy::ComputeEngine::getVectorSquareLength(const VectorData& vec) -> double {
  switch (vec.type_) {
    case DataType::Int8:
      return getSquareLength(reinterpret_cast<const int8_t*>(vec.data_.get()), vec.dim_);
    case DataType::Int16:
      return getSquareLength(reinterpret_cast<const int16_t*>(vec.data_.get()), vec.dim_);
    case DataType::Int32:
      return getSquareLength(reinterpret_cast<const int32_t*>(vec.data_.get()), vec.dim_);
    case DataType::Int64:
      return getSquareLength(reinterpret_cast<const int64_t*>(vec.data_.get()), vec.dim_);
    case DataType::Float32:
      return getSquareLength(reinterpret_cast<const float*>(vec.data_.get()), vec.dim_);
    case DataType::Float64:
      return getSquareLength(reinterpret_cast<const double*>(vec.data_.get()), vec.dim_);
    default:
      throw std::invalid_argument("Unsupported vector data type");
  }
}

auto candy::ComputeEngine::DotMultiply(const VectorData& vec1, const VectorData& vec2) -> double {
  if (vec1.dim_ != vec2.dim_) {
    throw std::invalid_argument("Vectors must be of the same dimension for dot product");
  }
  
  if (vec1.type_ != vec2.type_) {
    throw std::invalid_argument("Vectors must be of the same data type for dot product");
  }

  switch (vec1.type_) {
    case DataType::Int8:
      return dotProduct(reinterpret_cast<const int8_t*>(vec1.data_.get()),
                        reinterpret_cast<const int8_t*>(vec2.data_.get()),
                        vec1.dim_);
    case DataType::Int16:
      return dotProduct(reinterpret_cast<const int16_t*>(vec1.data_.get()),
                        reinterpret_cast<const int16_t*>(vec2.data_.get()),
                        vec1.dim_);
    case DataType::Int32:
      return dotProduct(reinterpret_cast<const int32_t*>(vec1.data_.get()),
                        reinterpret_cast<const int32_t*>(vec2.data_.get()),
                        vec1.dim_);
    case DataType::Int64:
      return dotProduct(reinterpret_cast<const int64_t*>(vec1.data_.get()),
                        reinterpret_cast<const int64_t*>(vec2.data_.get()),
                        vec1.dim_);
    case DataType::Float32:
      return dotProduct(reinterpret_cast<const float*>(vec1.data_.get()),
                        reinterpret_cast<const float*>(vec2.data_.get()),
                        vec1.dim_);
    case DataType::Float64:
      return dotProduct(reinterpret_cast<const double*>(vec1.data_.get()),
                        reinterpret_cast<const double*>(vec2.data_.get()),
                        vec1.dim_);
    default:
      throw std::invalid_argument("Unsupported vector data type");
  }
}

candy::ComputeEngine::ComputeEngine() = default;
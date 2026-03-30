// ComputeEngine.h
#pragma once
#include <algorithm>
#include <cmath>
#include <numeric>
#include <stdexcept>

#include "common/data_types.h"

namespace sageFlow {

class ComputeEngine {
 public:
  /**
   * @brief 相似度计算（纯计算，不保存 alpha 状态）
   *
   * 相似度定义：sim = exp(-alpha * L2(vec1, vec2))
   *
   * 说明：alpha 由上层 pipeline/算子配置绑定，调用时必须显式传入。
   */
  auto Similarity(const VectorData& vec1, const VectorData& vec2, double alpha) -> double;

  // Compute Euclidean distance between two VectorRecords
  auto EuclideanDistance(const VectorData &vec1, const VectorData &vec2) -> double;

  // Normalize the data in a VectorRecord
  auto normalizeVector(const VectorData &vec) -> VectorData;

  auto getVectorSquareLength(const VectorData &vec) -> double;

  auto dotmultiply(const VectorData &vec1, const VectorData &vec2) -> double;

  ComputeEngine();
 private:
  template <typename T>
  auto EuclideanDistanceImpl(const VectorData &vec1, const VectorData &vec2) -> double;
};

}  // namespace sageFlow

// ComputeEngine.h
#pragma once
#include <algorithm>
#include <cmath>
#include <numeric>
#include <stdexcept>

#include "common/data_types.h"

namespace candy {

class ComputeEngine {
 public:
  // Calculate cosine similarity between two VectorRecords
  auto calculateSimilarity(const VectorData &vec1, const VectorData &vec2) -> double;

  // Compute Euclidean distance between two VectorRecords
  auto computeEuclideanDistance(const VectorData &vec1, const VectorData &vec2) -> double;

  // Normalize the data in a VectorRecord
  auto normalizeVector(const VectorData &vec) -> VectorData;

  ComputeEngine();  // Prevent instantiation
};

}  // namespace candy

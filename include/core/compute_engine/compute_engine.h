// ComputeEngine.h

#include <core/common/data_types.h>

#include <algorithm>
#include <cmath>
#include <numeric>
#include <stdexcept>

namespace candy {

class ComputeEngine {
 public:
  // Calculate cosine similarity between two VectorRecords
  static auto calculateSimilarity(std::unique_ptr<VectorRecord> &record1,
                                  std::unique_ptr<VectorRecord> &record2) -> double;

  // Compute Euclidean distance between two VectorRecords
  static auto computeEuclideanDistance(std::unique_ptr<VectorRecord> &record1,
                                       std::unique_ptr<VectorRecord> &record2) -> double;

  // Normalize the data in a VectorRecord
  static auto normalizeVector(std::unique_ptr<VectorRecord> &record) -> std::unique_ptr<VectorRecord>;

  // Find top-K VectorRecords based on a scoring function
  static auto findTopK(std::vector<std::unique_ptr<VectorRecord>> &records, size_t k,
                       const std::function<double(std::unique_ptr<VectorRecord>&)> &scorer) -> std::vector<std::unique_ptr<VectorRecord>>;

  // Validate if two VectorRecords have data of the same size
  static void validateEqualSize(std::unique_ptr<VectorRecord> &record1, std::unique_ptr<VectorRecord> &record2);

  ComputeEngine() = delete;  // Prevent instantiation
};

}  // namespace candy

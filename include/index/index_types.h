#pragma once

namespace candy {

/**
 * @brief Enum defining the types of index implementations
 */
enum class IndexType {  // NOLINT
  None,       // No specific index type
  HNSW,       // Hierarchical Navigable Small World index
  BruteForce, // Brute force search index
};

}  // namespace candy
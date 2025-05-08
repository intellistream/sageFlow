#pragma once

#include <cstdint>
#include <memory>
#include <ostream>
#include <istream>

#include "common/base_types.h"

namespace candy {

  // Core data type definitions
enum class DataType {  // NOLINT
  None,
  Int8,
  Int16,
  Int32,
  Int64,
  Float32,
  Float64,
};

// Size of each data type in bytes
// Note: The array indices exactly match the DataType enum values
inline constexpr int DATA_TYPE_SIZE[] = {0, 1, 2, 4, 8, 4, 8};



/**
 * @brief Represents raw vector data with dimension and type information
 */
class VectorData {
 public:
  // Constructors
  VectorData(int32_t dim, DataType type, char *data);
  explicit VectorData(int32_t dim, DataType type);
  VectorData(const VectorData &other);  // Copy constructor
  
  // Move constructor
  VectorData(VectorData&& other) noexcept
      : dim_(other.dim_), type_(other.type_), data_(std::move(other.data_)) {
      other.dim_ = 0;
      other.type_ = DataType::None;
  }

  // Comparison operators
  auto operator==(const VectorData &other) const -> bool;
  auto operator!=(const VectorData &other) const -> bool;

  // Serialization
  auto Serialize(std::ostream &out) const -> bool;
  auto Deserialize(std::istream &in) -> bool;

  // Data members
  int32_t dim_;                   // Dimension of the vector
  DataType type_;                 // Data type of the vector
  std::unique_ptr<char[]> data_;  // Pointer to the vector data
};

}  // namespace candy
#include "common/vector_data.h"
#include <cstring>

namespace candy {

// Implement constructor with raw data
VectorData::VectorData(int32_t dim, DataType type, char *data)
    : dim_(dim), type_(type), data_(std::make_unique<char[]>(dim * DATA_TYPE_SIZE[static_cast<int>(type)])) {
  if (data != nullptr) {
    std::memcpy(data_.get(), data, dim * DATA_TYPE_SIZE[static_cast<int>(type)]);
  }
}

// Implement explicit constructor
VectorData::VectorData(int32_t dim, DataType type)
    : dim_(dim), type_(type), data_(std::make_unique<char[]>(dim * DATA_TYPE_SIZE[static_cast<int>(type)])) {
  // Initialize data to zeros
  std::memset(data_.get(), 0, dim * DATA_TYPE_SIZE[static_cast<int>(type)]);
}

// Implement copy constructor
VectorData::VectorData(const VectorData &other)
    : dim_(other.dim_), type_(other.type_),
      data_(std::make_unique<char[]>(other.dim_ * DATA_TYPE_SIZE[static_cast<int>(other.type_)])) {
  std::memcpy(data_.get(), other.data_.get(), dim_ * DATA_TYPE_SIZE[static_cast<int>(type_)]);
}

// Implement equality operator
auto VectorData::operator==(const VectorData &other) const -> bool {
  if (dim_ != other.dim_ || type_ != other.type_) {
    return false;
  }
  
  return std::memcmp(data_.get(), other.data_.get(), 
                    dim_ * DATA_TYPE_SIZE[static_cast<int>(type_)]) == 0;
}

// Implement inequality operator
auto VectorData::operator!=(const VectorData &other) const -> bool {
  return !(*this == other);
}

// Implement serialization method
auto VectorData::Serialize(std::ostream &out) const -> bool {
  // Write dimension
  out.write(reinterpret_cast<const char*>(&dim_), sizeof(dim_));
  
  // Write type
  int type_val = static_cast<int>(type_);
  out.write(reinterpret_cast<const char*>(&type_val), sizeof(type_val));
  
  // Write data
  int data_size = dim_ * DATA_TYPE_SIZE[static_cast<int>(type_)];
  out.write(data_.get(), data_size);
  
  return out.good();
}

// Implement deserialization method
auto VectorData::Deserialize(std::istream &in) -> bool {
  // Read dimension
  in.read(reinterpret_cast<char*>(&dim_), sizeof(dim_));
  if (!in.good()) return false;
  
  // Read type
  int type_val;
  in.read(reinterpret_cast<char*>(&type_val), sizeof(type_val));
  if (!in.good()) return false;
  type_ = static_cast<DataType>(type_val);
  
  // Allocate memory for data
  int data_size = dim_ * DATA_TYPE_SIZE[static_cast<int>(type_)];
  data_ = std::make_unique<char[]>(data_size);
  
  // Read data
  in.read(data_.get(), data_size);
  
  return in.good();
}

} // namespace candy
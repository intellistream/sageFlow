//
// Created by Pygon on 25-4-9.
//
#include <cstring>
#include <istream>
#include <ostream>

#include "common/data_types.h"

using namespace candy;

// VectorData Implementation
VectorData::VectorData(int32_t dim, DataType type)
    : dim_(dim), type_(type), data_(new char[dim * DATA_TYPE_SIZE[static_cast<int>(type)]]) {}

VectorData::VectorData(int32_t dim, DataType type, char *data)
    : dim_(dim), type_(type), data_(new char[dim * DATA_TYPE_SIZE[static_cast<int>(type)]]) {
  memcpy(data_.get(), data, dim * DATA_TYPE_SIZE[static_cast<int>(type)]);
}

VectorData::VectorData(const VectorData &other) : dim_(other.dim_), type_(other.type_) {
  data_ = std::make_unique<char[]>(dim_ * DATA_TYPE_SIZE[static_cast<int>(type_)]);        // Allocate memory for data
  memcpy(data_.get(), other.data_.get(), dim_ * DATA_TYPE_SIZE[static_cast<int>(type_)]);  // Copy data
}

auto VectorData::operator==(const VectorData &other) const -> bool {
  if (dim_ != other.dim_ || type_ != other.type_) {
    return false;  // Different dimensions or types, can't be equal
  }
  return memcmp(data_.get(), other.data_.get(), dim_ * DATA_TYPE_SIZE[static_cast<int>(type_)]) == 0;  // Compare data
}

auto VectorData::operator!=(const VectorData &other) const -> bool { return !(*this == other); }

auto VectorData::Serialize(std::ostream &out) const -> bool {
  out.write(reinterpret_cast<const char *>(&dim_), sizeof(dim_));
  out.write(reinterpret_cast<const char *>(&type_), sizeof(type_));
  out.write(data_.get(), dim_ * DATA_TYPE_SIZE[static_cast<int>(type_)]);
  return out.good();
}

auto VectorData::Deserialize(std::istream &in) -> bool {
  in.read(reinterpret_cast<char *>(&dim_), sizeof(dim_));
  in.read(reinterpret_cast<char *>(&type_), sizeof(type_));
  data_ = std::make_unique<char[]>(dim_ * DATA_TYPE_SIZE[static_cast<int>(type_)]);
  in.read(data_.get(), dim_ * DATA_TYPE_SIZE[static_cast<int>(type_)]);
  return in.good();
}

// No need to reimplement VectorRecord methods as they're now defined inline in the header
//
// Created by Pygon on 25-4-9.
//
#include "common/data_types.h"
#include <istream>

candy::VectorData::VectorData(const int32_t dim, const DataType type, char* data)
    : dim_(dim), type_(type), data_(data) {}

candy::VectorData::VectorData(const int32_t dim, const DataType type)
    : dim_(dim), type_(type), data_(new char[dim * DATA_TYPE_SIZE[type]]) {}

candy::VectorData::VectorData(const VectorData& other) : dim_(other.dim_), type_(other.type_) {
  data_ = std::make_unique<char[]>(dim_ * DATA_TYPE_SIZE[type_]);        // Allocate memory for data
  memcpy(data_.get(), other.data_.get(), dim_ * DATA_TYPE_SIZE[type_]);  // Copy data
}

auto candy::VectorData::operator==(const VectorData& other) const -> bool {
  if (dim_ != other.dim_) {
    return false;  // Check dimension equality
  }
  return memcmp(data_.get(), other.data_.get(), dim_ * DATA_TYPE_SIZE[type_]) == 0;  // Compare data
}

auto candy::VectorData::operator!=(const VectorData& other) const -> bool { return !(*this == other); }

auto candy::VectorData::Serialize(std::ostream &out) const -> bool {
  out.write(reinterpret_cast<const char*>(&dim_), sizeof(dim_));
  int type_int = static_cast<int>(type_);
  out.write(reinterpret_cast<const char*>(&type_int), sizeof(type_int));
  out.write(data_.get(), dim_ * DATA_TYPE_SIZE[type_]);
  return !out.fail();
}

auto candy::VectorData::Deserialize(std::istream &in) -> bool {
  in.read(reinterpret_cast<char*>(&dim_), sizeof(dim_));
  int type_int;
  in.read(reinterpret_cast<char*>(&type_int), sizeof(type_int));
  type_ = static_cast<DataType>(type_int);
  data_ = std::make_unique<char[]>(dim_ * DATA_TYPE_SIZE[type_]);
  in.read(data_.get(), dim_ * DATA_TYPE_SIZE[type_]);
  return !in.fail();
}

candy::VectorRecord::VectorRecord(uint64_t uid, int64_t timestamp, VectorData &&data)
    : uid_(uid), timestamp_(timestamp), data_(data) {}

candy::VectorRecord::VectorRecord(uint64_t uid, int64_t timestamp, const VectorData &data)
    : uid_(uid), timestamp_(timestamp), data_(data) {}

candy::VectorRecord::VectorRecord(uint64_t uid, int64_t timestamp, int32_t dim, DataType type, char* data)
    : uid_(uid), timestamp_(timestamp), data_(dim, type, data) {}

auto candy::VectorRecord::Serialize(std::ostream &out) const -> bool {
  out.write(reinterpret_cast<const char *>(&uid_), sizeof(uid_));
  out.write(reinterpret_cast<const char *>(&timestamp_), sizeof(timestamp_));
  return data_.Serialize(out);
}

auto candy::VectorRecord::Deserialize(std::istream &in) -> bool {
  in.read(reinterpret_cast<char *>(&uid_), sizeof(uid_));
  in.read(reinterpret_cast<char *>(&timestamp_), sizeof(timestamp_));
  if (in.fail()) {
    return false;
  }
  return data_.Deserialize(in);
}
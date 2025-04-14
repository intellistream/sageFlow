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

bool candy::VectorData::Serialize(std::ostream &out) const {
  out.write(reinterpret_cast<const char*>(&dim_), sizeof(dim_));
  int typeInt = static_cast<int>(type_);
  out.write(reinterpret_cast<const char*>(&typeInt), sizeof(typeInt));
  out.write(data_.get(), dim_ * DATA_TYPE_SIZE[type_]);
  return !out.fail();
}

bool candy::VectorData::Deserialize(std::istream &in) {
  in.read(reinterpret_cast<char*>(&dim_), sizeof(dim_));
  int typeInt;
  in.read(reinterpret_cast<char*>(&typeInt), sizeof(typeInt));
  type_ = static_cast<DataType>(typeInt);
  data_ = std::make_unique<char[]>(dim_ * DATA_TYPE_SIZE[type_]);
  in.read(data_.get(), dim_ * DATA_TYPE_SIZE[type_]);
  return !in.fail();
}

candy::VectorRecord::VectorRecord(const uint64_t& uid, const int64_t& timestamp, VectorData&& data)
    : uid_(uid), timestamp_(timestamp), data_(data) {}

candy::VectorRecord::VectorRecord(const uint64_t& uid, const int64_t& timestamp, const VectorData& data)
    : uid_(uid), timestamp_(timestamp), data_(data) {}

candy::VectorRecord::VectorRecord(const uint64_t& uid, const int64_t& timestamp, int32_t dim, DataType type, char* data)
    : uid_(uid), timestamp_(timestamp), data_(dim, type, data) {}

auto candy::VectorRecord::operator==(const VectorRecord& other) const -> bool {
  return uid_ == other.uid_ && timestamp_ == other.timestamp_ && data_ == other.data_;
}

bool candy::VectorRecord::Serialize(std::ostream &out) const {
  out.write(reinterpret_cast<const char*>(&uid_), sizeof(uid_));
  out.write(reinterpret_cast<const char*>(&timestamp_), sizeof(timestamp_));
  return data_.Serialize(out);
}

bool candy::VectorRecord::Deserialize(std::istream &in) {
  uint64_t uid;
  int64_t ts;
  in.read(reinterpret_cast<char*>(&uid), sizeof(uid));
  in.read(reinterpret_cast<char*>(&ts), sizeof(ts));
  if (in.fail()) {
    return false;
  }
  *const_cast<uint64_t*>(&uid_) = uid;
  *const_cast<int64_t*>(&timestamp_) = ts;
  return data_.Deserialize(in);
}
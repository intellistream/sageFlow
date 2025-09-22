//
// Created by Pygon on 25-4-9.
//
#include "common/data_types.h"
#include <iomanip>
#include <istream>
#include <cstring>

candy::VectorData::VectorData(const int32_t dim, const DataType type, char* data)
    : dim_(dim), type_(type) {
  // Deep copy the incoming buffer to avoid taking ownership of external memory
  const auto bytes = static_cast<size_t>(dim_) * DATA_TYPE_SIZE[type_];
  data_ = std::make_unique<char[]>(bytes);
  if (data != nullptr) {
    std::memcpy(data_.get(), data, bytes);
  } else {
    // Initialize to zero if null provided
    std::memset(data_.get(), 0, bytes);
  }
}

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

void candy::VectorData::printData(std::ostream &os) const {
  if (!data_) {
    os << "Data is null." << std::endl;
    return;
  }

  os << "VectorData (dim: " << dim_ << ", type: ";
  switch (type_) {
    case DataType::Int8:   os << "Int8";   break;
    case DataType::Int16:  os << "Int16";  break;
    case DataType::Int32:  os << "Int32";  break;
    case DataType::Int64:  os << "Int64";  break;
    case DataType::Float32:os << "Float32";break;
    case DataType::Float64:os << "Float64";break;
    case DataType::None:   os << "None";   break;
    default: os << "Unknown"; break;
  }
  os << "): [";

  if (type_ == DataType::None || dim_ == 0) {
    os << "]" << std::endl;
    return;
  }

  for (int32_t i = 0; i < dim_; ++i) {
    switch (type_) {
      case DataType::Int8:
        os << static_cast<int32_t>(reinterpret_cast<int8_t*>(data_.get())[i]);
        break;
      case DataType::Int16:
        os << reinterpret_cast<int16_t*>(data_.get())[i];
        break;
      case DataType::Int32:
        os << reinterpret_cast<int32_t*>(data_.get())[i];
        break;
      case DataType::Int64:
        os << reinterpret_cast<int64_t*>(data_.get())[i];
        break;
      case DataType::Float32:
        os << std::fixed << std::setprecision(6) << reinterpret_cast<float*>(data_.get())[i];
        break;
      case DataType::Float64:
        os << std::fixed << std::setprecision(6) << reinterpret_cast<double*>(data_.get())[i];
        break;
      case DataType::None: // Should not happen due to earlier check
        break;
    }
    if (i < dim_ - 1) {
      os << ", ";
    }
  }
  os << "]" << std::endl;
}

void candy::VectorRecord::printRecord(std::ostream &os) const {
  os << "VectorRecord (uid: " << uid_ << ", timestamp: " << timestamp_ << ")" << std::endl;
  data_.printData(os);
}

candy::VectorRecord::VectorRecord(const uint64_t& uid, const int64_t& timestamp, VectorData&& data)
  : uid_(uid), timestamp_(timestamp), data_(std::move(data)) {}

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
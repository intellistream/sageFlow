//
// Created by Pygon on 25-3-14.
//
#include "stream/data_stream_source/file_stream_source.h"
#include "common/data_types.h"

candy::FileStreamSource::FileStreamSource(std::string name)
    : DataStreamSource(std::move(name), DataStreamSourceType::File) {}

candy::FileStreamSource::FileStreamSource(std::string name, std::string file_path)
    : DataStreamSource(std::move(name), DataStreamSourceType::File), file_path_(std::move(file_path)) {}

void candy::FileStreamSource::Init() {
  records_.clear();
  records_.emplace_back(std::make_unique<VectorRecord>(1, 3, VectorData{3, DataType::Float32, reinterpret_cast<char *>(new float[3]{1, 2, 3})}));
  records_.emplace_back(std::make_unique<VectorRecord>(2, 3, VectorData{3, DataType::Float32, reinterpret_cast<char *>(new float[3]{4, 5, 6})}));
  records_.emplace_back(std::make_unique<VectorRecord>(3, 3, VectorData{3, DataType::Float32, reinterpret_cast<char *>(new float[3]{7, 8, 9})}));
}

auto candy::FileStreamSource::Next() -> std::unique_ptr<VectorRecord> {
  if (records_.empty()) {
    // load from file
  }
  if (records_.empty()) {
    return nullptr;
  }
  auto record = std::move(records_.back());
  records_.pop_back();
  return record;
}
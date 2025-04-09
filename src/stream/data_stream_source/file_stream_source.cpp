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
  records_.emplace_back(std::make_unique<VectorRecord>(1, 1, VectorData(3,DataType::Float32)));
  records_.emplace_back(std::make_unique<VectorRecord>(2, 2, VectorData(3,DataType::Float32)));
  records_.emplace_back(std::make_unique<VectorRecord>(3,3, VectorData(3,DataType::Float32)));
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
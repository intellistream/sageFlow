//
// Created by Pygon on 25-4-30.
//
#include "stream/data_stream_source/simple_stream_source.h"

#include <fstream>
#include <iostream>
#include <utility>

candy::SimpleStreamSource::SimpleStreamSource(std::string name)
    : DataStreamSource(std::move(name), DataStreamSourceType::None) {}

candy::SimpleStreamSource::SimpleStreamSource(std::string name, std::string file_path)
    : DataStreamSource(std::move(name), DataStreamSourceType::None), file_path_(std::move(file_path)) {}

void candy::SimpleStreamSource::Init() {
  if (file_path_.empty()) {
    // No file provided; use in-memory records only
    return;
  }
  std::ifstream file(file_path_, std::ios::binary);
  if (!file.is_open()) {
    std::cerr << "Error opening file: " << file_path_ << std::endl;
    return;
  }
  auto record_cnt = 0;
  file.read(reinterpret_cast<char*>(&record_cnt), sizeof(int32_t));
  records_.reserve(record_cnt);
  for (int i = 0; i < record_cnt; ++i) {
    auto record = std::make_unique<VectorRecord>(0, 0, 0, DataType::None, nullptr);
    if (!record->Deserialize(file)) {
      std::cerr << "Error deserializing record " << i << std::endl;
      break;
    }
    records_.push_back(std::move(record));
  }
  file.close();
}

auto candy::SimpleStreamSource::Next() -> std::unique_ptr<VectorRecord> {
  if (records_.empty()) {
    return nullptr;
  }
  auto record = std::move(records_.back());
  records_.pop_back();
  return record;
}
//
// Created by Pygon on 25-4-30.
//
#include "stream/data_stream_source/simple_stream_source.h"

#include <fstream>
#include <iostream>
#include <utility>
#include "utils/logger.h"

candy::SimpleStreamSource::SimpleStreamSource(std::string name)
    : DataStreamSource(std::move(name), DataStreamSourceType::None) {}

candy::SimpleStreamSource::SimpleStreamSource(std::string name, std::string file_path)
    : DataStreamSource(std::move(name), DataStreamSourceType::None), file_path_(std::move(file_path)) {}

void candy::SimpleStreamSource::Init() {
  if (file_path_.empty()) {
    // 测试环境下可为空：不加载任何记录
    CANDY_LOG_INFO("SOURCE", "SimpleStreamSource empty path name={} ", name_);
    return;
  }
  std::ifstream file(file_path_, std::ios::binary);
  if (!file.is_open()) {
    CANDY_LOG_ERROR("SOURCE", "open_fail path={} ", file_path_);
    return;
  }
  auto record_cnt = 0;
  file.read(reinterpret_cast<char*>(&record_cnt), sizeof(int32_t));
  records_.reserve(record_cnt);
  for (int i = 0; i < record_cnt; ++i) {
    auto record = std::make_unique<VectorRecord>(0, 0, 0, DataType::None, nullptr);
    if (!record->Deserialize(file)) {
      CANDY_LOG_ERROR("SOURCE", "deserialize_fail index={} path={} ", i, file_path_);
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
//
// Created by Pygon on 25-3-14.
//
#include "stream/data_stream_source/file_stream_source.h"

#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>

#include "common/data_types.h"

candy::FileStreamSource::FileStreamSource(std::string name)
    : DataStreamSource(std::move(name), DataStreamSourceType::File) {}

candy::FileStreamSource::FileStreamSource(std::string name, std::string file_path)
    : DataStreamSource(std::move(name), DataStreamSourceType::File), file_path_(std::move(file_path)) {}

void candy::FileStreamSource::Init() {
  running_ = true;
  std::thread([this]() {
    std::ifstream file(file_path_, std::ios::binary);
    if (!file.is_open()) {
      std::cerr << "Error opening file: " << file_path_ << std::endl;
      running_ = false;
      return;
    }
    auto record_cnt = 0;
    file.read(reinterpret_cast<char*>(&record_cnt), sizeof(int32_t));
    auto last_data_time = std::chrono::steady_clock::now();
    while (running_) {
      if (timeout_ms_ > 0) {
        auto elapsed =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - last_data_time)
                .count();
        if (static_cast<uint64_t>(elapsed) > timeout_ms_) {
          std::cout << "FileStreamSource timeout after " << elapsed << " ms" << std::endl;
          break;
        }
      }

      auto record = std::make_unique<VectorRecord>(0, 0, 0, DataType::None, nullptr);
      auto pos = file.tellg();
      if (!record->Deserialize(file)) {
        file.clear();
        file.seekg(pos);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        continue;
      }
      last_data_time = std::chrono::steady_clock::now();

      if (records_.size() >= getBufferSizeLimit() && getBufferSizeLimit() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        continue;
      }

      {
        std::lock_guard<std::mutex> lock(mtx_);
        records_.push_back(std::move(record));
      }
    }
    file.close();
    running_ = false;
  }).detach();
}

auto candy::FileStreamSource::Next() -> std::unique_ptr<VectorRecord> {
  std::lock_guard<std::mutex> lock(mtx_);
  if (records_.empty()) {
    return nullptr;
  }
  auto record = std::move(records_.back());
  records_.pop_back();
  return record;
}
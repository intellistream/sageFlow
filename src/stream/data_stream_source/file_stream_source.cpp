//
// Created by Pygon on 25-3-14.
//
#include "stream/data_stream_source/file_stream_source.h"

#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>
#include "utils/logger.h"

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
      CANDY_LOG_ERROR("SOURCE", "open_fail path={} ", file_path_);
      running_ = false;
      return;
    }
    enum class FileFormat { Unknown, HeaderCount, NoHeader };
    FileFormat fmt = FileFormat::Unknown;

    auto last_data_time = std::chrono::steady_clock::now();

    // Helper: try read one record at current position; on failure restore position
    auto try_read_one = [&file]() -> std::unique_ptr<VectorRecord> {
      auto pos = file.tellg();
      auto record = std::make_unique<VectorRecord>(0, 0, 0, DataType::None, nullptr);
      if (!record->Deserialize(file)) {
        file.clear();
        file.seekg(pos);
        return nullptr;
      }
      return record;
    };

    // Detect format and, if possible, enqueue the first record immediately
    file.clear();
    file.seekg(0, std::ios::beg);
    if (auto rec = try_read_one()) {
      // No header: we successfully parsed a record from offset 0
      fmt = FileFormat::NoHeader;
      last_data_time = std::chrono::steady_clock::now();
      {
        std::lock_guard<std::mutex> lock(mtx_);
        records_.push_back(std::move(rec));
      }
    } else {
      // Try treat first 4 bytes as record count header
      file.clear();
      file.seekg(0, std::ios::beg);
      int32_t dummy_cnt = 0;
      file.read(reinterpret_cast<char*>(&dummy_cnt), sizeof(dummy_cnt));
      if (!file.fail()) {
        fmt = FileFormat::HeaderCount;
      } else {
        // File may not have been fully written yet; keep Unknown and retry in loop
        file.clear();
        file.seekg(0, std::ios::beg);
      }
    }

    while (running_) {
      if (timeout_ms_ > 0) {
        auto elapsed =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - last_data_time)
                .count();
        if (static_cast<uint64_t>(elapsed) > timeout_ms_) {
          CANDY_LOG_WARN("SOURCE", "timeout elapsed_ms={} path={} ", elapsed, file_path_);
          break;
        }
      }

      if (records_.size() >= getBufferSizeLimit() && getBufferSizeLimit() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        continue;
      }

      if (fmt == FileFormat::Unknown) {
        // Retry detection if file was empty initially
        file.clear();
        file.seekg(0, std::ios::beg);
        if (auto rec = try_read_one()) {
          fmt = FileFormat::NoHeader;
          last_data_time = std::chrono::steady_clock::now();
          {
            std::lock_guard<std::mutex> lock(mtx_);
            records_.push_back(std::move(rec));
          }
          continue;
        }
        file.clear();
        file.seekg(0, std::ios::beg);
        int32_t dummy_cnt = 0;
        file.read(reinterpret_cast<char*>(&dummy_cnt), sizeof(dummy_cnt));
        if (!file.fail()) {
          fmt = FileFormat::HeaderCount;
        } else {
          // Still unknown; wait for more data
          file.clear();
          file.seekg(0, std::ios::beg);
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          continue;
        }
      }

      // Read next record based on detected format (both cases simply attempt to deserialize)
      if (auto rec = try_read_one()) {
        last_data_time = std::chrono::steady_clock::now();
        {
          std::lock_guard<std::mutex> lock(mtx_);
          records_.push_back(std::move(rec));
        }
        continue;
      }

      // No new complete record available at current file position
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
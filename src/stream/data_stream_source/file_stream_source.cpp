//
// Created by Pygon on 25-3-14.
//
#include "stream/data_stream_source/file_stream_source.h"

#include "stream/time/watermark_generator.h"
#include "stream/stream_environment.h"

#include <fstream>
#include <iostream>
#include <memory>
#include <spdlog/spdlog.h>
#include <string>
#include <vector>

namespace candy {

FileStreamSource::FileStreamSource(std::string name)
    : DataStreamSource(std::move(name), DataStreamSourceType::File) {}

FileStreamSource::FileStreamSource(std::string name, std::string file_path)
    : DataStreamSource(std::move(name), DataStreamSourceType::File), file_path_(std::move(file_path)) {
    // Initialize the watermark generator
    watermark_generator_ = std::make_unique<WatermarkGenerator>(name, nullptr);
    spdlog::info("FileStreamSource '{}' initialized with file path '{}'", name, file_path);
}

void FileStreamSource::Init() {
    std::ifstream file(file_path_, std::ios::binary);
    if (!file) {
        spdlog::error("Cannot open file: {}", file_path_);
        return;
    }
    spdlog::info("Loading records from file: {}", file_path_);
    watermark_generator_->startGeneration();

    // Read the header to get number of records if present
    int32_t record_count = 0;
    auto header_pos = file.tellg();
    file.read(reinterpret_cast<char*>(&record_count), sizeof(int32_t));
    if (!file) {
      // If header read fails, rewind and try to read records directly
      file.clear();
      file.seekg(header_pos);
      std::cout << "No header found in file, attempting to read records directly" << std::endl;
    } else {
      std::cout << "File contains " << record_count << " records according to header" << std::endl;
    }

    auto last_data_time = std::chrono::steady_clock::now();
    while (running_) {
      if (timeout_ms_ > 0) {
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - last_data_time
        ).count();
        if (static_cast<uint64_t>(elapsed) > timeout_ms_) {
          std::cout << "FileStreamSource timeout after " << elapsed << " ms" << std::endl;
          break;
        }
      }

      auto record = std::make_unique<VectorRecord>(0, 0, 0, DataType::None, nullptr);
      auto pos = file.tellg();
      if (!record->Deserialize(file)) {
        // Handle end of file or read error
        if (file.eof()) {
          std::cout << "Reached end of file" << std::endl;
          break;
        }
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
        updateMaxEventTime(record->timestamp_);

        records_.push_front(std::move(record));  // Use push_front so we can pop_back in FIFO order
      }

    }

    
    // Start the watermark generation if a stream environment is set
    if (stream_environment_) {
        StartWatermarkGeneration();
    }
}

auto FileStreamSource::Next() -> std::unique_ptr<VectorRecord> {
    std::lock_guard<std::mutex> lock(mtx_);
    
    if (records_.empty()) {
        return nullptr;
    }
    
    auto record = std::move(records_.back());
    records_.pop_back();
    
    // Update max event time when returning a record
    // Note: In this implementation, records are processed in reverse order
    // In a real implementation, you might want to preserve the natural order
    if (record) {
        updateMaxEventTime(record->timestamp_);
    }
    
    return record;
}

} // namespace candy
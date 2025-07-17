#include "function/file_source_function.h"

#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>

namespace candy {

void FileSourceFunction::Init() {
  if (running_.load()) {
    return;  // Already initialized
  }
  
  running_ = true;
  finished_ = false;
  
  reader_thread_ = std::thread([this]() {
    std::ifstream file(file_path_, std::ios::binary);
    if (!file.is_open()) {
      std::cerr << "Error opening file: " << file_path_ << std::endl;
      running_ = false;
      finished_ = true;
      return;
    }
    
    auto record_cnt = 0;
    file.read(reinterpret_cast<char*>(&record_cnt), sizeof(int32_t));
    auto last_data_time = std::chrono::steady_clock::now();
    
    while (running_) {
      if (timeout_ms_ > 0) {
        auto elapsed =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - last_data_time)
                .count();
        if (static_cast<uint64_t>(elapsed) > timeout_ms_) {
          std::cout << "FileSourceFunction timeout after " << elapsed << " ms" << std::endl;
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

      if (records_.size() >= buffer_size_limit_ && buffer_size_limit_ > 0) {
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
    finished_ = true;
  });
}

auto FileSourceFunction::Execute(Response &resp) -> Response {
  Response result;
  std::lock_guard<std::mutex> lock(mtx_);
  
  // Return a batch of records (or empty if no data available)
  while (!records_.empty() && result.size() < 100) {  // Batch size of 100
    result.push_back(std::move(records_.back()));
    records_.pop_back();
  }
  
  return result;
}

void FileSourceFunction::Close() {
  running_ = false;
  if (reader_thread_.joinable()) {
    reader_thread_.join();
  }
}

}  // namespace candy

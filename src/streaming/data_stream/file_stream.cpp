#include "streaming/data_stream/file_stream.h"

#include <fstream>
#include <iostream>
#include <thread>
#include <vector>
#include <mutex>
#include <chrono>

#include "proto/message.pb.h"

namespace candy {

FileStream::FileStream(std::string name, uint64_t timeout_ms) 
    : DataStream(std::move(name), DataFlowType::File), timeout_ms_(timeout_ms) {}

FileStream::FileStream(std::string name, std::string file_path, uint64_t timeout_ms)
    : DataStream(std::move(name), DataFlowType::File), 
      file_path_(std::move(file_path)), 
      running_(true),
      timeout_ms_(timeout_ms) {}

FileStream::~FileStream() {
  running_ = false;
}

auto FileStream::Next(RecordOrWatermark &record_or_watermark) -> bool {
  std::lock_guard<std::mutex> lock(this->mtx_);
  if (records_.empty()) {
    return false;
  }
  record_or_watermark = std::move(records_.front());
  records_.pop();
  return true;
}

auto FileStream::SetTimeout(uint64_t timeout_ms) -> void {
  timeout_ms_ = timeout_ms;
}

auto FileStream::Init() -> void {
  std::thread([this]() -> void {
    std::ifstream file(this->file_path_, std::ios::binary);
    if (!file.is_open()) {
      std::cerr << "Error opening file: " << this->file_path_ << std::endl;
      running_.store(false);
      return;
    }

    auto readExactBytes = [](std::ifstream& ifs, char* buffer, std::streamsize totalBytes) -> bool {
      std::streamsize bytesRead = 0;
      while (bytesRead < totalBytes) {
        ifs.read(buffer + bytesRead, totalBytes - bytesRead);
        std::streamsize currentRead = ifs.gcount();
        if (currentRead == 0) {
          if (ifs.eof()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            std::streampos lastPos = ifs.tellg();
            ifs.clear();
            if (ifs.tellg() == lastPos) {
              return false;  // 文件未增长
            }
          } else {
            std::cerr << "Error reading " << totalBytes << " bytes from file" << std::endl;
            return false;  // 读取失败
          }
        } else {
          bytesRead += currentRead;
        }
      }
      return true;
    };

    std::vector<char> line;
    uint64_t size;
    auto last_data_time = std::chrono::steady_clock::now();
    
    while (running_) {
      // 检查是否超时
      if (timeout_ms_ > 0) {
        auto current_time = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            current_time - last_data_time).count();
            
        if (elapsed > timeout_ms_) {
          std::cout << "FileStream timeout: No data received for " << elapsed 
                    << " ms (timeout: " << timeout_ms_ << " ms)" << std::endl;
          break;
        }
      }
      
      VectorMessage message;
      size = 0;
      if (!readExactBytes(file, reinterpret_cast<char*>(&size), sizeof(size))) {
        // 没有读到新数据，但不立即终止，等待超时
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        continue;
      }
      
      // 读到新数据，更新最后数据时间
      last_data_time = std::chrono::steady_clock::now();
      
      line.resize(size);
      assert(size < 1e4);
      if (!readExactBytes(file, &line[0], size)) {
        break;
      }
      if (!message.ParseFromArray(line.data(), size)) {
        std::cerr << "Error parsing message of size " << size << " at offset " << file.tellg() << std::endl;
        break;
      }
      VectorData data(message.data().begin(), message.data().end());
      {
        std::lock_guard<std::mutex> lock(this->mtx_);
        this->records_.push(std::make_unique<VectorRecord>(message.name(), std::move(data), message.timestamp()));
      }
    }
    file.close();
    running_ = false;
  }).detach();
}

}  // namespace candy
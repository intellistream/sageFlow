#include "streaming/data_stream/file_stream.h"

#include <fstream>
#include <iostream>
#include <thread>
#include <vector>

#include "proto/message.pb.h"

namespace candy {

auto FileStream::Next(std::unique_ptr<VectorRecord>& record) -> bool {
  std::lock_guard(this->mtx_);
  if (records_.empty()) {
    return false;
  }
  record = std::move(records_.front());
  records_.pop();
  return true;
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
    while (running_) {
      VectorMessage message;
      size = 0;
      if (!readExactBytes(file, reinterpret_cast<char*>(&size), sizeof(size))) {
        break;
      }
      line.resize(size);
      if (!readExactBytes(file, &line[0], size)) {
        break;
      }
      if (!message.ParseFromArray(line.data(), size)) {
        std::cerr << "Error parsing message of size " << size << " at offset " << file.tellg() << std::endl;
        break;
      }
      VectorData data(message.data().begin(), message.data().end());
      {
        std::lock_guard(this->mtx_);
        this->records_.push(std::make_unique<VectorRecord>(message.name(), std::move(data), message.timestamp()));
      }
    }
    file.close();
    running_ = false;
  }).detach();
}

}  // namespace candy
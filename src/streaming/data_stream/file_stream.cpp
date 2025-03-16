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
      std::cerr << "Error opening file" << std::endl;
      running_.store(false);
      return;
    }
    std::vector<char> line;
    uint64_t size;
    auto readExactBytes = [](std::ifstream& ifs, char* buffer, std::streamsize totalBytes) -> bool {
      std::streamsize bytesRead = 0;
      while (bytesRead < totalBytes) {
        ifs.read(buffer + bytesRead, totalBytes - bytesRead);
        std::streamsize currentRead = ifs.gcount();
        if (currentRead == 0) {
          if (ifs.eof()) {
            // 遇到文件末尾，等待文件增长
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            ifs.clear();  // 清除 eofbit 以继续读取
          } else {
            // 读取失败
            return false;
          }
        } else {
          bytesRead += currentRead;
        }
      }
      return true;
    };
    while (running_) {
      VectorMessage message;
      // while (file.peek() == EOF) {
      //   std::this_thread::sleep_for(std::chrono::milliseconds(100));
      //   file.clear();
      // }
      size = 0;
      readExactBytes(file,reinterpret_cast<char*>(&size), sizeof(size));
      // if (!file.read(reinterpret_cast<char*>(&size), sizeof(size)) || file.gcount() != sizeof(size)) {
      //   std::cerr << "Error reading size" << std::endl;
      //   break;
      // }
      line.resize(size);
      readExactBytes(file,&line[0],size);
      // while (file.peek() == EOF) {
      //   std::this_thread::sleep_for(std::chrono::milliseconds(100));
      //   file.clear();
      // }
      // if (!file.read(&line[0], size) || file.gcount() != size) {
      //   std::cerr << "Error reading data" << std::endl;
      //   break;
      // }

      if (!message.ParseFromArray(line.data(), size)) {
        std::cerr << "Error parsing message" << std::endl;
        break;
      }
      VectorData data(message.data().begin(), message.data().end());
      // std::cerr << "get" << message.timestamp() << std::endl;
      {
        std::lock_guard(this->mtx_);
        this->records_.push(std::make_unique<VectorRecord>(message.name(), std::move(data), message.timestamp()));
      }
    }
    file.close();
    running_ = false;
    return;
  }).detach();
}

}  // namespace candy
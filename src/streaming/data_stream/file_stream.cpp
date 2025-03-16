#include "streaming/data_stream/file_stream.h"
#include "proto/message.pb.h"
#include <thread>
#include <fstream>
#include <iostream>
#include <vector>
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
    VectorMessage message;
    int64_t size;
    while (running_) {
      if (!file.read(reinterpret_cast<char*>(&size), sizeof(size))) {
        std::cerr << "Error reading size" << std::endl;
          break;
      }

      line.resize(size);
      if (!file.read(&line[0], size)) {
        std::cerr << "Error reading data" << std::endl;
          break;
      }

      message.ParseFromArray(line.data(),size);
      VectorData data(message.data().begin(),message.data().end());
      this->records_.emplace(std::make_unique<VectorRecord>(message.name(), std::move(data), message.timestamp()));
      if (file.peek()==EOF) {
        running_=false;
      }
    }
    file.close();
    running_=false;
    std::cerr<<"Ter"<<std::endl;
    return;
  }).detach();
}

}  // namespace candy
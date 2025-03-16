#include "streaming/data_stream/file_stream.h"
#include "proto/message.pb.h"
#include <thread>
#include <fstream>
namespace candy {

auto FileStream::Next(std::unique_ptr<VectorRecord>& record) -> bool {
  if (records_.empty()) {
    return false;
  }
  record = std::move(records_.back());
  records_.pop_back();
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
    std::string line;
    while (running_) {
      uint64_t size;
      while (true) {
        // try {
        //   file.read(reinterpret_cast<char*>(&size), sizeof(size));
        // }
        file.read(reinterpret_cast<char*>(&size), sizeof(size));
        line.resize(size);
        file.read(&line[0], size);
        VectorMessage message;
        message.ParseFromString(line);
        VectorData data(message.data_size());
        std::copy(message.data().begin(), message.data().end(), data.begin());
        auto record = std::make_unique<VectorRecord>(message.name(), std::move(data), message.timestamp());
        if (file.eof()) {
          file.clear();
          std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
      }
    }
    file.close();
  }).detach();
}

}  // namespace candy
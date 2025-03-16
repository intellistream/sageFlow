#include "streaming/data_stream/tcp_stream.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>
#include <thread>

namespace candy {

TcpStream::TcpStream(std::string name) : DataStream(std::move(name), DataFlowType::Tcp) {}

TcpStream::TcpStream(std::string name, std::string ip_address, int port)
    : DataStream(std::move(name), DataFlowType::Tcp), ip_address_(std::move(ip_address)), port_(port), running_(true) {}

TcpStream::~TcpStream() {
  running_ = false;
}

auto TcpStream::Next(std::unique_ptr<VectorRecord>& record) -> bool {
  std::lock_guard<std::mutex> lock(this->mtx_);
  if (records_.empty()) return false;
  record = std::move(records_.front());
  records_.pop();
  return true;
}

auto TcpStream::Init() -> void {
  std::thread([this]() -> void {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
      std::cerr << "Failed to create socket" << std::endl;
      running_ = false;
      return;
    }
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_);
    inet_pton(AF_INET, ip_address_.c_str(), &server_addr.sin_addr);
    if (connect(sockfd, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
      std::cerr << "Failed to connect to " << ip_address_ << ":" << port_ << std::endl;
      close(sockfd);
      running_ = false;
      return;
    }
    auto readExactBytes = [sockfd](char* buffer, std::size_t totalBytes) -> bool {
      std::size_t bytesRead = 0;
      while (bytesRead < totalBytes) {
        ssize_t currentRead = read(sockfd, buffer + bytesRead, totalBytes - bytesRead);
        if (currentRead <= 0) return false;  // 连接关闭或错误
        bytesRead += currentRead;
      }
      return true;
    };
    std::vector<char> line;
    uint64_t size;
    while (running_) {
      VectorMessage message;
      size = 0;
      if (!readExactBytes(reinterpret_cast<char*>(&size), sizeof(size))) break;
      line.resize(size);
      if (!readExactBytes(&line[0], size)) break;
      if (!message.ParseFromArray(line.data(), size)) {
        std::cerr << "Error parsing message of size " << size << std::endl;
        break;
      }
      VectorData data(message.data().begin(), message.data().end());
      {
        std::lock_guard<std::mutex> lock(this->mtx_);
        records_.push(std::make_unique<VectorRecord>(message.name(), std::move(data), message.timestamp()));
      }
    }
    close(sockfd);
    running_ = false;
  }).detach();
}

}  // namespace candy
#include "streaming/data_stream/tcp_stream.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <iostream>
#include <thread>
#include <vector>

#include "proto/message.pb.h"

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
    // 创建 TCP 套接字
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
      std::cerr << "Failed to create socket" << std::endl;
      running_ = false;
      return;
    }

    // 设置非阻塞模式
    int flags = fcntl(sockfd, F_GETFL, 0);
    fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);

    // 配置服务器地址
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_);
    inet_pton(AF_INET, ip_address_.c_str(), &server_addr.sin_addr);

    // 异步连接
    int connect_result = connect(sockfd, (sockaddr*)&server_addr, sizeof(server_addr));
    if (connect_result < 0 && errno != EINPROGRESS) {
      std::cerr << "Failed to initiate connection to " << ip_address_ << ":" << port_ << std::endl;
      close(sockfd);
      running_ = false;
      return;
    }

    // 使用 poll 等待连接完成
    pollfd pfd{};
    pfd.fd = sockfd;
    pfd.events = POLLOUT;
    if (poll(&pfd, 1, 5000) <= 0 || !(pfd.revents & POLLOUT)) {  // 5秒超时
      std::cerr << "Connection timeout or error" << std::endl;
      close(sockfd);
      running_ = false;
      return;
    }

    // 检查连接是否成功
    int so_error;
    socklen_t len = sizeof(so_error);
    getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &so_error, &len);
    if (so_error != 0) {
      std::cerr << "Connection failed: " << strerror(so_error) << std::endl;
      close(sockfd);
      running_ = false;
      return;
    }
    std::cerr << "Connected to " << ip_address_ << ":" << port_ << std::endl;

    // 异步读取函数
    auto readExactBytes = [sockfd,this](char* buffer, std::size_t totalBytes) -> bool {
      std::size_t bytesRead = 0;
      while (bytesRead < totalBytes && running_) {
        pollfd pfd{};
        pfd.fd = sockfd;
        pfd.events = POLLIN;
        int poll_result = poll(&pfd, 1, 1000);  // 1秒超时
        if (poll_result <= 0) {
          if (poll_result == 0) continue;  // 超时，继续等待
          return false;  // 错误
        }
        if (pfd.revents & POLLIN) {
          ssize_t currentRead = read(sockfd, buffer + bytesRead, totalBytes - bytesRead);
          if (currentRead <= 0) {
            if (currentRead == 0 || errno != EAGAIN) return false;  // 连接关闭或错误
            continue;  // EAGAIN，继续等待
          }
          bytesRead += currentRead;
        }
      }
      return bytesRead == totalBytes;
    };

    std::vector<char> line;
    uint64_t size;
    while (running_) {
      VectorMessage message;
      size = 0;
      if (!readExactBytes(reinterpret_cast<char*>(&size), sizeof(size))) {
        if (running_) std::cerr << "Failed to read message size" << std::endl;
        break;
      }
      line.resize(size);
      if (!readExactBytes(line.data(), size)) {
        if (running_) std::cerr << "Failed to read message data" << std::endl;
        break;
      }
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
    std::cerr << "TcpStream thread terminated." << std::endl;
  }).detach();
}

}  // namespace candy
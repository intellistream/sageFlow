#include "streaming/data_stream/tcp_stream.h"

#include <iostream>
#include <thread>
#include <vector>
#include <boost/asio.hpp>

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
  if (records_.empty()) {
    return false;
  }
  record = std::move(records_.front());
  records_.pop();
  return true;
}

auto TcpStream::Init() -> void {
  std::thread([this]() -> void {
    try {
      // 初始化 Boost.Asio 的 io_context 和 socket
      boost::asio::io_context io_context;
      boost::asio::ip::tcp::socket socket(io_context);
      boost::asio::ip::tcp::resolver resolver(io_context);

      // 连接到指定的 IP 地址和端口
      boost::asio::connect(socket, resolver.resolve(this->ip_address_, std::to_string(this->port_)));

      // 定义读取指定字节数的函数
      auto readExactBytes = [&socket](char* buffer, std::size_t totalBytes) -> bool {
        std::size_t bytesRead = 0;
        while (bytesRead < totalBytes) {
          std::size_t currentRead = boost::asio::read(
              socket, boost::asio::buffer(buffer + bytesRead, totalBytes - bytesRead));
          if (currentRead == 0) {
            return false;  // 连接关闭
          }
          bytesRead += currentRead;
        }
        return true;
      };

      std::vector<char> line;
      uint64_t size;
      while (running_) {
        VectorMessage message;
        size = 0;
        // 读取消息大小
        if (!readExactBytes(reinterpret_cast<char*>(&size), sizeof(size))) {
          break;
        }
        line.resize(size);
        // 读取消息内容
        if (!readExactBytes(&line[0], size)) {
          break;
        }
        // 解析消息
        if (!message.ParseFromArray(line.data(), size)) {
          std::cerr << "Error parsing message of size " << size << std::endl;
          break;
        }
        VectorData data(message.data().begin(), message.data().end());
        {
          std::lock_guard<std::mutex> lock(this->mtx_);
          this->records_.push(std::make_unique<VectorRecord>(
              message.name(), std::move(data), message.timestamp()));
        }
      }
      socket.close();
    } catch (const std::exception& e) {
      std::cerr << "Error in TcpStream: " << e.what() << std::endl;
      running_ = false;
    }
    running_ = false;
  }).detach();
}

}  // namespace candy
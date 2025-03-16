#include <gtest/gtest.h>

#include <chrono>
#include <iostream>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "core/common/data_types.h"
#include "proto/message.pb.h"
#include "streaming/data_stream/tcp_stream.h"

namespace candy {

// 辅助函数：启动 TCP 服务器并发送记录
void startTcpServer(const std::vector<std::unique_ptr<VectorRecord>>& records, 
                    const std::string& ip, int port, std::atomic<bool>& running) {
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    std::cerr << "Failed to create server socket" << std::endl;
    return;
  }

  sockaddr_in server_addr{};
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(port);
  if (bind(server_fd, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    std::cerr << "Failed to bind server socket" << std::endl;
    close(server_fd);
    return;
  }
  if (listen(server_fd, 1) < 0) {
    std::cerr << "Failed to listen on server socket" << std::endl;
    close(server_fd);
    return;
  }

  sockaddr_in client_addr{};
  socklen_t client_len = sizeof(client_addr);
  int client_fd = accept(server_fd, (sockaddr*)&client_addr, &client_len);
  if (client_fd < 0) {
    std::cerr << "Failed to accept client connection" << std::endl;
    close(server_fd);
    return;
  }
  std::cerr << "TCP server accepted connection." << std::endl;

  for (const auto& rec : records) {
    VectorMessage msg;
    msg.set_name(rec->id_);
    msg.set_timestamp(rec->timestamp_);
    msg.mutable_data()->Assign(rec->data_->begin(), rec->data_->end());
    uint64_t size = msg.ByteSizeLong();

    // 发送消息大小
    write(client_fd, &size, sizeof(size));
    // 发送序列化消息
    std::vector<char> buffer(size);
    msg.SerializeToArray(buffer.data(), size);
    write(client_fd, buffer.data(), size);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));  // 确保数据发送完成
  close(client_fd);
  close(server_fd);
  running = false;
  std::cerr << "TCP server closed." << std::endl;
}

TEST(SourceTest, TcpStreamTest) {
  std::vector<std::unique_ptr<candy::VectorRecord>> records;
  records.push_back(std::make_unique<candy::VectorRecord>("id1", candy::VectorData(std::vector<float>{1.0, 2.0}), 1));
  records.push_back(std::make_unique<candy::VectorRecord>("id2", candy::VectorData(std::vector<float>{3.0, 4.0}), 2));
  records.push_back(std::make_unique<candy::VectorRecord>("id3", candy::VectorData(std::vector<float>{5.0, 6.0}), 3));

  std::cerr << "Preparing TCP server with 3 records..." << std::endl;
  std::string ip = "127.0.0.1";
  int port = 12345;
  std::atomic<bool> server_running(true);
  std::thread server_thread(startTcpServer, std::ref(records), ip, port, std::ref(server_running));

  std::this_thread::sleep_for(std::chrono::milliseconds(100));  // 等待服务器启动
  candy::TcpStream ts("test_tcp", ip, port);
  ts.Init();
  std::cerr << "TcpStream initialized, starting to read..." << std::endl;

  for (const auto& rec : records) {
    std::unique_ptr<candy::VectorRecord> temp;
    auto start_time = std::chrono::steady_clock::now();
    while (!ts.Next(temp)) {
      if (std::chrono::steady_clock::now() - start_time > std::chrono::seconds(5)) {
        FAIL() << "Timeout waiting for record: " << rec->id_;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_EQ(rec->id_, temp->id_);
    EXPECT_EQ(*rec->data_, *temp->data_);
    EXPECT_EQ(rec->timestamp_, temp->timestamp_);
    std::cerr << "Received record: " << temp->id_ << std::endl;
  }
  server_thread.join();
  std::cerr << "TcpStreamTest completed." << std::endl;
}

TEST(SourceTest, TcpStreamRealtimeTest) {
  const int count = 10000;
  std::vector<std::unique_ptr<candy::VectorRecord>> records;
  for (int i = 0; i < count; i++) {
    records.push_back(std::make_unique<candy::VectorRecord>(
        "id" + std::to_string(i),
        candy::VectorData(std::vector<float>{static_cast<float>(i), 2.0, 3.0}),
        i));
  }

  std::string ip = "127.0.0.1";
  int port = 12346;
  std::atomic<bool> server_running(true);
  std::cerr << "Starting TCP server for real-time test with " << count << " records..." << std::endl;
  std::thread server_thread(startTcpServer, std::ref(records), ip, port, std::ref(server_running));

  std::this_thread::sleep_for(std::chrono::milliseconds(100));  // 等待服务器启动
  candy::TcpStream ts("test_tcp_realtime", ip, port);
  ts.Init();
  std::cerr << "TcpStream initialized for real-time test." << std::endl;

  std::vector<std::unique_ptr<candy::VectorRecord>> recv_records;
  auto start_time = std::chrono::steady_clock::now();
  while (recv_records.size() < count) {
    std::unique_ptr<candy::VectorRecord> temp;
    while (!ts.Next(temp)) {
      if (std::chrono::steady_clock::now() - start_time > std::chrono::seconds(10)) {
        FAIL() << "Timeout waiting for " << count << " records, received: " << recv_records.size();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    recv_records.push_back(std::move(temp));
    if (recv_records.size() % 1000 == 0) {
      std::cerr << "Received " << recv_records.size() << " records." << std::endl;
    }
  }
  EXPECT_EQ(recv_records.size(), count);
  std::cerr << "All " << count << " records received." << std::endl;

  std::sort(recv_records.begin(), recv_records.end(),
            [](const std::unique_ptr<candy::VectorRecord>& a, std::unique_ptr<candy::VectorRecord>& b) -> bool {
              return a->timestamp_ < b->timestamp_;
            });
  for (int i = 0; i < count; i++) {
    EXPECT_EQ(records[i]->timestamp_, recv_records[i]->timestamp_);
    EXPECT_EQ(records[i]->id_, recv_records[i]->id_);
    EXPECT_EQ(*records[i]->data_, *recv_records[i]->data_);
  }
  server_thread.join();
  std::cerr << "TcpStreamRealtimeTest completed." << std::endl;
}

}  // namespace candy

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
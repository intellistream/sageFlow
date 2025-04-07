#include <gtest/gtest.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>

#include "core/common/data_types.h"
#include "proto/message.pb.h"
#include "streaming/data_stream/file_stream.h"

TEST(SourceTest, FileStreamTest) {
  std::vector<std::unique_ptr<candy::VectorRecord>> records;
  records.push_back(std::make_unique<candy::VectorRecord>("id1", candy::VectorData(std::vector<float>{1.0, 2.0}), 1));
  records.push_back(std::make_unique<candy::VectorRecord>("id2", candy::VectorData(std::vector<float>{3.0, 4.0}), 2));
  records.push_back(std::make_unique<candy::VectorRecord>("id3", candy::VectorData(std::vector<float>{5.0, 6.0}), 3));
  std::ofstream file("test.bin");
  for (const auto &rec : records) {
    VectorMessage msg;
    msg.set_name(rec->id_);
    msg.set_timestamp(rec->timestamp_);
    msg.mutable_data()->Assign(rec->data_->begin(), rec->data_->end());

    uint64_t size = msg.ByteSizeLong();
    file.write(reinterpret_cast<char *>(&size), sizeof(size));
    msg.SerializeToOstream(&file);
  }
  file.close();
  std::cerr << "data prepared" << std::endl;
  candy::FileStream fs("test.bin", "test.bin");
  fs.Init();
  for (const auto &rec : records) {
    candy::RecordOrWatermark record_or_watermark;
    while (!fs.Next(record_or_watermark)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // Check if we received a record (not a watermark)
    auto* temp = std::get_if<std::unique_ptr<candy::VectorRecord>>(&record_or_watermark);
    ASSERT_TRUE(temp != nullptr) << "Expected a VectorRecord but received a Watermark";

    EXPECT_EQ(rec->id_, (*temp)->id_);
    EXPECT_EQ(*rec->data_, *((*temp)->data_));
    EXPECT_EQ(rec->timestamp_, (*temp)->timestamp_);
  }
}

TEST(SourceTest, FileStreamRealtimeTest) {
  std::vector<std::unique_ptr<candy::VectorRecord>> records;
  const int count = 1000000;
  for (int i = 0; i < count; i++) {
    records.push_back(std::make_unique<candy::VectorRecord>(
        "randomname",
        candy::VectorData(std::vector<float>{1.0, 2.0, 3.0, 2.0, 2.0, 3.0, 2.0, 2.0, 3.0, 2.0, 2.0, 3.0, 2.0, 2.0, 3.0,
                                             2.0, 2.0, 3.0, 2.0}),
        i));
  }
  candy::FileStream fs("test2", "test2.bin");  // TODO: only work when the name is diff from test1
  fs.Init();
  std::atomic<bool> flag;
  flag = false;
  auto t = std::thread([&records,&flag]() {
    std::ofstream file("test2.bin", std::ios::binary);
    if (!file.is_open()) {
      std::cerr << "FAIL fILE OPEn" << std::endl;
      return;
    }
    flag=true;
    for (const auto &rec : records) {
      VectorMessage msg;
      msg.set_name(rec->id_);
      msg.set_timestamp(rec->timestamp_);
      msg.mutable_data()->Assign(rec->data_->begin(), rec->data_->end());
      uint64_t size = msg.ByteSizeLong();
      file.write(reinterpret_cast<char *>(&size), sizeof(size));
      file.flush();
      msg.SerializeToOstream(&file);
      file.flush();
    }
    file.close();
    std::cerr << "file closed" << std::endl;
  });

  if (!flag) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  std::vector<std::unique_ptr<candy::VectorRecord>> recv_records;
  while (recv_records.size() < count) {
    candy::RecordOrWatermark record_or_watermark;
    while (!fs.Next(record_or_watermark)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    // Check if we received a record (not a watermark)
    auto* temp = std::get_if<std::unique_ptr<candy::VectorRecord>>(&record_or_watermark);
    if (temp != nullptr) {
      recv_records.push_back(std::move(*temp));
    } else {
      // If we received a watermark, just log it and continue
      std::cerr << "Received a watermark: " << std::get<candy::Watermark>(record_or_watermark) << std::endl;
    }
  }
  EXPECT_EQ(recv_records.size(), count);
  std::cerr << "recv completed" << std::endl;
  std::sort(recv_records.begin(), recv_records.end(),
            [](const std::unique_ptr<candy::VectorRecord> &a, std::unique_ptr<candy::VectorRecord> &b) -> bool {
              return a->timestamp_ < b->timestamp_;
            });
  for (int i = 0; i < count; i++) {
    EXPECT_EQ(records[i]->timestamp_, recv_records[i]->timestamp_);
  }
  t.join();
}

TEST(SourceTest, FileStreamFileNotFound) {
  std::cerr << "Testing file not found scenario..." << std::endl;
  candy::FileStream fs("nonexistent", "nonexistent.bin");
  fs.Init();
  candy::RecordOrWatermark record_or_watermark;
  EXPECT_FALSE(fs.Next(record_or_watermark));  // 期望读取失败
  std::cerr << "File not found test completed." << std::endl;
}

TEST(SourceTest, FileStreamEmptyFile) {
  std::cerr << "Testing empty file scenario..." << std::endl;
  std::ofstream file("empty.bin");  // 创建空文件
  file.close();
  candy::FileStream fs("empty", "empty.bin");
  fs.Init();
  candy::RecordOrWatermark record_or_watermark;
  EXPECT_FALSE(fs.Next(record_or_watermark));  // 期望无数据
  std::cerr << "Empty file test completed." << std::endl;
}
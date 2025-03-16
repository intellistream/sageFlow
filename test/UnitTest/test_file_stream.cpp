#include <gtest/gtest.h>

#include <fstream>

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
    for (const auto &v : *rec->data_) {
      msg.add_data(v);
    }
    uint64_t size = msg.ByteSizeLong();
    file.write(reinterpret_cast<char *>(&size), sizeof(size));
    msg.SerializeToOstream(&file);
  }
  file.close();
  candy::FileStream fs("test.bin", "test.bin");
  fs.Init();
  for (const auto &rec : records) {
    std::unique_ptr<candy::VectorRecord> temp;
    fs.Next(std::ref(temp));
    EXPECT_EQ(*rec, *temp);
  }
}
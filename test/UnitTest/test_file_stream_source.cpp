#include <gtest/gtest.h>
#include <fstream>
#include <thread>
#include <filesystem>

#include "stream/data_stream_source/file_stream_source.h"
#include "common/data_types.h"

namespace candy {
TEST(FileStreamSourceTest, BasicLoad) {
  // Create a temporary file with one serialized VectorRecord
  auto test_file = std::filesystem::temp_directory_path() / "test_source.dat";
  {
    std::ofstream out(test_file.string(), std::ios::binary);
    int tmp_arr[2]{10,20};
    VectorData vd(2, DataType::Int32, reinterpret_cast<char*>(tmp_arr));
    VectorRecord record(1, 100, vd);
    record.Serialize(out);
  }

  // Instantiate FileStreamSource and initialize
  FileStreamSource source("TestSource", test_file.string());
  source.Init();

  // Allow background thread to read data
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Retrieve record
  auto rec = source.Next();
  ASSERT_NE(rec, nullptr);
  EXPECT_EQ(rec->uid_, 1u);
  EXPECT_EQ(rec->timestamp_, 100);
  EXPECT_EQ(rec->data_.dim_, 2);
  EXPECT_EQ(rec->data_.type_, DataType::Int32);

  // No more data
  EXPECT_EQ(source.Next(), nullptr);
}

TEST(FileStreamSourceTest, FileNotFound) {
  // Provide invalid file path
  FileStreamSource source("BadPathSource", (std::filesystem::temp_directory_path() / "non_exist_abcdef.bin").string());
  source.Init();
  // Wait a bit
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  // Expect no data
  auto rec = source.Next();
  EXPECT_EQ(rec, nullptr);
}

TEST(FileStreamSourceTest, LargeLoad) {
  auto big_test_file = (std::filesystem::temp_directory_path() / "test_source_large.dat").string();
  {
    std::ofstream out(big_test_file, std::ios::binary);
    for (int i = 0; i < 1500; ++i) {
      float tmp[4]{1.0f * i, 2.0f * i, 3.0f * i, 4.0f * i};
      VectorData vd(4, DataType::Float32, reinterpret_cast<char*>(tmp));
      VectorRecord record(i, i + 10000, vd);
      record.Serialize(out);
    }
  }

  FileStreamSource source("LargeLoadSource", big_test_file);
  source.Init();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  int count = 0;
  while (true) {
    auto rec = source.Next();
    if (!rec) break;
    count++;
  }
  EXPECT_EQ(count, 1500);
}
}  // namespace candy
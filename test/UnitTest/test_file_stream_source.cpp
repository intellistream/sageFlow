#include <gtest/gtest.h>
#include <fstream>
#include <thread>
#include <chrono>
#include <random>
#include <string>
#include <vector>

#include "stream/data_stream_source/file_stream_source.h"
#include "common/data_types.h"

namespace candy {

// Helper function to generate test vector records - matches the format in generate_vector_records.cpp
std::vector<std::unique_ptr<VectorRecord>> generateTestVectorRecords(int count, int64_t base_timestamp) {
    std::vector<std::unique_ptr<VectorRecord>> records;
    records.reserve(count);
    
    // Set up random number generator
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> uid_dist(1, 1000000);
    std::uniform_real_distribution<float> float_dist(-100.0f, 100.0f);
    
    for (int i = 0; i < count; ++i) {
        uint64_t uid = uid_dist(gen);
        int64_t timestamp = base_timestamp + i*10;  // Sequential timestamps with 10ms interval
        int32_t dim = 4;  // Fixed dimension for simplicity
        DataType type = DataType::Float32;  // Fixed type for simplicity
        
        // Create vector data
        auto data = std::make_unique<float[]>(dim);
        for (int j = 0; j < dim; ++j) {
            data[j] = float_dist(gen);
        }
        
        // Create vector record
        records.push_back(std::make_unique<VectorRecord>(
            uid, timestamp, dim, type, reinterpret_cast<char*>(data.release())
        ));
    }
    
    return records;
}

// Helper function to create test file with vector records - same format as generate_vector_records.cpp
void createTestFile(const std::string& filepath, int record_count) {
    std::ofstream file(filepath, std::ios::binary);
    ASSERT_TRUE(file.is_open());
    
    // Get current timestamp
    auto now = std::chrono::high_resolution_clock::now();
    int64_t base_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    
    // Write header (number of records)
    file.write(reinterpret_cast<char*>(&record_count), sizeof(int32_t));
    
    // Generate and write records
    auto records = generateTestVectorRecords(record_count, base_timestamp);
    for (const auto& record : records) {
        ASSERT_TRUE(record->Serialize(file));
    }
    
    file.close();
    return;
}

TEST(FileStreamSourceTest, BasicLoad) {
    // Create test file with header and 10 records
    const char* test_file = "/tmp/test_source.dat";
    const int record_count = 10;
    createTestFile(test_file, record_count);
    
    // Initialize FileStreamSource with the test file
    FileStreamSource source("TestSource", test_file);
    source.Init();
    
    // Allow background thread to process the file
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Read all records
    int count = 0;
    while (auto rec = source.Next()) {
        EXPECT_GT(rec->uid_, 0u);
        EXPECT_GT(rec->timestamp_, 0);
        EXPECT_EQ(rec->data_.dim_, 4);
        EXPECT_EQ(rec->data_.type_, DataType::Float32);
        count++;
    }
    
    // Verify we got all records
    EXPECT_EQ(count, record_count);
}

TEST(FileStreamSourceTest, FileNotFound) {
    // Provide invalid file path
    FileStreamSource source("BadPathSource", "/does/not/exist.bin");
    source.Init();
    
    // Wait for processing attempt
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    // Expect no data
    auto rec = source.Next();
    EXPECT_EQ(rec, nullptr);
}

TEST(FileStreamSourceTest, LargeLoad) {
    // Create a large test file (1500 records)
    const char* big_test_file = "/tmp/test_source_large.dat";
    const int record_count = 1500;
    createTestFile(big_test_file, record_count);
    
    // Initialize FileStreamSource
    FileStreamSource source("LargeLoadSource", big_test_file);
    source.Init();
    
    // Allow sufficient time for processing
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    
    // Read and count all records
    int count = 0;
    while (auto rec = source.Next()) {
        count++;
    }
    
    // Verify all records were read
    EXPECT_EQ(count, record_count);
}


TEST(FileStreamSourceTest, SequentialTimestamps) {
    // Create test file with sequential timestamps
    const char* test_file = "/tmp/test_timestamps.dat";
    const int record_count = 20;
    
    auto now = std::chrono::high_resolution_clock::now();
    int64_t base_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    
    {
        std::ofstream file(test_file, std::ios::binary);
        ASSERT_TRUE(file.is_open());
        
        // Write header
        file.write(reinterpret_cast<const char*>(&record_count), sizeof(int32_t));
        
        // Write records with strictly increasing timestamps
        for (int i = 0; i < record_count; ++i) {
            uint64_t uid = 1000 + i;
            int64_t timestamp = base_timestamp + i * 10;  // 10ms intervals
            
            auto data = std::make_unique<float[]>(4);
            for (int j = 0; j < 4; ++j) {
                data[j] = static_cast<float>(i + j);
            }
            
            VectorRecord record(uid, timestamp, 4, DataType::Float32, 
                               reinterpret_cast<char*>(data.release()));
            
            ASSERT_TRUE(record.Serialize(file));
        }
        file.close();
    }
    
    // Test that records come back with correct timestamps in order
    FileStreamSource source("TimestampSource", test_file);
    source.Init();
    
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    int64_t last_timestamp = 0;
    int count = 0;
    
    while (auto rec = source.Next()) {
        if (count > 0) {
            // Timestamps should be increasing (records come in FIFO order)
            EXPECT_GT(rec->timestamp_, last_timestamp);
            // 10ms interval between records
            EXPECT_EQ(rec->timestamp_, last_timestamp + 10);
        }
        last_timestamp = rec->timestamp_;
        count++;
    }
    
    EXPECT_EQ(count, record_count);
}

}  // namespace candy
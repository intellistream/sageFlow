#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <chrono>
#include <iostream>
#include <cstdlib>  // For std::system

#include "core/common/data_types.h"
#include "proto/message.pb.h"
#include "streaming/data_stream/kafka_stream.h"

namespace candy {

// Helper function to start Kafka broker
void startKafkaBroker() {
  std::cerr << "Starting Kafka broker..." << std::endl;
  int ret = std::system("bin/zookeeper-server-start.sh -daemon config/zookeeper.properties && "
                        "bin/kafka-server-start.sh -daemon config/server.properties");
  if (ret != 0) {
    FAIL() << "Failed to start Kafka broker. Ensure Kafka is installed and configured.";
  }
  std::this_thread::sleep_for(std::chrono::seconds(5));  // Wait for broker to start
  std::cerr << "Kafka broker started." << std::endl;
}

// Helper function to stop Kafka broker
void stopKafkaBroker() {
  std::cerr << "Stopping Kafka broker..." << std::endl;
  std::system("bin/kafka-server-stop.sh && bin/zookeeper-server-stop.sh");
  std::cerr << "Kafka broker stopped." << std::endl;
}

// Mock Kafka producer to simulate sending records to a Kafka topic
void mockKafkaProducer(const std::vector<std::unique_ptr<VectorRecord>>& records, 
                       const std::string& broker, const std::string& topic, 
                       std::atomic<bool>& running) {
  // Simulate a producer sending records to Kafka
  std::cerr << "Mock Kafka producer started for topic: " << topic << std::endl;
  for (const auto& rec : records) {
    if (!running) break;

    // Simulate sending a record to Kafka
    std::this_thread::sleep_for(std::chrono::milliseconds(100));  // Simulate delay
    std::cerr << "Produced record: " << rec->id_ << std::endl;
  }
  running = false;
  std::cerr << "Mock Kafka producer stopped." << std::endl;
}

class KafkaStreamTestEnvironment : public ::testing::Environment {
 public:
  void SetUp() override {
    startKafkaBroker();
  }

  void TearDown() override {
    stopKafkaBroker();
  }
};

TEST(SourceTest, KafkaStreamTest) {
  std::vector<std::unique_ptr<candy::VectorRecord>> records;
  records.push_back(std::make_unique<candy::VectorRecord>("id1", candy::VectorData(std::vector<float>{1.0, 2.0}), 1));
  records.push_back(std::make_unique<candy::VectorRecord>("id2", candy::VectorData(std::vector<float>{3.0, 4.0}), 2));
  records.push_back(std::make_unique<candy::VectorRecord>("id3", candy::VectorData(std::vector<float>{5.0, 6.0}), 3));

  std::string broker = "localhost:9092";
  std::string topic = "test_topic";
  std::atomic<bool> producer_running(true);

  // Check if Kafka broker is reachable
  RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  std::string errstr;
  if (conf->set("bootstrap.servers", broker, errstr) != RdKafka::Conf::CONF_OK) {
    delete conf;
    FAIL() << "Kafka broker is not reachable: " << errstr;
  }
  delete conf;

  std::thread producer_thread(mockKafkaProducer, std::ref(records), broker, topic, std::ref(producer_running));

  std::this_thread::sleep_for(std::chrono::milliseconds(100));  // Wait for producer to start
  candy::KafkaStream ks("test_kafka", broker, topic);
  ks.Init();
  std::cerr << "KafkaStream initialized, starting to read..." << std::endl;

  for (const auto& rec : records) {
    candy::RecordOrWatermark record_or_watermark;
    auto start_time = std::chrono::steady_clock::now();
    while (!ks.Next(record_or_watermark)) {
      if (std::chrono::steady_clock::now() - start_time > std::chrono::seconds(5)) {
        FAIL() << "Timeout waiting for record: " << rec->id_;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    // Check if we received a record (not a watermark)
    auto* temp = std::get_if<std::unique_ptr<candy::VectorRecord>>(&record_or_watermark);
    ASSERT_TRUE(temp != nullptr) << "Expected a VectorRecord but received a Watermark";
    
    EXPECT_EQ(rec->id_, (*temp)->id_);
    EXPECT_EQ(*rec->data_, *((*temp)->data_));
    EXPECT_EQ(rec->timestamp_, (*temp)->timestamp_);
    std::cerr << "Received record: " << (*temp)->id_ << std::endl;
  }
  producer_thread.join();
  std::cerr << "KafkaStreamTest completed." << std::endl;
}

TEST(SourceTest, KafkaStreamRealtimeTest) {
  const int count = 10000;
  std::vector<std::unique_ptr<candy::VectorRecord>> records;
  for (int i = 0; i < count; i++) {
    records.push_back(std::make_unique<candy::VectorRecord>(
        "id" + std::to_string(i),
        candy::VectorData(std::vector<float>{static_cast<float>(i), 2.0, 3.0}),
        i));
  }

  std::string broker = "localhost:9092";
  std::string topic = "test_realtime_topic";
  std::atomic<bool> producer_running(true);

  std::cerr << "Starting mock Kafka producer for real-time test with " << count << " records..." << std::endl;
  std::thread producer_thread(mockKafkaProducer, std::ref(records), broker, topic, std::ref(producer_running));

  std::this_thread::sleep_for(std::chrono::milliseconds(100));  // Wait for producer to start
  candy::KafkaStream ks("test_kafka_realtime", broker, topic);
  ks.Init();
  std::cerr << "KafkaStream initialized for real-time test." << std::endl;

  std::vector<std::unique_ptr<candy::VectorRecord>> recv_records;
  auto start_time = std::chrono::steady_clock::now();
  while (recv_records.size() < count) {
    candy::RecordOrWatermark record_or_watermark;
    while (!ks.Next(record_or_watermark)) {
      if (std::chrono::steady_clock::now() - start_time > std::chrono::seconds(10)) {
        FAIL() << "Timeout waiting for " << count << " records, received: " << recv_records.size();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    // Check if we received a record (not a watermark)
    auto* temp = std::get_if<std::unique_ptr<candy::VectorRecord>>(&record_or_watermark);
    if (temp != nullptr) {
      recv_records.push_back(std::move(*temp));
      if (recv_records.size() % 1000 == 0) {
        std::cerr << "Received " << recv_records.size() << " records." << std::endl;
      }
    } else {
      // If we received a watermark, just log it and continue
      std::cerr << "Received a watermark: " << std::get<candy::Watermark>(record_or_watermark) << std::endl;
    }
  }
  EXPECT_EQ(recv_records.size(), count);
  std::cerr << "All " << count << " records received." << std::endl;

  std::sort(recv_records.begin(), recv_records.end(),
            [](const std::unique_ptr<candy::VectorRecord>& a, const std::unique_ptr<candy::VectorRecord>& b) -> bool {
              return a->timestamp_ < b->timestamp_;
            });
  for (int i = 0; i < count; i++) {
    EXPECT_EQ(records[i]->timestamp_, recv_records[i]->timestamp_);
    EXPECT_EQ(records[i]->id_, recv_records[i]->id_);
    EXPECT_EQ(*records[i]->data_, *recv_records[i]->data_);
  }
  producer_thread.join();
  std::cerr << "KafkaStreamRealtimeTest completed." << std::endl;
}

TEST(KafkaStreamTest, ConsumeMessages) {
  candy::KafkaStream kafkaStream("test_stream", "localhost:9092", "test_topic");
  kafkaStream.Init();

  candy::RecordOrWatermark record_or_watermark;
  bool success = false;

  // Wait for up to 5 seconds for a record
  for (int i = 0; i < 50; ++i) {
    if (kafkaStream.Next(record_or_watermark)) {
      // Check if we received a record (not a watermark)
      auto* record = std::get_if<std::unique_ptr<candy::VectorRecord>>(&record_or_watermark);
      if (record != nullptr) {
        success = true;
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  EXPECT_TRUE(success) << "Timeout waiting for record";
}

}  // namespace candy

// Register the test environment to start and stop the Kafka broker
::testing::Environment* const kafka_env = ::testing::AddGlobalTestEnvironment(new candy::KafkaStreamTestEnvironment);

#include <gtest/gtest.h>
#include "execution/partitioner.h"
#include "execution/result_partition.h"
#include "execution/blocking_queue.h"
#include "common/data_types.h"
#include "test_utils/test_data_adapter.h"
#include <memory>
#include <vector>

namespace sageFlow {
namespace test {

// Test that KeyPartitioner uses timestamp for consistent hashing
TEST(PartitionerTest, KeyPartitionerTimestampBased) {
    KeyPartitioner partitioner;
    
    // Create test records with same timestamp
    std::vector<float> data = {1.0f, 2.0f, 3.0f};
    auto record1 = createVectorRecord(12345, 1000, data);
    auto record2 = createVectorRecord(67890, 1000, data);  // Different UID, same timestamp
    
    Response response1{ResponseType::Record, std::move(record1)};
    Response response2{ResponseType::Record, std::move(record2)};
    
    // Same timestamp should map to same partition
    size_t partition1 = partitioner.partition(response1, 4);
    size_t partition2 = partitioner.partition(response2, 4);
    
    EXPECT_EQ(partition1, partition2);
}

// Test that KeyPartitioner distributes different timestamps
TEST(PartitionerTest, KeyPartitionerDifferentTimestamps) {
    KeyPartitioner partitioner;
    
    std::vector<float> data = {1.0f, 2.0f, 3.0f};
    std::vector<size_t> partitions;
    
    // Records with different timestamps should distribute across partitions
    for (int i = 0; i < 100; ++i) {
        auto record = createVectorRecord(i, 1000 + i * 10, data);
        Response response{ResponseType::Record, std::move(record)};
        partitions.push_back(partitioner.partition(response, 4));
    }
    
    // Should use at least 2 of the 4 partitions (allowing for hash collisions)
    std::set<size_t> unique_partitions(partitions.begin(), partitions.end());
    EXPECT_GE(unique_partitions.size(), 2);
    
    // Verify partitions are valid
    for (size_t p : partitions) {
        EXPECT_LT(p, 4);
    }
}

// Test that RoundRobinPartitioner distributes evenly
TEST(PartitionerTest, RoundRobinDistribution) {
    RoundRobinPartitioner partitioner;
    
    std::vector<size_t> counts(4, 0);
    std::vector<float> data = {1.0f, 2.0f, 3.0f};
    
    for (int i = 0; i < 100; ++i) {
        auto record = createVectorRecord(i, 1000 + i, data);
        Response response{ResponseType::Record, std::move(record)};
        
        size_t partition = partitioner.partition(response, 4);
        EXPECT_LT(partition, 4);
        counts[partition]++;
    }
    
    // Each partition should get exactly 25 records
    for (size_t count : counts) {
        EXPECT_EQ(count, 25);
    }
}

// Test standard partitioning in ResultPartition
TEST(ResultPartitionTest, StandardPartitioning) {
    ResultPartition partition;
    
    // Create multiple queues (store raw pointers for later access)
    std::vector<std::shared_ptr<BlockingQueue>> raw_queues;
    std::vector<QueuePtr> queues;
    for (int i = 0; i < 3; ++i) {
        auto queue = std::make_shared<BlockingQueue>(10);
        raw_queues.push_back(queue);
        queues.push_back(queue);
    }
    
    // Setup with round-robin partitioner
    auto roundrobin = std::make_unique<RoundRobinPartitioner>();
    partition.setup(std::move(roundrobin), std::move(queues), 0);
    
    // Emit multiple records
    std::vector<float> data = {1.0f, 2.0f, 3.0f};
    
    for (int i = 0; i < 9; ++i) {
        auto record = createVectorRecord(i, 1000 + i, data);
        Response response{ResponseType::Record, std::move(record)};
        partition.emit(std::move(response), 0);
    }
    
    // Stop queues to prevent blocking pop
    for (auto& q : raw_queues) {
        q->stop();
    }
    
    // Count records in each queue
    std::vector<int> queue_counts(3, 0);
    for (size_t i = 0; i < 3; ++i) {
        while (true) {
            auto tagged = raw_queues[i]->pop();
            if (!tagged.has_value()) break;
            queue_counts[i]++;
        }
    }
    
    // Each queue should have exactly 3 records (9 records / 3 queues)
    for (int count : queue_counts) {
        EXPECT_EQ(count, 3);
    }
}

} // namespace test
} // namespace sageFlow

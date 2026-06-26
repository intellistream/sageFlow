#include <gtest/gtest.h>
#include "execution/partitioner.h"
#include "execution/result_partition.h"
#include "execution/blocking_queue.h"
#include "common/data_types.h"
#include "test_utils/test_data_adapter.h"
#include <memory>
#include <set>
#include <vector>

namespace sageFlow {
namespace test {

namespace {

RecordView makeRecordView(uint64_t uid, int64_t timestamp,
                          const std::vector<float>& data) {
    return RecordView(createVectorRecord(uid, timestamp, data));
}

Response makePairResponse(const RecordView& left, const RecordView& right,
                          double similarity = 0.9) {
    auto payload = std::make_unique<RecordPairPayload>(left, right, similarity);
    return Response{ResponseType::RecordPair, std::move(payload)};
}

}  // namespace

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

// LSHPartitionerAdapter: 相同向量应落在同一分区
TEST(PartitionerTest, LSHPartitionerAdapterStableHash) {
    auto vsp = std::make_shared<LSHPartitioner>(4, 4, 123, 0.05);
    LSHPartitionerAdapter adapter(vsp);

    std::vector<float> data = {0.2f, -0.1f, 0.5f, 0.3f};
    auto record1 = createVectorRecord(1, 1000, data);
    auto record2 = createVectorRecord(2, 1005, data);

    Response response1{ResponseType::Record, std::move(record1)};
    Response response2{ResponseType::Record, std::move(record2)};

    const size_t channels = 8;
    auto p1 = adapter.partition(response1, channels);
    auto p2 = adapter.partition(response2, channels);

    EXPECT_LT(p1, channels);
    EXPECT_LT(p2, channels);
    EXPECT_EQ(p1, p2);
}

// RecordPair routing uses the left record as the representative.
TEST(PartitionerTest, KeyPartitionerRecordPairUsesLeftTimestamp) {
    KeyPartitioner partitioner;
    const size_t channels = 8;

    auto left = makeRecordView(10, 1234, {1.0f, 2.0f});
    auto right = makeRecordView(20, 9876, {9.0f, 8.0f});
    auto pair = makePairResponse(left, right);

    size_t expected = std::hash<int64_t>{}(left->timestamp_) % channels;
    EXPECT_EQ(partitioner.partition(pair, channels), expected);
}

TEST(PartitionerTest, VectorHashPartitionerRecordPairUsesLeftVector) {
    VectorHashPartitioner partitioner;
    const size_t channels = 8;

    auto left = makeRecordView(10, 1000, {0.2f, -0.1f, 0.5f, 0.3f});
    auto right = makeRecordView(20, 1001, {9.0f, 8.0f, 7.0f, 6.0f});
    auto pair = makePairResponse(left, right);

    Response left_response{ResponseType::Record, std::make_unique<VectorRecord>(*left)};
    EXPECT_EQ(partitioner.partition(pair, channels),
              partitioner.partition(left_response, channels));
}

TEST(PartitionerTest, RoundRobinRecordPairBypassesContentKey) {
    RoundRobinPartitioner partitioner;
    auto left = makeRecordView(10, 1000, {1.0f, 2.0f});
    auto right = makeRecordView(20, 1001, {3.0f, 4.0f});
    auto pair = makePairResponse(left, right);

    EXPECT_EQ(partitioner.partition(pair, 3), 0u);
    EXPECT_EQ(partitioner.partition(pair, 3), 1u);
    EXPECT_EQ(partitioner.partition(pair, 3), 2u);
    EXPECT_EQ(partitioner.partition(pair, 3), 0u);
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

TEST(ResultPartitionTest, BroadcastPreservesRecordPairPayload) {
    ResultPartition partition;

    std::vector<std::shared_ptr<BlockingQueue>> raw_queues;
    std::vector<QueuePtr> queues;
    for (int i = 0; i < 3; ++i) {
        auto queue = std::make_shared<BlockingQueue>(10);
        raw_queues.push_back(queue);
        queues.push_back(queue);
    }

    partition.setup(std::make_unique<BroadcastPartitioner>(), std::move(queues), 0);

    auto left = makeRecordView(10, 1000, {1.0f, 2.0f});
    auto right = makeRecordView(20, 1001, {3.0f, 4.0f});
    auto pair = makePairResponse(left, right, 0.75);
    partition.emit(std::move(pair), 0);

    for (auto& q : raw_queues) {
        q->stop();
    }

    for (auto& q : raw_queues) {
        auto tagged = q->pop();
        ASSERT_TRUE(tagged.has_value());
        EXPECT_EQ(tagged->response.type_, ResponseType::RecordPair);
        ASSERT_NE(tagged->response.pair_, nullptr);
        EXPECT_EQ(tagged->response.pair_->left->uid_, 10u);
        EXPECT_EQ(tagged->response.pair_->right->uid_, 20u);
        EXPECT_DOUBLE_EQ(tagged->response.pair_->similarity, 0.75);
    }
}

} // namespace test
} // namespace sageFlow

#include <gtest/gtest.h>
#include "index/diskann_index.h"
#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"
#include "test_utils/test_data_generator.h"
#include "test_utils/test_data_adapter.h"
#include <filesystem>

namespace sageFlow {
namespace test {

class DiskANNIndexTest : public ::testing::Test {
protected:
    void SetUp() override {
        storage_ = std::make_shared<StorageManager>();
        concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage_);
    }

    void TearDown() override {
        // Clean up index files
        // We don't know the exact files, but they start with diskann_index_
        // Use try-catch to avoid crashing on cleanup
        try {
            for (const auto& entry : std::filesystem::directory_iterator(".")) {
                if (entry.path().filename().string().find("diskann_index_") == 0) {
                    std::filesystem::remove_all(entry.path());
                }
            }
        } catch (...) {}
    }

    std::shared_ptr<StorageManager> storage_;
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};

TEST_F(DiskANNIndexTest, BasicInsertAndQuery) {
    int dim = 128;
    FreshDiskANNParameters params;
    params.L = 100;
    params.R = 64;
    params.alpha = 1.2f;

    int index_id = concurrency_manager_->create_index("test_diskann", IndexType::FreshDiskANN, dim, params);
    ASSERT_GE(index_id, 0);

    // Generate data
    TestDataGenerator::Config config;
    config.vector_dim = dim;
    config.positive_pairs = 100;
    config.near_threshold_pairs = 0;
    config.negative_pairs = 0;
    config.random_tail = 0;
    TestDataGenerator generator(config);
    auto [records, matches] = generator.generateData();

    // Insert records
    for (auto& record : records) {
        // Clone record because insert takes ownership
        auto record_copy = std::make_unique<VectorRecord>(record->uid_, record->timestamp_, record->data_);
        bool success = concurrency_manager_->insert(index_id, std::move(record_copy));
        ASSERT_TRUE(success);
    }

    // Query
    // Use the first record as query
    auto query_record = records[0].get();
    int k = 5;
    auto results = concurrency_manager_->query(index_id, *query_record, k);
    
    ASSERT_EQ(results.size(), k);
    // The first result should be the query itself (distance 0)
    bool found = false;
    for (const auto& res : results) {
        if (res->uid_ == query_record->uid_) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);
}

} // namespace test
} // namespace sageFlow

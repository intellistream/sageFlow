#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <algorithm>
#include <iostream>

#include "index/hdr_forest.h"
#include "common/data_types.h"
#include "compute_engine/compute_engine.h"
#include "storage/storage_manager.h"

using namespace sageFlow;

class HDRRkNNTest : public ::testing::Test {
protected:
    void SetUp() override {
        storage_manager_ = std::make_shared<StorageManager>();
        storage_manager_->engine_ = std::make_shared<ComputeEngine>();
        
        // Use small clusters to force interaction
        index_ = std::make_shared<HDRForest>(1, 1); 
        index_->dimension_ = dimension_;
        index_->storage_manager_ = storage_manager_;
    }

    std::unique_ptr<VectorRecord> createRecord(uint64_t uid, const std::vector<float>& values) {
        char* raw_ptr = new char[dimension_ * sizeof(float)];
        float* data_ptr = reinterpret_cast<float*>(raw_ptr);
        std::copy(values.begin(), values.end(), data_ptr);
        
        std::unique_ptr<char[]> ptr(raw_ptr);

        return std::make_unique<VectorRecord>(
            uid,
            0, // timestamp
            dimension_,
            DataType::Float32,
            ptr.release()
        );
    }

    int dimension_ = 2;
    std::shared_ptr<StorageManager> storage_manager_;
    std::shared_ptr<HDRForest> index_;
};

TEST_F(HDRRkNNTest, RkNNUpdateOnErase) {
    // 1. Insert Node A (0,0) and Node B (0.1, 0.1) - they should be neighbors
    // Insert Node C (10, 10) - far away
    
    std::vector<float> vecA = {0.0f, 0.0f};
    std::vector<float> vecB = {0.1f, 0.1f};
    std::vector<float> vecC = {10.0f, 10.0f};

    storage_manager_->insert(createRecord(1, vecA));
    index_->insert(1);

    storage_manager_->insert(createRecord(2, vecB));
    index_->insert(2);

    storage_manager_->insert(createRecord(3, vecC));
    index_->insert(3);

    // 2. Verify A and B are neighbors
    // Query near A, should find A and B
    auto queryA = createRecord(999, {0.05f, 0.05f});
    auto results = index_->query(*queryA, 2);
    
    bool foundA = false;
    bool foundB = false;
    for(auto uid : results) {
        if(uid == 1) foundA = true;
        if(uid == 2) foundB = true;
    }
    ASSERT_TRUE(foundA && foundB) << "Initial state: A and B should be found";

    // 3. Delete B
    // This should trigger RkNN update: A should remove B from its neighbor list
    index_->erase(2);
    storage_manager_->erase(2); // Also remove from storage to ensure access would fail if attempted

    // 4. Query near A again
    // Should find A, but NOT B. And should not crash.
    results = index_->query(*queryA, 2);
    
    foundA = false;
    foundB = false;
    for(auto uid : results) {
        if(uid == 1) foundA = true;
        if(uid == 2) foundB = true;
    }
    
    EXPECT_TRUE(foundA) << "After erase: A should still be found";
    EXPECT_FALSE(foundB) << "After erase: B should NOT be found";
    
    // 5. Insert D (0.2, 0.2)
    // If A's neighbor list was corrupted or still pointed to B, this might fail or produce weird results
    std::vector<float> vecD = {0.2f, 0.2f};
    storage_manager_->insert(createRecord(4, vecD));
    index_->insert(4);
    
    results = index_->query(*queryA, 3);
    bool foundD = false;
    for(auto uid : results) {
        if(uid == 4) foundD = true;
    }
    EXPECT_TRUE(foundD) << "After inserting D: D should be found";
}

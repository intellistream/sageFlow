#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <random>
#include <algorithm>
#include <iostream>

#include "index/hdr_forest.h"
#include "common/data_types.h"
#include "compute_engine/compute_engine.h"
#include "storage/storage_manager.h"

using namespace sageFlow;

class HDRForestTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize storage manager
        storage_manager_ = std::make_shared<StorageManager>();
        storage_manager_->engine_ = std::make_shared<ComputeEngine>();
        
        // Initialize index
        // Test config: n_clusters=2, f_sections=5
        index_ = std::make_shared<HDRForest>(2, 5);
        index_->dimension_ = dimension_;
        index_->storage_manager_ = storage_manager_;

        std::cout << "[   INFO   ] SetUp: Init HDRForest, dim=" << dimension_ << ", clusters=2, sections=5" << std::endl;
    }

    std::unique_ptr<VectorRecord> createRecord(uint64_t uid, const std::vector<float>& values) {
        // VectorRecord takes ownership of the data pointer
        // Allocate raw bytes for the float array to avoid UB when deleting as char[]
        char* raw_ptr = new char[dimension_ * sizeof(float)];
        float* data_ptr = reinterpret_cast<float*>(raw_ptr);
        std::copy(values.begin(), values.end(), data_ptr);
        
        std::unique_ptr<char[]> ptr(raw_ptr);

        return std::make_unique<VectorRecord>(
            uid,
            0, // timestamp
            dimension_,
            DataType::Float32,
            std::move(ptr)
        );
    }

    int dimension_ = 4;
    std::shared_ptr<StorageManager> storage_manager_;
    std::shared_ptr<HDRForest> index_;
};

TEST_F(HDRForestTest, InsertionAndExactQuery) {
    std::cout << "[   INFO   ] >>> Test: InsertionAndExactQuery" << std::endl;
    std::vector<std::vector<float>> data = {
        {1.0f, 0.0f, 0.0f, 0.0f}, // 0
        {0.0f, 1.0f, 0.0f, 0.0f}, // 1
        {0.0f, 0.0f, 1.0f, 0.0f}, // 2
        {0.0f, 0.0f, 0.0f, 1.0f}, // 3
        {0.5f, 0.5f, 0.0f, 0.0f}  // 4
    };

    for (size_t i = 0; i < data.size(); ++i) {
        storage_manager_->insert(createRecord(i, data[i]));
        index_->insert(i);
    }
    std::cout << "[   INFO   ] Inserted " << data.size() << " records" << std::endl;

    auto query_rec = createRecord(999, {0.9f, 0.1f, 0.0f, 0.0f});
    auto results = index_->query(*query_rec, 1);
    
    std::cout << "[   INFO   ] Query results: " << results.size() << std::endl;
    if (!results.empty()) {
        std::cout << "[   INFO   ] Top 1 ID: " << results[0] << std::endl;
    }

    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], 0);
}

TEST_F(HDRForestTest, BatchInsertion) {
    std::cout << "[   INFO   ] >>> Test: BatchInsertion" << std::endl;
    
    int num_items = 20;
    for (int i = 0; i < num_items; ++i) {
        std::vector<float> vec(dimension_, static_cast<float>(i));
        storage_manager_->insert(createRecord(i, vec));
        index_->insert(i);
    }
    std::cout << "[   INFO   ] Inserted " << num_items << " records" << std::endl;
    
    auto query_rec = createRecord(999, {0.0f, 0.0f, 0.0f, 0.0f});
    auto results = index_->query(*query_rec, 5);
    
    std::cout << "[   INFO   ] Query returned " << results.size() << " results" << std::endl;

    EXPECT_LE(results.size(), 5);
    bool found_0 = false;
    for(auto uid : results) {
        if(uid == 0) found_0 = true;
    }
    EXPECT_TRUE(found_0);
}

TEST_F(HDRForestTest, Erase) {
    std::cout << "[   INFO   ] >>> Test: Erase" << std::endl;
    storage_manager_->insert(createRecord(1, {1.0f, 1.0f, 1.0f, 1.0f}));
    index_->insert(1);
    
    auto query_rec = createRecord(999, {1.0f, 1.0f, 1.0f, 1.0f});
    auto results = index_->query(*query_rec, 1);
    ASSERT_FALSE(results.empty());
    EXPECT_EQ(results[0], 1);
    
    index_->erase(1);
    std::cout << "[   INFO   ] Erased record 1" << std::endl;
    
    results = index_->query(*query_rec, 1);
    if (!results.empty()) {
        EXPECT_NE(results[0], 1);
    }
}

TEST_F(HDRForestTest, QueryForJoin) {
    std::cout << "[   INFO   ] >>> Test: QueryForJoin" << std::endl;
    storage_manager_->insert(createRecord(1, {1.0f, 0.0f, 0.0f, 0.0f}));
    index_->insert(1);
    
    storage_manager_->insert(createRecord(2, {0.0f, 1.0f, 0.0f, 0.0f}));
    index_->insert(2);
    
    auto query_rec = createRecord(999, {0.9f, 0.0f, 0.0f, 0.0f});
    
    double threshold = 0.8;
    auto results = index_->query_for_join(*query_rec, threshold); 
    std::cout << "[   INFO   ] Join query (thresh " << threshold << ") found " << results.size() << " results" << std::endl;
    
    bool found_1 = false;
    for(auto uid : results) {
        if(uid == 1) found_1 = true;
    }
    EXPECT_TRUE(found_1);
}

TEST_F(HDRForestTest, BuildForestAndRouting) {
    std::cout << "[   INFO   ] >>> Test: BuildForestAndRouting" << std::endl;
    int n = 50;
    std::vector<std::shared_ptr<VectorRecord>> records;
    
    for(int i=0; i<n; ++i) {
        std::vector<float> vec(dimension_);
        if (i < n/2) {
            for(int d=0; d<dimension_; ++d) vec[d] = 0.0f + (float)i*0.001f;
        } else {
            for(int d=0; d<dimension_; ++d) vec[d] = 10.0f + (float)(i-n/2)*0.001f;
        }
        
        storage_manager_->insert(createRecord(i, vec));
        
        char* raw_ptr = new char[dimension_ * sizeof(float)];
        float* data_ptr = reinterpret_cast<float*>(raw_ptr);
        std::copy(vec.begin(), vec.end(), data_ptr);
        std::unique_ptr<char[]> ptr(raw_ptr);

        auto rec = std::make_shared<VectorRecord>(
            (uint64_t)i, 0, dimension_, DataType::Float32, std::move(ptr)
        );
        records.push_back(rec);
    }
    
    index_->build_forest(records);
    std::cout << "[   INFO   ] Built forest with " << n << " records" << std::endl;
    
    auto query_rec = createRecord(999, {10.0f, 10.0f, 10.0f, 10.0f});
    auto results = index_->query(*query_rec, 5);
    
    ASSERT_FALSE(results.empty());
    for(auto uid : results) {
        EXPECT_GE(uid, n/2);
    }
    
    storage_manager_->insert(createRecord(100, {0.0f, 0.0f, 0.0f, 0.0f}));
    index_->insert(100);
    
    auto query_rec2 = createRecord(1000, {0.0f, 0.0f, 0.0f, 0.0f});
    auto results2 = index_->query(*query_rec2, 5);
    
    bool found = false;
    for(auto uid : results2) {
        if(uid == 100) found = true;
    }
    EXPECT_TRUE(found);
}

TEST_F(HDRForestTest, IntegrationWithLocalHDRTree) {
    std::cout << "[   INFO   ] >>> Test: IntegrationWithLocalHDRTree" << std::endl;
    int n = 100; 
    std::vector<std::shared_ptr<VectorRecord>> records;
    
    std::mt19937 gen(42);
    std::uniform_real_distribution<float> dist(0.0f, 1.0f);
    
    for(int i=0; i<n; ++i) {
        std::vector<float> vec(dimension_);
        for(int d=0; d<dimension_; ++d) vec[d] = dist(gen);
        
        storage_manager_->insert(createRecord(i, vec));
        
        char* raw_ptr = new char[dimension_ * sizeof(float)];
        float* data_ptr = reinterpret_cast<float*>(raw_ptr);
        std::copy(vec.begin(), vec.end(), data_ptr);
        std::unique_ptr<char[]> ptr(raw_ptr);

        auto rec = std::make_shared<VectorRecord>(
            (uint64_t)i, 0, dimension_, DataType::Float32, std::move(ptr)
        );
        records.push_back(rec);
    }
    
    index_->build_forest(records);
    
    auto query_rec = createRecord(999, {0.5f, 0.5f, 0.5f, 0.5f});
    auto results = index_->query(*query_rec, 10);
    
    EXPECT_FALSE(results.empty());
    EXPECT_LE(results.size(), 10);
}

TEST_F(HDRForestTest, PruningLogic) {
    std::cout << "[   INFO   ] >>> Test: PruningLogic" << std::endl;
    int n = 20;
    std::vector<std::shared_ptr<VectorRecord>> records;
    for(int i=0; i<n; ++i) {
        std::vector<float> vec(dimension_);
        if (i < n/2) {
            for(int d=0; d<dimension_; ++d) vec[d] = 0.0f; 
        } else {
            for(int d=0; d<dimension_; ++d) vec[d] = 100.0f; 
        }
        storage_manager_->insert(createRecord(i, vec));
        
        char* raw_ptr = new char[dimension_ * sizeof(float)];
        float* data_ptr = reinterpret_cast<float*>(raw_ptr);
        std::copy(vec.begin(), vec.end(), data_ptr);
        std::unique_ptr<char[]> ptr(raw_ptr);

        auto rec = std::make_shared<VectorRecord>(
            (uint64_t)i, 0, dimension_, DataType::Float32, std::move(ptr)
        );
        records.push_back(rec);
    }
    
    index_->build_forest(records);
    
    auto query_rec = createRecord(999, {0.0f, 0.0f, 0.0f, 0.0f});
    auto results = index_->query(*query_rec, 5);
    
    for(auto uid : results) {
        EXPECT_LT(uid, 10);
    }
}

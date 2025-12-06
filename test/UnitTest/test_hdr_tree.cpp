#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <random>
#include <algorithm>

#include "index/hdr_forest.h"
#include "common/data_types.h"
#include "compute_engine/compute_engine.h"
#include "storage/storage_manager.h"

using namespace sageFlow;

class HDRForestTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup storage manager
        storage_manager_ = std::make_shared<StorageManager>();
        storage_manager_->engine_ = std::make_shared<ComputeEngine>();
        
        // Setup index
        // n_clusters=2, f_sections=5 for testing
        index_ = std::make_shared<HDRForest>(2, 5);
        index_->dimension_ = dimension_;
        index_->storage_manager_ = storage_manager_;
    }

    std::unique_ptr<VectorRecord> createRecord(uint64_t uid, const std::vector<float>& values) {
        // VectorRecord takes ownership of the data pointer
        float* data_ptr = new float[dimension_];
        std::copy(values.begin(), values.end(), data_ptr);
        
        return std::make_unique<VectorRecord>(
            uid,
            0, // timestamp
            dimension_,
            DataType::Float32,
            reinterpret_cast<char*>(data_ptr)
        );
    }

    int dimension_ = 4;
    std::shared_ptr<StorageManager> storage_manager_;
    std::shared_ptr<HDRForest> index_;
};

TEST_F(HDRForestTest, InsertionAndExactQuery) {
    // Insert some vectors
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

    // Query for vector closest to {1.0, 0.0, 0.0, 0.0} -> should be 0
    auto query_rec = createRecord(999, {0.9f, 0.1f, 0.0f, 0.0f});
    auto results = index_->query(*query_rec, 1);
    
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], 0);
}

TEST_F(HDRForestTest, BatchInsertion) {
    // HDRForest uses a buffer. We want to ensure batch processing works.
    // We insert enough items to trigger batch processing or force it.
    // The default buffer size might be large, but we can check if insert works logically.
    
    int num_items = 20;
    for (int i = 0; i < num_items; ++i) {
        std::vector<float> vec(dimension_, static_cast<float>(i));
        storage_manager_->insert(createRecord(i, vec));
        index_->insert(i);
    }
    
    // Query something
    auto query_rec = createRecord(999, {0.0f, 0.0f, 0.0f, 0.0f});
    auto results = index_->query(*query_rec, 5);
    
    EXPECT_LE(results.size(), 5);
    // 0 should be closest to 0,0,0,0
    bool found_0 = false;
    for(auto uid : results) {
        if(uid == 0) found_0 = true;
    }
    EXPECT_TRUE(found_0);
}

TEST_F(HDRForestTest, Erase) {
    // Insert
    storage_manager_->insert(createRecord(1, {1.0f, 1.0f, 1.0f, 1.0f}));
    index_->insert(1);
    
    // Verify it exists
    auto query_rec = createRecord(999, {1.0f, 1.0f, 1.0f, 1.0f});
    auto results = index_->query(*query_rec, 1);
    ASSERT_FALSE(results.empty());
    EXPECT_EQ(results[0], 1);
    
    // Erase
    index_->erase(1);
    
    // Verify it's gone (or at least not returned if we query)
    // Note: HDRForest erase might be lazy or complex.
    // If we query, it should ideally not return the erased item.
    
    // However, if it's the ONLY item, query might return empty.
    results = index_->query(*query_rec, 1);
    if (!results.empty()) {
        EXPECT_NE(results[0], 1);
    }
}

TEST_F(HDRForestTest, QueryForJoin) {
    // Insert data
    storage_manager_->insert(createRecord(1, {1.0f, 0.0f, 0.0f, 0.0f}));
    index_->insert(1);
    
    storage_manager_->insert(createRecord(2, {0.0f, 1.0f, 0.0f, 0.0f}));
    index_->insert(2);
    
    // Query with threshold
    // Vector {0.9, 0.0, ...} should be close to 1
    auto query_rec = createRecord(999, {0.9f, 0.0f, 0.0f, 0.0f});
    
    // Threshold logic depends on implementation (similarity vs distance).
    // Assuming similarity threshold (e.g. cosine or derived).
    // If query_for_join uses distance threshold internally derived from similarity.
    
    // Let's assume high similarity threshold
    auto results = index_->query_for_join(*query_rec, 0.8); 
    
    bool found_1 = false;
    for(auto uid : results) {
        if(uid == 1) found_1 = true;
    }
    EXPECT_TRUE(found_1);
}

TEST_F(HDRForestTest, BuildForestAndRouting) {
    int n = 50;
    std::vector<std::shared_ptr<VectorRecord>> records;
    
    // Create data with 2 distinct clusters
    for(int i=0; i<n; ++i) {
        std::vector<float> vec(dimension_);
        if (i < n/2) {
            // Cluster 1: around 0.0
            for(int d=0; d<dimension_; ++d) vec[d] = 0.0f + (float)i*0.001f;
        } else {
            // Cluster 2: around 10.0
            for(int d=0; d<dimension_; ++d) vec[d] = 10.0f + (float)(i-n/2)*0.001f;
        }
        
        // Insert into storage (needed for query later)
        storage_manager_->insert(createRecord(i, vec));
        
        // Create shared_ptr copy for build_forest
        float* data_ptr = new float[dimension_];
        std::copy(vec.begin(), vec.end(), data_ptr);
        auto rec = std::make_shared<VectorRecord>(
            (uint64_t)i, 0, dimension_, DataType::Float32, reinterpret_cast<char*>(data_ptr)
        );
        records.push_back(rec);
    }
    
    // Build forest explicitly
    index_->build_forest(records);
    
    // Query for a point near Cluster 2
    auto query_rec = createRecord(999, {10.0f, 10.0f, 10.0f, 10.0f});
    auto results = index_->query(*query_rec, 5);
    
    ASSERT_FALSE(results.empty());
    for(auto uid : results) {
        // Should belong to the second cluster (ids >= 25)
        EXPECT_GE(uid, n/2);
    }
    
    // Insert a new point dynamically that belongs to Cluster 1
    // ID = 100, Vector = {0.0, ...}
    storage_manager_->insert(createRecord(100, {0.0f, 0.0f, 0.0f, 0.0f}));
    index_->insert(100);
    
    // Query near Cluster 1
    auto query_rec2 = createRecord(1000, {0.0f, 0.0f, 0.0f, 0.0f});
    auto results2 = index_->query(*query_rec2, 5);
    
    bool found = false;
    for(auto uid : results2) {
        if(uid == 100) found = true;
    }
    EXPECT_TRUE(found);
}

TEST_F(HDRForestTest, IntegrationWithLocalHDRTree) {
    int n = 100; // Enough data to trigger PCA training (min 10 per section)
    std::vector<std::shared_ptr<VectorRecord>> records;
    
    // Generate random data
    std::mt19937 gen(42);
    std::uniform_real_distribution<float> dist(0.0f, 1.0f);
    
    for(int i=0; i<n; ++i) {
        std::vector<float> vec(dimension_);
        for(int d=0; d<dimension_; ++d) vec[d] = dist(gen);
        
        storage_manager_->insert(createRecord(i, vec));
        
        float* data_ptr = new float[dimension_];
        std::copy(vec.begin(), vec.end(), data_ptr);
        auto rec = std::make_shared<VectorRecord>(
            (uint64_t)i, 0, dimension_, DataType::Float32, reinterpret_cast<char*>(data_ptr)
        );
        records.push_back(rec);
    }
    
    // Build forest
    index_->build_forest(records);
    
    // Verify that we can query and get results
    // This implicitly tests that the underlying HDRTree (R-Tree) is working
    // because query() prefers using rtree_index if PCA is trained.
    
    auto query_rec = createRecord(999, {0.5f, 0.5f, 0.5f, 0.5f});
    auto results = index_->query(*query_rec, 10);
    
    EXPECT_FALSE(results.empty());
    EXPECT_LE(results.size(), 10);
    
    // We can't easily access private members to check isPCATrained() directly 
    // without friend classes or public accessors, but successful query implies it works.
    // (Or we could add a public accessor for testing, but let's stick to black-box testing for now)
}

TEST_F(HDRForestTest, PruningLogic) {
    // Construct a scenario where pruning should happen
    // Cluster 1: [0, 0] -> Center [0, 0], min_dist=0, max_dist=1
    // Cluster 2: [100, 100] -> Center [100, 100], min_dist=0, max_dist=1
    
    // We manually construct the forest to control max_dknn
    // Since we can't easily access private members, we rely on the fact that
    // if pruning works, querying near Cluster 1 should NOT search Cluster 2.
    // But standard query returns results anyway.
    
    // To verify pruning, we can check if we get results from a cluster that SHOULD be pruned
    // if we force a very small k.
    
    // Actually, a better way is to use the fact that if we set max_dknn to be small,
    // and query far away, it should skip.
    
    // Let's build a forest with 2 clusters far apart.
    int n = 20;
    std::vector<std::shared_ptr<VectorRecord>> records;
    for(int i=0; i<n; ++i) {
        std::vector<float> vec(dimension_);
        if (i < n/2) {
            for(int d=0; d<dimension_; ++d) vec[d] = 0.0f; // Cluster 1
        } else {
            for(int d=0; d<dimension_; ++d) vec[d] = 100.0f; // Cluster 2
        }
        storage_manager_->insert(createRecord(i, vec));
        
        float* data_ptr = new float[dimension_];
        std::copy(vec.begin(), vec.end(), data_ptr);
        auto rec = std::make_shared<VectorRecord>(
            (uint64_t)i, 0, dimension_, DataType::Float32, reinterpret_cast<char*>(data_ptr)
        );
        records.push_back(rec);
    }
    
    index_->build_forest(records);
    
    // Query near Cluster 1
    auto query_rec = createRecord(999, {0.0f, 0.0f, 0.0f, 0.0f});
    auto results = index_->query(*query_rec, 5);
    
    // Should find items from Cluster 1 (ID < 10)
    for(auto uid : results) {
        EXPECT_LT(uid, 10);
    }
    
    // Note: To truly verify "Pruning" (skipping computation), we would need internal metrics/counters.
    // But functionally, it should still return correct results (nearest neighbors).
    // The pruning just optimizes speed.
    // However, if pruning is WRONG (prunes too much), we would miss valid neighbors.
    // So this test verifies that pruning does NOT break correctness.
}

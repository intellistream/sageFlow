#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <random>
#include <algorithm>

#include "index/hdr_forest.h"
#include "common/data_types.h"
#include "compute_engine/compute_engine.h"
#include "storage/storage_manager.h"
#include "utils/logger.h"

using namespace sageFlow;

class HDRForestTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 初始化存储管理器
        storage_manager_ = std::make_shared<StorageManager>();
        storage_manager_->engine_ = std::make_shared<ComputeEngine>();
        
        // 初始化索引
        // 测试配置：n_clusters=2, f_sections=5
        index_ = std::make_shared<HDRForest>(2, 5);
        index_->dimension_ = dimension_;
        index_->storage_manager_ = storage_manager_;

        SAGEFLOW_LOG_INFO("HDRForestTest", "SetUp: Initialized HDRForest with dim={}, clusters=2, sections=5", dimension_);
    }

    std::unique_ptr<VectorRecord> createRecord(uint64_t uid, const std::vector<float>& values) {
        // VectorRecord 接管数据指针的所有权
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
    SAGEFLOW_LOG_INFO("HDRForestTest", ">>> Test: InsertionAndExactQuery");
    // 插入一些向量
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
    SAGEFLOW_LOG_INFO("HDRForestTest", "Inserted {} records", data.size());

    // 查询距离 {1.0, 0.0, 0.0, 0.0} 最近的向量 -> 应该是 0
    auto query_rec = createRecord(999, {0.9f, 0.1f, 0.0f, 0.0f});
    auto results = index_->query(*query_rec, 1);
    
    SAGEFLOW_LOG_INFO("HDRForestTest", "Query result size: {}", results.size());
    if (!results.empty()) {
        SAGEFLOW_LOG_INFO("HDRForestTest", "Top result ID: {}", results[0]);
    }

    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], 0);
}

TEST_F(HDRForestTest, BatchInsertion) {
    SAGEFLOW_LOG_INFO("HDRForestTest", ">>> Test: BatchInsertion");
    // HDRForest 使用缓冲区。我们需要确保批量处理正常工作。
    
    int num_items = 20;
    for (int i = 0; i < num_items; ++i) {
        std::vector<float> vec(dimension_, static_cast<float>(i));
        storage_manager_->insert(createRecord(i, vec));
        index_->insert(i);
    }
    SAGEFLOW_LOG_INFO("HDRForestTest", "Inserted {} items for batch processing", num_items);
    
    // 执行查询
    auto query_rec = createRecord(999, {0.0f, 0.0f, 0.0f, 0.0f});
    auto results = index_->query(*query_rec, 5);
    
    SAGEFLOW_LOG_INFO("HDRForestTest", "Query returned {} results", results.size());

    EXPECT_LE(results.size(), 5);
    // 0 应该是距离 0,0,0,0 最近的
    bool found_0 = false;
    for(auto uid : results) {
        if(uid == 0) found_0 = true;
    }
    EXPECT_TRUE(found_0);
}

TEST_F(HDRForestTest, Erase) {
    SAGEFLOW_LOG_INFO("HDRForestTest", ">>> Test: Erase");
    // 插入
    storage_manager_->insert(createRecord(1, {1.0f, 1.0f, 1.0f, 1.0f}));
    index_->insert(1);
    SAGEFLOW_LOG_INFO("HDRForestTest", "Inserted record 1");
    
    // 验证其存在
    auto query_rec = createRecord(999, {1.0f, 1.0f, 1.0f, 1.0f});
    auto results = index_->query(*query_rec, 1);
    ASSERT_FALSE(results.empty());
    EXPECT_EQ(results[0], 1);
    SAGEFLOW_LOG_INFO("HDRForestTest", "Verified record 1 exists");
    
    // 删除
    index_->erase(1);
    SAGEFLOW_LOG_INFO("HDRForestTest", "Erased record 1");
    
    // 验证其已消失（或至少查询时不返回）
    results = index_->query(*query_rec, 1);
    if (!results.empty()) {
        EXPECT_NE(results[0], 1);
    }
    SAGEFLOW_LOG_INFO("HDRForestTest", "Verified record 1 is gone (or not top 1)");
}

TEST_F(HDRForestTest, QueryForJoin) {
    SAGEFLOW_LOG_INFO("HDRForestTest", ">>> Test: QueryForJoin");
    // 插入数据
    storage_manager_->insert(createRecord(1, {1.0f, 0.0f, 0.0f, 0.0f}));
    index_->insert(1);
    
    storage_manager_->insert(createRecord(2, {0.0f, 1.0f, 0.0f, 0.0f}));
    index_->insert(2);
    SAGEFLOW_LOG_INFO("HDRForestTest", "Inserted records 1 and 2");
    
    // 带阈值的查询
    auto query_rec = createRecord(999, {0.9f, 0.0f, 0.0f, 0.0f});
    
    // 假设高相似度阈值
    double threshold = 0.8;
    auto results = index_->query_for_join(*query_rec, threshold); 
    SAGEFLOW_LOG_INFO("HDRForestTest", "Query for join with threshold {}, found {} results", threshold, results.size());
    
    bool found_1 = false;
    for(auto uid : results) {
        if(uid == 1) found_1 = true;
    }
    EXPECT_TRUE(found_1);
}

TEST_F(HDRForestTest, BuildForestAndRouting) {
    SAGEFLOW_LOG_INFO("HDRForestTest", ">>> Test: BuildForestAndRouting");
    int n = 50;
    std::vector<std::shared_ptr<VectorRecord>> records;
    
    // 创建具有 2 个不同簇的数据
    for(int i=0; i<n; ++i) {
        std::vector<float> vec(dimension_);
        if (i < n/2) {
            // 簇 1：在 0.0 附近
            for(int d=0; d<dimension_; ++d) vec[d] = 0.0f + (float)i*0.001f;
        } else {
            // 簇 2：在 10.0 附近
            for(int d=0; d<dimension_; ++d) vec[d] = 10.0f + (float)(i-n/2)*0.001f;
        }
        
        storage_manager_->insert(createRecord(i, vec));
        
        float* data_ptr = new float[dimension_];
        std::copy(vec.begin(), vec.end(), data_ptr);
        auto rec = std::make_shared<VectorRecord>(
            (uint64_t)i, 0, dimension_, DataType::Float32, reinterpret_cast<char*>(data_ptr)
        );
        records.push_back(rec);
    }
    
    // 显式构建森林
    index_->build_forest(records);
    SAGEFLOW_LOG_INFO("HDRForestTest", "Built forest with {} records", n);
    
    // 查询簇 2 附近的点
    auto query_rec = createRecord(999, {10.0f, 10.0f, 10.0f, 10.0f});
    auto results = index_->query(*query_rec, 5);
    SAGEFLOW_LOG_INFO("HDRForestTest", "Query near Cluster 2 returned {} results", results.size());
    
    ASSERT_FALSE(results.empty());
    for(auto uid : results) {
        EXPECT_GE(uid, n/2);
    }
    
    // 动态插入一个属于簇 1 的新点
    storage_manager_->insert(createRecord(100, {0.0f, 0.0f, 0.0f, 0.0f}));
    index_->insert(100);
    SAGEFLOW_LOG_INFO("HDRForestTest", "Inserted dynamic record 100 near Cluster 1");
    
    // 查询簇 1 附近
    auto query_rec2 = createRecord(1000, {0.0f, 0.0f, 0.0f, 0.0f});
    auto results2 = index_->query(*query_rec2, 5);
    SAGEFLOW_LOG_INFO("HDRForestTest", "Query near Cluster 1 returned {} results", results2.size());
    
    bool found = false;
    for(auto uid : results2) {
        if(uid == 100) found = true;
    }
    EXPECT_TRUE(found);
}

TEST_F(HDRForestTest, IntegrationWithLocalHDRTree) {
    SAGEFLOW_LOG_INFO("HDRForestTest", ">>> Test: IntegrationWithLocalHDRTree");
    int n = 100; 
    std::vector<std::shared_ptr<VectorRecord>> records;
    
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
    
    index_->build_forest(records);
    SAGEFLOW_LOG_INFO("HDRForestTest", "Built forest with {} random records", n);
    
    auto query_rec = createRecord(999, {0.5f, 0.5f, 0.5f, 0.5f});
    auto results = index_->query(*query_rec, 10);
    SAGEFLOW_LOG_INFO("HDRForestTest", "Query returned {} results", results.size());
    
    EXPECT_FALSE(results.empty());
    EXPECT_LE(results.size(), 10);
}

TEST_F(HDRForestTest, PruningLogic) {
    SAGEFLOW_LOG_INFO("HDRForestTest", ">>> Test: PruningLogic");
    int n = 20;
    std::vector<std::shared_ptr<VectorRecord>> records;
    for(int i=0; i<n; ++i) {
        std::vector<float> vec(dimension_);
        if (i < n/2) {
            for(int d=0; d<dimension_; ++d) vec[d] = 0.0f; // 簇 1
        } else {
            for(int d=0; d<dimension_; ++d) vec[d] = 100.0f; // 簇 2
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
    SAGEFLOW_LOG_INFO("HDRForestTest", "Built forest with 2 distant clusters");
    
    // 查询簇 1 附近
    auto query_rec = createRecord(999, {0.0f, 0.0f, 0.0f, 0.0f});
    auto results = index_->query(*query_rec, 5);
    SAGEFLOW_LOG_INFO("HDRForestTest", "Query near Cluster 1 returned {} results", results.size());
    
    // 应该找到簇 1 中的项 (ID < 10)
    for(auto uid : results) {
        EXPECT_LT(uid, 10);
    }
}

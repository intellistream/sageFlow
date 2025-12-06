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
        // 初始化存储管理器
        storage_manager_ = std::make_shared<StorageManager>();
        storage_manager_->engine_ = std::make_shared<ComputeEngine>();
        
        // 初始化索引
        // 测试配置：n_clusters=2, f_sections=5
        index_ = std::make_shared<HDRForest>(2, 5);
        index_->dimension_ = dimension_;
        index_->storage_manager_ = storage_manager_;

        std::cout << "[   INFO   ] SetUp: 初始化 HDRForest, 维度=" << dimension_ << ", 簇数=2, 分段数=5" << std::endl;
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
    std::cout << "[   INFO   ] >>> 测试: 插入与精确查询 (InsertionAndExactQuery)" << std::endl;
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
    std::cout << "[   INFO   ] 已插入 " << data.size() << " 条记录" << std::endl;

    // 查询距离 {1.0, 0.0, 0.0, 0.0} 最近的向量 -> 应该是 0
    auto query_rec = createRecord(999, {0.9f, 0.1f, 0.0f, 0.0f});
    auto results = index_->query(*query_rec, 1);
    
    std::cout << "[   INFO   ] 查询结果数量: " << results.size() << std::endl;
    if (!results.empty()) {
        std::cout << "[   INFO   ] Top 1 结果 ID: " << results[0] << std::endl;
    }

    ASSERT_EQ(results.size(), 1) << "查询结果数量应为 1";
    EXPECT_EQ(results[0], 0) << "最近邻 ID 应为 0";
}

TEST_F(HDRForestTest, BatchInsertion) {
    std::cout << "[   INFO   ] >>> 测试: 批量插入 (BatchInsertion)" << std::endl;
    // HDRForest 使用缓冲区。我们需要确保批量处理正常工作。
    
    int num_items = 20;
    for (int i = 0; i < num_items; ++i) {
        std::vector<float> vec(dimension_, static_cast<float>(i));
        storage_manager_->insert(createRecord(i, vec));
        index_->insert(i);
    }
    std::cout << "[   INFO   ] 已插入 " << num_items << " 条记录用于批量处理" << std::endl;
    
    // 执行查询
    auto query_rec = createRecord(999, {0.0f, 0.0f, 0.0f, 0.0f});
    auto results = index_->query(*query_rec, 5);
    
    std::cout << "[   INFO   ] 查询返回了 " << results.size() << " 条结果" << std::endl;

    EXPECT_LE(results.size(), 5) << "结果数量不应超过 k=5";
    // 0 应该是距离 0,0,0,0 最近的
    bool found_0 = false;
    for(auto uid : results) {
        if(uid == 0) found_0 = true;
    }
    EXPECT_TRUE(found_0) << "结果中应包含 ID 0";
}

TEST_F(HDRForestTest, Erase) {
    std::cout << "[   INFO   ] >>> 测试: 删除 (Erase)" << std::endl;
    // 插入
    storage_manager_->insert(createRecord(1, {1.0f, 1.0f, 1.0f, 1.0f}));
    index_->insert(1);
    std::cout << "[   INFO   ] 已插入记录 1" << std::endl;
    
    // 验证其存在
    auto query_rec = createRecord(999, {1.0f, 1.0f, 1.0f, 1.0f});
    auto results = index_->query(*query_rec, 1);
    ASSERT_FALSE(results.empty()) << "插入后应能查询到结果";
    EXPECT_EQ(results[0], 1) << "插入后 Top 1 应为记录 1";
    std::cout << "[   INFO   ] 验证记录 1 存在" << std::endl;
    
    // 删除
    index_->erase(1);
    std::cout << "[   INFO   ] 已删除记录 1" << std::endl;
    
    // 验证其已消失（或至少查询时不返回）
    results = index_->query(*query_rec, 1);
    if (!results.empty()) {
        EXPECT_NE(results[0], 1) << "删除后 Top 1 不应是记录 1";
    }
    std::cout << "[   INFO   ] 验证记录 1 已被删除 (或不再是 Top 1)" << std::endl;
}

TEST_F(HDRForestTest, QueryForJoin) {
    std::cout << "[   INFO   ] >>> 测试: Join 查询 (QueryForJoin)" << std::endl;
    // 插入数据
    storage_manager_->insert(createRecord(1, {1.0f, 0.0f, 0.0f, 0.0f}));
    index_->insert(1);
    
    storage_manager_->insert(createRecord(2, {0.0f, 1.0f, 0.0f, 0.0f}));
    index_->insert(2);
    std::cout << "[   INFO   ] 已插入记录 1 和 2" << std::endl;
    
    // 带阈值的查询
    auto query_rec = createRecord(999, {0.9f, 0.0f, 0.0f, 0.0f});
    
    // 假设高相似度阈值
    double threshold = 0.8;
    auto results = index_->query_for_join(*query_rec, threshold); 
    std::cout << "[   INFO   ] Join 查询 (阈值 " << threshold << ") 找到 " << results.size() << " 条结果" << std::endl;
    
    bool found_1 = false;
    for(auto uid : results) {
        if(uid == 1) found_1 = true;
    }
    EXPECT_TRUE(found_1) << "Join 查询结果应包含记录 1";
}

TEST_F(HDRForestTest, BuildForestAndRouting) {
    std::cout << "[   INFO   ] >>> 测试: 构建森林与路由 (BuildForestAndRouting)" << std::endl;
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
    std::cout << "[   INFO   ] 已构建森林，包含 " << n << " 条记录" << std::endl;
    
    // 查询簇 2 附近的点
    auto query_rec = createRecord(999, {10.0f, 10.0f, 10.0f, 10.0f});
    auto results = index_->query(*query_rec, 5);
    std::cout << "[   INFO   ] 簇 2 附近查询返回 " << results.size() << " 条结果" << std::endl;
    
    ASSERT_FALSE(results.empty()) << "查询结果不应为空";
    for(auto uid : results) {
        EXPECT_GE(uid, n/2) << "结果 ID 应属于簇 2 (>= " << n/2 << ")";
    }
    
    // 动态插入一个属于簇 1 的新点
    storage_manager_->insert(createRecord(100, {0.0f, 0.0f, 0.0f, 0.0f}));
    index_->insert(100);
    std::cout << "[   INFO   ] 动态插入记录 100 (靠近簇 1)" << std::endl;
    
    // 查询簇 1 附近
    auto query_rec2 = createRecord(1000, {0.0f, 0.0f, 0.0f, 0.0f});
    auto results2 = index_->query(*query_rec2, 5);
    std::cout << "[   INFO   ] 簇 1 附近查询返回 " << results2.size() << " 条结果" << std::endl;
    
    bool found = false;
    for(auto uid : results2) {
        if(uid == 100) found = true;
    }
    EXPECT_TRUE(found) << "查询结果应包含动态插入的记录 100";
}

TEST_F(HDRForestTest, IntegrationWithLocalHDRTree) {
    std::cout << "[   INFO   ] >>> 测试: 本地 HDRTree 集成 (IntegrationWithLocalHDRTree)" << std::endl;
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
    std::cout << "[   INFO   ] 已构建森林，包含 " << n << " 条随机记录" << std::endl;
    
    auto query_rec = createRecord(999, {0.5f, 0.5f, 0.5f, 0.5f});
    auto results = index_->query(*query_rec, 10);
    std::cout << "[   INFO   ] 查询返回 " << results.size() << " 条结果" << std::endl;
    
    EXPECT_FALSE(results.empty()) << "查询结果不应为空";
    EXPECT_LE(results.size(), 10) << "结果数量不应超过 k=10";
}

TEST_F(HDRForestTest, PruningLogic) {
    std::cout << "[   INFO   ] >>> 测试: 剪枝逻辑 (PruningLogic)" << std::endl;
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
    std::cout << "[   INFO   ] 已构建森林，包含 2 个远距离簇" << std::endl;
    
    // 查询簇 1 附近
    auto query_rec = createRecord(999, {0.0f, 0.0f, 0.0f, 0.0f});
    auto results = index_->query(*query_rec, 5);
    std::cout << "[   INFO   ] 簇 1 附近查询返回 " << results.size() << " 条结果" << std::endl;
    
    // 应该找到簇 1 中的项 (ID < 10)
    for(auto uid : results) {
        EXPECT_LT(uid, 10) << "结果 ID 应属于簇 1 (< 10)";
    }
}

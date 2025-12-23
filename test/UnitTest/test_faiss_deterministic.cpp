#include <gtest/gtest.h>
#include <vector>
#include <memory>
#include <cmath>
#include <iostream>
#include <cstring> 

#include "index/faiss_index.h"
#include "common/data_types.h"
#include "storage/storage_manager.h"
#include "compute_engine/compute_engine.h"
#include "utils/logger.h"

namespace sageFlow {

class FaissDeterministicTest : public ::testing::Test {
protected:
    void SetUp() override {
        storage_ = std::make_shared<StorageManager>();
        storage_->engine_ = std::make_shared<ComputeEngine>();
        // 初始化日志，方便调试
        // Logger::init(LogLevel::INFO); 
    }

    std::unique_ptr<VectorRecord> createRecord(uint64_t uid, const std::vector<float>& data) {
        int32_t dim = static_cast<int32_t>(data.size());
        size_t data_size = dim * sizeof(float);
        char* raw_ptr = new char[data_size];
        std::memcpy(raw_ptr, data.data(), data_size);
        return std::make_unique<VectorRecord>(uid, 1000, dim, DataType::Float32, raw_ptr);
    }

    void insertRecord(uint64_t id, std::vector<float> data) {
        storage_->insert(createRecord(id, data));
    }

    std::shared_ptr<StorageManager> storage_;
};

// ==========================================
// 测试 1: 验证 L2 距离下的阈值转换逻辑
// ==========================================
TEST_F(FaissDeterministicTest, L2_Threshold_Accuracy) {
    int dim = 2;
    // [修改点] 使用 "Flat" 代替 "IVF1,Flat"
    // Flat 索引不需要训练，数据会立即插入，适合小数据量测试
    std::string description = "Flat"; 
    
    auto index = std::make_shared<FaissIndex>(dim, description, 0 /* L2 */);
    index->storage_manager_ = storage_;

    insertRecord(1, {0.0f, 0.0f}); 
    insertRecord(2, {3.0f, 4.0f}); // Dist=5.0 -> Sim=exp(-0.1*5)≈0.6065
    insertRecord(3, {6.0f, 8.0f}); // Dist=10.0 -> Sim=exp(-0.1*10)≈0.3678

    EXPECT_TRUE(index->insert(1));
    EXPECT_TRUE(index->insert(2));
    EXPECT_TRUE(index->insert(3));

    auto query = createRecord(99, {0.0f, 0.0f});

    // 场景 A: 阈值 0.9 (只有 ID 1)
    auto res_strict = index->query_for_join(*query, 0.9);
    EXPECT_EQ(res_strict.size(), 1);
    if(!res_strict.empty()) EXPECT_EQ(res_strict[0], 1);

    // 场景 B: 阈值 0.5 (ID 1 和 ID 2)
    auto res_medium = index->query_for_join(*query, 0.5);
    EXPECT_EQ(res_medium.size(), 2);
    
    // 打印结果验证
    bool has_1 = false, has_2 = false;
    for(auto id : res_medium) {
        if(id == 1) has_1 = true;
        if(id == 2) has_2 = true;
    }
    EXPECT_TRUE(has_1);
    EXPECT_TRUE(has_2);
}

// ==========================================
// 测试 2: 验证 Inner Product (IP) 支持
// ==========================================
TEST_F(FaissDeterministicTest, IP_Metric_Accuracy) {
    int dim = 2;
    // [修改点] 使用 "Flat"
    std::string description = "Flat"; 
    auto index = std::make_shared<FaissIndex>(dim, description, 1 /* IP */);
    index->storage_manager_ = storage_;

    insertRecord(1, {1.0f, 0.0f}); 
    insertRecord(2, {0.5f, 0.866f}); // dot=0.5
    insertRecord(3, {0.0f, 1.0f});   // dot=0.0

    EXPECT_TRUE(index->insert(1));
    EXPECT_TRUE(index->insert(2));
    EXPECT_TRUE(index->insert(3));

    auto query = createRecord(99, {1.0f, 0.0f});

    // 场景: 阈值 0.4 (包含 1 和 2)
    // 注意：IP 模式下 query_for_join 直接透传阈值，要求 score > threshold
    auto results = index->query_for_join(*query, 0.4);
    EXPECT_EQ(results.size(), 2);
    
    // 场景: 阈值 0.6 (只包含 1)
    auto results_strict = index->query_for_join(*query, 0.6);
    EXPECT_EQ(results_strict.size(), 1);
}

} // namespace sageFlow
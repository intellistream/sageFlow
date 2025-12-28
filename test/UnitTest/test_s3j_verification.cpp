#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <cmath>
#include <cstring>

#include "common/data_types.h" 
#include "operator/join_operator_methods/s3j_method.h"
#include "state/partitioned_vector_state.h"
#include "execution/vector_space_partitioner.h"
#include "execution/runtime_context.h"

using namespace sageFlow;

// Mock 分区器
class MockPartitioner : public VectorSpacePartitioner {
public:
    MockPartitioner(int dim) { }

    size_t partition(const VectorRecord& record, size_t num_partitions) override {
        return 0; // 总是返回 0
    }

    bool isBoundaryVector(const VectorRecord& record, size_t num_partitions) override {
        return false; 
    }

    std::vector<size_t> getCandidatePartitions(const VectorRecord& query, size_t num_partitions,
                                               size_t num_probes) override {
        return {0}; 
    }

    void train(const std::vector<VectorRecord>&) {}
    bool isInitialized() const { return true; }
    std::string getModelInfo() const { return "Mock"; }
};

class S3JVerificationTest : public ::testing::Test {
protected:
    void SetUp() override {
        config.similarity_threshold = 0.9; 
        config.dimension = 2;
        config.num_partitions = 1;
        config.enable_adaptive = false; 
        config.enable_metrics = false;
        
        auto partitioner = std::make_shared<MockPartitioner>(2);
        state = std::make_unique<PartitionedVectorState>(1, partitioner, 100, false);
        
        method = std::make_unique<S3JMethod>(0.9, config);
        method->setWindowStates(nullptr, state.get());
        
        // 使用正确的构造函数初始化 RuntimeContext
        RuntimeContext context(0, 1);
        method->open(context, nullptr, state.get());
    }

    void TearDown() override {
        method->close();
    }

    std::unique_ptr<VectorRecord> createRecord(uint64_t uid, float x, float y) {
        //  使用正确的枚举值 DataType::Float32
        VectorData vdata(2, DataType::Float32);
        
        // 准备原始数据
        float raw_data[2] = {x, y};
        size_t size = 2 * sizeof(float);
        
        // 将数据拷贝到 VectorData 的内部 buffer 中
        // VectorData 的 data_ 是 unique_ptr<char[]>
        std::memcpy(vdata.data_.get(), raw_data, size);

        //  使用构造函数初始化 VectorRecord
        auto rec = std::make_unique<VectorRecord>(
            uid,    
            1000,   // timestamp
            std::move(vdata)   
        );
        
        return rec;
    }

    S3JConfig config;
    std::unique_ptr<PartitionedVectorState> state;
    std::unique_ptr<S3JMethod> method;
};

// 
TEST_F(S3JVerificationTest, InnerSetPruningAndMatching) {
    // 1. 创建 Workset
    auto centroid = createRecord(999, 0.0f, 0.0f);
    state->createWorkset(1, std::move(centroid));
    
    S3JWorkset* ws = state->getWorkset(1);
    ASSERT_NE(ws, nullptr);
    
    // 2. 填充数据
    // Inner Set: 距离 0.01 (<= 0.05)
    ws->inner_set->addRecord(createRecord(101, 0.01f, 0.0f), 0);
    // Outer Set: 距离 0.15 (> 0.05)
    ws->outer_set->addRecord(createRecord(102, 0.15f, 0.0f), 0);
    
    // 3. 查询
    // Query 距离质心 0.01，触发 Inner Set 剪枝
    auto query = createRecord(201, 0.01f, 0.0f);
    auto results = method->ExecuteEager(*query, 0); 
    
    // 4. 验证
    bool found_101 = false;
    bool found_102 = false;
    for(const auto& res : results) {
        if (res->uid_ == 101) found_101 = true;
        if (res->uid_ == 102) found_102 = true;
    }
    
    EXPECT_TRUE(found_101) << "Should match record 101 from Inner Set";
    EXPECT_FALSE(found_102) << "Should NOT match record 102 (too far)";
}

TEST_F(S3JVerificationTest, BoundaryMatching) {
    auto centroid = createRecord(888, 1.0f, 1.0f);
    state->createWorkset(2, std::move(centroid));
    S3JWorkset* ws = state->getWorkset(2);
    
    ws->outer_set->addRecord(createRecord(301, 1.05f, 1.0f), 0);
    
    auto query = createRecord(401, 1.08f, 1.0f);
    auto results = method->ExecuteEager(*query, 0);
    
    bool found_301 = false;
    for(const auto& res : results) {
        if (res->uid_ == 301) found_301 = true;
    }
    EXPECT_TRUE(found_301) << "Should match record 301 from Outer Set";
}

TEST_F(S3JVerificationTest, PruningFarClusters) {
    auto centroid = createRecord(777, 10.0f, 10.0f);
    state->createWorkset(3, std::move(centroid));
    S3JWorkset* ws = state->getWorkset(3);
    
    ws->inner_set->addRecord(createRecord(501, 0.0f, 0.0f), 0);
    
    auto query = createRecord(601, 0.0f, 0.0f);
    auto results = method->ExecuteEager(*query, 0);
    
    EXPECT_EQ(results.size(), 0) << "Should prune the far workset";
}
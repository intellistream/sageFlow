#include "gtest/gtest.h"
#include "index/hdr_forest.h"
#include "common/data_types.h"
#include "compute_engine/compute_engine.h"
#include <memory>
#include <vector>
#include <cstring>

using namespace sageFlow;

TEST(HDRForestTest, BasicInsertAndQuery) {
    // Create index
    auto index = std::make_shared<HDRForest>(2, 5);
    index->dimension_ = 4;
    
    auto storage = std::make_shared<StorageManager>();
    storage->engine_ = std::make_shared<ComputeEngine>();
    index->storage_manager_ = storage;
    
    // Insert data into storage
    for (int i = 0; i < 10; ++i) {
        int dim = 4;
        float* data = new float[dim];
        for(int j=0; j<dim; ++j) data[j] = (float)i + j*0.1f;
        
        // VectorRecord takes ownership of char* data in one of its constructors
        // VectorRecord(const uint64_t &uid, const int64_t &timestamp, int32_t dim, DataType type, char *data);
        
        auto record = std::make_unique<VectorRecord>(
            (uint64_t)i, 
            (int64_t)0, 
            dim, 
            DataType::Float32, 
            reinterpret_cast<char*>(data)
        );
        
        storage->insert(std::move(record));
        index->insert(i);
    }
    
    // Query
    int dim = 4;
    float* qdata = new float[dim];
    for(int j=0; j<dim; ++j) qdata[j] = 0.0f; // Close to 0
    
    VectorRecord query_record(
        (uint64_t)999, 
        (int64_t)0, 
        dim, 
        DataType::Float32, 
        reinterpret_cast<char*>(qdata)
    );
    
    auto results = index->query(query_record, 5);
    EXPECT_LE(results.size(), 5);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

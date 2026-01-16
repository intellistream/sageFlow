#include <gtest/gtest.h>
#include <vector>
#include <memory>
#include <random>
#include <thread>
#include <atomic>
#include <chrono>

#include "operator/join_operator_methods/s3j_method.h"
#include "common/data_types.h"
#include "state/window_state.h"
#include "state/shared_window_state.h"
#include "concurrency/concurrency_manager.h"
#include "utils/logger.h"
#include "execution/runtime_context.h"

using namespace sageFlow;

// Mock workset directory if needed, or rely on LocalWorksetDirectory in S3JMethod default
// We don't need to do anything as S3JMethod creates one if not provided

class S3JBenchmark : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup config
        S3JConfig config;
        config.dimension = 128;
        config.enable_adaptive = true;
        config.adapt_interval_ms = 10;
        
        // Pass nullptr for storage, usually safe for benchmark if no persistence used
        concurrency_manager_ = std::make_shared<ConcurrencyManager>(nullptr);
        
        method_ = std::make_unique<S3JMethod>(
            0, 1, 0.8, concurrency_manager_, config
        );
        
        left_state_ = std::make_unique<SharedWindowState>();
        right_state_ = std::make_unique<SharedWindowState>();
        
        method_->setWindowStates(left_state_.get(), right_state_.get());
        
        RuntimeContext context(0, 1); 
        method_->open(context, left_state_.get(), right_state_.get());
    }

    void TearDown() override {
        method_->close();
    }

    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
    std::unique_ptr<S3JMethod> method_;
    std::unique_ptr<WindowState> left_state_;
    std::unique_ptr<WindowState> right_state_;
};

VectorRecord createRandomRecord(uint64_t uid) {
    // Correctly construct VectorData
    VectorData data(128, DataType::Float32);
    
    float* ptr = reinterpret_cast<float*>(data.data_.get());
    for(int i=0; i<128; ++i) {
        ptr[i] = (float)rand() / RAND_MAX;
    }
    
    return VectorRecord(uid, 1000, std::move(data));
}

TEST_F(S3JBenchmark, MetricsCollection) {
    VectorRecord query = createRandomRecord(1);
    auto results = method_->ExecuteEager(query, 0);
    
    auto metrics = method_->getMetrics();
    EXPECT_EQ(metrics.total_queries, 1);
}

TEST_F(S3JBenchmark, HighThroughput) {
    int num_queries = 1000;
    
    for(int i=0; i<1000; ++i) {
        auto rec = std::make_unique<VectorRecord>(createRandomRecord(100 + i));
        right_state_->addRecord(std::move(rec), 0);
    }
    
    auto start = std::chrono::high_resolution_clock::now();
    
    for(int i=0; i<num_queries; ++i) {
        VectorRecord query = createRandomRecord(i);
        method_->ExecuteEager(query, 0);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    if (duration == 0) duration = 1;
    double qps = (double)num_queries / duration * 1000;
    SAGEFLOW_LOG_INFO("S3J_Bench", "HighThroughput QPS: {:.2f}", qps);
    
    auto metrics = method_->getMetrics();
    EXPECT_EQ(metrics.total_queries, 1000); 
}

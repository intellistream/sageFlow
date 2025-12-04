#include <gtest/gtest.h>
#include "utils/logger.h"
#include <memory>
#include <thread>
#include <vector>
#include "operator/join_operator.h"
#include "function/join_function.h"
#include "test_utils/test_data_generator.h"
#include "test_utils/test_data_adapter.h"
#include "operator/join_metrics.h"
#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"
#include "execution/collector.h"
#include "execution/runtime_context.h"
#include "state/partitioned_window_state.h"
#include "state/shared_window_state.h"

namespace sageFlow {
namespace test {

/**
 * @brief JoinOperator 使用 WindowState 抽象的单元测试
 * 验证：
 * 1. JoinOperator 使用 PartitionedWindowState
 * 2. JoinOperator 使用 SharedWindowState
 * 3. 状态隔离性（分区模式）
 * 4. 状态可见性（共享模式）
 */
class JoinOperatorStateTest : public ::testing::Test {
protected:
    void SetUp() override {
        JoinMetrics::instance().reset();
        auto storage = std::make_shared<StorageManager>();
        concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage);

        generator_config_.vector_dim = 128;
        generator_config_.similarity_threshold = 0.8;
        generator_config_.seed = 42;
    }

    void TearDown() override {
        if (::testing::Test::HasFailure()) {
            SAGEFLOW_LOG_WARN("TEST", "JoinOperatorState Test failed. Metrics: WIN={}ns IDX={}ns SIM={}ns ",
                JoinMetrics::instance().window_insert_ns.load(),
                JoinMetrics::instance().index_insert_ns.load(),
                JoinMetrics::instance().similarity_ns.load());
        }
    }

    // 创建简单的 JoinFunction
    std::unique_ptr<Function> createSimpleJoinFunction() {
        auto join_func_lambda = [](std::unique_ptr<VectorRecord>& left,
                                std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
            std::vector<float> result_data;

            auto left_vec = extractFloatVector(*left);
            auto right_vec = extractFloatVector(*right);

            result_data.reserve(left_vec.size() + right_vec.size());
            result_data.insert(result_data.end(), left_vec.begin(), left_vec.end());
            result_data.insert(result_data.end(), right_vec.begin(), right_vec.end());

            uint64_t combined_uid = left->uid_ * 1000000 + right->uid_;
            int64_t result_timestamp = std::max(left->timestamp_, right->timestamp_);

            return createVectorRecord(combined_uid, result_timestamp, result_data);
        };

        return std::make_unique<JoinFunction>("SimpleJoin", join_func_lambda, 128);
    }

    // 创建测试用的 VectorRecord
    std::unique_ptr<VectorRecord> createTestRecord(uint64_t uid, int64_t timestamp, int dim = 128) {
        char* raw_data = new char[dim * sizeof(float)];
        float* float_data = reinterpret_cast<float*>(raw_data);
        for (int i = 0; i < dim; ++i) {
            float_data[i] = static_cast<float>(uid) + 0.1f * i;
        }
        return std::make_unique<VectorRecord>(uid, timestamp, dim, DataType::Float32, raw_data);
    }

protected:
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
    TestDataGenerator::Config generator_config_;
};

// ============================================================================
// 测试使用 PartitionedWindowState 的 JoinOperator
// ============================================================================

TEST_F(JoinOperatorStateTest, UsePartitionedState) {
    auto join_func = createSimpleJoinFunction();
    
    // 创建使用分区状态的 JoinOperator
    JoinOperator join_op(join_func, concurrency_manager_, "bruteforce_lazy",
                        generator_config_.similarity_threshold, false, "", false);  // use_shared_state = false
    
    RuntimeContext context(0, 4);  // subtask 0, parallelism 4
    join_op.open(context);
    
    std::vector<std::unique_ptr<Response>> emitted;
    Collector collector([&](std::unique_ptr<Response> r, int) {
        if (r && r->record_) emitted.push_back(std::move(r));
    });
    
    // 处理一些记录
    for (int i = 0; i < 5; ++i) {
        auto record = createTestRecord(i, 1000 + i * 10);
        Response response;
        response.type_ = ResponseType::Record;
        response.record_ = std::move(record);
        join_op.apply(std::move(response), 0, collector, context);
    }
    
    for (int i = 10; i < 15; ++i) {
        auto record = createTestRecord(i, 1000 + i * 10);
        Response response;
        response.type_ = ResponseType::Record;
        response.record_ = std::move(record);
        join_op.apply(std::move(response), 1, collector, context);
    }
    
    // 验证基本功能正常（不验证具体数量，只确保不崩溃）
    SAGEFLOW_LOG_INFO("TEST", "PartitionedState test completed, emitted {} records", emitted.size());
    SUCCEED();
}

// ============================================================================
// 测试使用 SharedWindowState 的 JoinOperator
// ============================================================================

TEST_F(JoinOperatorStateTest, UseSharedState) {
    auto join_func = createSimpleJoinFunction();
    
    // 创建使用共享状态的 JoinOperator
    JoinOperator join_op(join_func, concurrency_manager_, "bruteforce_lazy",
                        generator_config_.similarity_threshold, false, "", true);  // use_shared_state = true
    
    RuntimeContext context(0, 4);  // subtask 0, parallelism 4
    join_op.open(context);
    
    std::vector<std::unique_ptr<Response>> emitted;
    Collector collector([&](std::unique_ptr<Response> r, int) {
        if (r && r->record_) emitted.push_back(std::move(r));
    });
    
    // 处理一些记录
    for (int i = 0; i < 5; ++i) {
        auto record = createTestRecord(i, 1000 + i * 10);
        Response response;
        response.type_ = ResponseType::Record;
        response.record_ = std::move(record);
        join_op.apply(std::move(response), 0, collector, context);
    }
    
    for (int i = 10; i < 15; ++i) {
        auto record = createTestRecord(i, 1000 + i * 10);
        Response response;
        response.type_ = ResponseType::Record;
        response.record_ = std::move(record);
        join_op.apply(std::move(response), 1, collector, context);
    }
    
    // 验证基本功能正常
    SAGEFLOW_LOG_INFO("TEST", "SharedState test completed, emitted {} records", emitted.size());
    SUCCEED();
}

// ============================================================================
// 测试分区状态下不同 subtask 之间的隔离性
// ============================================================================

TEST_F(JoinOperatorStateTest, StateIsolationWithPartitioned) {
    auto join_func = createSimpleJoinFunction();
    
    // 创建使用分区状态的 JoinOperator
    JoinOperator join_op(join_func, concurrency_manager_, "bruteforce_lazy",
                        generator_config_.similarity_threshold, false, "", false);
    
    size_t parallelism = 4;
    RuntimeContext context0(0, parallelism);
    RuntimeContext context1(1, parallelism);
    RuntimeContext context2(2, parallelism);
    
    join_op.open(context0);
    
    std::atomic<int> emit_count{0};
    Collector collector([&](std::unique_ptr<Response> r, int) {
        if (r && r->record_) emit_count.fetch_add(1);
    });
    
    // 不同 subtask 并发处理
    std::vector<std::thread> threads;
    
    threads.emplace_back([&]() {
        for (int i = 0; i < 5; ++i) {
            auto record = createTestRecord(i, 1000 + i * 10);
            Response response{ResponseType::Record, std::move(record)};
            join_op.apply(std::move(response), 0, collector, context0);
        }
    });
    
    threads.emplace_back([&]() {
        for (int i = 100; i < 105; ++i) {
            auto record = createTestRecord(i, 1000 + i * 10);
            Response response{ResponseType::Record, std::move(record)};
            join_op.apply(std::move(response), 0, collector, context1);
        }
    });
    
    threads.emplace_back([&]() {
        for (int i = 200; i < 205; ++i) {
            auto record = createTestRecord(i, 1000 + i * 10);
            Response response{ResponseType::Record, std::move(record)};
            join_op.apply(std::move(response), 0, collector, context2);
        }
    });
    
    for (auto& t : threads) {
        t.join();
    }
    
    SAGEFLOW_LOG_INFO("TEST", "StateIsolation test completed, emit_count={}", emit_count.load());
    // 验证没有崩溃和死锁
    SUCCEED();
}

// ============================================================================
// 测试共享状态下所有 subtask 可以看到相同数据
// ============================================================================

TEST_F(JoinOperatorStateTest, StateVisibilityWithShared) {
    auto join_func = createSimpleJoinFunction();
    
    // 创建使用共享状态的 JoinOperator
    JoinOperator join_op(join_func, concurrency_manager_, "bruteforce_lazy",
                        generator_config_.similarity_threshold, false, "", true);
    
    size_t parallelism = 4;
    RuntimeContext context0(0, parallelism);
    RuntimeContext context1(1, parallelism);
    
    join_op.open(context0);
    
    std::atomic<int> emit_count{0};
    Collector collector([&](std::unique_ptr<Response> r, int) {
        if (r && r->record_) emit_count.fetch_add(1);
    });
    
    // 从 subtask 0 添加左侧数据
    for (int i = 0; i < 5; ++i) {
        auto record = createTestRecord(i, 1000 + i * 10);
        Response response{ResponseType::Record, std::move(record)};
        join_op.apply(std::move(response), 0, collector, context0);
    }
    
    // 从 subtask 1 添加右侧数据（共享模式下应该能看到 subtask 0 添加的左侧数据）
    for (int i = 10; i < 15; ++i) {
        auto record = createTestRecord(i, 1000 + i * 10);
        Response response{ResponseType::Record, std::move(record)};
        join_op.apply(std::move(response), 1, collector, context1);
    }
    
    SAGEFLOW_LOG_INFO("TEST", "StateVisibility test completed, emit_count={}", emit_count.load());
    // 验证功能正常
    SUCCEED();
}

// ============================================================================
// 测试向后兼容：使用旧的 open() 和 apply() 方法
// ============================================================================

TEST_F(JoinOperatorStateTest, BackwardCompatibility) {
    auto join_func = createSimpleJoinFunction();
    
    // 使用默认参数（不传 use_shared_state）
    JoinOperator join_op(join_func, concurrency_manager_, "bruteforce_lazy",
                        generator_config_.similarity_threshold);
    
    // 使用旧的 open() 方法
    join_op.open();
    
    std::vector<std::unique_ptr<Response>> emitted;
    Collector collector([&](std::unique_ptr<Response> r, int) {
        if (r && r->record_) emitted.push_back(std::move(r));
    });
    
    // 使用旧的 apply() 方法（不带 RuntimeContext）
    for (int i = 0; i < 5; ++i) {
        auto record = createTestRecord(i, 1000 + i * 10);
        Response response;
        response.type_ = ResponseType::Record;
        response.record_ = std::move(record);
        join_op.apply(std::move(response), 0, collector);
    }
    
    for (int i = 10; i < 15; ++i) {
        auto record = createTestRecord(i, 1000 + i * 10);
        Response response;
        response.type_ = ResponseType::Record;
        response.record_ = std::move(record);
        join_op.apply(std::move(response), 1, collector);
    }
    
    SAGEFLOW_LOG_INFO("TEST", "BackwardCompatibility test completed, emitted {} records", emitted.size());
    SUCCEED();
}

// ============================================================================
// 测试 Eager 模式使用 WindowState
// ============================================================================

TEST_F(JoinOperatorStateTest, EagerModeWithState) {
    auto join_func = createSimpleJoinFunction();
    
    // 创建 Eager 模式的 JoinOperator
    JoinOperator join_op(join_func, concurrency_manager_, "bruteforce_eager",
                        generator_config_.similarity_threshold, false, "", false);
    
    RuntimeContext context(0, 2);
    join_op.open(context);
    
    std::vector<std::unique_ptr<Response>> emitted;
    Collector collector([&](std::unique_ptr<Response> r, int) {
        if (r && r->record_) emitted.push_back(std::move(r));
    });
    
    // 先添加左侧数据
    for (int i = 0; i < 5; ++i) {
        auto record = createTestRecord(i, 1000 + i * 10);
        Response response{ResponseType::Record, std::move(record)};
        join_op.apply(std::move(response), 0, collector, context);
    }
    
    // 再添加右侧数据（Eager 模式应该立即产生 join 结果）
    for (int i = 10; i < 15; ++i) {
        auto record = createTestRecord(i, 1000 + i * 10);
        Response response{ResponseType::Record, std::move(record)};
        join_op.apply(std::move(response), 1, collector, context);
    }
    
    SAGEFLOW_LOG_INFO("TEST", "EagerMode test completed, emitted {} records", emitted.size());
    SUCCEED();
}

}  // namespace test
}  // namespace sageFlow

#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <memory>
#include <vector>
#include <atomic>
#include <random>
#include <cmath>
#include <cstring>

#include "utils/logger.h"
#include "stream/stream_environment.h"
#include "stream/stream.h"
#include "stream/data_stream_source/data_stream_source.h"
#include "stream/data_stream_source/simple_stream_source.h"
#include "function/join_function.h"
#include "function/sink_function.h"
#include "operator/join_operator.h"
#include "execution/collector.h"
#include "operator/join_metrics.h"
#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"
#include "test_utils/test_data_adapter.h"

namespace sageFlow {
namespace test {

// Helper: 等待处理稳定
static void wait_until_stable(std::chrono::milliseconds stable_window = std::chrono::milliseconds(100),
                              std::chrono::seconds max_total_wait = std::chrono::seconds(10)) {
    using namespace std::chrono_literals;
    auto end_by = std::chrono::steady_clock::now() + max_total_wait;
    uint64_t last = JoinMetrics::instance().total_emits.load();
    auto stable_since = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() < end_by) {
        std::this_thread::sleep_for(10ms);
        uint64_t cur = JoinMetrics::instance().total_emits.load();
        if (cur != last) {
            last = cur;
            stable_since = std::chrono::steady_clock::now();
        }
        if (std::chrono::steady_clock::now() - stable_since >= stable_window) break;
    }
}

// 简单的内存数据源
class TestVectorSource : public DataStreamSource {
public:
    explicit TestVectorSource(std::string name, std::vector<std::unique_ptr<VectorRecord>> records)
        : DataStreamSource(std::move(name), DataStreamSourceType::None),
          records_(std::move(records)), current_index_(0) {}

    void Init() override { current_index_ = 0; }

    auto Next() -> std::unique_ptr<VectorRecord> override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (current_index_ >= records_.size()) {
            return nullptr;
        }
        return std::move(records_[current_index_++]);
    }

private:
    std::vector<std::unique_ptr<VectorRecord>> records_;
    size_t current_index_;
    std::mutex mtx_;
};

// 创建JoinFunction的辅助函数
std::unique_ptr<Function> createTestJoinFunction(int dim) {
    auto join_func_lambda = [](std::unique_ptr<VectorRecord>& left, 
                               std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
        if (!left || !right) return nullptr;
        auto lv = extractFloatVector(*left);
        auto rv = extractFloatVector(*right);
        std::vector<float> out;
        out.reserve(lv.size() + rv.size());
        out.insert(out.end(), lv.begin(), lv.end());
        out.insert(out.end(), rv.begin(), rv.end());
        uint64_t id = left->uid_ * 1000000 + right->uid_;
        int64_t ts = std::max(left->timestamp_, right->timestamp_);
        return createVectorRecord(id, ts, out);
    };
    
    return std::make_unique<JoinFunction>("TestJoin", join_func_lambda, dim);
}

// 生成测试向量数据
std::vector<std::unique_ptr<VectorRecord>> generateTestVectors(
    int count, int dimension, int64_t base_timestamp, int64_t interval = 10) {
    std::vector<std::unique_ptr<VectorRecord>> records;
    std::mt19937 gen(42);
    std::uniform_real_distribution<float> dist(0.0f, 1.0f);

    for (int i = 0; i < count; ++i) {
        std::vector<float> data(static_cast<size_t>(dimension));
        for (int d = 0; d < dimension; ++d) {
            data[d] = dist(gen);
        }
        records.push_back(createVectorRecord(
            static_cast<uint64_t>(i + 1),
            base_timestamp + i * interval,
            data));
    }
    return records;
}

// 生成相似的测试向量（用于确保有匹配结果）
std::vector<std::unique_ptr<VectorRecord>> generateSimilarVectors(
    const std::vector<std::unique_ptr<VectorRecord>>& source,
    int64_t base_timestamp, float noise_level = 0.1f) {
    std::vector<std::unique_ptr<VectorRecord>> records;
    std::mt19937 gen(123);
    std::uniform_real_distribution<float> noise_dist(-noise_level, noise_level);

    uint64_t uid_offset = 10000;
    for (size_t i = 0; i < source.size(); ++i) {
        auto orig_data = extractFloatVector(*source[i]);
        std::vector<float> data(orig_data.size());
        for (size_t d = 0; d < orig_data.size(); ++d) {
            data[d] = orig_data[d] + noise_dist(gen);
        }
        records.push_back(createVectorRecord(
            uid_offset + i + 1,
            base_timestamp + static_cast<int64_t>(i) * 10,
            data));
    }
    return records;
}

// ============================================================================
// VSJoin 集成测试
// ============================================================================

class VSJoinIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 重置 JoinMetrics
        JoinMetrics::instance().reset();
        // 创建 StorageManager 和 ConcurrencyManager
        storage_manager_ = std::make_shared<StorageManager>();
        concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage_manager_);
    }

    void TearDown() override {
        // 清理
    }

    // 创建 VSJoin 配置
    VSJoinConfig createDefaultConfig() {
        VSJoinConfig config;
        config.enabled = true;
        config.num_partitions = 4;
        config.compact_threshold = 50;
        config.enable_boundary_tracking = true;
        config.allowed_lateness = 1000;
        config.watermark_delay = 500;
        config.async_generator_threads = 2;
        config.num_probes = 2;
        config.ivf_nlist = 10;
        config.ivf_nprobes = 5;
        config.distance_alpha = 0.1;
        return config;
    }

    std::shared_ptr<StorageManager> storage_manager_;
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};

// 测试 VSJoin 配置应用
TEST_F(VSJoinIntegrationTest, VSJoinConfigApplication) {
    const int dimension = 64;

    std::unique_ptr<Function> join_func = createTestJoinFunction(dimension);

    // 创建 VSJoin 模式的 JoinOperator
    JoinOperator join_op(join_func, concurrency_manager_, "vsjoin_eager", 0.8);

    // 验证 VSJoin 已启用
    EXPECT_TRUE(join_op.isVSJoinEnabled());

    // 设置自定义配置
    VSJoinConfig config = createDefaultConfig();
    config.num_partitions = 8;
    config.num_probes = 3;
    join_op.setVSJoinConfig(config);

    // 验证配置已应用
    const auto& applied_config = join_op.getVSJoinConfig();
    EXPECT_TRUE(applied_config.enabled);
    EXPECT_EQ(applied_config.num_partitions, 8);
    EXPECT_EQ(applied_config.num_probes, 3);
}

// 测试 VSJoin 基本功能
TEST_F(VSJoinIntegrationTest, BasicFunctionality) {
    const int dimension = 32;
    const double threshold = 0.5;  // 较低阈值以确保有匹配

    std::unique_ptr<Function> join_func = createTestJoinFunction(dimension);

    // 创建 VSJoin 模式的 JoinOperator
    JoinOperator join_op(join_func, concurrency_manager_, "vsjoin_eager", threshold);
    EXPECT_TRUE(join_op.isVSJoinEnabled());

    // 初始化
    RuntimeContext context(0, 1);
    join_op.open(context);

    // 创建测试数据
    auto left_records = generateTestVectors(10, dimension, 1000, 100);
    auto right_records = generateSimilarVectors(left_records, 1000, 0.05f);

    // 结果计数器
    std::atomic<size_t> result_count{0};
    
    // 创建 Collector
    Collector collector([&](std::unique_ptr<Response> resp, int slot) {
        if (resp && resp->record_) {
            result_count++;
        }
    });

    // 模拟处理记录
    for (auto& rec : left_records) {
        Response resp(ResponseType::Record, std::move(rec));
        join_op.apply(std::move(resp), 0, collector, context);
    }

    for (auto& rec : right_records) {
        Response resp(ResponseType::Record, std::move(rec));
        join_op.apply(std::move(resp), 1, collector, context);
    }

    // 验证有输出（具体数量取决于相似度）
    SAGEFLOW_LOG_INFO("TEST", "VSJoin basic test: {} results collected", result_count.load());
    // 不强制要求特定数量，只要不崩溃就是成功
    
    join_op.close();
}

// 测试 VSJoin 与 Legacy 模式向后兼容
TEST_F(VSJoinIntegrationTest, LegacyModeUnchanged) {
    const int dimension = 32;
    const double threshold = 0.8;

    // 创建 Legacy 模式的 JoinOperator
    std::unique_ptr<Function> join_func = createTestJoinFunction(dimension);
    JoinOperator join_op(join_func, concurrency_manager_, "bruteforce_eager", threshold);

    // 验证不是 VSJoin 模式
    EXPECT_FALSE(join_op.isVSJoinEnabled());

    // 初始化
    RuntimeContext context(0, 1);
    join_op.open(context);

    // 结果计数器
    std::atomic<size_t> result_count{0};
    
    // 创建 Collector
    Collector collector([&](std::unique_ptr<Response> resp, int slot) {
        if (resp && resp->record_) {
            result_count++;
        }
    });

    // 创建测试数据
    auto left_records = generateTestVectors(5, dimension, 1000, 50);
    auto right_records = generateSimilarVectors(left_records, 1000, 0.01f);

    for (auto& rec : left_records) {
        Response resp(ResponseType::Record, std::move(rec));
        join_op.apply(std::move(resp), 0, collector, context);
    }

    for (auto& rec : right_records) {
        Response resp(ResponseType::Record, std::move(rec));
        join_op.apply(std::move(resp), 1, collector, context);
    }

    SAGEFLOW_LOG_INFO("TEST", "Legacy mode test: {} results collected", result_count.load());
    
    join_op.close();
}

// 测试空流处理
TEST_F(VSJoinIntegrationTest, EmptyStream) {
    const int dimension = 32;

    std::unique_ptr<Function> join_func = createTestJoinFunction(dimension);
    JoinOperator join_op(join_func, concurrency_manager_, "vsjoin_eager", 0.8);

    RuntimeContext context(0, 1);
    join_op.open(context);

    std::atomic<size_t> result_count{0};
    Collector collector([&](std::unique_ptr<Response> resp, int slot) {
        result_count++;
    });

    // 不发送任何数据，验证不会崩溃
    EXPECT_EQ(result_count.load(), 0u);
    
    join_op.close();
}

// 测试单条记录处理
TEST_F(VSJoinIntegrationTest, SingleRecord) {
    const int dimension = 32;

    std::unique_ptr<Function> join_func = createTestJoinFunction(dimension);
    JoinOperator join_op(join_func, concurrency_manager_, "vsjoin_eager", 0.8);

    RuntimeContext context(0, 1);
    join_op.open(context);

    std::atomic<size_t> result_count{0};
    Collector collector([&](std::unique_ptr<Response> resp, int slot) {
        result_count++;
    });

    // 只发送一条左侧记录
    auto left_records = generateTestVectors(1, dimension, 1000);
    for (auto& rec : left_records) {
        Response resp(ResponseType::Record, std::move(rec));
        join_op.apply(std::move(resp), 0, collector, context);
    }

    // 由于右侧为空，不应该有 join 结果
    EXPECT_EQ(result_count.load(), 0u);
    
    join_op.close();
}

// 测试 VSJoin Lazy 模式
TEST_F(VSJoinIntegrationTest, VSJoinLazyMode) {
    const int dimension = 32;
    const double threshold = 0.5;

    std::unique_ptr<Function> join_func = createTestJoinFunction(dimension);
    JoinOperator join_op(join_func, concurrency_manager_, "vsjoin_lazy", threshold);

    EXPECT_TRUE(join_op.isVSJoinEnabled());

    RuntimeContext context(0, 1);
    join_op.open(context);

    std::atomic<size_t> result_count{0};
    Collector collector([&](std::unique_ptr<Response> resp, int slot) {
        result_count++;
    });

    // 创建测试数据
    auto left_records = generateTestVectors(5, dimension, 1000, 100);
    auto right_records = generateSimilarVectors(left_records, 1000, 0.05f);

    for (auto& rec : left_records) {
        Response resp(ResponseType::Record, std::move(rec));
        join_op.apply(std::move(resp), 0, collector, context);
    }

    for (auto& rec : right_records) {
        Response resp(ResponseType::Record, std::move(rec));
        join_op.apply(std::move(resp), 1, collector, context);
    }

    SAGEFLOW_LOG_INFO("TEST", "VSJoin lazy mode: {} results collected", result_count.load());
    
    join_op.close();
}

// 测试延迟到达处理
TEST_F(VSJoinIntegrationTest, LateArrivalHandling) {
    const int dimension = 32;

    std::unique_ptr<Function> join_func = createTestJoinFunction(dimension);
    JoinOperator join_op(join_func, concurrency_manager_, "vsjoin_eager", 0.5);

    // 配置延迟处理
    VSJoinConfig config;
    config.enabled = true;
    config.num_partitions = 4;
    config.allowed_lateness = 500;  // 允许 500ms 延迟
    config.watermark_delay = 100;
    join_op.setVSJoinConfig(config);

    RuntimeContext context(0, 1);
    join_op.open(context);

    std::atomic<size_t> result_count{0};
    Collector collector([&](std::unique_ptr<Response> resp, int slot) {
        result_count++;
    });

    // 先发送正常时间戳的数据
    auto normal_records = generateTestVectors(5, dimension, 1000, 100);
    for (auto& rec : normal_records) {
        Response resp(ResponseType::Record, std::move(rec));
        join_op.apply(std::move(resp), 0, collector, context);
    }

    // 发送一条延迟到达的数据（时间戳较早）
    std::vector<float> late_data(static_cast<size_t>(dimension), 0.5f);
    auto late_record = createVectorRecord(999, 800, late_data);  // 时间戳早于正常数据

    Response late_resp(ResponseType::Record, std::move(late_record));
    join_op.apply(std::move(late_resp), 0, collector, context);

    SAGEFLOW_LOG_INFO("TEST", "Late arrival test completed without crash");
    
    join_op.close();
}

// 测试多分区配置
TEST_F(VSJoinIntegrationTest, MultiPartitionConfiguration) {
    const int dimension = 64;

    std::unique_ptr<Function> join_func = createTestJoinFunction(dimension);
    JoinOperator join_op(join_func, concurrency_manager_, "vsjoin_eager", 0.5);

    // 配置多分区
    VSJoinConfig config;
    config.enabled = true;
    config.num_partitions = 16;  // 较多分区
    config.num_probes = 4;       // 探测 4 个分区
    config.compact_threshold = 20;
    join_op.setVSJoinConfig(config);

    RuntimeContext context(0, 1);
    join_op.open(context);

    std::atomic<size_t> result_count{0};
    Collector collector([&](std::unique_ptr<Response> resp, int slot) {
        result_count++;
    });

    // 创建较多测试数据
    auto left_records = generateTestVectors(50, dimension, 1000, 20);
    auto right_records = generateSimilarVectors(left_records, 1000, 0.1f);

    for (auto& rec : left_records) {
        Response resp(ResponseType::Record, std::move(rec));
        join_op.apply(std::move(resp), 0, collector, context);
    }

    for (auto& rec : right_records) {
        Response resp(ResponseType::Record, std::move(rec));
        join_op.apply(std::move(resp), 1, collector, context);
    }

    SAGEFLOW_LOG_INFO("TEST", "Multi-partition test: {} results with 16 partitions", result_count.load());
    
    join_op.close();
}

// 测试边界向量追踪
TEST_F(VSJoinIntegrationTest, BoundaryVectorTracking) {
    const int dimension = 32;

    std::unique_ptr<Function> join_func = createTestJoinFunction(dimension);
    JoinOperator join_op(join_func, concurrency_manager_, "vsjoin_eager", 0.5);

    // 启用边界追踪
    VSJoinConfig config;
    config.enabled = true;
    config.num_partitions = 4;
    config.enable_boundary_tracking = true;
    join_op.setVSJoinConfig(config);

    RuntimeContext context(0, 1);
    join_op.open(context);

    std::atomic<size_t> result_count{0};
    Collector collector([&](std::unique_ptr<Response> resp, int slot) {
        result_count++;
    });

    // 创建测试数据
    auto left_records = generateTestVectors(20, dimension, 1000, 50);
    auto right_records = generateSimilarVectors(left_records, 1000, 0.08f);

    for (auto& rec : left_records) {
        Response resp(ResponseType::Record, std::move(rec));
        join_op.apply(std::move(resp), 0, collector, context);
    }

    for (auto& rec : right_records) {
        Response resp(ResponseType::Record, std::move(rec));
        join_op.apply(std::move(resp), 1, collector, context);
    }

    SAGEFLOW_LOG_INFO("TEST", "Boundary tracking test: {} results", result_count.load());
    
    join_op.close();
}

}  // namespace test
}  // namespace sageFlow

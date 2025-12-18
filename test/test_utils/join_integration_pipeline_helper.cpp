/**
 * @file join_integration_pipeline_helper.cpp
 * @brief E-03: Join 集成测试 Pipeline 辅助类实现
 */

#include "test_utils/join_integration_pipeline_helper.h"
#include "test_utils/test_data_adapter.h"
#include "stream/stream_environment.h"
#include "stream/stream.h"
#include "stream/data_stream_source/data_stream_source.h"
#include "function/join_function.h"
#include "function/sink_function.h"
#include "operator/join_config_validator.h"
#include "operator/join_metrics.h"
#include "utils/logger.h"

#include <chrono>
#include <thread>
#include <atomic>

namespace sageFlow {
namespace test {

// ==================== TestVectorStreamSource ====================
// 内部使用的测试数据源，将内存中的 VectorRecord 注入到流中

class TestVectorStreamSource : public DataStreamSource {
public:
    explicit TestVectorStreamSource(std::string name, 
                                    std::vector<std::unique_ptr<VectorRecord>> records)
        : DataStreamSource(std::move(name), DataStreamSourceType::None)
        , records_(std::move(records)) {}
    
    void Init() override { 
        idx_ = 0; 
    }
    
    auto Next() -> std::unique_ptr<VectorRecord> override {
        if (idx_ >= records_.size()) return nullptr;
        return std::move(records_[idx_++]);
    }

private:
    std::vector<std::unique_ptr<VectorRecord>> records_;
    size_t idx_{0};
};

// ==================== ExecutableTestPipelineImpl ====================

class ExecutableTestPipelineImpl : public ExecutableTestPipeline {
public:
    ExecutableTestPipelineImpl(
        std::shared_ptr<DataStreamSource> left_source,
        std::shared_ptr<DataStreamSource> right_source,
        std::shared_ptr<MatchCollectorSink> sink,
        const JoinStrategyConfig& config,
        int parallelism)
        : left_source_(std::move(left_source))
        , right_source_(std::move(right_source))
        , sink_(std::move(sink))
        , config_(config)
        , parallelism_(parallelism) {}
    
    PipelineExecutionResult execute() override {
        PipelineExecutionResult result;
        
        try {
            // 创建新的 StreamEnvironment
            StreamEnvironment env;
            
            // 重置指标
            JoinMetrics::instance().reset();
            
            // 获取 Join 方法字符串
            std::string join_method = JoinIntegrationPipelineHelper::getJoinMethodString(config_);
            
            // 创建 Join Function
            auto join_func = std::make_unique<JoinFunction>(
                "IntegrationTestJoin",
                [](std::unique_ptr<VectorRecord>& left,
                   std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
                    // 提取向量
                    auto lv = extractFloatVector(*left);
                    auto rv = extractFloatVector(*right);
                    
                    // 合并向量
                    std::vector<float> out;
                    out.reserve(lv.size() + rv.size());
                    out.insert(out.end(), lv.begin(), lv.end());
                    out.insert(out.end(), rv.begin(), rv.end());
                    
                    // 生成合并后的 UID：left_uid * 1000000 + right_uid % 1000000
                    constexpr uint64_t kModuloBase = 1000000ULL;
                    uint64_t id = left->uid_ * kModuloBase + right->uid_ % kModuloBase;
                    int64_t ts = std::max(left->timestamp_, right->timestamp_);
                    
                    return createVectorRecord(id, ts, out);
                },
                config_.dimension);
            
            // 设置窗口参数
            uint64_t trigger_interval = static_cast<uint64_t>(
                std::max<int64_t>(config_.step_size_ms, 1));
            join_func->setWindow(config_.window_size_ms, trigger_interval);
            
            // 重置 sink
            sink_->reset();
            
            // 创建 SinkFunction 包装器
            auto sink_ptr = sink_;
            auto sink_func = std::make_unique<SinkFunction>(
                "IntegrationTestSink",
                [sink_ptr](std::unique_ptr<VectorRecord>& rec) {
                    if (sink_ptr) {
                        sink_ptr->invoke(rec);
                    }
                });
            
            // 构建 Pipeline
            left_source_->join(right_source_, std::move(join_func), 
                              join_method, config_.similarity_threshold, 
                              static_cast<size_t>(parallelism_))
                ->writeSink(std::move(sink_func), 1);
            
            // 添加流到环境
            env.addStream(left_source_);
            env.addStream(right_source_);
            
            // 执行
            auto start = std::chrono::high_resolution_clock::now();
            env.execute();
            
            // 等待完成
            waitForCompletion(env);
            
            // 停止并等待终止
            env.stop();
            env.awaitTermination();
            
            auto end = std::chrono::high_resolution_clock::now();
            result.execution_time_ms = 
                std::chrono::duration<double, std::milli>(end - start).count();
            
            // 收集结果
            result.matches = sink_->getMatches();
            result.left_processed = static_cast<int64_t>(
                JoinMetrics::instance().total_records_left.load());
            result.right_processed = static_cast<int64_t>(
                JoinMetrics::instance().total_records_right.load());
            result.success = true;
            
        } catch (const std::exception& e) {
            result.success = false;
            result.error_message = e.what();
            SAGEFLOW_LOG_ERROR("PipelineHelper", "Pipeline execution failed: {}", e.what());
        }
        
        return result;
    }
    
    std::vector<MatchPair> getMatches() const override {
        return sink_ ? sink_->getMatches() : std::vector<MatchPair>{};
    }

private:
    void waitForCompletion(StreamEnvironment& env) {
        using namespace std::chrono_literals;
        
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        
        // 等待输入被处理（检查 total_records 计数）
        // 注意：不能等待 completed 计数，因为在短窗口内记录可能不会过期
        uint64_t expected_records = 0;  // 我们不知道预期数量，所以等待输出稳定
        
        // 等待第一条输入被处理
        for (;;) {
            uint64_t total_left = JoinMetrics::instance().total_records_left.load();
            uint64_t total_right = JoinMetrics::instance().total_records_right.load();
            
            if (total_left > 0 && total_right > 0) {
                break;  // 至少有一些输入已处理
            }
            
            if (std::chrono::steady_clock::now() >= deadline) {
                SAGEFLOW_LOG_WARN("PipelineHelper", 
                    "Timeout waiting for input processing");
                break;
            }
            
            std::this_thread::sleep_for(5ms);
        }
        
        // 等待输出稳定（连续一段时间没有新输出）
        const auto stable_window = 200ms;  // 需要更长的稳定时间
        const auto max_wait = std::chrono::seconds(10);
        uint64_t last = JoinMetrics::instance().total_emits.load();
        auto stable_since = std::chrono::steady_clock::now();
        auto end_by = std::chrono::steady_clock::now() + max_wait;
        
        while (std::chrono::steady_clock::now() < end_by) {
            std::this_thread::sleep_for(5ms);
            uint64_t cur = JoinMetrics::instance().total_emits.load();
            if (cur != last) {
                last = cur;
                stable_since = std::chrono::steady_clock::now();
            }
            if (std::chrono::steady_clock::now() - stable_since >= stable_window) {
                break;
            }
        }
    }
    
    std::shared_ptr<DataStreamSource> left_source_;
    std::shared_ptr<DataStreamSource> right_source_;
    std::shared_ptr<MatchCollectorSink> sink_;
    JoinStrategyConfig config_;
    int parallelism_;
};

// ==================== MatchCollectorSink 实现 ====================

void MatchCollectorSink::open() {
    std::lock_guard<std::mutex> lock(mutex_);
    matches_.clear();
    processed_count_ = 0;
}

void MatchCollectorSink::invoke(std::unique_ptr<VectorRecord>& record) {
    if (!record) return;
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // 从合并的 UID 中解析出 left_uid 和 right_uid
    // 合并规则：id = left_uid * 1000000 + right_uid % 1000000
    constexpr uint64_t kModuloBase = 1000000ULL;
    uint64_t combined_id = record->uid_;
    uint64_t left_uid = combined_id / kModuloBase;
    uint64_t right_uid = combined_id % kModuloBase;
    
    MatchPair pair;
    pair.left_uid = left_uid;
    pair.right_uid = right_uid;
    pair.similarity = 0.0;  // 相似度需要从其他地方获取（如 metadata）
    
    matches_.push_back(pair);
    processed_count_++;
}

void MatchCollectorSink::close() {
    // 清理资源（如果需要）
}

std::vector<MatchPair> MatchCollectorSink::getMatches() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return matches_;
}

int64_t MatchCollectorSink::getProcessedCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return processed_count_;
}

void MatchCollectorSink::reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    matches_.clear();
    processed_count_ = 0;
}

// ==================== JoinIntegrationPipelineHelper 实现 ====================

std::unique_ptr<ExecutableTestPipeline> 
JoinIntegrationPipelineHelper::createPipeline(
    std::vector<std::unique_ptr<VectorRecord>> left_stream,
    std::vector<std::unique_ptr<VectorRecord>> right_stream,
    const JoinStrategyConfig& config,
    int parallelism) {
    
    // 创建内存数据源
    auto left_source = std::make_shared<TestVectorStreamSource>(
        "IntegrationLeft", std::move(left_stream));
    auto right_source = std::make_shared<TestVectorStreamSource>(
        "IntegrationRight", std::move(right_stream));
    
    return createPipeline(left_source, right_source, config, parallelism);
}

std::unique_ptr<ExecutableTestPipeline> 
JoinIntegrationPipelineHelper::createPipeline(
    std::shared_ptr<DataStreamSource> left_source,
    std::shared_ptr<DataStreamSource> right_source,
    const JoinStrategyConfig& config,
    int parallelism) {
    
    // 创建结果收集器
    auto sink = std::make_shared<MatchCollectorSink>();
    
    return std::make_unique<ExecutableTestPipelineImpl>(
        std::move(left_source),
        std::move(right_source),
        std::move(sink),
        config,
        parallelism);
}

std::unique_ptr<ExecutableTestPipeline> 
JoinIntegrationPipelineHelper::createSelfJoinPipeline(
    std::vector<std::unique_ptr<VectorRecord>> stream,
    const JoinStrategyConfig& config,
    int parallelism) {
    
    // 复制数据到两个流
    std::vector<std::unique_ptr<VectorRecord>> left_stream;
    std::vector<std::unique_ptr<VectorRecord>> right_stream;
    
    left_stream.reserve(stream.size());
    right_stream.reserve(stream.size());
    
    constexpr uint64_t kRightUidOffset = 500000;
    
    for (auto& record : stream) {
        if (!record) continue;
        
        auto vec = extractFloatVector(*record);
        
        // 左流保持原 UID
        left_stream.push_back(createVectorRecord(
            record->uid_, record->timestamp_, vec));
        
        // 右流添加偏移
        right_stream.push_back(createVectorRecord(
            record->uid_ + kRightUidOffset, record->timestamp_, vec));
    }
    
    return createPipeline(std::move(left_stream), std::move(right_stream), 
                          config, parallelism);
}

std::unique_ptr<ExecutableTestPipeline> 
JoinIntegrationPipelineHelper::createValidatedPipeline(
    std::vector<std::unique_ptr<VectorRecord>> left_stream,
    std::vector<std::unique_ptr<VectorRecord>> right_stream,
    const JoinStrategyConfig& config,
    int parallelism) {
    
    // 验证配置
    auto validation = JoinConfigValidator::validate(config);
    
    if (!validation.valid) {
        std::string error_msg = "Invalid join configuration: ";
        for (const auto& err : validation.errors) {
            error_msg += err + "; ";
        }
        throw std::runtime_error(error_msg);
    }
    
    // 警告日志
    for (const auto& warning : validation.warnings) {
        SAGEFLOW_LOG_WARN("PipelineHelper", "Config warning: {}", warning);
    }
    
    return createPipeline(std::move(left_stream), std::move(right_stream), 
                          config, parallelism);
}

std::string JoinIntegrationPipelineHelper::getJoinMethodString(
    const JoinStrategyConfig& config) {
    
    std::string algo_str;
    switch (config.algorithm) {
        case JoinAlgorithm::BRUTEFORCE:
            algo_str = "bruteforce";
            break;
        case JoinAlgorithm::IVF:
            algo_str = "ivf";
            break;
        case JoinAlgorithm::HNSW:
            algo_str = "hnsw";
            break;
        case JoinAlgorithm::HDR_TREE:
            algo_str = "hdr_tree";
            break;
        case JoinAlgorithm::CLUSTERED_JOIN:
            algo_str = "clustered_join";
            break;
        case JoinAlgorithm::S3J:
            algo_str = "s3j";
            break;
        case JoinAlgorithm::VSJOIN:
            algo_str = "vsjoin";
            break;
        default:
            algo_str = "bruteforce";
            break;
    }
    
    // 添加 eager/lazy 后缀
    // 注意：当前实现都使用 eager 模式
    std::string suffix = config.is_eager ? "_eager" : "_eager";  // lazy 已移除
    
    return algo_str + suffix;
}

}  // namespace test
}  // namespace sageFlow

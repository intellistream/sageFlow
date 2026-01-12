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
#include "operator/utils/join_config_validator.h"
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
        int parallelism,
        size_t expected_left = 0,
        size_t expected_right = 0)
        : left_source_(std::move(left_source))
        , right_source_(std::move(right_source))
        , sink_(std::move(sink))
        , config_(config)
        , parallelism_(parallelism)
        , expected_left_(expected_left)
        , expected_right_(expected_right) {}
    
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
                    // 注意：集成测试需要稳定的 (left_uid, right_uid) 来计算 recall/precision 和做去重，
                    // 不能依赖把 pair 编进 uid 的“可逆解码”（实际应用中 uid 不保证有偏移/不冲突）。
                    //
                    // 这里采用“显式携带 pair”的方案：
                    // - record.uid_：仅用于下游快速去重/标识（使用 hash，避免依赖偏移与取模）
                    // - record.data_：携带 [left_uid, right_uid] 两个 int64
                    //
                    // 同时保持 payload 很小，避免 multicast 下 OOM。
                    uint64_t l = left->uid_;
                    uint64_t r = right->uid_;
                    uint64_t h = l ^ (r + 0x9e3779b97f4a7c15ULL + (l << 6) + (l >> 2));
                    int64_t ts = std::max(left->timestamp_, right->timestamp_);
                    return createInt64VectorRecord(
                        h, ts, {static_cast<int64_t>(l), static_cast<int64_t>(r)});
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
            // 优先使用完整策略配置，回退到字符串 API
            std::shared_ptr<Stream> join_stream;
            if (config_.algorithm != JoinAlgorithm::BRUTEFORCE || 
                config_.clustered_multicast_k != 0) {
                // 使用策略配置 API（支持完整配置）
                // 注意：由于 Stream::join() 没有直接接受策略配置的重载，
                // 我们先调用字符串 API，然后在返回的 Stream 上设置策略配置
                join_stream = left_source_->join(right_source_, std::move(join_func), 
                                  join_method, config_.similarity_threshold, 
                                  static_cast<size_t>(parallelism_));
                // 设置完整策略配置
                join_stream->setJoinStrategyConfig(config_);
            } else {
                // 回退到字符串 API（向后兼容）
                join_stream = left_source_->join(right_source_, std::move(join_func), 
                                  join_method, config_.similarity_threshold, 
                                  static_cast<size_t>(parallelism_));
            }
            
            join_stream->writeSink(std::move(sink_func), 1);
            
            // 添加流到环境
            env.addStream(left_source_);
            env.addStream(right_source_);
            
            // 执行
            auto start_wall = std::chrono::high_resolution_clock::now();
            auto start_steady = std::chrono::steady_clock::now();
            env.execute();
            
            // 等待完成
            auto wait_stats = waitForCompletion(env, start_steady);
            
            // 停止并等待终止
            env.stop();
            env.awaitTermination();
            
            auto end_wall = std::chrono::high_resolution_clock::now();
            result.execution_time_ms = 
                std::chrono::duration<double, std::milli>(end_wall - start_wall).count();
            
            // 算法口径：Join emits stable 时间点（并行 makespan）
            result.join_time_ms = wait_stats.join_time_ms;
            result.sink_wait_ms = wait_stats.sink_wait_ms;
            result.total_emits = wait_stats.total_emits;
            result.sink_processed = wait_stats.sink_processed;
            result.sink_dedup = wait_stats.sink_dedup;
            
            // 收集结果
            result.matches = sink_->getMatches();
            result.dedup_count = sink_->getDedupCount();
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
    struct WaitStats {
        double join_time_ms = 0.0;     // time to emits stable
        double sink_wait_ms = 0.0;     // time spent waiting sink catch-up
        uint64_t total_emits = 0;
        uint64_t sink_processed = 0;
        uint64_t sink_dedup = 0;
    };

    WaitStats waitForCompletion(StreamEnvironment& env, std::chrono::steady_clock::time_point start_steady) {
        using namespace std::chrono_literals;
        WaitStats stats;
        
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        
        // 策略：等待所有输入被处理，然后等待短暂的输出稳定期
        // 这与 test_join_datasource_modes 的等待策略保持一致
        
        // 阶段1：等待所有输入被处理
        if (expected_left_ > 0 && expected_right_ > 0) {
            // 已知预期数量，等待所有输入被处理
            for (;;) {
                uint64_t total_left = JoinMetrics::instance().total_records_left.load();
                uint64_t total_right = JoinMetrics::instance().total_records_right.load();
                
                // 检查是否所有输入都已处理
                bool inputs_drained = (total_left >= expected_left_ && total_right >= expected_right_);
                if (inputs_drained) {
                    break;
                }
                
                if (std::chrono::steady_clock::now() >= deadline) {
                    SAGEFLOW_LOG_WARN("PipelineHelper", 
                        "Timeout waiting for input processing: left={}/{} right={}/{}",
                        total_left, expected_left_, total_right, expected_right_);
                    break;
                }
                
                std::this_thread::sleep_for(5ms);
            }
        } else {
            // 未知预期数量，等待至少有一些输入被处理
            for (;;) {
                uint64_t total_left = JoinMetrics::instance().total_records_left.load();
                uint64_t total_right = JoinMetrics::instance().total_records_right.load();
                
                if (total_left > 0 && total_right > 0) {
                    break;
                }
                
                if (std::chrono::steady_clock::now() >= deadline) {
                    SAGEFLOW_LOG_WARN("PipelineHelper", 
                        "Timeout waiting for input processing");
                    break;
                }
                
                std::this_thread::sleep_for(5ms);
            }
        }
        
        // 阶段2：等待 JoinOperator emits 稳定
        // 所有输入处理完后，JoinOperator 需要时间产生所有输出
        // 注意：高并行度 + 全局锁竞争下，total_emits 可能会出现 >200ms 的“停顿”
        // （线程在等待锁/调度），这并不代表 Join 已经完成。
        // 这里按并行度放大稳定窗口，避免测试框架过早 stop 导致召回率“随机波动”。
        const auto emits_stable_window =
            (parallelism_ >= 16) ? 2000ms :
            (parallelism_ >= 8)  ? 1000ms : 200ms;
        const auto max_emit_wait = std::chrono::seconds(60);
        auto emit_end_by = std::chrono::steady_clock::now() + max_emit_wait;
        uint64_t last_emits = JoinMetrics::instance().total_emits.load();
        auto emits_stable_since = std::chrono::steady_clock::now();
        auto emits_stable_at = std::chrono::steady_clock::now();
        
        while (std::chrono::steady_clock::now() < emit_end_by) {
            std::this_thread::sleep_for(20ms);
            uint64_t cur_emits = JoinMetrics::instance().total_emits.load();
            
            if (cur_emits != last_emits) {
                last_emits = cur_emits;
                emits_stable_since = std::chrono::steady_clock::now();
            }
            
            if (std::chrono::steady_clock::now() - emits_stable_since >= emits_stable_window) {
                emits_stable_at = std::chrono::steady_clock::now();
                SAGEFLOW_LOG_INFO("PipelineHelper", 
                    "JoinOperator emits stable at {}", cur_emits);
                break;
            }
        }

        // 记录 Join 算法完成时间（并行 makespan）
        stats.total_emits = JoinMetrics::instance().total_emits.load();
        stats.join_time_ms = std::chrono::duration<double, std::milli>(emits_stable_at - start_steady).count();
        
        // 阶段3：等待 Sink 处理完所有 emits
        // 同理：在高并行度下，sink 的 processed/dedup 也可能出现短暂停顿，
        // 不应因为 200ms 不变就提前结束，否则会把“还没消费完”的 emits 当成最终结果。
        const auto sink_stable_window =
            (parallelism_ >= 16) ? 2000ms :
            (parallelism_ >= 8)  ? 1000ms : 200ms;
        const auto max_sink_wait = std::chrono::seconds(30);
        auto sink_end_by = std::chrono::steady_clock::now() + max_sink_wait;
        uint64_t target_emits = JoinMetrics::instance().total_emits.load();
        auto sink_wait_start = std::chrono::steady_clock::now();
        uint64_t last_sink_total = 0;
        auto sink_stable_since = std::chrono::steady_clock::now();
        
        while (std::chrono::steady_clock::now() < sink_end_by) {
            std::this_thread::sleep_for(10ms);
            
            uint64_t sink_count = sink_ ? static_cast<uint64_t>(sink_->getProcessedCount()) : 0;
            uint64_t dedup_count = sink_ ? static_cast<uint64_t>(sink_->getDedupCount()) : 0;
            uint64_t sink_total = sink_count + dedup_count;

            // 稳定检测：如果 sink_total 不再变化一段时间，则认为已经追赶到极限（避免因差 1 条而等满 30s）
            if (sink_total != last_sink_total) {
                last_sink_total = sink_total;
                sink_stable_since = std::chrono::steady_clock::now();
            }

            // Sink 会对重复的 combined_id 进行去重：
            // total_emits = processed_count + dedup_count（理想情况下）
            if (sink_total >= target_emits) {
                SAGEFLOW_LOG_INFO("PipelineHelper", 
                    "Sink processed all emits (including dedup): target={} processed={} dedup={}",
                    target_emits, sink_count, dedup_count);
                break;
            }

            // 达不到 target 但已经稳定：仅在差距极小（<=1）时提前结束等待，
            // 否则继续等到 timeout，避免在高并行度/锁竞争时“误判稳定”。
            if (std::chrono::steady_clock::now() - sink_stable_since >= sink_stable_window) {
                if (target_emits > sink_total && (target_emits - sink_total) <= 1) {
                    SAGEFLOW_LOG_INFO("PipelineHelper",
                        "Sink catch-up stabilized with tiny gap (processed+dedup={} < target={}), stop waiting early",
                        sink_total, target_emits);
                    break;
                }
                // gap still large: keep waiting
                sink_stable_since = std::chrono::steady_clock::now();
            }
        }

        stats.sink_wait_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - sink_wait_start).count();
        
        uint64_t final_sink = sink_ ? static_cast<uint64_t>(sink_->getProcessedCount()) : 0;
        uint64_t final_dedup = sink_ ? static_cast<uint64_t>(sink_->getDedupCount()) : 0;
        uint64_t final_emits = JoinMetrics::instance().total_emits.load();
        stats.total_emits = final_emits;
        stats.sink_processed = final_sink;
        stats.sink_dedup = final_dedup;
        if (final_sink + final_dedup < final_emits) {
            SAGEFLOW_LOG_WARN("PipelineHelper", 
                "Sink did not process all emits: target={} processed={} dedup={}",
                final_emits, final_sink, final_dedup);
        }

        return stats;
    }
    
    std::shared_ptr<DataStreamSource> left_source_;
    std::shared_ptr<DataStreamSource> right_source_;
    std::shared_ptr<MatchCollectorSink> sink_;
    JoinStrategyConfig config_;
    int parallelism_;
    size_t expected_left_ = 0;
    size_t expected_right_ = 0;
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

    uint64_t left_uid = 0;
    uint64_t right_uid = 0;

    // 优先从 payload 解码 (left_uid,right_uid)：Int64[2]
    if (record->data_.type_ == DataType::Int64 && record->data_.dim_ == 2) {
        const int64_t* p = reinterpret_cast<const int64_t*>(record->data_.data_.get());
        left_uid = static_cast<uint64_t>(p[0]);
        right_uid = static_cast<uint64_t>(p[1]);
    } else {
        // 向后兼容：旧测试把 pair 编进 uid（不推荐，仅保留兼容）
        constexpr uint64_t kModuloBase = 1000000ULL;
        uint64_t combined_id = record->uid_;
        left_uid = combined_id / kModuloBase;
        right_uid = combined_id % kModuloBase;
    }

    // Sink 层去重：相同的 (left_uid,right_uid) 只处理一次
    PairKey key{left_uid, right_uid};
    if (!seen_pairs_.insert(key).second) {
        dedup_count_++;
        return;
    }
    
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
    seen_pairs_.clear();
    processed_count_ = 0;
    dedup_count_ = 0;
}

// ==================== JoinIntegrationPipelineHelper 实现 ====================

std::unique_ptr<ExecutableTestPipeline> 
JoinIntegrationPipelineHelper::createPipeline(
    std::vector<std::unique_ptr<VectorRecord>> left_stream,
    std::vector<std::unique_ptr<VectorRecord>> right_stream,
    const JoinStrategyConfig& config,
    int parallelism) {
    
    // 记录预期的记录数（在移动之前）
    size_t expected_left = left_stream.size();
    size_t expected_right = right_stream.size();
    
    // 创建内存数据源
    auto left_source = std::make_shared<TestVectorStreamSource>(
        "IntegrationLeft", std::move(left_stream));
    auto right_source = std::make_shared<TestVectorStreamSource>(
        "IntegrationRight", std::move(right_stream));
    
    // 创建结果收集器
    auto sink = std::make_shared<MatchCollectorSink>();
    
    return std::make_unique<ExecutableTestPipelineImpl>(
        std::move(left_source),
        std::move(right_source),
        std::move(sink),
        config,
        parallelism,
        expected_left,
        expected_right);
}

std::unique_ptr<ExecutableTestPipeline> 
JoinIntegrationPipelineHelper::createPipeline(
    std::shared_ptr<DataStreamSource> left_source,
    std::shared_ptr<DataStreamSource> right_source,
    const JoinStrategyConfig& config,
    int parallelism) {
    
    // 创建结果收集器
    auto sink = std::make_shared<MatchCollectorSink>();
    
    // 注意：这个重载不知道预期记录数，将使用默认值（等待输出稳定）
    return std::make_unique<ExecutableTestPipelineImpl>(
        std::move(left_source),
        std::move(right_source),
        std::move(sink),
        config,
        parallelism,
        0,  // expected_left unknown
        0); // expected_right unknown
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

    // 运行时可验证的关键约束补充：ClusteredJoin 需要 num_partitions == parallelism
    if (config.algorithm == JoinAlgorithm::CLUSTERED_JOIN &&
        config.num_partitions > 0 &&
        config.num_partitions != parallelism) {
        SAGEFLOW_LOG_WARN("PipelineHelper",
            "Config warning: ClusteredJoin: num_partitions={} should equal parallelism={} to avoid recall loss.",
            config.num_partitions, parallelism);
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

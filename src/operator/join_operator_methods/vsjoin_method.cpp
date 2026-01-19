#include "operator/join_operator_methods/vsjoin_method.h"
#include "utils/logger.h"

#include <stdexcept>

namespace sageFlow {

VSJoinMethod::VSJoinMethod(const VSJoinConfig& config,
                           std::shared_ptr<ConcurrencyManager> concurrency_manager)
    : BaseMethod(0.8)  // 默认相似度阈值
    , config_(config)
    , concurrency_manager_(std::move(concurrency_manager)) {
    
    // 如果配置中有相似度阈值，则使用
    // 注意：BaseMethod 的构造函数需要传入阈值，但 VSJoin 的阈值在 config 中
    // 这里先用默认值，在 initVerifier 时使用正确的阈值
}

VSJoinMethod::~VSJoinMethod() {
    close();
}

void VSJoinMethod::initialize(size_t subtask_index, size_t parallelism) {
    if (initialized_) {
        SAGEFLOW_LOG_WARN("VSJoinMethod", "Already initialized, skipping");
        return;
    }
    
    SAGEFLOW_LOG_INFO("VSJoinMethod", "Initializing with {} partitions, subtask={}/{}",
                      config_.num_partitions, subtask_index, parallelism);
    
    // 按顺序初始化组件
    initPartitioner();
    initStates();
    initIndices(subtask_index);
    initCoordinator();
    initAsyncGenerators();
    initVerifier();
    
    initialized_ = true;
    SAGEFLOW_LOG_INFO("VSJoinMethod", "Initialization completed successfully");
}

void VSJoinMethod::close() {
    if (!initialized_) {
        return;
    }
    
    SAGEFLOW_LOG_INFO("VSJoinMethod", "Closing VSJoin components");
    
    // 关闭异步生成器（等待所有任务完成）
    if (left_async_generator_) {
        left_async_generator_->shutdown();
        left_async_generator_.reset();
    }
    if (right_async_generator_) {
        right_async_generator_->shutdown();
        right_async_generator_.reset();
    }
    
    // 重置其他组件
    coordinator_.reset();
    left_state_.reset();
    right_state_.reset();
    left_index_.reset();
    right_index_.reset();
    verifier_.reset();
    partitioner_.reset();
    
    initialized_ = false;
    SAGEFLOW_LOG_INFO("VSJoinMethod", "Close completed");
}

void VSJoinMethod::initPartitioner() {
    // 使用 LSH 分区器
    partitioner_ = std::make_shared<LSHPartitioner>(
        config_.dimension,
        /*num_hash_functions=*/8,
        /*seed=*/42,
        /*boundary_threshold=*/0.1);
    
    SAGEFLOW_LOG_DEBUG("VSJoinMethod", "Partitioner initialized: dimension={}", config_.dimension);
}

void VSJoinMethod::initStates() {
    left_state_ = std::make_unique<PartitionedVectorState>(
        static_cast<size_t>(config_.num_partitions),
        partitioner_,
        config_.compact_threshold,
        config_.enable_boundary_tracking);
    
    right_state_ = std::make_unique<PartitionedVectorState>(
        static_cast<size_t>(config_.num_partitions),
        partitioner_,
        config_.compact_threshold,
        config_.enable_boundary_tracking);
    
    SAGEFLOW_LOG_DEBUG("VSJoinMethod", "States initialized: partitions={} compact_threshold={}",
                      config_.num_partitions, config_.compact_threshold);
}

void VSJoinMethod::initIndices(size_t subtask_index) {
    left_index_ = std::make_shared<PartitionedIndex>(
        static_cast<size_t>(config_.num_partitions),
        config_.dimension,
        partitioner_,
        config_.ivf_nlist,
        config_.ivf_nprobes);
    
    right_index_ = std::make_shared<PartitionedIndex>(
        static_cast<size_t>(config_.num_partitions),
        config_.dimension,
        partitioner_,
        config_.ivf_nlist,
        config_.ivf_nprobes);
    
    // 通过 ConcurrencyManager 注册索引
    if (concurrency_manager_) {
        std::string prefix = "vsjoin_method_" + std::to_string(subtask_index);
        concurrency_manager_->register_index(prefix + "_left", left_index_);
        concurrency_manager_->register_index(prefix + "_right", right_index_);
    }
    
    SAGEFLOW_LOG_DEBUG("VSJoinMethod", "Indices initialized: nlist={} nprobes={}",
                      config_.ivf_nlist, config_.ivf_nprobes);
}

void VSJoinMethod::initCoordinator() {
    coordinator_ = std::make_unique<PartitionCoordinator>(
        static_cast<size_t>(config_.num_partitions),
        partitioner_,
        config_.allowed_lateness,
        config_.watermark_delay);
    
    SAGEFLOW_LOG_DEBUG("VSJoinMethod", "Coordinator initialized: lateness={} watermark_delay={}",
                      config_.allowed_lateness, config_.watermark_delay);
}

void VSJoinMethod::initAsyncGenerators() {
    left_async_generator_ = std::make_unique<AsyncCandidateGenerator>(
        left_index_,
        config_.async_generator_threads);
    
    right_async_generator_ = std::make_unique<AsyncCandidateGenerator>(
        right_index_,
        config_.async_generator_threads);
    
    SAGEFLOW_LOG_DEBUG("VSJoinMethod", "Async generators initialized: threads={}",
                      config_.async_generator_threads);
}

void VSJoinMethod::initVerifier() {
    verifier_ = std::make_shared<DistanceVerifier>(
        join_similarity_threshold_,
        config_.distance_alpha);
    
    SAGEFLOW_LOG_DEBUG("VSJoinMethod", "Verifier initialized: threshold={} alpha={}",
                      join_similarity_threshold_, config_.distance_alpha);
}

bool VSJoinMethod::processRecord(std::unique_ptr<VectorRecord> record, int slot, 
                                  size_t subtask_index) {
    if (!initialized_ || !record) {
        return false;
    }
    
    int64_t timestamp = record->timestamp_;
    uint64_t uid = record->uid_;
    
    // 1. 处理延迟到达
    auto process_result = coordinator_->processRecord(*record);
    
    if (process_result.status == ArrivalStatus::TOO_LATE) {
        SAGEFLOW_LOG_DEBUG("VSJoinMethod", "Dropping too late record uid={}", uid);
        return false;
    }
    
    if (process_result.status == ArrivalStatus::LATE) {
        // 延迟记录缓冲处理
        coordinator_->bufferLateRecord(std::make_unique<VectorRecord>(*record));
        SAGEFLOW_LOG_DEBUG("VSJoinMethod", "Buffered late record uid={}", uid);
    }
    
    // 2. 确定当前记录属于哪一侧
    PartitionedVectorState* current_state = (slot == left_slot_id_) 
        ? left_state_.get() : right_state_.get();
    PartitionedIndex* current_index = (slot == left_slot_id_) 
        ? left_index_.get() : right_index_.get();
    
    // 3. 更新状态
    current_state->addRecord(std::make_unique<VectorRecord>(*record), subtask_index);
    
    // 4. 插入到分区索引
    if (current_index->storage_manager_) {
        current_index->storage_manager_->insert(std::make_unique<VectorRecord>(*record));
    }
    current_index->insert(uid);
    
    // 5. 更新分区协调器的记录计数
    coordinator_->updatePartitionCount(process_result.partition_id, 1);
    
    return true;
}

void VSJoinMethod::evictExpired(int64_t current_timestamp, int64_t window_size, 
                                 size_t subtask_index) {
    if (!initialized_) {
        return;
    }
    
    left_state_->evictExpired(current_timestamp, window_size, subtask_index);
    right_state_->evictExpired(current_timestamp, window_size, subtask_index);
}

std::vector<std::unique_ptr<VectorRecord>> VSJoinMethod::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot,
    size_t /*subtask_index*/) {
    
    std::vector<std::unique_ptr<VectorRecord>> results;
    
    if (!initialized_) {
        SAGEFLOW_LOG_WARN("VSJoinMethod", "Not initialized, returning empty results");
        return results;
    }
    
    // 确定查询的目标侧
    PartitionedVectorState* target_state = (query_slot == left_slot_id_) 
        ? right_state_.get() : left_state_.get();
    
    // 获取候选分区
    auto candidate_partitions = coordinator_->routeQuery(query_record, config_.num_probes);
    
    // 从目标状态中获取相关记录用于 join
    auto candidate_records = target_state->getRecordsForQuery(query_record, config_.num_probes);
    
    SAGEFLOW_LOG_DEBUG("VSJoinMethod", "ExecuteEager: query_uid={} candidate_count={}", 
                      query_record.uid_, candidate_records.size());
    
    // 验证候选
    for (const VectorRecord* cand_ptr : candidate_records) {
        if (!cand_ptr) continue;
        
        // 使用距离验证器验证
        auto result = verifier_->verify(query_record, *cand_ptr);
        if (result.passed) {
            // 创建候选记录副本
            results.push_back(std::make_unique<VectorRecord>(*cand_ptr));
        }
    }
    
    SAGEFLOW_LOG_DEBUG("VSJoinMethod", "ExecuteEager completed: results={}", results.size());
    
    return results;
}

}  // namespace sageFlow

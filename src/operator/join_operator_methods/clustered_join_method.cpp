#include "operator/join_operator_methods/clustered_join_method.h"
#include "operator/utils/join_method_registry.h"

#include <algorithm>
#include <cmath>
#include <cstring>

#include "utils/logger.h"

namespace {

/**
 * @brief 从 VectorRecord 提取 float 向量
 * @param record 向量记录
 * @return float 向量
 */
std::vector<float> extractVector(const sageFlow::VectorRecord& record) {
    const auto& vector_data = record.data_;
    int32_t dim = vector_data.dim_;
    
    if (dim <= 0) {
        return {};
    }
    
    const float* float_ptr = reinterpret_cast<const float*>(vector_data.data_.get());
    std::vector<float> result(static_cast<size_t>(dim));
    std::memcpy(result.data(), float_ptr, static_cast<size_t>(dim) * sizeof(float));
    return result;
}

} // anonymous namespace

namespace sageFlow {

// ==================== 构造函数 ====================

ClusteredJoinMethod::ClusteredJoinMethod(const Config& config)
    : BaseMethod(config.similarity_threshold)
    , config_(config) {
  SAGEFLOW_LOG_INFO("ClusteredJoin", 
      "Created with threshold={}, dimension={}, index_type={}",
      config.similarity_threshold, 
      config.dimension,
      static_cast<int>(config.index_type));
}

ClusteredJoinMethod::ClusteredJoinMethod(double similarity_threshold, int dimension)
    : BaseMethod(similarity_threshold) {
  config_.similarity_threshold = similarity_threshold;
  config_.dimension = dimension;
  
  SAGEFLOW_LOG_INFO("ClusteredJoin", 
      "Created with threshold={}, dimension={} (default config)",
      similarity_threshold, dimension);
}

// ==================== 生命周期 ====================

void ClusteredJoinMethod::initialize(
    const RuntimeContext& context,
    std::shared_ptr<ConcurrencyManager> concurrency_manager) {
  
  if (initialized_) {
    SAGEFLOW_LOG_WARN("ClusteredJoin", "Already initialized, skipping");
    return;
  }
  
  subtask_index_ = context.getSubtaskIndex();
  parallelism_ = context.getParallelism();
  concurrency_manager_ = std::move(concurrency_manager);
  
  // 默认 effective_parallelism_ = parallelism_
  // 如果 CentroidPartitioner 未训练，由 JoinOperator 调用 setEffectiveParallelism(1)
  effective_parallelism_ = parallelism_;
  
  if (!concurrency_manager_) {
    SAGEFLOW_LOG_ERROR("ClusteredJoin", "ConcurrencyManager is null");
    return;
  }
  
  // 注意：不再在此处创建索引
  // 索引由 JoinOperator 创建，通过 setIndexIds() 传入
  
  initialized_ = true;
  
  SAGEFLOW_LOG_INFO("ClusteredJoin",
      "Initialized subtask {} of {}, effective_parallelism={}",
      subtask_index_, parallelism_, effective_parallelism_);
}

void ClusteredJoinMethod::setIndexIds(int left_index_id, int right_index_id) {
  left_index_id_ = left_index_id;
  right_index_id_ = right_index_id;
  
  SAGEFLOW_LOG_INFO("ClusteredJoin",
      "Index IDs set: left={}, right={}", left_index_id_, right_index_id_);
}

void ClusteredJoinMethod::setWindowStates(WindowState* left_state, 
                                           WindowState* right_state) {
  left_state_ = left_state;
  right_state_ = right_state;
  
  SAGEFLOW_LOG_INFO("ClusteredJoin",
      "WindowStates set: left={}, right={}, index_type={}",
      (left_state != nullptr), (right_state != nullptr),
      static_cast<int>(config_.index_type));
      
  if (config_.index_type == ClusteredIndexType::BRUTEFORCE) {
    SAGEFLOW_LOG_INFO("ClusteredJoin",
        "BruteForce mode: will use WindowState directly instead of ConcurrencyManager");
  }
}

void ClusteredJoinMethod::close() {
  // 重置索引 ID（索引由 ConcurrencyManager 管理生命周期）
  left_index_id_ = -1;
  right_index_id_ = -1;
  
  initialized_ = false;
  
  SAGEFLOW_LOG_INFO("ClusteredJoin", 
      "Closed subtask {} of {}", subtask_index_, parallelism_);
}

// ==================== 索引配置 ====================

IndexType ClusteredJoinMethod::getPreferredIndexType() const {
  switch (config_.index_type) {
    case ClusteredIndexType::BRUTEFORCE:
      return IndexType::BruteForce;
    case ClusteredIndexType::IVF:
      return IndexType::IVF;
    case ClusteredIndexType::HNSW:
      return IndexType::HNSW;
    default:
      return IndexType::BruteForce;
  }
}

IndexParameters ClusteredJoinMethod::getPreferredIndexParams() const {
  switch (config_.index_type) {
    case ClusteredIndexType::BRUTEFORCE:
      return NoParameters{};
      
    case ClusteredIndexType::IVF: {
      IVFParameters params;
      params.nlist = config_.ivf_nlist;
      params.nprobes = config_.ivf_nprobes;
      params.rebuild_threshold = 1.5;
      return params;
    }
    
    case ClusteredIndexType::HNSW: {
      HNSWParameters params;
      params.m = config_.hnsw_m;
      params.ef_construction = config_.hnsw_ef_construction;
      params.ef_search = config_.hnsw_ef_search;
      return params;
    }
    
    default:
      return NoParameters{};
  }
}

// ==================== BaseMethod 接口实现 ====================

std::vector<std::unique_ptr<VectorRecord>> ClusteredJoinMethod::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot,
    size_t subtask_index) {
  
  std::vector<std::unique_ptr<VectorRecord>> results;
  
  if (!initialized_) {
    SAGEFLOW_LOG_WARN("ClusteredJoin", 
        "Not initialized, returning empty results");
    return results;
  }
  
  // 根据索引类型选择数据源
  // BruteForce 模式: 直接从 WindowState 获取快照（与 BruteForceBaseline 一致）
  // IVF/HNSW 模式: 通过 ConcurrencyManager 查询索引
  
  if (config_.index_type == ClusteredIndexType::BRUTEFORCE) {
    // ========== BruteForce 模式：使用 WindowState ==========
    return executeEagerBruteForce(query_record, query_slot, subtask_index);
  } else {
    // ========== IVF/HNSW 模式：使用 ConcurrencyManager ==========
    return executeEagerIndexed(query_record, query_slot, subtask_index);
  }
}

std::vector<std::unique_ptr<VectorRecord>> ClusteredJoinMethod::executeEagerBruteForce(
    const VectorRecord& query_record,
    int query_slot,
    size_t subtask_index) {
  
  std::vector<std::unique_ptr<VectorRecord>> results;
  
  // 获取对侧窗口状态
  WindowState* target_state = (query_slot == 0) ? right_state_ : left_state_;
  
  if (!target_state) {
    SAGEFLOW_LOG_WARN("ClusteredJoin", 
        "No target WindowState for slot {}", query_slot);
    return results;
  }
  
  // 获取窗口快照（线程安全）
  // 使用传入的 subtask_index 而不是内部存储的 subtask_index_
  auto records_snapshot = target_state->getRecordsSnapshot(subtask_index);
  
  uint64_t query_uid = query_record.uid_;
  std::vector<float> query_vec = extractVector(query_record);
  
  if (query_vec.empty()) {
    SAGEFLOW_LOG_WARN("ClusteredJoin", "Query vector is empty for uid={}", query_uid);
    return results;
  }
  
  // 暴力搜索满足相似度阈值的记录
  for (const auto& record_ptr : records_snapshot) {
    if (!record_ptr) continue;
    
    uint64_t candidate_uid = record_ptr->uid_;
    
    // 跳过自身
    if (candidate_uid == query_uid) continue;
    
    // 提取候选向量
    std::vector<float> candidate_vec = extractVector(*record_ptr);
    if (candidate_vec.empty()) continue;
    
    // 计算相似度
    double similarity = computeSimilarity(query_vec, candidate_vec);
    
    if (similarity >= config_.similarity_threshold) {
      // 直接输出所有匹配 - Sink 层会进行去重
      // (移除 Owner-Computes，与 executeEagerIndexed 保持一致)
      results.push_back(std::make_unique<VectorRecord>(*record_ptr));
      
      SAGEFLOW_LOG_DEBUG("ClusteredJoin", 
          "BruteForce match: subtask {} found ({}, {}), sim={:.4f}", 
          subtask_index, query_uid, candidate_uid, similarity);
    }
  }
  
  // 每 1000 次查询打印一次窗口大小
  static thread_local uint64_t query_count = 0;
  if (++query_count % 1000 == 0) {
    SAGEFLOW_LOG_INFO("ClusteredJoin", 
        "BF subtask={}: query_uid={}, window_size={}, results={}",
        subtask_index, query_uid, records_snapshot.size(), results.size());
  }
  
  return results;
}

std::vector<std::unique_ptr<VectorRecord>> ClusteredJoinMethod::executeEagerIndexed(
    const VectorRecord& query_record,
    int query_slot,
    size_t subtask_index) {
  
  std::vector<std::unique_ptr<VectorRecord>> results;
  
  // 查询对侧索引
  int target_index = getOppositeIndexId(query_slot);
  
  if (target_index < 0) {
    SAGEFLOW_LOG_DEBUG("ClusteredJoin", 
        "No opposite index available for slot {}", query_slot);
    return results;
  }
  
  // 通过 ConcurrencyManager 查询候选项
  auto candidates = concurrency_manager_->query_for_join(
      target_index, query_record, config_.similarity_threshold, similarity_alpha_);
  
  // 直接输出所有匹配 - Sink 层会进行去重
  // 
  // 移除 Owner-Computes 去重原因：
  // 1. Sink 层已实现基于 combined_id 的去重（见 MatchCollectorSink::invoke）
  // 2. Owner-Computes 在 multicast 模式下会错误过滤结果
  //    - k=1 时只有 owner 分区处理，部分匹配可能输出
  //    - k>1 时向量被多播到多个分区，但只有 owner 分区输出
  //      导致 k 越大反而召回率越低（与预期相反）
  // 3. Sink 是单线程(parallelism=1)，无锁开销
  
  for (const auto& candidate : candidates) {
    if (!candidate) continue;
    
    results.push_back(std::make_unique<VectorRecord>(*candidate));
    
    SAGEFLOW_LOG_DEBUG("ClusteredJoin", 
        "Indexed match: subtask {} found candidate uid={}", 
        subtask_index_, candidate->uid_);
  }
  
  SAGEFLOW_LOG_DEBUG("ClusteredJoin", 
      "executeEagerIndexed: query_uid={}, slot={}, candidates={}, results={}", 
      query_record.uid_, query_slot, candidates.size(), results.size());
  
  return results;
}

double ClusteredJoinMethod::computeSimilarity(
    const std::vector<float>& a, 
    const std::vector<float>& b) const {
  
  if (a.empty() || b.empty()) {
    SAGEFLOW_LOG_WARN("ClusteredJoin", "Empty vector in similarity computation");
    return 0.0;
  }
  
  if (a.size() != b.size()) {
    SAGEFLOW_LOG_WARN("ClusteredJoin",
        "Vector dimension mismatch: {} vs {}", a.size(), b.size());
    return 0.0;
  }
  
  // 使用 L2 距离 + 指数衰减转换为相似度
  // 与 ComputeEngine::Similarity 和 BruteForceBaseline 保持一致
  double distance_sq = 0.0;
  for (size_t i = 0; i < a.size(); ++i) {
    double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
    distance_sq += diff * diff;
  }
  double distance = std::sqrt(distance_sq);
  
  // ClusteredJoinMethod 的候选获取通常走索引层 query_for_join()，
  // 相似度过滤由 Index 内部使用 ComputeEngine 完成。
  // 这里保留一个统一实现，用于某些暴力验证/回退路径。
  return std::exp(-similarity_alpha_ * distance);
}

}  // namespace sageFlow

// ==================== 方法自注册 ====================
REGISTER_JOIN_METHOD(
    sageFlow::JoinAlgorithm::CLUSTERED_JOIN,
    (sageFlow::JoinMethodRegistry::MethodInfo{
        "ClusteredJoin",
        "ClusteredJoin with unified architecture (CentroidPartitioner + PartitionedWindowState + partitioned index). "
        "Uses Owner-Computes rule for deduplication in partitioned mode. "
        "Shares the same apply() flow as other Join methods.",
        sageFlow::JoinAlgorithm::CLUSTERED_JOIN,
        true,   // supports_eager
        false,  // supports_lazy (deprecated)
        sageFlow::PartitionStrategy::CENTROID,
        sageFlow::WindowStateType::PARTITIONED,
        ""      // paper_reference
    }),
    [](const sageFlow::JoinStrategyConfig& config,
       std::shared_ptr<sageFlow::ConcurrencyManager> /*cm*/,
       int /*dim*/,
       int /*left_idx*/,
       int /*right_idx*/) {
        // 配置 ClusteredJoinMethod
        // 注意：索引由 JoinOperator 创建，通过 setIndexIds() 传入
        sageFlow::ClusteredJoinMethod::Config cj_config;
        cj_config.similarity_threshold = config.similarity_threshold;
        cj_config.dimension = config.dimension;
        cj_config.window_size_ms = config.window_size_ms;
        cj_config.index_type = config.clustered_index_type;
        cj_config.ivf_nlist = config.ivf_nlist;
        cj_config.ivf_nprobes = config.ivf_nprobes;
        cj_config.hnsw_m = config.hnsw_m;
        cj_config.hnsw_ef_construction = config.hnsw_ef_construction;
        cj_config.hnsw_ef_search = config.hnsw_ef_search;
        
        auto method = std::make_unique<sageFlow::ClusteredJoinMethod>(cj_config);
        // 注意：initialize() 和 setIndexIds() 将在 JoinOperator::open(context) 中调用
        return method;
    });

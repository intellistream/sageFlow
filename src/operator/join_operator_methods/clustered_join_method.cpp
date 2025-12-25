#include "operator/join_operator_methods/clustered_join_method.h"
#include "operator/utils/join_method_registry.h"

#include <algorithm>
#include <cmath>
#include <cstring>

#include "utils/logger.h"

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
  
  if (!concurrency_manager_) {
    SAGEFLOW_LOG_ERROR("ClusteredJoin", "ConcurrencyManager is null");
    return;
  }
  
  // 每个 subtask 创建独立的索引
  std::string prefix = "clustered_p" + std::to_string(subtask_index_);
  left_index_id_ = createIndex(prefix + "_left");
  right_index_id_ = createIndex(prefix + "_right");
  
  if (left_index_id_ < 0 || right_index_id_ < 0) {
    SAGEFLOW_LOG_ERROR("ClusteredJoin", 
        "Failed to create indexes: left={}, right={}", 
        left_index_id_, right_index_id_);
    return;
  }
  
  initialized_ = true;
  
  SAGEFLOW_LOG_INFO("ClusteredJoin",
      "Initialized subtask {} of {}, left_idx={}, right_idx={}, index_type={}",
      subtask_index_, parallelism_, left_index_id_, right_index_id_,
      static_cast<int>(config_.index_type));
}

int ClusteredJoinMethod::createIndex(const std::string& name) {
  IndexType type;
  IndexParameters params;
  
  switch (config_.index_type) {
    case ClusteredIndexType::BRUTEFORCE:
      type = IndexType::BruteForce;
      params = NoParameters{};
      break;
      
    case ClusteredIndexType::IVF: {
      type = IndexType::IVF;
      IVFParameters ivf_params;
      ivf_params.nlist = config_.ivf_nlist;
      ivf_params.nprobes = config_.ivf_nprobes;
      ivf_params.rebuild_threshold = 1.5;  // 默认值
      params = ivf_params;
      break;
    }
      
    case ClusteredIndexType::HNSW: {
      type = IndexType::HNSW;
      HNSWParameters hnsw_params;
      hnsw_params.m = config_.hnsw_m;
      hnsw_params.ef_construction = config_.hnsw_ef_construction;
      hnsw_params.ef_search = config_.hnsw_ef_search;
      params = hnsw_params;
      break;
    }
      
    default:
      SAGEFLOW_LOG_WARN("ClusteredJoin", 
          "Unknown index type {}, using BruteForce", 
          static_cast<int>(config_.index_type));
      type = IndexType::BruteForce;
      params = NoParameters{};
  }
  
  return concurrency_manager_->create_index(name, type, config_.dimension, params);
}

void ClusteredJoinMethod::close() {
  // 清理窗口状态
  left_window_.clear();
  right_window_.clear();
  left_uids_.clear();
  right_uids_.clear();
  
  // 注意：索引由 ConcurrencyManager 管理生命周期
  // 这里只重置 ID
  left_index_id_ = -1;
  right_index_id_ = -1;
  
  initialized_ = false;
  
  SAGEFLOW_LOG_INFO("ClusteredJoin", 
      "Closed subtask {} of {}", subtask_index_, parallelism_);
}

// ==================== 状态管理 ====================

void ClusteredJoinMethod::addRecord(std::unique_ptr<VectorRecord> record, int slot) {
  if (!initialized_) {
    SAGEFLOW_LOG_WARN("ClusteredJoin", "Not initialized, ignoring addRecord");
    return;
  }
  
  if (!record) {
    return;
  }
  
  uint64_t uid = record->uid_;
  int index_id = getCurrentIndexId(slot);
  auto& window = getCurrentWindow(slot);
  auto& uids = getCurrentUids(slot);
  
  // 检查是否已存在（避免重复）
  if (uids.find(uid) != uids.end()) {
    SAGEFLOW_LOG_DEBUG("ClusteredJoin", 
        "Record {} already exists in window, skipping", uid);
    return;
  }
  
  // 插入到索引
  auto record_copy = std::make_unique<VectorRecord>(*record);
  bool inserted = concurrency_manager_->insert(index_id, std::move(record_copy));
  
  if (!inserted) {
    SAGEFLOW_LOG_WARN("ClusteredJoin", 
        "Failed to insert record {} to index {}", uid, index_id);
    return;
  }
  
  // 添加到窗口和 UID 集合
  uids.insert(uid);
  window.push_back(std::move(record));
  
  SAGEFLOW_LOG_DEBUG("ClusteredJoin", 
      "Added record {} to slot {} (window size: {})", 
      uid, slot, window.size());
}

void ClusteredJoinMethod::evictExpired(int64_t current_timestamp) {
  if (!initialized_) {
    return;
  }
  
  int64_t threshold = current_timestamp - config_.window_size_ms;
  
  // 清理左窗口
  while (!left_window_.empty() && 
         left_window_.front()->timestamp_ < threshold) {
    uint64_t uid = left_window_.front()->uid_;
    concurrency_manager_->erase(left_index_id_, uid);
    left_uids_.erase(uid);
    left_window_.pop_front();
  }
  
  // 清理右窗口
  while (!right_window_.empty() && 
         right_window_.front()->timestamp_ < threshold) {
    uint64_t uid = right_window_.front()->uid_;
    concurrency_manager_->erase(right_index_id_, uid);
    right_uids_.erase(uid);
    right_window_.pop_front();
  }
}

// ==================== BaseMethod 接口实现 ====================

std::vector<std::unique_ptr<VectorRecord>> ClusteredJoinMethod::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot) {
  
  std::vector<std::unique_ptr<VectorRecord>> results;
  
  if (!initialized_) {
    SAGEFLOW_LOG_WARN("ClusteredJoin", 
        "Not initialized, returning empty results");
    return results;
  }
  
  uint64_t query_uid = query_record.uid_;
  
  // 根据索引类型选择查询策略
  if (config_.index_type == ClusteredIndexType::BRUTEFORCE) {
    // BruteForce 模式：直接遍历本地窗口（不使用索引）
    // 因为 BruteForce 索引会查询整个 StorageManager，无法区分左右流
    results = executeEagerBruteForce(query_record, query_slot);
  } else {
    // IVF/HNSW 模式：使用索引查询（IVF 维护自己的 inverted_lists_）
    results = executeEagerWithIndex(query_record, query_slot);
  }
  
  SAGEFLOW_LOG_DEBUG("ClusteredJoin", 
      "ExecuteEager: query_uid={}, slot={}, results={}", 
      query_uid, query_slot, results.size());
  
  return results;
}

std::vector<std::unique_ptr<VectorRecord>> ClusteredJoinMethod::executeEagerBruteForce(
    const VectorRecord& query_record,
    int query_slot) {
  
  std::vector<std::unique_ptr<VectorRecord>> results;
  uint64_t query_uid = query_record.uid_;
  
  // 获取对侧窗口
  const auto& opposite_window = getOppositeWindow(query_slot);
  
  // 提取查询向量
  const auto& query_data = query_record.data_;
  if (query_data.dim_ <= 0 || !query_data.data_) {
    return results;
  }
  const float* query_vec = reinterpret_cast<const float*>(query_data.data_.get());
  int dim = query_data.dim_;
  
  // 遍历对侧窗口中的所有记录
  for (const auto& record_ptr : opposite_window) {
    if (!record_ptr) continue;
    
    uint64_t candidate_uid = record_ptr->uid_;
    
    // 提取候选向量
    const auto& cand_data = record_ptr->data_;
    if (cand_data.dim_ != dim || !cand_data.data_) continue;
    const float* cand_vec = reinterpret_cast<const float*>(cand_data.data_.get());
    
    // 计算 L2 距离并转换为相似度
    double dist_sq = 0.0;
    for (int i = 0; i < dim; ++i) {
      double diff = static_cast<double>(query_vec[i]) - static_cast<double>(cand_vec[i]);
      dist_sq += diff * diff;
    }
    double distance = std::sqrt(dist_sq);
    constexpr double kAlpha = 0.1;
    double similarity = std::exp(-kAlpha * distance);
    
    // 检查相似度阈值
    if (similarity < config_.similarity_threshold) {
      continue;
    }
    
    // Owner-Computes 去重
    uint64_t left_uid = (query_slot == 0) ? query_uid : candidate_uid;
    uint64_t right_uid = (query_slot == 0) ? candidate_uid : query_uid;
    
    if (isOwner(left_uid, right_uid)) {
      results.push_back(std::make_unique<VectorRecord>(*record_ptr));
      
      SAGEFLOW_LOG_DEBUG("ClusteredJoin", 
          "BF Owner match: subtask {} owns ({}, {}) sim={:.4f}", 
          subtask_index_, left_uid, right_uid, similarity);
    }
  }
  
  return results;
}

std::vector<std::unique_ptr<VectorRecord>> ClusteredJoinMethod::executeEagerWithIndex(
    const VectorRecord& query_record,
    int query_slot) {
  
  std::vector<std::unique_ptr<VectorRecord>> results;
  
  // 在对侧索引中查询候选项
  int target_index = getOppositeIndexId(query_slot);
  if (target_index < 0) {
    return results;
  }
  
  auto candidates = concurrency_manager_->query_for_join(
      target_index, query_record, config_.similarity_threshold);
  
  // 应用 Owner-Computes 去重
  uint64_t query_uid = query_record.uid_;
  const auto& opposite_uids = getOppositeUids(query_slot);
  
  for (const auto& candidate : candidates) {
    if (!candidate) {
      continue;
    }
    
    uint64_t candidate_uid = candidate->uid_;
    
    // 检查候选项是否仍在窗口中（可能已被驱逐）
    if (opposite_uids.find(candidate_uid) == opposite_uids.end()) {
      SAGEFLOW_LOG_DEBUG("ClusteredJoin", 
          "Candidate {} not in window, skipping", candidate_uid);
      continue;
    }
    
    // Owner-Computes 去重
    uint64_t left_uid = (query_slot == 0) ? query_uid : candidate_uid;
    uint64_t right_uid = (query_slot == 0) ? candidate_uid : query_uid;
    
    if (isOwner(left_uid, right_uid)) {
      results.push_back(std::make_unique<VectorRecord>(*candidate));
      
      SAGEFLOW_LOG_DEBUG("ClusteredJoin", 
          "Index Owner match: subtask {} owns ({}, {})", 
          subtask_index_, left_uid, right_uid);
    }
  }
  
  return results;
}

}  // namespace sageFlow

// ==================== 方法自注册 ====================
REGISTER_JOIN_METHOD(
    sageFlow::JoinAlgorithm::CLUSTERED_JOIN,
    (sageFlow::JoinMethodRegistry::MethodInfo{
        "ClusteredJoin",
        "ClusteredJoin with independent indexes per subtask. "
        "Uses Owner-Computes rule for deduplication. "
        "Data distributed via CentroidPartitioner with multicast for boundary vectors.",
        sageFlow::JoinAlgorithm::CLUSTERED_JOIN,
        true,   // supports_eager
        false,  // supports_lazy (deprecated)
        sageFlow::PartitionStrategy::CENTROID,
        sageFlow::WindowStateType::PARTITIONED,
        ""      // paper_reference
    }),
    [](const sageFlow::JoinStrategyConfig& config,
       std::shared_ptr<sageFlow::ConcurrencyManager> cm,
       int /*dim*/,
       int /*left_idx*/,
       int /*right_idx*/) {
        // 注意：新设计中不使用外部传入的 index_id
        // 每个 subtask 在 initialize() 时创建自己的索引
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
        // 旧参数（兼容性）
        cj_config.num_partitions = config.num_partitions;
        cj_config.overlap_ratio = config.clustered_overlap_ratio;
        cj_config.rebalance_threshold = config.clustered_rebalance_threshold;
        cj_config.use_border_replication = config.clustered_border_replication;
        cj_config.training_samples = config.clustered_training_samples;
        
        auto method = std::make_unique<sageFlow::ClusteredJoinMethod>(cj_config);
        // 注意：initialize() 将在 JoinOperator::open(context) 中调用
        return method;
    });

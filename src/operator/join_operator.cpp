/*
修改 Eager/Lazy 通过process 中的 IsEagerAlgorithm 修改
修改调用的方法， 则修改 using JoinWay 后面的等于号
*/
#include "operator/join_operator.h"
#include "operator/join_operator_methods/join_methods.h"
#include "operator/join_metrics.h"
#include "utils/monitoring.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <algorithm>
#include <cmath>
#include <limits>

#include "utils/logger.h"

#include "spdlog/fmt/bundled/chrono.h"

namespace sageFlow {

bool JoinOperator::createIndexPair(IndexType type, const std::string& prefix) {
    if (!concurrency_manager_) return false;
    left_index_id_ = concurrency_manager_->create_index(prefix + "_left", type, join_func_->getDim());
    right_index_id_ = concurrency_manager_->create_index(prefix + "_right", type, join_func_->getDim());
    return left_index_id_ != -1 && right_index_id_ != -1;
}

bool JoinOperator::createIndexPair(IndexType type, const std::string& prefix, const IndexParameters& params) {
    if (!concurrency_manager_) return false;
    left_index_id_ = concurrency_manager_->create_index(prefix + "_left", type, join_func_->getDim(), params);
    right_index_id_ = concurrency_manager_->create_index(prefix + "_right", type, join_func_->getDim(), params);
    return left_index_id_ != -1 && right_index_id_ != -1;
}

static inline std::string to_lower_copy(std::string v) {
    std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c){return char(std::tolower(c));});
    return v;
}

int64_t JoinOperator::logicalWindowLowerBound(int64_t reference_timestamp) const {
    const int64_t window_size = join_func_ ? join_func_->getWindowSize() : 0;
    if (window_size <= 0) {
        return std::numeric_limits<int64_t>::min();
    }
    if (reference_timestamp <= std::numeric_limits<int64_t>::min() + window_size) {
        return std::numeric_limits<int64_t>::min();
    }
    return reference_timestamp - window_size;
}

bool JoinOperator::isRecordFresh(const std::unique_ptr<VectorRecord>& record, int64_t logical_lower_bound) const {
    if (!record) {
        return false;
    }
    return record->timestamp_ >= logical_lower_bound;
}

JoinOperator::JoinOperator(std::unique_ptr<Function> &join_func,
                           const std::shared_ptr<ConcurrencyManager> &concurrency_manager,
                           const std::string& join_method_name_raw,
                           double join_similarity_threshold,
                           bool enable_profiling,
                           const std::string& profile_output_path,
                           bool use_shared_state)
    : Operator(OperatorType::JOIN), concurrency_manager_(concurrency_manager),
      join_similarity_threshold_(join_similarity_threshold),
      enable_profiling_(enable_profiling),
      use_shared_state_(use_shared_state) {
    // 注意：use_shared_state_ 可能在后面根据算法类型被覆盖
    join_func_ = std::unique_ptr<JoinFunction>(dynamic_cast<JoinFunction*>(join_func.release()));
    if (!join_func_) {
        throw std::runtime_error("JoinOperator: join_func is not a JoinFunction");
    }
    if (!concurrency_manager_) {
        throw std::runtime_error("JoinOperator: concurrency_manager is a nullptr");
    }

    // Initialize GPERFTOOLS profiler if enabled
    if (enable_profiling_) {
        std::string profile_path = profile_output_path.empty() 
            ? "profiles/join_operator_profile.prof" 
            : profile_output_path;
        profiler_ = std::make_unique<PerformanceMonitor>(profile_path);
        SAGEFLOW_LOG_INFO("JOIN", "GPERFTOOLS profiling enabled output={}", profile_path);
    }

    std::string join_method_name = to_lower_copy(join_method_name_raw);

    // 解析模式（_eager / _lazy）
    if (join_method_name.rfind("_eager") != std::string::npos) {
        is_eager_ = true;
    } else if (join_method_name.rfind("_lazy") != std::string::npos) {
        is_eager_ = false;
    } else {
        // 未指定时默认 lazy
        is_eager_ = false;
    }

    // 提取算法前缀（截掉最后一个 '_' 之后的部分）
    std::string algo = join_method_name;
    auto pos = algo.rfind('_');
    if (pos != std::string::npos) algo = algo.substr(0, pos);

    if (algo == "ivf") {
        index_kind_ = InternalIndexKind::IVF;
        // IVF 使用共享索引，需要 SharedWindowState 以确保所有并行实例看到完整窗口
        use_shared_state_ = true;
        // Calculate IVF parameters based on window size
        // nlist = 4 * sqrt(window_size/step_size), rebuild_threshold = 2.0
        int64_t window_size = join_func_->getWindowSize();
        int64_t step_size = join_func_->getStepSize();
        // Calculate actual vector count in window
        int64_t vector_count = (step_size > 0) ? (window_size / step_size) : window_size;
        int nlist = static_cast<int>(4.0 * std::sqrt(static_cast<double>(vector_count)));
        // Ensure nlist is at least 1
        nlist = std::max(nlist, 1);
        
        // Calculate nprobes to search at least 30% of clusters for better recall in high-concurrency scenarios
        // Cap at 60% to maintain reasonable performance
        int nprobes = std::max(15, std::min(nlist * 60 / 100, nlist * 30 / 100));
        
        IVFParameters ivf_params{
            .nlist = nlist,
            .rebuild_threshold = 2.0,
            .nprobes = nprobes
        };
        
        if (createIndexPair(IndexType::IVF, "join_ivf", ivf_params)) {
            use_index_ = true;
            join_method_ = std::make_unique<IvfJoinMethod>(left_index_id_, right_index_id_,
                                                           join_similarity_threshold_, concurrency_manager_);
        } else {
            index_kind_ = InternalIndexKind::NONE;
            use_index_ = false;
            join_method_ = std::make_unique<BruteForceJoinMethod>(
              -1, -1, join_similarity_threshold_, concurrency_manager_);
        }
    } else if (algo == "bruteforce" || algo == "bf" ) {
        index_kind_ = InternalIndexKind::BRUTEFORCE;
        // BruteForce 使用共享索引，需要 SharedWindowState 以确保所有并行实例看到完整窗口
        use_shared_state_ = true;
        if (createIndexPair(IndexType::BruteForce, "join_bf")) {
            use_index_ = true;
            join_method_ = std::make_unique<BruteForceJoinMethod>(left_index_id_, right_index_id_,
                                                                  join_similarity_threshold_, concurrency_manager_);
        } else {
            index_kind_ = InternalIndexKind::NONE;
            use_index_ = false;
            join_method_ = std::make_unique<BruteForceJoinMethod>(
              -1, -1, join_similarity_threshold_, concurrency_manager_);
        }
    } else if (algo == "hnsw") {
        // HNSW 模式
        // HNSW 使用共享索引，需要 SharedWindowState 以确保所有并行实例看到完整窗口
        use_shared_state_ = true;
        // 计算 HNSW 参数
        HNSWParameters hnsw_params{
            .m = 16,
            .ef_construction = 200,
            .ef_search = 100
        };
        
        if (createIndexPair(IndexType::HNSW, "join_hnsw", hnsw_params)) {
            use_index_ = true;
            HNSWJoinMethod::Config hnsw_config;
            hnsw_config.m = hnsw_params.m;
            hnsw_config.ef_construction = hnsw_params.ef_construction;
            hnsw_config.ef_search = hnsw_params.ef_search;
            join_method_ = std::make_unique<HNSWJoinMethod>(left_index_id_, right_index_id_,
                                                            join_similarity_threshold_, concurrency_manager_,
                                                            hnsw_config);
            SAGEFLOW_LOG_INFO("JOIN", "HNSW mode enabled, m={} ef_construction={} ef_search={} is_eager={}",
                             hnsw_params.m, hnsw_params.ef_construction, hnsw_params.ef_search, is_eager_);
        } else {
            index_kind_ = InternalIndexKind::NONE;
            use_index_ = false;
            join_method_ = std::make_unique<BruteForceJoinMethod>(
              -1, -1, join_similarity_threshold_, concurrency_manager_);
            SAGEFLOW_LOG_WARN("JOIN", "Failed to create HNSW index pair, falling back to BruteForce");
        }
    } else if (algo == "vsjoin") {
        // VSJoin 模式：启用 VSJoin 配置，组件在 open() 中初始化
        vsjoin_config_.enabled = true;
        // VSJoin 不使用传统的索引系统，使用自己的 PartitionedIndex
        index_kind_ = InternalIndexKind::NONE;
        use_index_ = false;
        // 仍然创建一个基础的 join_method_ 作为后备
        join_method_ = std::make_unique<BruteForceJoinMethod>(
          -1, -1, join_similarity_threshold_, concurrency_manager_);
        SAGEFLOW_LOG_INFO("JOIN", "VSJoin mode enabled, is_eager={}", is_eager_);
    } else {
        index_kind_ = InternalIndexKind::NONE;
        use_index_ = false;
        is_eager_ = false;
        join_method_ = std::make_unique<BruteForceJoinMethod>(
          -1, -1, join_similarity_threshold_, concurrency_manager_);
    }
}

JoinOperator::~JoinOperator() {
    // 关闭 VSJoin 组件
    closeVSJoinComponents();
    
    // Stop profiling if it was enabled
    if (profiler_) {
        profiler_->StopProfiling();
        SAGEFLOW_LOG_INFO("JOIN", "GPERFTOOLS profiling stopped");
    }
}

void JoinOperator::open() {
  if (is_open_) return;
  is_open_ = true;
  
  // Start profiling when operator opens
  if (profiler_) {
      profiler_->StartProfiling();
      SAGEFLOW_LOG_INFO("JOIN", "GPERFTOOLS profiling started");
  }
  
  // 为向后兼容，使用默认的 RuntimeContext 创建状态
  RuntimeContext default_context(0, parallelism_);
  
  // 根据配置创建窗口状态
  if (use_shared_state_) {
      left_state_ = std::make_unique<SharedWindowState>();
      right_state_ = std::make_unique<SharedWindowState>();
      SAGEFLOW_LOG_INFO("JOIN", "Using SharedWindowState (via legacy open)");
  } else {
      left_state_ = std::make_unique<PartitionedWindowState>(parallelism_);
      right_state_ = std::make_unique<PartitionedWindowState>(parallelism_);
      SAGEFLOW_LOG_INFO("JOIN", "Using PartitionedWindowState with parallelism={} (via legacy open)", 
                       parallelism_);
  }
}

void JoinOperator::open(const RuntimeContext& context) {
  if (is_open_) return;
  is_open_ = true;
  
  // Start profiling when operator opens
  if (profiler_) {
      profiler_->StartProfiling();
      SAGEFLOW_LOG_INFO("JOIN", "GPERFTOOLS profiling started");
  }
  
  // 如果启用了 VSJoin 模式，初始化 VSJoin 组件
  if (vsjoin_config_.enabled) {
      initVSJoinComponents(context);
      SAGEFLOW_LOG_INFO("JOIN", "JoinOperator opened with VSJoin mode: subtask={}/{}", 
                       context.getSubtaskIndex(), context.getParallelism());
      return;
  }
  
  // 根据配置创建窗口状态
  if (use_shared_state_) {
      left_state_ = std::make_unique<SharedWindowState>();
      right_state_ = std::make_unique<SharedWindowState>();
      SAGEFLOW_LOG_INFO("JOIN", "Using SharedWindowState");
  } else {
      left_state_ = std::make_unique<PartitionedWindowState>(context.getParallelism());
      right_state_ = std::make_unique<PartitionedWindowState>(context.getParallelism());
      SAGEFLOW_LOG_INFO("JOIN", "Using PartitionedWindowState with parallelism={}", 
                       context.getParallelism());
  }
  
  SAGEFLOW_LOG_INFO("JOIN", "JoinOperator opened: subtask={}/{}, shared_state={}", 
                   context.getSubtaskIndex(), context.getParallelism(), 
                   use_shared_state_);
}

auto JoinOperator::updateSideThreadSafe(
    std::deque<std::unique_ptr<VectorRecord>>& records,
    std::shared_mutex& records_mutex,
    int index_id_for_cc,
    std::unique_ptr<VectorRecord>& data_ptr,
    int64_t now_time_stamp,
    int slot) -> bool {
    // 备注：本函数中将分别对窗口插入/过期与索引插入/删除进行分段计时；
    // 同时为每一次加锁补充 lock_wait 统计，避免漏计导致占比异常。
    std::unique_ptr<VectorRecord> data_for_index_insert = nullptr;
    if (use_index_ && concurrency_manager_ && index_id_for_cc != -1) {
        data_for_index_insert = std::make_unique<VectorRecord>(*data_ptr);
    }
    uint64_t before_lock = metrics_timestamp();
    std::unique_lock<std::shared_mutex> lock(records_mutex);
    metrics_record_lock_wait_dual(before_lock, JoinMetrics::instance().window_insert_ns);
    
    if (slot == 0) {
      JoinMetrics::instance().total_records_left.fetch_add(1, std::memory_order_relaxed);
    } else {
      JoinMetrics::instance().total_records_right.fetch_add(1, std::memory_order_relaxed);
    }
    // 窗口插入阶段（仅插入）
    {
        MetricsTimer t_window_ins(JoinMetrics::instance().window_insert_ns);
        records.emplace_back(std::move(data_ptr));
    }

    if (use_index_ && concurrency_manager_ && data_for_index_insert && index_id_for_cc != -1) {
        MetricsTimer t_idx(JoinMetrics::instance().index_insert_ns);
        // lock.unlock();
        concurrency_manager_->insert(index_id_for_cc, std::move(data_for_index_insert));
        // lock.lock();
    }

    auto& window = (slot == 0) ? join_func_->threadSafeWindowL : join_func_->threadSafeWindowR;

    int64_t timelimit = window.windowTimeLimit(now_time_stamp);
    if (retention_buffer_ > 0) {
        if (timelimit <= std::numeric_limits<int64_t>::min() + retention_buffer_) {
            timelimit = std::numeric_limits<int64_t>::min();
        } else {
            timelimit -= retention_buffer_;
        }
    }
    // 窗口过期阶段（包含过期判定与容器维护；索引删除单独计时）
    SAGEFLOW_LOG_DEBUG("JOIN", "Expiring records before timestamp {} now={} current_size={} ", timelimit, now_time_stamp, records.size());
    size_t expired_count = 0;
    try {
        // 过期阶段的容器维护开销：将每次 pop_front 计入 window_insert_ns，索引删除计入 index_insert_ns。
        while (!records.empty() && records.front()->timestamp_ <= timelimit) {
            uint64_t expired_uid = records.front()->uid_;
            {
                MetricsTimer t_window_expire_unit(JoinMetrics::instance().window_insert_ns);
                records.pop_front();
                ++expired_count;
            }
            if (use_index_ && concurrency_manager_ && index_id_for_cc != -1) {
                MetricsTimer t_idx_del(JoinMetrics::instance().index_insert_ns);
                // lock.unlock();
                concurrency_manager_->erase(index_id_for_cc, expired_uid);
                // lock.lock();
            }
        }
        SAGEFLOW_LOG_DEBUG("JOIN", "Expiration loop finished. current_size={} ", records.size());
    } catch (const std::exception& e) {
        SAGEFLOW_LOG_ERROR("JOIN", "Exception during expiration: what={} ", e.what());
    }
    if (expired_count > 0) {
        if (slot == 0) {
            JoinMetrics::instance().window_records_left_completed.fetch_add(expired_count, std::memory_order_relaxed);
        } else {
            JoinMetrics::instance().window_records_right_completed.fetch_add(expired_count, std::memory_order_relaxed);
        }
    }

    SAGEFLOW_LOG_DEBUG("JOIN", "Before unlocking records mutex. size={} ", records.size());
    lock.unlock();
    SAGEFLOW_LOG_DEBUG("JOIN", "After unlocking records mutex; computing trigger.");
    bool needTrigger = false;
    try {
        needTrigger = window.isNeedTrigger(now_time_stamp);
    } catch (const std::exception& e) {
        SAGEFLOW_LOG_ERROR("JOIN", "Exception during isNeedTrigger: what={} ", e.what());
        throw;
    }
    SAGEFLOW_LOG_DEBUG("JOIN", "isNeedTrigger={} ", needTrigger ? 1 : 0);
    return needTrigger;
}

// ================== 旧 fallback 接口（仍保留） ==================
auto JoinOperator::lazy_process(const int slot) -> std::optional<Response> {
  std::shared_lock<std::shared_mutex> left_lock(left_records_mutex_);
  std::shared_lock<std::shared_mutex> right_lock(right_records_mutex_);
  if (left_records_.empty() || right_records_.empty()) return std::nullopt;
  left_lock.unlock(); right_lock.unlock();
  std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> local_return_pool;
  // 不再执行旧Excute逻辑，这里直接返回空；保留接口防止外部调用崩溃
  return std::nullopt;
}

auto JoinOperator::eager_process(const int slot) -> std::optional<Response> {
  std::shared_lock<std::shared_mutex> left_lock(left_records_mutex_);
  std::shared_lock<std::shared_mutex> right_lock(right_records_mutex_);
  if ((slot==0 && left_records_.empty()) || (slot==1 && right_records_.empty())) return std::nullopt;
  return std::nullopt;
}

auto JoinOperator::process(Response& input_data, int slot) -> std::optional<Response> {
    if (!input_data.record_) return std::nullopt;
    std::unique_ptr<VectorRecord> data_ptr = std::make_unique<VectorRecord>(*input_data.record_);
    int64_t now_time_stamp = data_ptr->timestamp_;

    bool trigger_flag = (slot == 0)
        ? updateSideThreadSafe(left_records_, left_records_mutex_, left_index_id_, data_ptr, now_time_stamp, slot)
        : updateSideThreadSafe(right_records_, right_records_mutex_, right_index_id_, data_ptr, now_time_stamp, slot);

    if (!trigger_flag) return std::nullopt;

    // 新框架中 process 不直接返回结果（改由 apply / collector 机制），保持兼容返回空
    return std::nullopt;
}

auto JoinOperator::getCandidates(
    const std::unique_ptr<VectorRecord>& data_ptr, int slot) -> std::vector<std::unique_ptr<VectorRecord>> {
    MetricsTimer t_fetch(JoinMetrics::instance().candidate_fetch_ns);
    if (is_eager_) {
        return join_method_->ExecuteEager(*data_ptr, slot);
    }
    const int64_t logical_lower_bound = logicalWindowLowerBound(data_ptr->timestamp_);
    std::deque<std::unique_ptr<VectorRecord>> query_records_copy; // 改为 deque
    if (slot == left_slot_id_) {
        uint64_t before_wait = metrics_timestamp();
        std::shared_lock<std::shared_mutex> lk(left_records_mutex_);
        metrics_record_lock_wait(before_wait);
        for (auto &p : left_records_) {
                    if (isRecordFresh(p, logical_lower_bound)) {
                        query_records_copy.emplace_back(std::make_unique<VectorRecord>(*p));
                    }
        }
    } else {
        uint64_t before_wait = metrics_timestamp();
        std::shared_lock<std::shared_mutex> lk(right_records_mutex_);
        metrics_record_lock_wait(before_wait);
        for (auto &p : right_records_)
                    if (isRecordFresh(p, logical_lower_bound)) {
                        query_records_copy.emplace_back(std::make_unique<VectorRecord>(*p));
                    }
    }
    return join_method_->ExecuteLazy(query_records_copy, slot);
}

auto JoinOperator::getCandidatesWithLocksHeld(
    const std::unique_ptr<VectorRecord>& data_ptr, int slot) -> std::vector<std::unique_ptr<VectorRecord>> {
    // This version assumes both window locks are already held by caller
    MetricsTimer t_fetch(JoinMetrics::instance().candidate_fetch_ns);
    if (is_eager_) {
        return join_method_->ExecuteEager(*data_ptr, slot);
    }
    const int64_t logical_lower_bound = logicalWindowLowerBound(data_ptr->timestamp_);
    std::deque<std::unique_ptr<VectorRecord>> query_records_copy;
    if (slot == left_slot_id_) {
        for (auto &p : left_records_) {
                    if (isRecordFresh(p, logical_lower_bound)) {
                        query_records_copy.emplace_back(std::make_unique<VectorRecord>(*p));
                    }
        }
    } else {
        for (auto &p : right_records_) {
                    if (isRecordFresh(p, logical_lower_bound)) {
                        query_records_copy.emplace_back(std::make_unique<VectorRecord>(*p));
                    }
        }
    }
    return join_method_->ExecuteLazy(query_records_copy, slot);
}

auto JoinOperator::validateCandidateInWindow(
    const std::unique_ptr<VectorRecord>& candidate,
    const std::deque<std::unique_ptr<VectorRecord>>& window_records,
    int64_t logical_lower_bound) -> bool {
    if (!isRecordFresh(candidate, logical_lower_bound)) {
        return false;
    }
    for (auto &r : window_records) {
        if (!isRecordFresh(r, logical_lower_bound)) {
            continue;
        }
        if (r->uid_ == candidate->uid_) {
            return true;
        }
    }
    return false;
}

void JoinOperator::executeJoinForCandidates(
    const std::vector<std::unique_ptr<VectorRecord>>& candidates,
    const std::unique_ptr<VectorRecord>& data_ptr,
    int slot,
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>>& local_return_pool) {
    // 注：similarity_ns 仅用于粗粒度的候选比对阶段计时；
    MetricsTimer t_similarity(JoinMetrics::instance().similarity_ns);
    
    // Critical fix: Lock the opposite window BEFORE validating candidates
    // to prevent race condition where candidates expire between index query and validation
    if (slot == 0) {
        uint64_t before_wait = metrics_timestamp();
        std::shared_lock<std::shared_mutex> rk(right_records_mutex_);
        metrics_record_lock_wait(before_wait);
        // Now we hold the lock, validate and join each candidate
        executeJoinForCandidatesWithLockHeld(candidates, data_ptr, slot, right_records_, local_return_pool);
    } else {
        uint64_t before_wait = metrics_timestamp();
        std::shared_lock<std::shared_mutex> lk(left_records_mutex_);
        metrics_record_lock_wait(before_wait);
        // Now we hold the lock, validate and join each candidate
        executeJoinForCandidatesWithLockHeld(candidates, data_ptr, slot, left_records_, local_return_pool);
    }
}

void JoinOperator::executeJoinForCandidatesWithLockHeld(
    const std::vector<std::unique_ptr<VectorRecord>>& candidates,
    const std::unique_ptr<VectorRecord>& data_ptr,
    int slot,
    const std::deque<std::unique_ptr<VectorRecord>>& opposite_window,
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>>& local_return_pool) {
    MetricsTimer t_similarity(JoinMetrics::instance().similarity_ns);
    const int64_t logical_lower_bound = logicalWindowLowerBound(data_ptr->timestamp_);
    if (!isRecordFresh(data_ptr, logical_lower_bound)) {
        return;
    }
    
    // IMPORTANT: Validation is necessary even in eager mode to filter out:
    // 1. Expired records: Shared index may have records not yet expired from index but already expired from windows
    // 2. Timing issues: Ensures candidates are actually in the current window state
    // 
    // NOTE: This causes window fragmentation in multi-instance scenarios where candidates from other
    // instances' windows fail validation. This is a known trade-off - without validation we get
    // incorrect joins with expired records.
    for (auto &cand : candidates) {
        if (!isRecordFresh(cand, logical_lower_bound)) {
            continue;
        }
        if (validateCandidateInWindow(cand, opposite_window, logical_lower_bound)) {
            std::unique_ptr<VectorRecord> left_copy;
            std::unique_ptr<VectorRecord> right_copy;
            
            if (slot == 0) {
                left_copy = std::make_unique<VectorRecord>(*data_ptr);
                right_copy = std::make_unique<VectorRecord>(*cand);
            } else {
                left_copy = std::make_unique<VectorRecord>(*cand);
                right_copy = std::make_unique<VectorRecord>(*data_ptr);
            }
            
            uint64_t log_left_uid = left_copy->uid_;
            uint64_t log_right_uid = right_copy->uid_;
            Response lhs{ResponseType::Record, std::move(left_copy)};
            Response rhs{ResponseType::Record, std::move(right_copy)};
            {
                MetricsTimer t_joinF(JoinMetrics::instance().join_function_ns);
                try {
                    auto res = join_func_->Execute(lhs, rhs);
                    uint64_t result_uid = res.record_ ? res.record_->uid_ : 0;
                    if (res.record_) {
                        local_return_pool.emplace_back(left_slot_id_, std::move(res.record_));
                    }
                    SAGEFLOW_LOG_DEBUG("JOIN_EXEC", "slot={} result_uid={} left_uid={} right_uid={} ",
                                   slot, result_uid, log_left_uid, log_right_uid);
                } catch (const std::exception& e) {
                    SAGEFLOW_LOG_ERROR("JOIN_EXEC", "slot={} left_dim={} right_dim={} left_uid={} right_uid={} what={} ",
                                     slot,
                                     (lhs.record_ ? lhs.record_->data_.dim_ : -1),
                                     (rhs.record_ ? rhs.record_->data_.dim_ : -1),
                                     (lhs.record_ ? lhs.record_->uid_ : 0),
                                     (rhs.record_ ? rhs.record_->uid_ : 0),
                                     e.what());
                    throw;
                }
            }
        }
    }
}

void JoinOperator::executeLazyJoin(
    const std::vector<std::unique_ptr<VectorRecord>>& candidates,
    int slot,
    int64_t query_timestamp,
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>>& local_return_pool) {
    // 统一计量 Lazy 路径的候选匹配阶段
    MetricsTimer t_similarity(JoinMetrics::instance().similarity_ns);
    const int64_t logical_lower_bound = logicalWindowLowerBound(query_timestamp);
    if (slot == left_slot_id_) {
        uint64_t before_wait_r = metrics_timestamp();
        std::shared_lock<std::shared_mutex> rk(right_records_mutex_);
        metrics_record_lock_wait(before_wait_r);
        uint64_t before_wait_l = metrics_timestamp();
        std::shared_lock<std::shared_mutex> lk(left_records_mutex_);
        metrics_record_lock_wait(before_wait_l);
        for (auto &l : left_records_) {
            if (!isRecordFresh(l, logical_lower_bound)) continue;
            for (auto &cand : candidates) {
                if (!isRecordFresh(cand, logical_lower_bound)) continue;
                if (validateCandidateInWindow(cand, right_records_, logical_lower_bound)) {
                    auto left_copy = std::make_unique<VectorRecord>(*l);
                    auto right_copy = std::make_unique<VectorRecord>(*cand);
                    Response lhs{ResponseType::Record, std::move(left_copy)};
                    Response rhs{ResponseType::Record, std::move(right_copy)};
                    try {
                        MetricsTimer t_joinF(JoinMetrics::instance().join_function_ns);
                        auto res = join_func_->Execute(lhs, rhs);
                        if (res.record_) local_return_pool.emplace_back(left_slot_id_, std::move(res.record_));
                    } catch (const std::exception& e) {
                        SAGEFLOW_LOG_ERROR("JOIN_LAZY", "slot={} left_dim={} right_dim={} left_uid={} right_uid={} what={} ",
                                         slot,
                                         (lhs.record_ ? lhs.record_->data_.dim_ : -1),
                                         (rhs.record_ ? rhs.record_->data_.dim_ : -1),
                                         (lhs.record_ ? lhs.record_->uid_ : 0),
                                         (rhs.record_ ? rhs.record_->uid_ : 0),
                                         e.what());
                        throw;
                    }
                }
            }
        }
    } else {
        uint64_t before_wait_l = metrics_timestamp();
        std::shared_lock<std::shared_mutex> lk(left_records_mutex_);
        metrics_record_lock_wait(before_wait_l);
        uint64_t before_wait_r = metrics_timestamp();
        std::shared_lock<std::shared_mutex> rk(right_records_mutex_);
        metrics_record_lock_wait(before_wait_r);
        for (auto &r : right_records_) {
            if (!isRecordFresh(r, logical_lower_bound)) continue;
            for (auto &cand : candidates) {
                if (!isRecordFresh(cand, logical_lower_bound)) continue;
                if (validateCandidateInWindow(cand, left_records_, logical_lower_bound)) {
                    auto left_copy = std::make_unique<VectorRecord>(*cand);
                    auto right_copy = std::make_unique<VectorRecord>(*r);
                    Response lhs{ResponseType::Record, std::move(left_copy)};
                    Response rhs{ResponseType::Record, std::move(right_copy)};
                    try {
                        MetricsTimer t_joinF(JoinMetrics::instance().join_function_ns);
                        auto res = join_func_->Execute(lhs, rhs);
                        if (res.record_) local_return_pool.emplace_back(left_slot_id_, std::move(res.record_));
                    } catch (const std::exception& e) {
                        SAGEFLOW_LOG_ERROR("JOIN_LAZY", "slot={} left_dim={} right_dim={} left_uid={} right_uid={} what={} ",
                                         slot,
                                         (lhs.record_ ? lhs.record_->data_.dim_ : -1),
                                         (rhs.record_ ? rhs.record_->data_.dim_ : -1),
                                         (lhs.record_ ? lhs.record_->uid_ : 0),
                                         (rhs.record_ ? rhs.record_->uid_ : 0),
                                         e.what());
                        throw;
                    }
                }
            }
        }
    }
}

void JoinOperator::executeLazyJoinWithLocksHeld(
    const std::vector<std::unique_ptr<VectorRecord>>& candidates,
    int slot,
    int64_t query_timestamp,
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>>& local_return_pool) {
    // This version assumes both window locks are already held by caller
    MetricsTimer t_similarity(JoinMetrics::instance().similarity_ns);
    // IMPORTANT: For lazy mode, we must keep validation to avoid Cartesian product explosion
    // While validation against local window causes some window fragmentation, removing it entirely
    // creates N×M join operations which causes severe performance degradation (timeouts).
    // TODO: Find a better approach that avoids both fragmentation AND explosion
    const int64_t logical_lower_bound = logicalWindowLowerBound(query_timestamp);
    if (slot == left_slot_id_) {
        for (auto &l : left_records_) {
            if (!isRecordFresh(l, logical_lower_bound)) continue;
            for (auto &cand : candidates) {
                if (!isRecordFresh(cand, logical_lower_bound)) continue;
                if (validateCandidateInWindow(cand, right_records_, logical_lower_bound)) {
                    auto left_copy = std::make_unique<VectorRecord>(*l);
                    auto right_copy = std::make_unique<VectorRecord>(*cand);
                    Response lhs{ResponseType::Record, std::move(left_copy)};
                    Response rhs{ResponseType::Record, std::move(right_copy)};
                    try {
                        MetricsTimer t_joinF(JoinMetrics::instance().join_function_ns);
                        auto res = join_func_->Execute(lhs, rhs);
                        if (res.record_) local_return_pool.emplace_back(left_slot_id_, std::move(res.record_));
                    } catch (const std::exception& e) {
                        SAGEFLOW_LOG_ERROR("JOIN_LAZY", "slot={} left_dim={} right_dim={} left_uid={} right_uid={} what={} ",
                                         slot,
                                         (lhs.record_ ? lhs.record_->data_.dim_ : -1),
                                         (rhs.record_ ? rhs.record_->data_.dim_ : -1),
                                         (lhs.record_ ? lhs.record_->uid_ : 0),
                                         (rhs.record_ ? rhs.record_->uid_ : 0),
                                         e.what());
                        throw;
                    }
                }
            }
        }
    } else {
        for (auto &r : right_records_) {
            if (!isRecordFresh(r, logical_lower_bound)) continue;
            for (auto &cand : candidates) {
                if (!isRecordFresh(cand, logical_lower_bound)) continue;
                if (validateCandidateInWindow(cand, left_records_, logical_lower_bound)) {
                    auto left_copy = std::make_unique<VectorRecord>(*cand);
                    auto right_copy = std::make_unique<VectorRecord>(*r);
                    Response lhs{ResponseType::Record, std::move(left_copy)};
                    Response rhs{ResponseType::Record, std::move(right_copy)};
                    try {
                        MetricsTimer t_joinF(JoinMetrics::instance().join_function_ns);
                        auto res = join_func_->Execute(lhs, rhs);
                        if (res.record_) local_return_pool.emplace_back(left_slot_id_, std::move(res.record_));
                    } catch (const std::exception& e) {
                        SAGEFLOW_LOG_ERROR("JOIN_LAZY", "slot={} left_dim={} right_dim={} left_uid={} right_uid={} what={} ",
                                         slot,
                                         (lhs.record_ ? lhs.record_->data_.dim_ : -1),
                                         (rhs.record_ ? rhs.record_->data_.dim_ : -1),
                                         (lhs.record_ ? lhs.record_->uid_ : 0),
                                         (rhs.record_ ? rhs.record_->uid_ : 0),
                                         e.what());
                        throw;
                    }
                }
            }
        }
    }
}

auto JoinOperator::apply(Response&& record, int slot, Collector& collector) -> void {
    // 统计 apply 处理总耗时（一次调用一次计数）
    metrics_increment(JoinMetrics::instance().apply_processing_count);
    MetricsTimer t_apply(JoinMetrics::instance().apply_processing_ns);
    // 记录进入算子的实时时刻，用于端到端延迟统计
    const uint64_t apply_enter_ns = metrics_timestamp();
    if (!record.record_) return;
    std::unique_ptr<VectorRecord> data_ptr = std::make_unique<VectorRecord>(*record.record_);
    int64_t now_time_stamp = data_ptr->timestamp_;
    SAGEFLOW_LOG_DEBUG("JOIN_APPLY", "Apply called slot={} uid={} ts={} dim={} ", slot, data_ptr->uid_, now_time_stamp, data_ptr->data_.dim_);
    // 重要：为窗口存储拷贝一份，避免 data_ptr 在 updateSideThreadSafe 中被移动导致后续 eager 路径解引用空指针
    auto store_ptr = std::make_unique<VectorRecord>(*data_ptr);
    bool trigger_flag = (slot == left_slot_id_)
        ? updateSideThreadSafe(left_records_, left_records_mutex_, left_index_id_, store_ptr, now_time_stamp, slot)
        : updateSideThreadSafe(right_records_, right_records_mutex_, right_index_id_, store_ptr, now_time_stamp, slot);
    if (!trigger_flag) {
      return;
    }

    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> local_return_pool;
    size_t left_sz = 0, right_sz = 0;
    
    // Acquire both locks in consistent order (left first, then right) to avoid deadlock
    uint64_t before_lock_L = metrics_timestamp();
    std::shared_lock<std::shared_mutex> lkL(left_records_mutex_);
    metrics_record_lock_wait(before_lock_L);
    uint64_t before_lock_R = metrics_timestamp();
    std::shared_lock<std::shared_mutex> lkR(right_records_mutex_);
    metrics_record_lock_wait(before_lock_R);
    
    // Now holding both locks, get window sizes and candidates safely
    left_sz = left_records_.size();
    right_sz = right_records_.size();
    
    auto candidates = getCandidatesWithLocksHeld(data_ptr, slot);
    
    SAGEFLOW_LOG_DEBUG("JOIN_APPLY", "slot={} cand={} left_win={} right_win={} eager={} use_index={} ",
           slot, candidates.size(), left_sz, right_sz, (is_eager_?1:0), (use_index_?1:0));

    if (is_eager_) {
        // Execute join while holding both locks to ensure consistency
        executeJoinForCandidatesWithLockHeld(candidates, data_ptr, slot, 
                                              slot == left_slot_id_ ? right_records_ : left_records_,
                                              local_return_pool);
    } else {
        executeLazyJoinWithLocksHeld(candidates, slot, now_time_stamp, local_return_pool);
        // Release locks before clearing for lazy mode
        lkL.unlock();
        lkR.unlock();
        
        // 清理窗口前加锁等待计入 lock_wait 与 window_insert_ns（视为窗口阶段的一部分）
        uint64_t before_wait_L = metrics_timestamp();
        std::unique_lock<std::shared_mutex> wlkL(left_records_mutex_);
        metrics_record_lock_wait_dual(before_wait_L, JoinMetrics::instance().window_insert_ns);
        
        uint64_t before_wait_R = metrics_timestamp();
        std::unique_lock<std::shared_mutex> wlkR(right_records_mutex_);
        metrics_record_lock_wait_dual(before_wait_R, JoinMetrics::instance().window_insert_ns);
        
        left_records_.clear();
        right_records_.clear();
    }
    
    // Locks released here automatically when going out of scope
    
    {
        MetricsTimer t_emit(JoinMetrics::instance().emit_ns);
        for (auto &p : local_return_pool) {
            Response out{ResponseType::Record, std::move(p.second)};
            collector.collect(std::make_unique<Response>(std::move(out)), p.first);
            metrics_increment(JoinMetrics::instance().total_emits);
            // 端到端延迟：从 apply 进入到对应结果发射的时长（按每条结果计）
            metrics_record_e2e_latency(apply_enter_ns);
        }
    }
}

// ================== WindowState 辅助方法 ==================

std::vector<std::unique_ptr<VectorRecord>> JoinOperator::getCandidatesFromState(
    const VectorRecord* data_ptr,
    WindowState* state,
    size_t subtask_index) {
    
    MetricsTimer t_fetch(JoinMetrics::instance().candidate_fetch_ns);
    
    if (is_eager_) {
        // Eager 模式：使用索引直接获取候选项
        // 注意：state 是对面的窗口状态（opposite_state），我们需要传递记录来源的 slot
        // 当 state == right_state_ 时，说明记录来自左流（slot=0），需要查询右索引
        // 当 state == left_state_ 时，说明记录来自右流（slot=1），需要查询左索引
        // ExecuteEager 内部使用 otherIndexId(slot) 来选择正确的索引
        int query_slot = (state == right_state_.get()) ? left_slot_id_ : right_slot_id_;
        return join_method_->ExecuteEager(*data_ptr, query_slot);
    }
    
    // Lazy 模式：从窗口状态获取所有记录
    std::deque<std::unique_ptr<VectorRecord>> query_records_copy;
    const auto& records = state->getRecords(subtask_index);
    for (const auto& p : records) {
        if (p) {
            query_records_copy.emplace_back(std::make_unique<VectorRecord>(*p));
        }
    }
    
    // 与 Eager 模式相同的逻辑：state 是对面的窗口状态
    int slot = (state == right_state_.get()) ? left_slot_id_ : right_slot_id_;
    return join_method_->ExecuteLazy(query_records_copy, slot);
}

auto JoinOperator::updateSideWithState(
    WindowState* state,
    int index_id_for_cc,
    std::unique_ptr<VectorRecord> data_ptr,
    int64_t now_time_stamp,
    int slot,
    size_t subtask_index) -> bool {
    
    // 为索引创建记录副本
    std::unique_ptr<VectorRecord> data_for_index_insert = nullptr;
    if (use_index_ && concurrency_manager_ && index_id_for_cc != -1) {
        data_for_index_insert = std::make_unique<VectorRecord>(*data_ptr);
    }
    
    // 统计插入的记录数
    if (slot == left_slot_id_) {
        JoinMetrics::instance().total_records_left.fetch_add(1, std::memory_order_relaxed);
    } else {
        JoinMetrics::instance().total_records_right.fetch_add(1, std::memory_order_relaxed);
    }
    
    // 添加记录到窗口状态
    {
        MetricsTimer t_window_ins(JoinMetrics::instance().window_insert_ns);
        state->addRecord(std::move(data_ptr), subtask_index);
    }
    
    // 插入索引
    if (use_index_ && concurrency_manager_ && data_for_index_insert && index_id_for_cc != -1) {
        MetricsTimer t_idx(JoinMetrics::instance().index_insert_ns);
        concurrency_manager_->insert(index_id_for_cc, std::move(data_for_index_insert));
    }
    
    // 获取窗口配置
    auto& window = (slot == left_slot_id_) ? join_func_->threadSafeWindowL : join_func_->threadSafeWindowR;
    int64_t timelimit = window.windowTimeLimit(now_time_stamp);
    
    // 清理过期记录（WindowState 内部会处理锁）
    {
        MetricsTimer t_window_evict(JoinMetrics::instance().window_insert_ns);
        state->evictExpired(now_time_stamp, join_func_->getWindowSize(), subtask_index);
    }
    
    // 检查是否需要触发
    bool needTrigger = false;
    try {
        needTrigger = window.isNeedTrigger(now_time_stamp);
    } catch (const std::exception& e) {
        SAGEFLOW_LOG_ERROR("JOIN", "Exception during isNeedTrigger: what={} ", e.what());
        throw;
    }
    
    return needTrigger;
}

void JoinOperator::executeJoinWithState(
    const VectorRecord* data_ptr,
    WindowState* opposite_state,
    int slot,
    size_t subtask_index,
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>>& local_return_pool) {
    
    MetricsTimer t_similarity(JoinMetrics::instance().similarity_ns);
    
    // 获取候选项
    auto candidates = getCandidatesFromState(data_ptr, opposite_state, subtask_index);
    
    // 获取对面窗口的记录用于验证
    const auto& opposite_records = opposite_state->getRecords(subtask_index);
    
    for (const auto& cand : candidates) {
        // 验证候选项仍在窗口中
        bool valid = false;
        for (const auto& r : opposite_records) {
            if (r && r->uid_ == cand->uid_) {
                valid = true;
                break;
            }
        }
        
        if (valid) {
            std::unique_ptr<VectorRecord> left_copy;
            std::unique_ptr<VectorRecord> right_copy;
            
            if (slot == left_slot_id_) {
                left_copy = std::make_unique<VectorRecord>(*data_ptr);
                right_copy = std::make_unique<VectorRecord>(*cand);
            } else {
                left_copy = std::make_unique<VectorRecord>(*cand);
                right_copy = std::make_unique<VectorRecord>(*data_ptr);
            }
            
            Response lhs{ResponseType::Record, std::move(left_copy)};
            Response rhs{ResponseType::Record, std::move(right_copy)};
            
            try {
                MetricsTimer t_joinF(JoinMetrics::instance().join_function_ns);
                auto res = join_func_->Execute(lhs, rhs);
                if (res.record_) {
                    local_return_pool.emplace_back(left_slot_id_, std::move(res.record_));
                }
            } catch (const std::exception& e) {
                SAGEFLOW_LOG_ERROR("JOIN_STATE", "Exception in executeJoinWithState: what={}", e.what());
                throw;
            }
        }
    }
}

// ================== 带 RuntimeContext 的 apply 方法 ==================

auto JoinOperator::apply(Response&& record, int slot, Collector& collector,
                        const RuntimeContext& context) -> void {
    // 如果启用了 VSJoin 模式，使用 VSJoin 专用路径
    if (vsjoin_config_.enabled) {
        applyVSJoin(std::move(record), slot, collector, context);
        return;
    }
    
    // 统计 apply 处理总耗时
    metrics_increment(JoinMetrics::instance().apply_processing_count);
    MetricsTimer t_apply(JoinMetrics::instance().apply_processing_ns);
    const uint64_t apply_enter_ns = metrics_timestamp();
    
    if (!record.record_) return;
    
    size_t subtask_index = context.getSubtaskIndex();
    std::unique_ptr<VectorRecord> data_ptr = std::make_unique<VectorRecord>(*record.record_);
    int64_t now_time_stamp = data_ptr->timestamp_;
    
    SAGEFLOW_LOG_DEBUG("JOIN_APPLY", "Apply (with context) called slot={} uid={} ts={} subtask={}/{}", 
                      slot, data_ptr->uid_, now_time_stamp, 
                      subtask_index, context.getParallelism());
    
    // 检查是否已创建状态对象
    if (!left_state_ || !right_state_) {
        SAGEFLOW_LOG_WARN("JOIN", "WindowState not initialized, falling back to legacy apply");
        apply(std::move(record), slot, collector);
        return;
    }
    
    // 确定当前记录属于哪一侧
    WindowState* current_state = (slot == left_slot_id_) 
        ? left_state_.get() : right_state_.get();
    WindowState* opposite_state = (slot == left_slot_id_) 
        ? right_state_.get() : left_state_.get();
    int index_id = (slot == left_slot_id_) ? left_index_id_ : right_index_id_;
    
    // 保存数据指针副本用于后续 join
    auto data_for_join = std::make_unique<VectorRecord>(*data_ptr);
    
    // 更新当前侧窗口
    bool trigger_flag = updateSideWithState(
        current_state, index_id, std::move(data_ptr), now_time_stamp, slot, subtask_index);
    
    if (!trigger_flag) {
        return;
    }
    
    // 执行 Join 操作
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> local_return_pool;
    
    if (is_eager_) {
        // Eager 模式：只用当前记录与对面窗口进行 join
        executeJoinWithState(data_for_join.get(), opposite_state, slot, 
                            subtask_index, local_return_pool);
    } else {
        // Lazy 模式：获取当前侧所有记录与对面窗口的候选项进行 join
        const auto& current_records = current_state->getRecords(subtask_index);
        auto candidates = getCandidatesFromState(nullptr, opposite_state, subtask_index);
        
        MetricsTimer t_similarity(JoinMetrics::instance().similarity_ns);
        const auto& opposite_records = opposite_state->getRecords(subtask_index);
        
        for (const auto& l : current_records) {
            if (!l) continue;
            for (const auto& cand : candidates) {
                // 验证候选项仍在窗口中
                bool valid = false;
                for (const auto& r : opposite_records) {
                    if (r && r->uid_ == cand->uid_) {
                        valid = true;
                        break;
                    }
                }
                
                if (valid) {
                    std::unique_ptr<VectorRecord> left_copy;
                    std::unique_ptr<VectorRecord> right_copy;
                    
                    if (slot == left_slot_id_) {
                        left_copy = std::make_unique<VectorRecord>(*l);
                        right_copy = std::make_unique<VectorRecord>(*cand);
                    } else {
                        left_copy = std::make_unique<VectorRecord>(*cand);
                        right_copy = std::make_unique<VectorRecord>(*l);
                    }
                    
                    Response lhs{ResponseType::Record, std::move(left_copy)};
                    Response rhs{ResponseType::Record, std::move(right_copy)};
                    
                    try {
                        MetricsTimer t_joinF(JoinMetrics::instance().join_function_ns);
                        auto res = join_func_->Execute(lhs, rhs);
                        if (res.record_) {
                            local_return_pool.emplace_back(left_slot_id_, std::move(res.record_));
                        }
                    } catch (const std::exception& e) {
                        SAGEFLOW_LOG_ERROR("JOIN_LAZY_STATE", "Exception: what={}", e.what());
                        throw;
                    }
                }
            }
        }
    }
    
    // 发送 Join 结果
    {
        MetricsTimer t_emit(JoinMetrics::instance().emit_ns);
        for (auto& p : local_return_pool) {
            Response out{ResponseType::Record, std::move(p.second)};
            collector.collect(std::make_unique<Response>(std::move(out)), p.first);
            metrics_increment(JoinMetrics::instance().total_emits);
            metrics_record_e2e_latency(apply_enter_ns);
        }
    }
    
    SAGEFLOW_LOG_DEBUG("JOIN_APPLY", "Apply (with context) completed: slot={} results={} subtask={}/{}", 
                      slot, local_return_pool.size(), subtask_index, context.getParallelism());
}

// ================== VSJoin 相关方法实现 ==================

void JoinOperator::setVSJoinConfig(const VSJoinConfig& config) {
    vsjoin_config_ = config;
    SAGEFLOW_LOG_INFO("JOIN", "VSJoin config set: enabled={}, partitions={}, probes={}", 
                     config.enabled, config.num_partitions, config.num_probes);
}

void JoinOperator::initVSJoinComponents(const RuntimeContext& context) {
    SAGEFLOW_LOG_INFO("JOIN", "Initializing VSJoin components with {} partitions, subtask={}/{}",
                      vsjoin_config_.num_partitions, 
                      context.getSubtaskIndex(), context.getParallelism());
    
    int dimension = join_func_->getDim();
    
    // 1. 初始化向量空间分区器（使用 LSH）
    vsjoin_partitioner_ = std::make_shared<LSHPartitioner>(
        dimension, 
        /*num_hash_functions=*/8,
        /*seed=*/42,
        /*boundary_threshold=*/0.1);
    
    // 2. 初始化分区向量状态（双层窗口 + 向量空间分区）
    left_vsjoin_state_ = std::make_unique<PartitionedVectorState>(
        static_cast<size_t>(vsjoin_config_.num_partitions),
        vsjoin_partitioner_,
        vsjoin_config_.compact_threshold,
        vsjoin_config_.enable_boundary_tracking);
    
    right_vsjoin_state_ = std::make_unique<PartitionedVectorState>(
        static_cast<size_t>(vsjoin_config_.num_partitions),
        vsjoin_partitioner_,
        vsjoin_config_.compact_threshold,
        vsjoin_config_.enable_boundary_tracking);
    
    // 3. 初始化分区索引
    left_vsjoin_index_ = std::make_shared<PartitionedIndex>(
        static_cast<size_t>(vsjoin_config_.num_partitions),
        dimension,
        vsjoin_partitioner_,
        vsjoin_config_.ivf_nlist,
        vsjoin_config_.ivf_nprobes);
    
    right_vsjoin_index_ = std::make_shared<PartitionedIndex>(
        static_cast<size_t>(vsjoin_config_.num_partitions),
        dimension,
        vsjoin_partitioner_,
        vsjoin_config_.ivf_nlist,
        vsjoin_config_.ivf_nprobes);
    
    // 通过 ConcurrencyManager 注册索引
    // 遵循索引创建规范：所有索引必须通过 ConcurrencyManager 注册才能获得正确的 storage 配置
    if (concurrency_manager_) {
        std::string prefix = "vsjoin_" + std::to_string(context.getSubtaskIndex());
        concurrency_manager_->register_index(prefix + "_left", left_vsjoin_index_);
        concurrency_manager_->register_index(prefix + "_right", right_vsjoin_index_);
    }
    
    // 4. 初始化分区协调器
    vsjoin_coordinator_ = std::make_unique<PartitionCoordinator>(
        static_cast<size_t>(vsjoin_config_.num_partitions),
        vsjoin_partitioner_,
        vsjoin_config_.allowed_lateness,
        vsjoin_config_.watermark_delay);
    
    // 5. 初始化异步候选生成器
    left_async_generator_ = std::make_unique<AsyncCandidateGenerator>(
        left_vsjoin_index_,
        vsjoin_config_.async_generator_threads);
    
    right_async_generator_ = std::make_unique<AsyncCandidateGenerator>(
        right_vsjoin_index_,
        vsjoin_config_.async_generator_threads);
    
    // 6. 初始化距离验证器
    vsjoin_verifier_ = std::make_shared<DistanceVerifier>(
        join_similarity_threshold_,
        vsjoin_config_.distance_alpha);
    
    SAGEFLOW_LOG_INFO("JOIN", "VSJoin components initialized successfully");
}

std::unique_ptr<VectorRecord> JoinOperator::extractVectorRecord(const Response& record) {
    if (!record.record_) {
        return nullptr;
    }
    return std::make_unique<VectorRecord>(*record.record_);
}

void JoinOperator::applyVSJoin(Response&& record, int slot, Collector& collector,
                               const RuntimeContext& context) {
    // 统计 apply 处理总耗时
    metrics_increment(JoinMetrics::instance().apply_processing_count);
    MetricsTimer t_apply(JoinMetrics::instance().apply_processing_ns);
    
    auto vec_record = extractVectorRecord(record);
    if (!vec_record) return;
    
    size_t subtask_index = context.getSubtaskIndex();
    int64_t now_time_stamp = vec_record->timestamp_;
    uint64_t uid = vec_record->uid_;
    
    SAGEFLOW_LOG_DEBUG("JOIN_VSJOIN", "VSJoin apply: slot={} uid={} ts={} subtask={}/{}", 
                      slot, uid, now_time_stamp, 
                      subtask_index, context.getParallelism());
    
    // 1. 处理延迟到达
    auto process_result = vsjoin_coordinator_->processRecord(*vec_record);
    
    if (process_result.status == ArrivalStatus::TOO_LATE) {
        SAGEFLOW_LOG_DEBUG("JOIN_VSJOIN", "Dropping too late record uid={}", uid);
        return;
    }
    
    if (process_result.status == ArrivalStatus::LATE) {
        // 延迟记录缓冲处理 - 创建副本
        vsjoin_coordinator_->bufferLateRecord(std::make_unique<VectorRecord>(*vec_record));
        SAGEFLOW_LOG_DEBUG("JOIN_VSJOIN", "Buffered late record uid={}", uid);
    }
    
    // 2. 确定当前记录属于哪一侧
    PartitionedVectorState* current_state = (slot == left_slot_id_) 
        ? left_vsjoin_state_.get() : right_vsjoin_state_.get();
    PartitionedIndex* current_index = (slot == left_slot_id_) 
        ? left_vsjoin_index_.get() : right_vsjoin_index_.get();
    
    // 保存一份用于 join 的记录副本
    auto record_for_join = std::make_unique<VectorRecord>(*vec_record);
    
    // 3. 更新状态和索引
    {
        MetricsTimer t_window_ins(JoinMetrics::instance().window_insert_ns);
        current_state->addRecord(std::move(vec_record), subtask_index);
    }
    
    // 插入到分区索引
    // PartitionedIndex::insert(uid) 需要从 storage_manager_ 获取记录
    // 索引通过 ConcurrencyManager::register_index() 已获得 storage_manager_ 引用
    {
        MetricsTimer t_idx(JoinMetrics::instance().index_insert_ns);
        // 先存储记录到 StorageManager（通过索引的 storage_manager_ 引用）
        if (current_index->storage_manager_) {
            current_index->storage_manager_->insert(
                std::make_unique<VectorRecord>(*record_for_join));
        }
        // 然后插入索引
        current_index->insert(record_for_join->uid_);
    }
    
    // 4. 更新分区协调器的记录计数
    vsjoin_coordinator_->updatePartitionCount(process_result.partition_id, 1);
    
    // 5. 窗口过期清理
    {
        MetricsTimer t_window_evict(JoinMetrics::instance().window_insert_ns);
        int64_t window_size = join_func_->getWindowSize();
        current_state->evictExpired(now_time_stamp, window_size, subtask_index);
    }
    
    // 6. 检查是否需要触发 join
    auto& window = (slot == left_slot_id_) ? join_func_->threadSafeWindowL : join_func_->threadSafeWindowR;
    bool trigger_flag = window.isNeedTrigger(now_time_stamp);
    
    if (!trigger_flag) {
        return;
    }
    
    // 7. 执行 join（根据 eager/lazy 模式）
    if (is_eager_) {
        executeVSJoinEager(*record_for_join, slot, collector, context);
    } else {
        executeVSJoinLazy(slot, collector, context);
    }
}

void JoinOperator::executeVSJoinEager(const VectorRecord& query, int slot,
                                       Collector& collector, const RuntimeContext& context) {
    MetricsTimer t_similarity(JoinMetrics::instance().similarity_ns);
    const uint64_t apply_enter_ns = metrics_timestamp();
    
    // 确定查询的目标侧
    PartitionedVectorState* target_state = (slot == left_slot_id_) 
        ? right_vsjoin_state_.get() : left_vsjoin_state_.get();
    
    size_t subtask_index = context.getSubtaskIndex();
    
    // 获取候选分区
    auto candidate_partitions = vsjoin_coordinator_->routeQuery(query, vsjoin_config_.num_probes);
    
    // 从目标状态中获取相关记录用于 join
    auto candidate_records = target_state->getRecordsForQuery(query, vsjoin_config_.num_probes);
    
    SAGEFLOW_LOG_DEBUG("JOIN_VSJOIN", "Eager: query_uid={} candidate_count={}", 
                      query.uid_, candidate_records.size());
    
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> local_return_pool;
    
    // 验证候选并执行 join
    for (const VectorRecord* cand_ptr : candidate_records) {
        if (!cand_ptr) continue;
        
        // 使用距离验证器验证
        auto result = vsjoin_verifier_->verify(query, *cand_ptr);
        if (result.passed) {
            // 生成 join 结果
            std::unique_ptr<VectorRecord> left_copy;
            std::unique_ptr<VectorRecord> right_copy;
            
            if (slot == left_slot_id_) {
                left_copy = std::make_unique<VectorRecord>(query);
                right_copy = std::make_unique<VectorRecord>(*cand_ptr);
            } else {
                left_copy = std::make_unique<VectorRecord>(*cand_ptr);
                right_copy = std::make_unique<VectorRecord>(query);
            }
            
            Response lhs{ResponseType::Record, std::move(left_copy)};
            Response rhs{ResponseType::Record, std::move(right_copy)};
            
            try {
                MetricsTimer t_joinF(JoinMetrics::instance().join_function_ns);
                auto res = join_func_->Execute(lhs, rhs);
                if (res.record_) {
                    local_return_pool.emplace_back(left_slot_id_, std::move(res.record_));
                }
            } catch (const std::exception& e) {
                SAGEFLOW_LOG_ERROR("JOIN_VSJOIN", "Exception in executeVSJoinEager: what={}", e.what());
                throw;
            }
        }
    }
    
    // 发送 Join 结果
    {
        MetricsTimer t_emit(JoinMetrics::instance().emit_ns);
        for (auto& p : local_return_pool) {
            Response out{ResponseType::Record, std::move(p.second)};
            collector.collect(std::make_unique<Response>(std::move(out)), p.first);
            metrics_increment(JoinMetrics::instance().total_emits);
            metrics_record_e2e_latency(apply_enter_ns);
        }
    }
    
    SAGEFLOW_LOG_DEBUG("JOIN_VSJOIN", "Eager completed: results={}", local_return_pool.size());
}

void JoinOperator::executeVSJoinLazy(int slot, Collector& collector,
                                      const RuntimeContext& context) {
    MetricsTimer t_similarity(JoinMetrics::instance().similarity_ns);
    const uint64_t apply_enter_ns = metrics_timestamp();
    
    size_t subtask_index = context.getSubtaskIndex();
    
    // 获取当前侧和目标侧的状态
    PartitionedVectorState* current_state = (slot == left_slot_id_) 
        ? left_vsjoin_state_.get() : right_vsjoin_state_.get();
    PartitionedVectorState* target_state = (slot == left_slot_id_) 
        ? right_vsjoin_state_.get() : left_vsjoin_state_.get();
    
    // 获取当前侧所有记录
    const auto& current_records = current_state->getRecords(subtask_index);
    
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> local_return_pool;
    
    // 对当前侧每条记录进行 join
    for (const auto& query_ptr : current_records) {
        if (!query_ptr) continue;
        
        // 从目标状态中获取相关记录
        auto candidate_records = target_state->getRecordsForQuery(*query_ptr, vsjoin_config_.num_probes);
        
        for (const VectorRecord* cand_ptr : candidate_records) {
            if (!cand_ptr) continue;
            
            // 使用距离验证器验证
            auto result = vsjoin_verifier_->verify(*query_ptr, *cand_ptr);
            if (result.passed) {
                std::unique_ptr<VectorRecord> left_copy;
                std::unique_ptr<VectorRecord> right_copy;
                
                if (slot == left_slot_id_) {
                    left_copy = std::make_unique<VectorRecord>(*query_ptr);
                    right_copy = std::make_unique<VectorRecord>(*cand_ptr);
                } else {
                    left_copy = std::make_unique<VectorRecord>(*cand_ptr);
                    right_copy = std::make_unique<VectorRecord>(*query_ptr);
                }
                
                Response lhs{ResponseType::Record, std::move(left_copy)};
                Response rhs{ResponseType::Record, std::move(right_copy)};
                
                try {
                    MetricsTimer t_joinF(JoinMetrics::instance().join_function_ns);
                    auto res = join_func_->Execute(lhs, rhs);
                    if (res.record_) {
                        local_return_pool.emplace_back(left_slot_id_, std::move(res.record_));
                    }
                } catch (const std::exception& e) {
                    SAGEFLOW_LOG_ERROR("JOIN_VSJOIN", "Exception in executeVSJoinLazy: what={}", e.what());
                    throw;
                }
            }
        }
    }
    
    // 发送 Join 结果
    {
        MetricsTimer t_emit(JoinMetrics::instance().emit_ns);
        for (auto& p : local_return_pool) {
            Response out{ResponseType::Record, std::move(p.second)};
            collector.collect(std::make_unique<Response>(std::move(out)), p.first);
            metrics_increment(JoinMetrics::instance().total_emits);
            metrics_record_e2e_latency(apply_enter_ns);
        }
    }
    
    SAGEFLOW_LOG_DEBUG("JOIN_VSJOIN", "Lazy completed: results={}", local_return_pool.size());
}

void JoinOperator::closeVSJoinComponents() {
    // 关闭异步候选生成器
    if (left_async_generator_) {
        left_async_generator_->shutdown();
        SAGEFLOW_LOG_DEBUG("JOIN", "Left async generator shutdown");
    }
    if (right_async_generator_) {
        right_async_generator_->shutdown();
        SAGEFLOW_LOG_DEBUG("JOIN", "Right async generator shutdown");
    }
    
    // 清理其他 VSJoin 组件（智能指针会自动析构）
    if (vsjoin_config_.enabled) {
        SAGEFLOW_LOG_INFO("JOIN", "VSJoin components closed");
    }
}

} // namespace sageFlow

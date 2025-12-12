/*
 * JoinOperator 实现
 * 所有方法均使用 Eager 模式：每条记录到达时立即执行查询
 */
#include "operator/join_operator.h"
#include "operator/join_operator_methods/join_methods.h"
#include "operator/join_operator_methods/bruteforce_baseline.h"
#include "operator/join_operator_methods/ivf_method.h"
#include "operator/join_metrics.h"
#include "operator/join_strategy_factory.h"
#include "operator/join_config_validator.h"
#include "execution/partitioner_factory.h"
#include "utils/monitoring.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <algorithm>
#include <cmath>
#include <limits>
#include <thread>
#include <set>
#include <unordered_set>

#include "utils/logger.h"

#include "spdlog/fmt/bundled/chrono.h"

namespace sageFlow {

// 调试统计变量 - 文件作用域，支持跨函数访问
static std::atomic<uint64_t> g_total_candidates{0};
static std::atomic<uint64_t> g_total_queries{0};
static std::atomic<uint64_t> g_total_filtered_by_ts{0};

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

    // 所有方法均使用 Eager 模式：每条记录到达时立即执行查询
    // 移除了 lazy 支持，历史原因保留对 "_eager" 后缀的兼容
    is_eager_ = true;

    // 提取算法前缀（兼容旧的 "_eager"/"_lazy" 后缀格式）
    std::string algo = join_method_name;
    // 移除可能存在的后缀
    if (algo.rfind("_eager") != std::string::npos) {
        algo = algo.substr(0, algo.rfind("_eager"));
    } else if (algo.rfind("_lazy") != std::string::npos) {
        algo = algo.substr(0, algo.rfind("_lazy"));
    }

    if (algo == "ivf") {
        index_kind_ = InternalIndexKind::IVF;
        // IVF 使用共享状态，需要 SharedWindowState 以确保所有并行实例看到完整窗口
        use_shared_state_ = true;
        
        // 计算 IVF 参数
        // 注意：IVF 是为批处理设计的近似索引，在流式场景下有以下限制：
        // 1. 聚类质心在数据量少时不准确
        // 2. 新插入的向量可能被分配到错误的簇
        // 3. 召回率依赖于 nprobes/nlist 比例
        int64_t window_size = join_func_->getWindowSize();
        int64_t step_size = join_func_->getStepSize();
        int64_t vector_count = (step_size > 0) ? (window_size / step_size) : window_size;
        int nlist = std::max(1, static_cast<int>(4.0 * std::sqrt(static_cast<double>(vector_count))));
        // nprobes 设为 80%，牺牲性能换取高召回率
        // 流式 Join 需要高召回率（>90%），所以使用高 nprobes/nlist 比例
        int nprobes = std::max(3, nlist * 30 / 100);
        
        // IVF 需要使用索引加速，创建索引对
        IVFParameters ivf_params{
            .nlist = nlist,
            .rebuild_threshold = 2.0,
            .nprobes = nprobes
        };
        
        if (createIndexPair(IndexType::IVF, "join_ivf", ivf_params)) {
            use_index_ = true;
            
            // 创建 IVFMethod 配置
            IVFMethod::Config ivf_config;
            ivf_config.similarity_threshold = join_similarity_threshold_;
            ivf_config.use_existing_index = true;  // 使用 ConcurrencyManager 中的索引
            ivf_config.nlist = nlist;
            ivf_config.nprobes = nprobes;
            
            join_method_ = std::make_unique<IVFMethod>(ivf_config);
            SAGEFLOW_LOG_INFO("JOIN", "IVF mode enabled with index, nlist={} nprobes={}",
                             nlist, nprobes);
        } else {
            // 索引创建失败，降级到 BruteForce
            use_index_ = false;
            join_method_ = std::make_unique<BruteForceBaseline>(join_similarity_threshold_);
            SAGEFLOW_LOG_WARN("JOIN", "Failed to create IVF index pair, falling back to BruteForce");
        }
                         
    } else if (algo == "bruteforce" || algo == "bf" ) {
        index_kind_ = InternalIndexKind::BRUTEFORCE;
        // BruteForce 使用共享状态
        use_shared_state_ = true;
        use_index_ = false;  // 新架构：使用 WindowState 而非外部索引
        
        // 创建 BruteForceBaseline - 直接从 WindowState 查找
        join_method_ = std::make_unique<BruteForceBaseline>(join_similarity_threshold_);
        SAGEFLOW_LOG_INFO("JOIN", "BruteForce mode enabled (WindowState-based)");
        
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
        join_method_ = std::make_unique<BruteForceJoinMethod>(
          -1, -1, join_similarity_threshold_, concurrency_manager_);
    }
}

// ============================================================
// E-01: 使用策略配置的新构造函数
// ============================================================

JoinOperator::JoinOperator(std::unique_ptr<Function> &join_func,
                           const std::shared_ptr<ConcurrencyManager> &concurrency_manager,
                           const JoinStrategyConfig& config,
                           bool enable_profiling,
                           const std::string& profile_output_path)
    : Operator(OperatorType::JOIN),
      concurrency_manager_(concurrency_manager),
      join_similarity_threshold_(config.similarity_threshold),
      enable_profiling_(enable_profiling),
      strategy_config_(config),
      use_strategy_config_(true) {

    join_func_ = std::unique_ptr<JoinFunction>(dynamic_cast<JoinFunction*>(join_func.release()));
    if (!join_func_) {
        throw std::runtime_error("JoinOperator: join_func is not a JoinFunction");
    }
    if (!concurrency_manager_) {
        throw std::runtime_error("JoinOperator: concurrency_manager is a nullptr");
    }

    // 配置 join_func_ 的窗口参数（如果 config 中有指定）
    if (config.window_size_ms > 0) {
        join_func_->setWindow(config.window_size_ms, config.step_size_ms);
    }

    // Initialize GPERFTOOLS profiler if enabled
    if (enable_profiling_) {
        std::string profile_path = profile_output_path.empty()
            ? "profiles/join_operator_profile.prof"
            : profile_output_path;
        profiler_ = std::make_unique<PerformanceMonitor>(profile_path);
        SAGEFLOW_LOG_INFO("JOIN", "GPERFTOOLS profiling enabled (strategy config), output={}", profile_path);
    }

    // 策略配置模式：组件初始化延迟到 open() 中执行
    // 因为需要 RuntimeContext 获取 parallelism
    is_eager_ = true;  // 所有方法使用 Eager 模式
    index_kind_ = InternalIndexKind::NONE;
    use_index_ = false;

    SAGEFLOW_LOG_INFO("JOIN", "JoinOperator created with strategy config: algorithm={} partition={} window_state={}",
                     toString(config.algorithm),
                     toString(config.partition_strategy),
                     toString(config.window_state_type));
}

JoinOperator::~JoinOperator() {
    // 输出候选统计信息（调试用）
    static std::atomic<int> destructor_count{0};
    if (destructor_count.fetch_add(1) == 0) {
        SAGEFLOW_LOG_INFO("JOIN_DEBUG", "Candidate stats: total_queries={} total_candidates={} avg_per_query={:.2f} filtered_by_ts={}",
            g_total_queries.load(), g_total_candidates.load(),
            g_total_queries.load() > 0 ? static_cast<double>(g_total_candidates.load()) / g_total_queries.load() : 0.0,
            g_total_filtered_by_ts.load());
    }
    
    // 关闭 VSJoin 组件
    closeVSJoinComponents();
    
    // Stop profiling if it was enabled
    if (profiler_) {
        profiler_->StopProfiling();
        SAGEFLOW_LOG_INFO("JOIN", "GPERFTOOLS profiling stopped");
    }
}

void JoinOperator::open() {
  // Legacy open - 委托给带 RuntimeContext 的版本
  // 使用默认的 RuntimeContext (subtask_index=0, parallelism=parallelism_)
  RuntimeContext default_context(0, parallelism_);
  open(default_context);
}

void JoinOperator::open(const RuntimeContext& context) {
  // 使用 std::call_once 确保初始化只执行一次，即使在多线程环境下
  std::call_once(init_flag_, [this, &context]() {
    is_open_ = true;
    parallelism_ = context.getParallelism();
    
    // Start profiling when operator opens
    if (profiler_) {
        profiler_->StartProfiling();
        SAGEFLOW_LOG_INFO("JOIN", "GPERFTOOLS profiling started");
    }
    
    // E-01: 如果使用策略配置模式，通过 JoinStrategyFactory 初始化组件
    if (use_strategy_config_) {
        initializeWithStrategyConfig(context);
        return;
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
  
  left_state_->setEvictionBufferMultiplier(1.5);
  right_state_->setEvictionBufferMultiplier(1.5);
  
  // 初始化新架构的 Join 方法（传入 WindowState）
  if (join_method_) {
      // 尝试作为 BruteForceBaseline 初始化 - 不使用索引，直接从 WindowState 查找
      if (auto* bf = dynamic_cast<BruteForceBaseline*>(join_method_.get())) {
          bf->open(context, left_state_.get(), right_state_.get());
          SAGEFLOW_LOG_INFO("JOIN", "BruteForceBaseline method initialized with WindowState");
      }
      // 尝试作为 IVFMethod 初始化 - 使用 ConcurrencyManager 中的索引
      else if (auto* ivf = dynamic_cast<IVFMethod*>(join_method_.get())) {
          // 传递索引 ID 和 ConcurrencyManager
          ivf->setIndexIds(left_index_id_, right_index_id_);
          ivf->open(context, left_state_.get(), right_state_.get(), concurrency_manager_.get());
          SAGEFLOW_LOG_INFO("JOIN", "IVFMethod initialized with ConcurrencyManager index, left_idx={} right_idx={}",
                           left_index_id_, right_index_id_);
      }
  }
  
  SAGEFLOW_LOG_INFO("JOIN", "JoinOperator opened: subtask={}/{}, shared_state={}", 
                   context.getSubtaskIndex(), context.getParallelism(), 
                   use_shared_state_);
  }); // end of std::call_once
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
        // 过期阶段的容器维护开销：将每次 pop_front 计入 expire_ns，索引删除计入 index_insert_ns。
        while (!records.empty() && records.front()->timestamp_ <= timelimit) {
            uint64_t expired_uid = records.front()->uid_;
            {
                MetricsTimer t_window_expire_unit(JoinMetrics::instance().expire_ns);
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
    // 所有方法均使用 Eager 模式：直接通过索引查询候选项
    return join_method_->ExecuteEager(*data_ptr, slot);
}

auto JoinOperator::getCandidatesWithLocksHeld(
    const std::unique_ptr<VectorRecord>& data_ptr, int slot) -> std::vector<std::unique_ptr<VectorRecord>> {
    // This version assumes both window locks are already held by caller
    MetricsTimer t_fetch(JoinMetrics::instance().candidate_fetch_ns);
    // 所有方法均使用 Eager 模式：直接通过索引查询候选项
    return join_method_->ExecuteEager(*data_ptr, slot);
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
    
    SAGEFLOW_LOG_DEBUG("JOIN_APPLY", "slot={} cand={} left_win={} right_win={} use_index={} ",
           slot, candidates.size(), left_sz, right_sz, (use_index_?1:0));

    // 所有方法均使用 Eager 模式：每条记录到达时立即执行查询
    // Execute join while holding both locks to ensure consistency
    executeJoinForCandidatesWithLockHeld(candidates, data_ptr, slot, 
                                          slot == left_slot_id_ ? right_records_ : left_records_,
                                          local_return_pool);
    
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
    
    // 所有方法均使用 Eager 模式：使用索引直接获取候选项
    // 注意：state 是对面的窗口状态（opposite_state），我们需要传递记录来源的 slot
    // 当 state == right_state_ 时，说明记录来自左流（slot=0），需要查询右索引
    // 当 state == left_state_ 时，说明记录来自右流（slot=1），需要查询左索引
    // ExecuteEager 内部使用 otherIndexId(slot) 来选择正确的索引
    int query_slot = (state == right_state_.get()) ? left_slot_id_ : right_slot_id_;
    return join_method_->ExecuteEager(*data_ptr, query_slot);
}

// 批量删除阈值：当过期记录数超过此值时，触发批量删除
constexpr size_t kBatchDeleteThreshold = 100;

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
    
    // 更新全局最大已见时间戳（原子操作，支持乱序）
    // 使用 compare_exchange 确保只更新为更大的值
    std::atomic<int64_t>& max_seen_ts = (slot == left_slot_id_) 
        ? max_seen_left_ts_ : max_seen_right_ts_;
    int64_t current_max = max_seen_ts.load(std::memory_order_relaxed);
    while (now_time_stamp > current_max && 
           !max_seen_ts.compare_exchange_weak(current_max, now_time_stamp,
                                               std::memory_order_release,
                                               std::memory_order_relaxed)) {
        // 重试直到成功或发现更大的值
    }
    
    // 安全 evict 策略：
    // - 多线程模式：使用双侧的 max_seen_ts 的最小值，确保乱序安全
    // - 单线程模式：直接使用当前侧的时间戳，因为没有并发问题
    // - 如果某一侧还没有记录（INT64_MIN），则使用另一侧的时间戳
    int64_t left_max = max_seen_left_ts_.load(std::memory_order_acquire);
    int64_t right_max = max_seen_right_ts_.load(std::memory_order_acquire);
    
    constexpr int64_t kMinTimestamp = std::numeric_limits<int64_t>::min();
    int64_t safe_evict_ts;
    
    // 处理初始状态：如果某侧还没有记录，使用另一侧的时间戳
    if (left_max == kMinTimestamp && right_max == kMinTimestamp) {
        // 两侧都没有记录，不需要 evict
        safe_evict_ts = kMinTimestamp;
    } else if (left_max == kMinTimestamp) {
        // 左侧没有记录，使用右侧时间戳
        safe_evict_ts = right_max;
    } else if (right_max == kMinTimestamp) {
        // 右侧没有记录，使用左侧时间戳
        safe_evict_ts = left_max;
    } else {
        // 两侧都有记录，使用最小值确保安全
        safe_evict_ts = std::min(left_max, right_max);
    }
    {
        MetricsTimer t_window_evict(JoinMetrics::instance().expire_ns);
        state->evictExpired(safe_evict_ts, join_func_->getWindowSize(), subtask_index);
    }
    
    // 检查是否需要批量删除 Index/Storage 中的过期记录
    // 只有当过期记录数超过阈值时才执行，避免频繁删除
    if (use_index_ && concurrency_manager_ && index_id_for_cc != -1) {
        size_t expired_count = state->getExpiredCount(subtask_index);
        if (expired_count >= kBatchDeleteThreshold) {
            // 获取并清空过期 UID buffer
            auto expired_uids = state->flushExpiredUids(subtask_index);
            
            // 批量从 Index/Storage 中删除
            for (uint64_t uid : expired_uids) {
                concurrency_manager_->erase(index_id_for_cc, uid);
            }
            
            SAGEFLOW_LOG_DEBUG("JOIN_STATE", 
                "Batch deleted {} expired records from index {}", 
                expired_uids.size(), index_id_for_cc);
        }
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
    
    // 获取候选项（已通过索引相似度过滤）- 内部有 candidate_fetch_ns 计时
    auto candidates = getCandidatesFromState(data_ptr, opposite_state, subtask_index);
    
    // 从这里开始计时 similarity_ns（不包括候选项获取时间）
    MetricsTimer t_similarity(JoinMetrics::instance().similarity_ns);
    
    // 调试：统计候选数量（使用全局变量）
    g_total_candidates.fetch_add(candidates.size(), std::memory_order_relaxed);
    g_total_queries.fetch_add(1, std::memory_order_relaxed);
    
    // 计算时间窗口边界
    // 候选项的时间戳必须在 [data_ptr.timestamp - window_size, data_ptr.timestamp + window_size] 范围内
    // 即 |data_ptr.timestamp - cand.timestamp| <= window_size
    int64_t window_size = join_func_->getWindowSize();
    int64_t window_lower_bound = data_ptr->timestamp_ - window_size;
    int64_t window_upper_bound = data_ptr->timestamp_ + window_size;
    
    for (const auto& cand : candidates) {
        // 使用时间戳直接判断候选项是否在窗口范围内
        // 这避免了与 evictExpired 的竞争条件
        if (cand->timestamp_ < window_lower_bound || cand->timestamp_ > window_upper_bound) {
            g_total_filtered_by_ts.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        
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
            // 暂停 similarity_ns 计时，避免与 join_function_ns 重复计算
            t_similarity.pause();
            MetricsTimer t_joinF(JoinMetrics::instance().join_function_ns);
            auto res = join_func_->Execute(lhs, rhs);
            t_joinF.stop();
            t_similarity.resume();
            if (res.record_) {
                // 调试：输出实际匹配的 uid 对
                static std::atomic<uint64_t> match_debug_count{0};
                uint64_t mdc = match_debug_count.fetch_add(1, std::memory_order_relaxed);
                if (mdc < 20) {
                    SAGEFLOW_LOG_INFO("JOIN_MATCH_DETAIL",
                        "Match: slot={} query_uid={} cand_uid={} result_uid={}",
                        slot, data_ptr->uid_, cand->uid_, res.record_->uid_);
                }
                local_return_pool.emplace_back(left_slot_id_, std::move(res.record_));
            }
        } catch (const std::exception& e) {
            SAGEFLOW_LOG_ERROR("JOIN_STATE", "Exception in executeJoinWithState: what={}", e.what());
            throw;
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
    
    // ====== 自适应并发策略 ======
    // 根据并行度选择不同的策略
    
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> local_return_pool;
    
    if (context.getParallelism() <= 1) {
        // ====== 单线程：IQ 策略（无锁，无需第二次查询） ======
        // 
        // 在单线程模式下，所有记录是串行处理的：
        // 1. 当 L1 查询时，对侧窗口包含所有已处理的右流记录
        // 2. Query2 查到的结果和 Query1 完全相同（因为中间没有其他记录被处理）
        // 3. 匹配的完整性由"先 Insert 后 Query"保证：
        //    - L1 的 Query 会搜索右窗口，找到所有已插入的 R*
        //    - R1 的 Query 会搜索左窗口，找到 L1（因为 L1 已经 Insert 了）
        // 
        // 因此单线程下只需 Insert -> Query，无需 QIQ
        
        // 阶段1：Insert 当前记录到对应窗口
        updateSideWithState(
            current_state, index_id, std::move(data_ptr), now_time_stamp, slot, subtask_index);
        
        // 阶段2：Query 对侧窗口查找匹配
        executeJoinWithState(data_for_join.get(), opposite_state, slot, 
                            subtask_index, local_return_pool);
    } else {
        // ====== 高并行度：全局读写锁 + QIQ 策略 ======
        // 结合读写锁和 QIQ，保证高召回率
        // 
        // 问题：即使用锁，如果只查询一次（Insert 之前），两个同时到达的记录
        // A 和 B 可能互相丢失：
        //   - A 的 Query 在 B 的 Insert 之前 → A 找不到 B
        //   - B 的 Query 在 A 的 Insert 之前 → B 找不到 A
        // 
        // 解决：Query1 -> Insert -> Query2，与低并行度一致
        
        std::unordered_set<uint64_t> matched_uids;
        
        // 阶段1：第一次 Query（读锁，可并行）
        {
            uint64_t before_lock = metrics_timestamp();
            std::shared_lock<std::shared_mutex> read_lock(join_rw_mutex_);
            metrics_record_lock_wait(before_lock);
            executeJoinWithState(data_for_join.get(), opposite_state, slot, 
                                subtask_index, local_return_pool);
        }
        
        for (const auto& p : local_return_pool) {
            if (p.second) {
                matched_uids.insert(p.second->uid_);
            }
        }
        
        // 阶段2：Insert（写锁，独占）
        {
            uint64_t before_lock = metrics_timestamp();
            std::unique_lock<std::shared_mutex> write_lock(join_rw_mutex_);
            metrics_record_lock_wait(before_lock);
            updateSideWithState(
                current_state, index_id, std::move(data_ptr), now_time_stamp, slot, subtask_index);
        }
        
        // 阶段3：第二次 Query（读锁，捕获同时插入的记录）
        {
            uint64_t before_lock = metrics_timestamp();
            std::shared_lock<std::shared_mutex> read_lock(join_rw_mutex_);
            metrics_record_lock_wait(before_lock);
            std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> second_query_results;
            executeJoinWithState(data_for_join.get(), opposite_state, slot, 
                                subtask_index, second_query_results);
            
            for (auto& p : second_query_results) {
                if (p.second && matched_uids.find(p.second->uid_) == matched_uids.end()) {
                    matched_uids.insert(p.second->uid_);
                    local_return_pool.push_back(std::move(p));
                }
            }
        }
    }
    
    // 调试：记录匹配数量
    static std::atomic<uint64_t> total_matches{0};
    static std::atomic<uint64_t> debug_count{0};
    total_matches.fetch_add(local_return_pool.size(), std::memory_order_relaxed);
    uint64_t dc = debug_count.fetch_add(1, std::memory_order_relaxed);
    if (dc % 500 == 0) {
        SAGEFLOW_LOG_INFO("JOIN_DEBUG_MATCH", 
            "subtask={}/{} slot={} uid={} matches_this={} total_matches={}",
            subtask_index, context.getParallelism(), slot, 
            data_for_join->uid_, local_return_pool.size(), total_matches.load());
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
        MetricsTimer t_window_evict(JoinMetrics::instance().expire_ns);
        int64_t window_size = join_func_->getWindowSize();
        current_state->evictExpired(now_time_stamp, window_size, subtask_index);
    }
    
    // 6. 检查是否需要触发 join
    auto& window = (slot == left_slot_id_) ? join_func_->threadSafeWindowL : join_func_->threadSafeWindowR;
    bool trigger_flag = window.isNeedTrigger(now_time_stamp);
    
    if (!trigger_flag) {
        return;
    }
    
    // 7. 执行 join（所有方法均使用 Eager 模式）
    executeVSJoinEager(*record_for_join, slot, collector, context);
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

// ============================================================
// E-01: 使用策略配置初始化
// ============================================================

void JoinOperator::initializeWithStrategyConfig(const RuntimeContext& context) {
    // 1. 验证配置
    JoinConfigValidator::throwIfInvalid(strategy_config_);

    SAGEFLOW_LOG_INFO("JOIN", "Initializing with strategy config: algorithm={} parallelism={}",
                     toString(strategy_config_.algorithm), context.getParallelism());

    // 2. 使用 JoinStrategyFactory 创建组件
    auto components = JoinStrategyFactory::create(
        strategy_config_, concurrency_manager_, context.getParallelism());

    // 3. 设置 JoinMethod
    join_method_ = std::move(components.join_method);

    // 4. 设置 WindowState
    left_state_ = std::move(components.left_state);
    right_state_ = std::move(components.right_state);

    // 5. 设置索引 ID
    left_index_id_ = components.left_index_id;
    right_index_id_ = components.right_index_id;

    // 6. 根据窗口状态类型设置标志
    use_shared_state_ = (strategy_config_.window_state_type == WindowStateType::SHARED);

    // 7. 设置 eviction buffer multiplier
    if (left_state_) {
        left_state_->setEvictionBufferMultiplier(1.5);
    }
    if (right_state_) {
        right_state_->setEvictionBufferMultiplier(1.5);
    }

    // 8. 初始化 JoinMethod 与 WindowState 的关联
    if (join_method_) {
        // 尝试作为 BruteForceBaseline 初始化
        if (auto* bf = dynamic_cast<BruteForceBaseline*>(join_method_.get())) {
            bf->open(context, left_state_.get(), right_state_.get());
            SAGEFLOW_LOG_INFO("JOIN", "BruteForceBaseline method initialized via strategy config");
        }
        // 尝试作为 IVFMethod 初始化
        else if (auto* ivf = dynamic_cast<IVFMethod*>(join_method_.get())) {
            ivf->setIndexIds(left_index_id_, right_index_id_);
            ivf->open(context, left_state_.get(), right_state_.get(), concurrency_manager_.get());
            use_index_ = true;
            SAGEFLOW_LOG_INFO("JOIN", "IVFMethod initialized via strategy config, left_idx={} right_idx={}",
                             left_index_id_, right_index_id_);
        }
        // 尝试作为 HNSWJoinMethod 初始化
        else if (auto* hnsw = dynamic_cast<HNSWJoinMethod*>(join_method_.get())) {
            use_index_ = true;
            SAGEFLOW_LOG_INFO("JOIN", "HNSWJoinMethod initialized via strategy config");
        }
    }

    // 9. 处理 VSJoin 特殊组件
    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        vsjoin_config_.enabled = true;
        vsjoin_config_.num_partitions = strategy_config_.num_partitions;
        // TODO: 如果有其他 VSJoin 组件，在此处设置
        // Issue URL: https://github.com/intellistream/sageFlow/issues/85
        SAGEFLOW_LOG_INFO("JOIN", "VSJoin mode enabled via strategy config");
    }

    SAGEFLOW_LOG_INFO("JOIN", "JoinOperator initialized with strategy config: subtask={}/{} shared_state={}",
                     context.getSubtaskIndex(), context.getParallelism(), use_shared_state_);
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

std::unique_ptr<IPartitioner> JoinOperator::getPreferredPartitioner(
    int dimension, int num_partitions) const {
    // 根据 Join 配置返回适当的分区器
    
    if (vsjoin_config_.enabled) {
        // VSJoin 模式：使用 LSH 分区器
        // 如果没有指定维度/分区数，使用 VSJoin 配置的值
        int actual_dim = (dimension > 0) ? dimension : 128;  // 默认维度
        int actual_partitions = (num_partitions > 0) ? num_partitions : vsjoin_config_.num_partitions;
        
        return std::make_unique<LSHIPartitioner>(
            actual_dim,
            8,  // num_hash_functions
            actual_partitions,
            42,  // seed
            0.1  // boundary_threshold
        );
    }
    
    // 共享索引 Join（bruteforce/ivf）：使用 RoundRobin 实现负载均衡
    // 返回 nullptr 让 ConnectionStrategy 使用默认的 RoundRobin
    return nullptr;
}

} // namespace sageFlow

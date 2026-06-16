/*
 * JoinOperator 实现
 * 所有方法均使用 Eager 模式：每条记录到达时立即执行查询
 */
#include "operator/join_operator.h"
#include "operator/join_operator_methods/join_methods.h"
#include "operator/join_operator_methods/bruteforce_baseline.h"
#include "operator/join_operator_methods/ivf_method.h"
#include "operator/join_operator_methods/hdr_tree_method.h"
#include "operator/join_operator_methods/clustered_join_method.h"
#include "operator/join_operator_methods/lsh_method.h"
#include "operator/join_metrics.h"
#include "operator/utils/join_strategy_factory.h"
#include "operator/utils/join_config_validator.h"
#include "operator/join_operator_methods/vsjoin_method.h"
#include "execution/partitioner_factory.h"
#include "execution/centroid_partitioner.h"
#include "utils/monitoring.h"
#include "operator/join_operator_methods/vsjoin_components/partition_assignment.h"
#include "operator/join_operator_methods/vsjoin_components/load_monitor.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <algorithm>
#include <cmath>
#include <limits>
#include <thread>
#include <set>
#include <chrono>
#include <cstdlib>
#include <cctype>
#include <unordered_set>

#include "utils/logger.h"

#include <mutex>

#include "spdlog/fmt/bundled/chrono.h"

namespace sageFlow {

void JoinOperator::startGlobalIndexRebuilder() {
    std::call_once(rebuild_thread_started_, [this]() {
        rebuild_running_.store(true, std::memory_order_release);
        rebuild_interval_ms_.store(strategy_config_.vsjoin_rebuild_interval_ms, std::memory_order_release);

        rebuild_thread_ = std::make_unique<std::thread>(&JoinOperator::globalIndexRebuildLoop, this);

        SAGEFLOW_LOG_INFO("VSJOIN_REBUILDER",
            "Background rebuild thread started (interval={}ms, parallelism={})",
            rebuild_interval_ms_.load(), parallelism_);
    });
}

void JoinOperator::stopGlobalIndexRebuilder() {
    if (rebuild_running_.exchange(false)) {
        if (rebuild_thread_ && rebuild_thread_->joinable()) {
            rebuild_thread_->join();
        }
        SAGEFLOW_LOG_INFO("VSJOIN_REBUILDER", "Background rebuild thread stopped");
    }
}

void JoinOperator::globalIndexRebuildLoop() {
    while (rebuild_running_.load(std::memory_order_acquire)) {
        const int64_t interval_ms = rebuild_interval_ms_.load(std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));

        if (!rebuild_running_.load(std::memory_order_acquire)) {
            break;
        }

        if (!left_state_ || !right_state_) {
            SAGEFLOW_LOG_WARN("VSJOIN_REBUILD", "WindowState not ready, skip rebuild");
            continue;
        }

        // 保持快照的所有权直到 rebuild 完成，避免悬空指针
        std::vector<std::vector<std::shared_ptr<const VectorRecord>>> left_snapshots;
        std::vector<std::vector<std::shared_ptr<const VectorRecord>>> right_snapshots;
        left_snapshots.reserve(parallelism_);
        right_snapshots.reserve(parallelism_);

        std::unordered_set<uint64_t> seen_left_uids;
        std::unordered_set<uint64_t> seen_right_uids;
        std::vector<const VectorRecord*> unique_left_records;
        std::vector<const VectorRecord*> unique_right_records;

        for (size_t p = 0; p < parallelism_; ++p) {
            left_snapshots.push_back(left_state_->getRecordsSnapshot(p));
            right_snapshots.push_back(right_state_->getRecordsSnapshot(p));

            for (const auto& r : left_snapshots.back()) {
                if (r && seen_left_uids.insert(r->uid_).second) {
                    unique_left_records.push_back(r.get());
                }
            }
            for (const auto& r : right_snapshots.back()) {
                if (r && seen_right_uids.insert(r->uid_).second) {
                    unique_right_records.push_back(r.get());
                }
            }
        }

        const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        const int64_t window_lower = logicalWindowLowerBound(now_ms);

        std::vector<const VectorRecord*> valid_left_records;
        std::vector<const VectorRecord*> valid_right_records;
        valid_left_records.reserve(unique_left_records.size());
        valid_right_records.reserve(unique_right_records.size());

        for (const auto* r : unique_left_records) {
            if (r && r->timestamp_ >= window_lower) {
                valid_left_records.push_back(r);
            }
        }
        for (const auto* r : unique_right_records) {
            if (r && r->timestamp_ >= window_lower) {
                valid_right_records.push_back(r);
            }
        }

        // ====== 3. 构建新的 Global Index（离线）并原子切换 ======
        if (concurrency_manager_ && vsjoin_global_left_id_ >= 0 && vsjoin_global_right_id_ >= 0) {
            IVFParameters global_ivf_params;
            global_ivf_params.nlist = strategy_config_.ivf_nlist;
            global_ivf_params.nprobes = strategy_config_.ivf_nprobes;
            global_ivf_params.rebuild_threshold = strategy_config_.ivf_rebuild_threshold;

            const int new_left_id = concurrency_manager_->build_index_from_records(
                "vsjoin_global_left_rebuilt",
                IndexType::IVF,
                join_func_ ? join_func_->getDim() : strategy_config_.dimension,
                global_ivf_params,
                valid_left_records);

            const int new_right_id = concurrency_manager_->build_index_from_records(
                "vsjoin_global_right_rebuilt",
                IndexType::IVF,
                join_func_ ? join_func_->getDim() : strategy_config_.dimension,
                global_ivf_params,
                valid_right_records);

            bool left_swapped = false;
            bool right_swapped = false;
            if (new_left_id >= 0) {
                left_swapped = concurrency_manager_->replace_index_by_id(vsjoin_global_left_id_, new_left_id);
            }
            if (new_right_id >= 0) {
                right_swapped = concurrency_manager_->replace_index_by_id(vsjoin_global_right_id_, new_right_id);
            }

            SAGEFLOW_LOG_INFO(
                "VSJOIN_REBUILD",
                "Global index rebuilt: {} unique left ({} valid), {} unique right ({} valid), swapped(L={}, R={})",
                unique_left_records.size(), valid_left_records.size(),
                unique_right_records.size(), valid_right_records.size(),
                left_swapped ? 1 : 0,
                right_swapped ? 1 : 0);
        } else {
            SAGEFLOW_LOG_INFO(
                "VSJOIN_REBUILD",
                "Global index rebuild tick: {} unique left ({} valid), {} unique right ({} valid) (skip swap: cm/global_id not ready)",
                unique_left_records.size(), valid_left_records.size(),
                unique_right_records.size(), valid_right_records.size());
        }
    }
}

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
    : Operator(OperatorType::JOIN), 
      concurrency_manager_(concurrency_manager),
      strategy_config_(),
      join_similarity_threshold_(join_similarity_threshold),
      enable_profiling_(enable_profiling) {
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
        SAGEFLOW_LOG_INFO("JOIN", "GPERFTOOLS profiling enabled (string method), output={}", profile_path);
    }

    // 统一初始化路径：将字符串方法名转换为 JoinStrategyConfig
    // 所有组件（join_method_, WindowState, 索引）将在 open() 中通过 initializeWithStrategyConfig() 统一初始化
    strategy_config_ = createJoinStrategyConfigFromMethodName(
        join_method_name_raw,
        join_similarity_threshold,
        join_func_->getDim(),
        join_func_->getWindowSize(),
        join_func_->getStepSize());
    
    use_strategy_config_ = true;  // 启用策略配置模式，统一在 open() 中初始化
    
    // 设置 is_eager_（所有方法使用 Eager 模式）
    is_eager_ = true;
    index_kind_ = InternalIndexKind::NONE;
    use_index_ = false;
    
    // 注意：use_shared_state_ 不再在构造函数中设置，而是由 strategy_config_.window_state_type 决定
    // 在 initializeWithStrategyConfig() 中会根据配置设置 use_shared_state_
    
    SAGEFLOW_LOG_INFO("JOIN", "JoinOperator created with string method '{}' -> config: algorithm={} partition={} window_state={}",
                     join_method_name_raw,
                     toString(strategy_config_.algorithm),
                     toString(strategy_config_.partition_strategy),
                     toString(strategy_config_.window_state_type));
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
    stopGlobalIndexRebuilder();

    static std::atomic<int> destructor_count{0};
    if (destructor_count.fetch_add(1) == 0) {
        // 输出 QIQ 三阶段统计
        auto& m = JoinMetrics::instance();
        uint64_t q1_count = m.qiq_q1_count.load();
        uint64_t insert_count = m.qiq_insert_count.load();
        uint64_t q2_count = m.qiq_q2_count.load();
        
        double q1_avg_us = q1_count > 0 ? static_cast<double>(m.qiq_q1_ns.load()) / q1_count / 1000.0 : 0.0;
        double insert_avg_us = insert_count > 0 ? static_cast<double>(m.qiq_insert_ns.load()) / insert_count / 1000.0 : 0.0;
        double q2_avg_us = q2_count > 0 ? static_cast<double>(m.qiq_q2_ns.load()) / q2_count / 1000.0 : 0.0;
        
        SAGEFLOW_LOG_INFO("JOIN_QIQ_STATS", 
            "QIQ per-vector avg time (including lock wait): Q1={:.2f}us ({} calls), Insert={:.2f}us ({} calls), Q2={:.2f}us ({} calls), Total={:.2f}us",
            q1_avg_us, q1_count, insert_avg_us, insert_count, q2_avg_us, q2_count,
            q1_avg_us + insert_avg_us + q2_avg_us);
    }
    
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
  validateRuntimeContext(context);
  // 使用 std::call_once 确保初始化只执行一次，即使在多线程环境下
  std::call_once(init_flag_, [this, &context]() {
    is_open_ = true;
    parallelism_ = context.getParallelism();
    
    // Start profiling when operator opens
    if (profiler_) {
        profiler_->StartProfiling();
        SAGEFLOW_LOG_INFO("JOIN", "GPERFTOOLS profiling started");
    }
    
    // 统一初始化路径：构造函数已将字符串方法名转换为 JoinStrategyConfig。
    initializeWithStrategyConfig(context);

    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        startGlobalIndexRebuilder();
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
        if (auto* lsh = dynamic_cast<LSHMethod*>(join_method_.get())) {
                if (data_ptr) {
                        lsh->onRecordAdded(*data_ptr, slot);
                }
        }
    // 窗口插入阶段（仅插入）
    {
        MetricsTimer t_window_ins(JoinMetrics::instance().window_insert_ns);
        records.emplace_back(std::move(data_ptr));
    }
    metrics_increment(JoinMetrics::instance().window_insert_count);

    if (use_index_ && concurrency_manager_ && data_for_index_insert && index_id_for_cc != -1) {
        MetricsTimer t_idx(JoinMetrics::instance().index_insert_ns);
        // lock.unlock();
        concurrency_manager_->insert(index_id_for_cc, std::move(data_for_index_insert));
        // lock.lock();
        metrics_increment(JoinMetrics::instance().index_op_count);
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
                metrics_increment(JoinMetrics::instance().index_op_count);
            }
        }
        SAGEFLOW_LOG_DEBUG("JOIN", "Expiration loop finished. current_size={} ", records.size());
    } catch (const std::exception& e) {
        SAGEFLOW_LOG_ERROR("JOIN", "Exception during expiration: what={} ", e.what());
    }
    if (expired_count > 0) {
        metrics_increment(JoinMetrics::instance().expire_count, expired_count);
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
    (void)input_data;
    (void)slot;
    // Join results require Collector emission; the supported execution path is apply().
    return std::nullopt;
}

auto JoinOperator::getCandidates(
    const std::unique_ptr<VectorRecord>& data_ptr, int slot) -> std::vector<RecordView> {
    MetricsTimer t_fetch(JoinMetrics::instance().candidate_fetch_ns);
    metrics_increment(JoinMetrics::instance().candidate_fetch_count);
    // 所有方法均使用 Eager 模式：直接通过索引查询候选项
    return join_method_->ExecuteEager(*data_ptr, slot);
}

auto JoinOperator::getCandidatesWithLocksHeld(
    const std::unique_ptr<VectorRecord>& data_ptr, int slot) -> std::vector<RecordView> {
    // This version assumes both window locks are already held by caller
    MetricsTimer t_fetch(JoinMetrics::instance().candidate_fetch_ns);
    metrics_increment(JoinMetrics::instance().candidate_fetch_count);
    // 所有方法均使用 Eager 模式：直接通过索引查询候选项
    return join_method_->ExecuteEager(*data_ptr, slot);
}

auto JoinOperator::validateCandidateInWindow(
    const RecordView& candidate,
    const std::deque<std::unique_ptr<VectorRecord>>& window_records,
    int64_t logical_lower_bound) -> bool {
    if (!candidate || candidate->timestamp_ < logical_lower_bound) {
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
    const std::vector<RecordView>& candidates,
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
    const std::vector<RecordView>& candidates,
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
        if (!cand || cand->timestamp_ < logical_lower_bound) {
            continue;
        }
        metrics_increment(JoinMetrics::instance().similarity_count);
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
                metrics_increment(JoinMetrics::instance().join_function_count);
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
    RuntimeContext default_context(0, parallelism_);
    apply(std::move(record), slot, collector, default_context);
}

// ================== WindowState 辅助方法 ==================

std::vector<RecordView> JoinOperator::getCandidatesFromState(
    const VectorRecord* data_ptr,
    WindowState* state,
    size_t subtask_index) {
    
    MetricsTimer t_fetch(JoinMetrics::instance().candidate_fetch_ns);
    metrics_increment(JoinMetrics::instance().candidate_fetch_count);
    
    // 所有方法均使用 Eager 模式：使用索引直接获取候选项
    // 注意：state 是对面的窗口状态（opposite_state），我们需要传递记录来源的 slot
    // 当 state == right_state_ 时，说明记录来自左流（slot=0），需要查询右索引
    // 当 state == left_state_ 时，说明记录来自右流（slot=1），需要查询左索引
    // ExecuteEager 内部使用 otherIndexId(slot) 来选择正确的索引
    int query_slot = (state == right_state_.get()) ? left_slot_id_ : right_slot_id_;
    // 传递 subtask_index 以确保 PartitionedWindowState 访问正确的分区
    return join_method_->ExecuteEager(*data_ptr, query_slot, subtask_index);
}

auto JoinOperator::updateSideWithState(
    WindowState* state,
    WindowState* opposite_state,
    int index_id_for_cc,
    RecordView data_ptr,
    int64_t now_time_stamp,
    int slot,
    size_t subtask_index) -> bool {
    if (!data_ptr) {
        return false;
    }
    
    // 统计插入的记录数
    if (slot == left_slot_id_) {
        JoinMetrics::instance().total_records_left.fetch_add(1, std::memory_order_relaxed);
    } else {
        JoinMetrics::instance().total_records_right.fetch_add(1, std::memory_order_relaxed);
    }

    if (auto* lsh = dynamic_cast<LSHMethod*>(join_method_.get())) {
        if (data_ptr) {
            lsh->onRecordAdded(*data_ptr, slot);
        }
    }
    
    // 添加记录到窗口状态
    {
        MetricsTimer t_window_ins(JoinMetrics::instance().window_insert_ns);
        state->addRecord(data_ptr, subtask_index);
    }
    metrics_increment(JoinMetrics::instance().window_insert_count);
    
    // 插入索引
    if (use_index_ && concurrency_manager_ && index_id_for_cc != -1) {
        MetricsTimer t_idx(JoinMetrics::instance().index_insert_ns);

        // VSJoin 特殊处理：只插入到本分区的 Local Index
        if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
            const int local_index_id = localIndexIdForSlotAndSubtask(slot, subtask_index);
            if (local_index_id >= 0) {
                concurrency_manager_->insert(local_index_id, data_ptr);
            }

            SAGEFLOW_LOG_DEBUG("VSJOIN", "subtask_{} inserted to local_id={}", subtask_index, local_index_id);
        } else {
            concurrency_manager_->insert(index_id_for_cc, data_ptr);
        }

        metrics_increment(JoinMetrics::instance().index_op_count);
    }
    
    state->updateMaxSeenTimestamp(now_time_stamp, subtask_index);
    const int64_t safe_evict_ts =
        state->getSafeEvictTimestamp(subtask_index, opposite_state);

    if (safe_evict_ts != std::numeric_limits<int64_t>::min()) {
        size_t before_size = state->size(subtask_index);
        MetricsTimer t_window_evict(JoinMetrics::instance().expire_ns);
        state->evictExpired(safe_evict_ts, join_func_->getWindowSize(), subtask_index);
        size_t after_size = state->size(subtask_index);
        if (before_size > after_size) {
            metrics_increment(JoinMetrics::instance().expire_count, before_size - after_size);
        }
    }
    
    // 检查是否需要批量删除 Index/Storage 中的过期记录
    // 只有当过期记录数超过阈值时才执行，避免频繁删除
    const int erase_index_id = (strategy_config_.algorithm == JoinAlgorithm::VSJOIN)
        ? localIndexIdForSlotAndSubtask(slot, subtask_index)
        : index_id_for_cc;
    if (use_index_ && concurrency_manager_ && erase_index_id != -1) {
        size_t expired_count = state->getExpiredCount(subtask_index);
        if (expired_count >= batch_delete_threshold_) {
            // 获取并清空过期 UID buffer
            auto expired_uids = state->flushExpiredUids(subtask_index);
            
            // 批量从 Index/Storage 中删除
            for (uint64_t uid : expired_uids) {
                concurrency_manager_->erase(erase_index_id, uid);
                metrics_increment(JoinMetrics::instance().index_op_count);
            }
            
            SAGEFLOW_LOG_DEBUG("JOIN_STATE", 
                "Batch deleted {} expired records from index {}", 
                expired_uids.size(), erase_index_id);
        }
    }
    
    // WindowState path uses per-record IQ as the only trigger model.
    // Legacy isNeedTrigger/SlidingWindow state is intentionally not consulted here.
    return true;
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
    
    // 计算时间窗口边界
    // 候选项的时间戳必须在 [data_ptr.timestamp - window_size, data_ptr.timestamp + window_size] 范围内
    // 即 |data_ptr.timestamp - cand.timestamp| <= window_size
    int64_t window_size = join_func_->getWindowSize();
    int64_t window_lower_bound = data_ptr->timestamp_ - window_size;
    int64_t window_upper_bound = data_ptr->timestamp_ + window_size;
    
    for (const auto& cand : candidates) {
        // 使用时间戳直接判断候选项是否在窗口范围内
        if (cand->timestamp_ < window_lower_bound || cand->timestamp_ > window_upper_bound) {
            continue;
        }
        metrics_increment(JoinMetrics::instance().similarity_count);
        
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
            metrics_increment(JoinMetrics::instance().join_function_count);
            auto res = join_func_->Execute(lhs, rhs);
            t_joinF.stop();
            t_similarity.resume();
            if (res.record_) {
                local_return_pool.emplace_back(left_slot_id_, std::move(res.record_));
            }
        } catch (const std::exception& e) {
            SAGEFLOW_LOG_ERROR("JOIN_STATE", "Exception in executeJoinWithState: what={}", e.what());
            throw;
        }
    }
}

// ================== VSJoin 路由：计算目标 subtask 集合 ==================
// 说明：从 apply() 抽离，逻辑与原内联实现逐字一致——通过 preferred partitioner
// （当前为 CentroidPartitioner，支持 multicast）得到物理分区并映射为 subtask，
// 去重并在为空时回退到当前 subtask。仅承担"计算目标"职责，不做插入/查询。
std::vector<size_t> JoinOperator::computeVSJoinTargetSubtasks(
    const Response& record,
    const RuntimeContext& context,
    size_t subtask_index) const {
    auto preferred_partitioner = getPreferredPartitioner(
        join_func_ ? join_func_->getDim() : strategy_config_.dimension,
        static_cast<int>(context.getParallelism()));

    const size_t P = static_cast<size_t>(context.getParallelism());
    std::vector<size_t> target_subtasks;

    // VSJoin debug routing stats (enabled by env SAGEFLOW_VSJOIN_DEBUG_ROUTING=1)
    const bool vsjoin_debug_routing = []() {
        if (const char* v = std::getenv("SAGEFLOW_VSJOIN_DEBUG_ROUTING")) {
            return std::string(v) == "1";
        }
        return false;
    }();
    static std::atomic<uint64_t> vsjoin_route_events{0};
    static std::atomic<uint64_t> vsjoin_route_total_targets{0};
    static std::atomic<uint64_t> vsjoin_route_multicast_events{0};
    static std::atomic<uint64_t> vsjoin_route_fallback_events{0};
    static std::mutex vsjoin_route_mu;
    static std::unordered_map<size_t, uint64_t> vsjoin_route_target_hist;

    if (preferred_partitioner && preferred_partitioner->supportsMulticast()) {
        auto physical_pids = preferred_partitioner->partitionMulti(record, P);
        for (size_t pid : physical_pids) {
            target_subtasks.push_back(pid % P);
        }
        if (physical_pids.size() > 1) {
            vsjoin_route_multicast_events.fetch_add(1, std::memory_order_relaxed);
        }
    } else if (preferred_partitioner) {
        target_subtasks.push_back(preferred_partitioner->partition(record, P) % P);
    } else {
        target_subtasks.push_back(subtask_index);
        vsjoin_route_fallback_events.fetch_add(1, std::memory_order_relaxed);
    }

    std::sort(target_subtasks.begin(), target_subtasks.end());
    target_subtasks.erase(std::unique(target_subtasks.begin(), target_subtasks.end()), target_subtasks.end());

    if (target_subtasks.empty()) {
        target_subtasks.push_back(subtask_index);
        vsjoin_route_fallback_events.fetch_add(1, std::memory_order_relaxed);
    }

    // record routing stats (sampled)
    vsjoin_route_events.fetch_add(1, std::memory_order_relaxed);
    vsjoin_route_total_targets.fetch_add(target_subtasks.size(), std::memory_order_relaxed);
    if (vsjoin_debug_routing) {
        {
            std::lock_guard<std::mutex> lk(vsjoin_route_mu);
            for (size_t t : target_subtasks) {
                vsjoin_route_target_hist[t] += 1;
            }
        }
        const uint64_t n = vsjoin_route_events.load(std::memory_order_relaxed);
        if (n == 1 || (n % 20000 == 0)) {
            // Print a compact snapshot periodically.
            uint64_t total_targets = vsjoin_route_total_targets.load(std::memory_order_relaxed);
            uint64_t mc = vsjoin_route_multicast_events.load(std::memory_order_relaxed);
            uint64_t fb = vsjoin_route_fallback_events.load(std::memory_order_relaxed);
            size_t nonzero = 0;
            uint64_t minc = std::numeric_limits<uint64_t>::max();
            uint64_t maxc = 0;
            {
                std::lock_guard<std::mutex> lk(vsjoin_route_mu);
                nonzero = vsjoin_route_target_hist.size();
                for (const auto& kv : vsjoin_route_target_hist) {
                    minc = std::min(minc, kv.second);
                    maxc = std::max(maxc, kv.second);
                }
            }
            if (minc == std::numeric_limits<uint64_t>::max()) {
                minc = 0;
            }
            double avg_targets = (n > 0) ? static_cast<double>(total_targets) / static_cast<double>(n) : 0.0;
            SAGEFLOW_LOG_INFO("VSJOIN_ROUTING",
                "p={} subtask={}/{} routed_records={} avg_targets={:.3f} multicast_events={} fallback_events={} active_targets={} min_per_target={} max_per_target={}",
                P, subtask_index, context.getParallelism(), n, avg_targets, mc, fb, nonzero, minc, maxc);
        }
    }

    return target_subtasks;
}

void JoinOperator::validateRuntimeContext(const RuntimeContext& context) const {
    const size_t parallelism = context.getParallelism();
    const size_t subtask_index = context.getSubtaskIndex();
    if (parallelism == 0) {
        throw std::runtime_error("JoinOperator: RuntimeContext parallelism must be greater than zero");
    }
    if (subtask_index >= parallelism) {
        throw std::runtime_error("JoinOperator: RuntimeContext subtask_index must be less than parallelism");
    }
    if ((left_state_ || right_state_) && parallelism != parallelism_) {
        throw std::runtime_error("JoinOperator: RuntimeContext parallelism differs from initialized operator parallelism");
    }
}

void JoinOperator::recordVSJoinSubtaskDebugStats(
    int slot,
    size_t subtask_index,
    const RuntimeContext& context) const {
    const bool vsjoin_debug_subtask = []() {
        if (const char* v = std::getenv("SAGEFLOW_VSJOIN_DEBUG_SUBTASK")) {
            return std::string(v) == "1";
        }
        return false;
    }();

    if (!vsjoin_debug_subtask || strategy_config_.algorithm != JoinAlgorithm::VSJOIN) {
        return;
    }

    struct VSJoinSubtaskStatsBucket {
        std::unordered_map<size_t, uint64_t> in_left;
        std::unordered_map<size_t, uint64_t> in_right;
        std::atomic<uint64_t> events{0};
    };
    static std::mutex vsjoin_subtask_mu;
    static std::unordered_map<size_t, VSJoinSubtaskStatsBucket> vsjoin_subtask_buckets;

    const size_t p_runtime = static_cast<size_t>(context.getParallelism());
    {
        std::lock_guard<std::mutex> lk(vsjoin_subtask_mu);
        auto& bucket = vsjoin_subtask_buckets[p_runtime];
        if (slot == left_slot_id_) {
            bucket.in_left[subtask_index] += 1;
        } else {
            bucket.in_right[subtask_index] += 1;
        }
    }

    uint64_t n = 0;
    {
        std::lock_guard<std::mutex> lk(vsjoin_subtask_mu);
        n = vsjoin_subtask_buckets[p_runtime].events.fetch_add(1, std::memory_order_relaxed) + 1;
    }

    if (n != 1 && (n % 50000 != 0)) {
        return;
    }

    size_t active = 0;
    uint64_t total_left = 0;
    uint64_t total_right = 0;
    uint64_t min_total = std::numeric_limits<uint64_t>::max();
    uint64_t max_total = 0;
    {
        std::lock_guard<std::mutex> lk(vsjoin_subtask_mu);
        auto it = vsjoin_subtask_buckets.find(p_runtime);
        if (it != vsjoin_subtask_buckets.end()) {
            auto& bucket = it->second;
            std::unordered_set<size_t> keys;
            keys.reserve(bucket.in_left.size() + bucket.in_right.size());
            for (const auto& kv : bucket.in_left) keys.insert(kv.first);
            for (const auto& kv : bucket.in_right) keys.insert(kv.first);
            active = keys.size();
            for (size_t key : keys) {
                uint64_t left = 0;
                uint64_t right = 0;
                auto left_it = bucket.in_left.find(key);
                if (left_it != bucket.in_left.end()) left = left_it->second;
                auto right_it = bucket.in_right.find(key);
                if (right_it != bucket.in_right.end()) right = right_it->second;
                const uint64_t total = left + right;
                total_left += left;
                total_right += right;
                min_total = std::min(min_total, total);
                max_total = std::max(max_total, total);
            }
        }
    }
    if (min_total == std::numeric_limits<uint64_t>::max()) min_total = 0;
    SAGEFLOW_LOG_INFO("VSJOIN_SUBTASK",
                      "p={} events={} active_subtasks={} total_in(L={},R={}) min_total_per_subtask={} max_total_per_subtask={}",
                      p_runtime, n, active, total_left, total_right, min_total, max_total);
}

int JoinOperator::indexIdForSlot(int slot) const {
    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        return (slot == left_slot_id_) ? vsjoin_global_left_id_ : vsjoin_global_right_id_;
    }
    return (slot == left_slot_id_) ? left_index_id_ : right_index_id_;
}

int JoinOperator::localIndexIdForSlotAndSubtask(int slot, size_t subtask_index) const {
    const auto& local_ids = (slot == left_slot_id_)
        ? vsjoin_local_left_ids_
        : vsjoin_local_right_ids_;
    return (subtask_index < local_ids.size()) ? local_ids[subtask_index] : -1;
}

void JoinOperator::emitJoinResults(
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>>& local_return_pool,
    Collector& collector,
    uint64_t apply_enter_ns) {
    MetricsTimer t_emit(JoinMetrics::instance().emit_ns);
    for (auto& p : local_return_pool) {
        Response out{ResponseType::Record, std::move(p.second)};
        collector.collect(std::make_unique<Response>(std::move(out)), p.first);
        metrics_increment(JoinMetrics::instance().total_emits);
        metrics_increment(JoinMetrics::instance().emit_count);
        metrics_record_e2e_latency(apply_enter_ns);
    }
}

// ================== 带 RuntimeContext 的 apply 方法 ==================

auto JoinOperator::apply(Response&& record, int slot, Collector& collector,
                        const RuntimeContext& context) -> void {
    validateRuntimeContext(context);

    // 统计 apply 处理总耗时
    metrics_increment(JoinMetrics::instance().apply_processing_count);
    MetricsTimer t_apply(JoinMetrics::instance().apply_processing_ns);
    const uint64_t apply_enter_ns = metrics_timestamp();

    if (!record.record_) return;

    size_t subtask_index = context.getSubtaskIndex();
    const VectorRecord* input_record = record.record_.get();
    int64_t now_time_stamp = input_record->timestamp_;

    recordVSJoinSubtaskDebugStats(slot, subtask_index, context);

    SAGEFLOW_LOG_DEBUG("JOIN_APPLY", "Apply (with context) called slot={} uid={} ts={} subtask={}/{}",
                      slot, input_record->uid_, now_time_stamp,
                      subtask_index, context.getParallelism());

    // 检查是否已创建状态对象
    if (!left_state_ || !right_state_) {
        SAGEFLOW_LOG_WARN("JOIN", "WindowState not initialized; opening operator before apply");
        open(context);
        if (!left_state_ || !right_state_) {
            throw std::runtime_error("JoinOperator: WindowState initialization failed before apply");
        }
    }

    std::vector<size_t> target_subtasks;
    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        // Routing must read the unique_ptr-backed record before apply takes shared ownership.
        target_subtasks = computeVSJoinTargetSubtasks(record, context, subtask_index);
    }

    RecordView data_view = std::move(record.record_);
    const VectorRecord* data_ptr = data_view.get();
    
    // 确定当前记录属于哪一侧
    WindowState* current_state = (slot == left_slot_id_) 
        ? left_state_.get() : right_state_.get();
    WindowState* opposite_state = (slot == left_slot_id_) 
        ? right_state_.get() : left_state_.get();
    
    const int index_id = indexIdForSlot(slot);
    
    // ====== 并发策略：统一 IQ（Insert -> Query），不使用算子级全局锁 ======
    //
    // 每条向量的处理顺序与重构前完全一致：先把"自己"插入所在侧窗口/索引，
    // 再到"对侧"窗口/索引查询匹配。这样保证每对相似记录至少被一侧发现一次
    // （A 先于 A.query 完成 insert；若 B.insert 晚于 A.query，则 B.query 必能看到 A）。
    //
    // 并发可见性由各组件自身的内部锁保证，无需算子层再叠加粗粒度全局锁：
    // - SharedWindowState 内部用 shared_mutex 保护窗口容器；
    // - ConcurrencyManager / Index / StorageManager 各自线程安全；
    // 因此共享索引在多线程下不再被单一 join_rw_mutex_ 串行化（这是 p>1 负优化根因）。
    //
    // 注意：VSJoin 走多播路由，可能把同一条记录写入多个 subtask 分区。
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> local_return_pool;

    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        // VSJoin：先计算目标 subtask 集合（支持 multicast），再对每个目标做 IQ。
        // 语义：subtask 只查询/写入自己的分区数据，不跨分区探测。
        for (size_t target_subtask : target_subtasks) {
            updateSideWithState(current_state, opposite_state, index_id, data_view,
                                now_time_stamp, slot, target_subtask);

            executeJoinWithState(data_ptr, opposite_state, slot, target_subtask, local_return_pool);
        }
    } else {
        // 非 VSJoin（含共享索引 bruteforce/ivf/hnsw 与分区 clustered_join）：
        // 阶段1：Insert 当前记录到对应窗口和索引
        updateSideWithState(
            current_state, opposite_state, index_id, data_view, now_time_stamp, slot, subtask_index);

        // 阶段2：Query 对侧窗口查找匹配
        executeJoinWithState(data_ptr, opposite_state, slot,
                            subtask_index, local_return_pool);
    }
    
    emitJoinResults(local_return_pool, collector, apply_enter_ns);
    
    SAGEFLOW_LOG_DEBUG("JOIN_APPLY", "Apply (with context) completed: slot={} results={} subtask={}/{}", 
                      slot, local_return_pool.size(), subtask_index, context.getParallelism());
}

// ============================================================
// E-01: 使用策略配置初始化
// ============================================================

void JoinOperator::initializeWithStrategyConfig(const RuntimeContext& context) {
    // 1. 验证配置
    JoinConfigValidator::throwIfInvalid(strategy_config_);

    SAGEFLOW_LOG_INFO("JOIN", "Initializing with strategy config: algorithm={} parallelism={}",
                     toString(strategy_config_.algorithm), context.getParallelism());

    // 1.1 运行时关键约束：ClusteredJoin 需要 num_partitions == parallelism
    // 说明：CentroidPartitioner 内部会将 partition_idx 映射到 channel（subtask）空间；
    // 若两者不一致，会出现逻辑分区折叠/多播语义失真，导致召回损失且难以诊断。
    //
    // 这里做“运行时纠正”而不是直接抛异常：
    // - 集成测试/外部配置经常只设置了算法但没显式配 num_partitions；
    // - 对 ClusteredJoin 来说，num_partitions 与 parallelism 强绑定，运行时以 parallelism 为准更合理。
    if (strategy_config_.algorithm == JoinAlgorithm::CLUSTERED_JOIN) {
        const auto runtime_p = static_cast<size_t>(context.getParallelism());
        if (strategy_config_.num_partitions != static_cast<int>(runtime_p)) {
            SAGEFLOW_LOG_WARN("JOIN",
                "ClusteredJoin runtime constraint auto-fix: num_partitions={} -> parallelism={}",
                strategy_config_.num_partitions, runtime_p);
            strategy_config_.num_partitions = static_cast<int>(runtime_p);
        }
    }

    // 1.2 IVF 动态参数
    // 根据窗口大小和向量到达间隔计算 nlist/nprobes。
    // 
    // 公式：N = window_size_ms / time_interval_ms（窗口内预期向量数）
    // nlist = 4 * sqrt(N)（经验公式）
    // nprobes = 30% * nlist （保持召回率和性能平衡，如果有概率丢失召回而不是稳定丢失召回的情况不要随便调大参数）
    if (strategy_config_.algorithm == JoinAlgorithm::IVF) {
        const int64_t window_size = strategy_config_.window_size_ms;
        const int64_t time_interval = strategy_config_.time_interval_ms;
        
        // 使用 time_interval_ms 计算窗口内预期向量数
        const int64_t vector_count =
            (time_interval > 0) ? (window_size / time_interval) : window_size;

        // nlist 使用 4*sqrt(N)，但至少 32 个聚类以保证召回率
        int nlist = std::max(32, static_cast<int>(4.0 * std::sqrt(static_cast<double>(std::max<int64_t>(1, vector_count)))));
        int nprobes = std::max(3, nlist * 30 / 100);

        SAGEFLOW_LOG_INFO("JOIN",
            "IVF dynamic params (strategy-config): window={}ms time_interval={}ms N≈{} -> nlist={} nprobes={}",
            window_size, time_interval, vector_count, nlist, nprobes);

        strategy_config_.ivf_nlist = nlist;
        strategy_config_.ivf_nprobes = nprobes;
    }
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

    // ==================== VSJoin 专用：索引 ID 下发 ====================
    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        // Task08: logical partitions = P * V
        num_logical_partitions_ = static_cast<size_t>(context.getParallelism()) * virtual_nodes_per_partition_;
        partition_assignment_ = std::make_unique<VSJoinPartitionAssignment>(num_logical_partitions_,
                                                                            static_cast<size_t>(context.getParallelism()));
        load_monitor_ = std::make_unique<VSJoinLoadMonitor>(static_cast<size_t>(context.getParallelism()));

        vsjoin_global_left_id_ = components.global_left_id;
        vsjoin_global_right_id_ = components.global_right_id;
        vsjoin_local_left_ids_ = components.local_left_ids;
        vsjoin_local_right_ids_ = components.local_right_ids;

        auto* vsjoin_method = dynamic_cast<VSJoinMethod*>(join_method_.get());
        if (vsjoin_method) {
            vsjoin_method->setGlobalIndexIds(vsjoin_global_left_id_, vsjoin_global_right_id_);
            vsjoin_method->setLocalIndexIds(vsjoin_local_left_ids_, vsjoin_local_right_ids_);
            vsjoin_method->setWindowStates(left_state_.get(), right_state_.get());
        }

        SAGEFLOW_LOG_INFO("VSJOIN", "JoinOperator received index ids: global(L={}, R={}) local_sizes(L={}, R={})",
                         vsjoin_global_left_id_, vsjoin_global_right_id_,
                         vsjoin_local_left_ids_.size(), vsjoin_local_right_ids_.size());
    }

    // 5.1 启用索引插入/查询路径（用于 IVF/HNSW/HDR 等通过 ConcurrencyManager 管理索引的方法）
    // 注意：BRUTEFORCE 使用 BruteForceBaseline，不依赖索引。
    // 特殊处理：VSJoin 使用 vsjoin_global_* 和 vsjoin_local_* 索引
    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        use_index_ = (vsjoin_global_left_id_ != -1 && vsjoin_global_right_id_ != -1);
        SAGEFLOW_LOG_INFO("VSJOIN", "use_index_={} (global_left={}, global_right={})",
                         use_index_, vsjoin_global_left_id_, vsjoin_global_right_id_);
    } else {
        use_index_ = (left_index_id_ != -1 && right_index_id_ != -1);
    }
    // 5.2 设置 index_kind_（与字符串路径保持一致）
    switch (strategy_config_.algorithm) {
        case JoinAlgorithm::IVF:
            index_kind_ = InternalIndexKind::IVF;
            break;
        case JoinAlgorithm::HNSW:
            index_kind_ = InternalIndexKind::NONE;
            break;
        case JoinAlgorithm::HDR_TREE:
            index_kind_ = InternalIndexKind::HDR_TREE;
            break;
        case JoinAlgorithm::BRUTEFORCE:
            index_kind_ = InternalIndexKind::BRUTEFORCE;
            break;
        default:
            index_kind_ = InternalIndexKind::NONE;
            break;
    }

    // 5.3 计算批量删除阈值（与字符串路径保持一致）
    // 这是关键！默认值 50 太小会导致频繁删除索引记录，影响召回率
    {
        int64_t window_size = join_func_ ? join_func_->getWindowSize() : strategy_config_.window_size_ms;
        size_t computed_threshold = window_size * parallelism_ / kBatchDeleteDivisor;
        batch_delete_threshold_ = std::max(kMinBatchDeleteThreshold, computed_threshold);
        SAGEFLOW_LOG_INFO("JOIN", "Batch delete threshold computed (strategy config): {} (window={}, parallelism={})",
                         batch_delete_threshold_, window_size, parallelism_);
    }


    // 6. 根据窗口状态类型设置标志
    use_shared_state_ = (strategy_config_.window_state_type == WindowStateType::SHARED);

    // 7. 设置 eviction buffer multiplier
    // 高并行度下（shared state + 多线程），输入处理顺序可能出现“大幅乱序”，
    // eviction 需要更大的 buffer 才能避免误删导致召回率抖动/下降。
    //
    // 经验默认值（可被环境变量覆盖）：
    // - p>=16: 32
    // - p>=8 : 16
    // - p>=4 : 8
    // - else : 1.5
    //
    // Debug override:
    //   SAGEFLOW_EVICTION_MULTIPLIER=FLOAT (e.g. 1.5 / 3.0 / 5.0)
    double eviction_multiplier = 1.5;
    if (context.getParallelism() >= 4) {
        eviction_multiplier = std::min(32.0, 2.0 * static_cast<double>(context.getParallelism()));
    }
    if (const char* v = std::getenv("SAGEFLOW_EVICTION_MULTIPLIER")) {
        try {
            eviction_multiplier = std::stod(v);
        } catch (...) {
            // ignore invalid override
        }
    }
    if (left_state_) {
        left_state_->setEvictionBufferMultiplier(eviction_multiplier);
    }
    if (right_state_) {
        right_state_->setEvictionBufferMultiplier(eviction_multiplier);
    }
    SAGEFLOW_LOG_INFO("JOIN", "Eviction buffer multiplier set to {} for parallelism={}",
                     eviction_multiplier, context.getParallelism());

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
        // 尝试作为 LSHMethod 初始化（纯窗口 + LSH 桶过滤）
        else if (auto* lsh = dynamic_cast<LSHMethod*>(join_method_.get())) {
            lsh->open(context, left_state_.get(), right_state_.get());
            SAGEFLOW_LOG_INFO("JOIN", "LSHMethod initialized via strategy config");
        }
        // VSJoin 将通过 VSJoinMethod 处理，不再需要特殊初始化
        // 参考: include/operator/join_operator_methods/vsjoin_method.h
        
        // ClusteredJoinMethod 初始化（重构版：统一架构）
        // ClusteredJoin 现在使用与其他 Join 方法相同的流程：
        // - 使用 JoinOperator 的 left_state_/right_state_
        // - 使用 JoinOperator 的 left_index_id_/right_index_id_
        // - 通过 setIndexIds() 传递索引 ID
        else if (auto* clustered = dynamic_cast<ClusteredJoinMethod*>(join_method_.get())) {
            // 初始化 ClusteredJoinMethod
            clustered->initialize(context, concurrency_manager_);
            
            // 传递 WindowState（用于 BruteForce 模式直接访问窗口数据）
            // BruteForce 模式绕过 ConcurrencyManager，与 BruteForceBaseline 架构一致
            clustered->setWindowStates(left_state_.get(), right_state_.get());
            
            // 传递索引 ID（用于 IVF/HNSW 模式，索引由 JoinStrategyFactory 创建）
            clustered->setIndexIds(left_index_id_, right_index_id_);
            
            clustered->setEffectiveParallelism(1);  // 禁用 Owner-Computes 去重
            
            use_index_ = true;
            
            SAGEFLOW_LOG_INFO("JOIN", "ClusteredJoinMethod initialized via strategy config, "
                             "subtask={}/{} left_idx={} right_idx={} effective_p={} "
                             "index_type={}",
                             context.getSubtaskIndex(), context.getParallelism(),
                             left_index_id_, right_index_id_,
                             clustered->getEffectiveParallelism(),
                             static_cast<int>(strategy_config_.clustered_index_type));
        }
    }

    SAGEFLOW_LOG_INFO("JOIN", "JoinOperator initialized with strategy config: subtask={}/{} shared_state={}",
                     context.getSubtaskIndex(), context.getParallelism(), use_shared_state_);
}

std::unique_ptr<IPartitioner> JoinOperator::getPreferredPartitioner(
    int dimension, int num_partitions) const {
    // 根据 Join 配置返回适当的分区器
    
    if (use_strategy_config_) {
        switch (strategy_config_.algorithm) {
            case JoinAlgorithm::CLUSTERED_JOIN: {
                // ClusteredJoin 使用 CentroidPartitioner
                // 构建配置：使用策略配置中的参数
                CentroidPartitioner::Config cp_config;
                cp_config.num_partitions = (num_partitions > 0) 
                    ? num_partitions : strategy_config_.num_partitions;
                cp_config.overlap_ratio = strategy_config_.clustered_overlap_ratio;
                cp_config.dimension = (dimension > 0) 
                    ? dimension : strategy_config_.dimension;
                cp_config.seed = 42;
                cp_config.rebalance_threshold = strategy_config_.clustered_rebalance_threshold;
                // 关键：设置 multicast_k 用于控制向量分发到多少个分区
                cp_config.multicast_k = strategy_config_.clustered_multicast_k;
                // 关键修复：设置冷启动训练参数
                cp_config.training_samples = static_cast<size_t>(strategy_config_.clustered_training_samples);
                cp_config.enable_cold_start = strategy_config_.enable_cold_start;
                
                auto partitioner = std::make_unique<CentroidPartitioner>(cp_config);
                
                // 设置多播模式（边界向量复制）
                partitioner->setMulticastEnabled(strategy_config_.clustered_multicast_enabled);
                
                SAGEFLOW_LOG_INFO("JOIN", "Created CentroidPartitioner for ClusteredJoin: "
                                 "partitions={} overlap={:.2f} multicast={} multicast_k={} "
                                 "training_samples={} cold_start={}",
                                 cp_config.num_partitions, cp_config.overlap_ratio,
                                 strategy_config_.clustered_multicast_enabled,
                                 cp_config.multicast_k,
                                 cp_config.training_samples, cp_config.enable_cold_start);
                
                return partitioner;
            }
            
            case JoinAlgorithm::S3J: {
                // S3J 也使用 CentroidPartitioner，但使用 S3J 特有参数
                CentroidPartitioner::Config cp_config;
                cp_config.num_partitions = (num_partitions > 0) 
                    ? num_partitions : strategy_config_.s3j_num_centroids;
                cp_config.overlap_ratio = strategy_config_.clustered_overlap_ratio;
                cp_config.dimension = (dimension > 0) 
                    ? dimension : strategy_config_.dimension;
                cp_config.seed = 42;
                
                return std::make_unique<CentroidPartitioner>(cp_config);
            }
            
            case JoinAlgorithm::VSJOIN: {
                // 临时方案：VSJoin 先复用 ClusteredJoin 的 CentroidPartitioner 以获得多播能力（multicast_k）。
                // TODO(vsjoin): 实现 LSHPartitionerAdapter 的多播接口（supportsMulticast/partitionMulti + k），
                // Issue URL: https://github.com/intellistream/sageFlow/issues/102
                // 再切回 LSH 分区。
                CentroidPartitioner::Config cp_config;
                cp_config.num_partitions = (num_partitions > 0)
                    ? num_partitions
                    : strategy_config_.num_partitions;
                cp_config.overlap_ratio = strategy_config_.clustered_overlap_ratio;
                cp_config.dimension = (dimension > 0)
                    ? dimension
                    : strategy_config_.dimension;
                cp_config.seed = 42;
                cp_config.rebalance_threshold = strategy_config_.clustered_rebalance_threshold;
                cp_config.multicast_k = strategy_config_.clustered_multicast_k;
                cp_config.training_samples = static_cast<size_t>(strategy_config_.clustered_training_samples);
                cp_config.enable_cold_start = strategy_config_.enable_cold_start;

                auto partitioner = std::make_unique<CentroidPartitioner>(cp_config);
                partitioner->setMulticastEnabled(strategy_config_.clustered_multicast_enabled);
                return partitioner;
            }
            
            case JoinAlgorithm::BRUTEFORCE:
            case JoinAlgorithm::IVF:
            case JoinAlgorithm::HNSW:
            case JoinAlgorithm::HDR_TREE:
            default:
                // 共享索引 Join：使用 RoundRobin 实现负载均衡
                // 返回 nullptr 让 ConnectionStrategy 使用默认的 RoundRobin
                return nullptr;
        }
    }
    
    // 向后兼容：没有使用策略配置时返回 nullptr（使用默认 RoundRobin）
    return nullptr;
}

} // namespace sageFlow

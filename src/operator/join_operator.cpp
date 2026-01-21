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
        if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
            startGlobalIndexRebuilder();
        }
        return;
    }
  
    // VSJoin 特殊处理：启动后台重建线程
    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        startGlobalIndexRebuilder();
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
  
    // 高并行度需要更大的 eviction buffer 来容忍处理延迟
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
    left_state_->setEvictionBufferMultiplier(eviction_multiplier);
    right_state_->setEvictionBufferMultiplier(eviction_multiplier);
    SAGEFLOW_LOG_INFO("JOIN", "Eviction buffer multiplier set to {} for parallelism={}",
                    eviction_multiplier, context.getParallelism());
    
    // 计算批量删除阈值：基于窗口向量数量和并行度动态调整
    {
        int64_t window_size = join_func_ ? join_func_->getWindowSize() : 10000;
        size_t computed_threshold = window_size * parallelism_ / kBatchDeleteDivisor;
        batch_delete_threshold_ = std::max(kMinBatchDeleteThreshold, computed_threshold);
        SAGEFLOW_LOG_INFO("JOIN", "Batch delete threshold computed: {} (window={}, parallelism={})",
                        batch_delete_threshold_, window_size, parallelism_);
    }
    
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
        // LSH path: initialize hyperplane tables with window state pointers
        else if (auto* lsh = dynamic_cast<LSHMethod*>(join_method_.get())) {
            lsh->open(context, left_state_.get(), right_state_.get());
            SAGEFLOW_LOG_INFO("JOIN", "LSHMethod initialized with WindowState");
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
    metrics_increment(JoinMetrics::instance().candidate_fetch_count);
    // 所有方法均使用 Eager 模式：直接通过索引查询候选项
    return join_method_->ExecuteEager(*data_ptr, slot);
}

auto JoinOperator::getCandidatesWithLocksHeld(
    const std::unique_ptr<VectorRecord>& data_ptr, int slot) -> std::vector<std::unique_ptr<VectorRecord>> {
    // This version assumes both window locks are already held by caller
    MetricsTimer t_fetch(JoinMetrics::instance().candidate_fetch_ns);
    metrics_increment(JoinMetrics::instance().candidate_fetch_count);
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
            metrics_increment(JoinMetrics::instance().emit_count);
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

    if (auto* lsh = dynamic_cast<LSHMethod*>(join_method_.get())) {
        if (data_ptr) {
            lsh->onRecordAdded(*data_ptr, slot);
        }
    }
    
    // 添加记录到窗口状态
    {
        MetricsTimer t_window_ins(JoinMetrics::instance().window_insert_ns);
        state->addRecord(std::move(data_ptr), subtask_index);
    }
    metrics_increment(JoinMetrics::instance().window_insert_count);
    
    // 插入索引
    if (use_index_ && concurrency_manager_ && data_for_index_insert && index_id_for_cc != -1) {
        MetricsTimer t_idx(JoinMetrics::instance().index_insert_ns);

        // VSJoin 特殊处理：只插入到本分区的 Local Index
        if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
            const auto& local_ids = (slot == left_slot_id_)
                ? vsjoin_local_left_ids_
                : vsjoin_local_right_ids_;

            int local_index_id = (subtask_index < local_ids.size())
                ? local_ids[subtask_index]
                : -1;

            if (local_index_id >= 0) {
                concurrency_manager_->insert(local_index_id, std::move(data_for_index_insert));
            }

            SAGEFLOW_LOG_DEBUG("VSJOIN", "subtask_{} inserted to local_id={}", subtask_index, local_index_id);
        } else {
            concurrency_manager_->insert(index_id_for_cc, std::move(data_for_index_insert));
        }

        metrics_increment(JoinMetrics::instance().index_op_count);
    }
    
    // 获取窗口配置
    auto& window = (slot == left_slot_id_) ? join_func_->threadSafeWindowL : join_func_->threadSafeWindowR;
    int64_t timelimit = window.windowTimeLimit(now_time_stamp);
    
    // 更新时间戳追踪
    // - 分区策略：使用 WindowState 的分区级别时间戳追踪
    // - 共享策略：使用全局时间戳追踪
    if (isPartitionedStrategy()) {
        // 分区策略：更新分区级别的 max_seen_timestamp
        state->updateMaxSeenTimestamp(now_time_stamp, subtask_index);
    } else {
        // 共享策略：更新全局时间戳（原子操作，支持乱序）
        std::atomic<int64_t>& max_seen_ts = (slot == left_slot_id_) 
            ? max_seen_left_ts_ : max_seen_right_ts_;
        int64_t current_max = max_seen_ts.load(std::memory_order_relaxed);
        while (now_time_stamp > current_max && 
               !max_seen_ts.compare_exchange_weak(current_max, now_time_stamp,
                                                   std::memory_order_release,
                                                   std::memory_order_relaxed)) {
            // 重试直到成功或发现更大的值
        }
    }
    
    // 安全 evict 策略：
    // - 分区策略：使用 WindowState 的分区级别 getSafeEvictTimestamp
    // - 共享策略：使用全局双侧 max_seen_ts 的最小值
    int64_t safe_evict_ts;
    
    if (isPartitionedStrategy()) {
        // 分区策略：直接使用当前分区的 max_seen_ts
        // 因为分区是隔离的，不需要考虑其他分区的状态
        safe_evict_ts = state->getSafeEvictTimestamp(subtask_index, nullptr);
    } else {
        // 共享策略：需要等待双侧都推进，取最小值确保安全
        int64_t left_max = max_seen_left_ts_.load(std::memory_order_acquire);
        int64_t right_max = max_seen_right_ts_.load(std::memory_order_acquire);
        
        constexpr int64_t kMinTimestamp = std::numeric_limits<int64_t>::min();
        
        // 安全 evict 策略：只有当双侧都有记录时才进行 evict
        // 这是因为在高并行度下，某些 subtask 可能只收到一侧数据，
        // 如果使用单侧时间戳进行 evict，可能会删除另一侧需要匹配的记录
        if (left_max == kMinTimestamp || right_max == kMinTimestamp) {
            // 某侧还没有记录，暂不 evict，避免删除潜在匹配
            safe_evict_ts = kMinTimestamp;
        } else {
            // 两侧都有记录，使用最小值确保安全
            safe_evict_ts = std::min(left_max, right_max);
        }
    }
    
    {
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
    if (use_index_ && concurrency_manager_ && index_id_for_cc != -1) {
        size_t expired_count = state->getExpiredCount(subtask_index);
        if (expired_count >= batch_delete_threshold_) {
            // 获取并清空过期 UID buffer
            auto expired_uids = state->flushExpiredUids(subtask_index);
            
            // 批量从 Index/Storage 中删除
            for (uint64_t uid : expired_uids) {
                concurrency_manager_->erase(index_id_for_cc, uid);
                metrics_increment(JoinMetrics::instance().index_op_count);
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

// ================== 带 RuntimeContext 的 apply 方法 ==================

auto JoinOperator::apply(Response&& record, int slot, Collector& collector,
                        const RuntimeContext& context) -> void {
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
    // 根据策略类型和并行度选择不同的并发控制：
    // 1. 分区策略（Centroid/LSH）：分区内无锁竞争，直接使用 IQ
    // 2. 共享策略 + 单线程：无竞争，直接使用 IQ
    // 3. 共享策略 + 多线程：需要 QIQ 保证召回率
    
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> local_return_pool;
    
    // 并发策略选择：
    // - 分区策略 or 单线程：无竞争，使用“无锁 IQ”（Insert -> Query）
    // - 共享状态 + 多线程：使用“带全局锁的 IQ”（写锁 Insert + 读锁 Query）
    //
    // 由于性能问题，QIQ方法暂时不使用，后续再考虑使用。
    const bool use_lockless_iq = isPartitionedStrategy() || (context.getParallelism() <= 1);
    bool force_qiq = false;
    if (!isPartitionedStrategy() && context.getParallelism() > 1) {
        if (const char* v = std::getenv("SAGEFLOW_JOIN_HIGH_P_STRATEGY")) {
            std::string s(v);
            std::transform(s.begin(), s.end(), s.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (s == "qiq") {
                // 默认禁止 QIQ（会系统性丢召回）。仅允许在显式打开“危险开关”时用于实验复现：
                //   SAGEFLOW_ALLOW_UNSAFE_QIQ=1
                //   SAGEFLOW_JOIN_HIGH_P_STRATEGY=QIQ
                const char* allow = std::getenv("SAGEFLOW_ALLOW_UNSAFE_QIQ");
                const bool allow_qiq = (allow && std::string(allow) == "1");
                if (allow_qiq) {
                    force_qiq = true;
                    static std::atomic<bool> warned{false};
                    bool expected = false;
                    if (warned.compare_exchange_strong(expected, true)) {
                        SAGEFLOW_LOG_WARN("JOIN",
                            "UNSAFE: enabling QIQ due to SAGEFLOW_ALLOW_UNSAFE_QIQ=1; this mode is known to reduce recall in shared+multi-thread.");
                    }
                } else {
                    static std::atomic<bool> warned{false};
                    bool expected = false;
                    if (warned.compare_exchange_strong(expected, true)) {
                        SAGEFLOW_LOG_WARN("JOIN",
                            "Ignoring SAGEFLOW_JOIN_HIGH_P_STRATEGY=QIQ (set SAGEFLOW_ALLOW_UNSAFE_QIQ=1 to force for experiments). Using iq_locked.");
                    }
                    force_qiq = false;
                }
            } else if (s == "iq") {
                force_qiq = false;
            }
        }
    }
    if (use_lockless_iq) {
        // ====== IQ 策略（无锁，适用于分区模式或单线程） ======
        // 
        // 在分区模式下，每个分区有独立的 WindowState 和索引：
        // 1. 数据通过 CentroidPartitioner 路由到对应的 subtask
        // 2. 同一分区内的数据由同一个 subtask 串行处理
        // 3. 因此分区内无并发竞争，只需 Insert -> Query
        
        // 阶段1：Insert 当前记录到对应窗口和索引
        updateSideWithState(
            current_state, index_id, std::move(data_ptr), now_time_stamp, slot, subtask_index);
        
        // 阶段2：Query 对侧窗口查找匹配
        executeJoinWithState(data_for_join.get(), opposite_state, slot, 
                            subtask_index, local_return_pool);
    } else if (!force_qiq) {
        // ====== 共享策略 + 多线程：全局读写锁 + IQ 策略（Insert-Query）======
        // 
        // QIQ 策略存在固有缺陷：Q2 只能看到在它执行时已完成的 Insert。
        // 如果两个记录 A 和 B 几乎同时到达：
        //   - A 的 Q1 在 B 的 Insert 之前 → A 找不到 B
        //   - A 的 Q2 也可能在 B 的 Insert 之前 → A 仍然找不到 B
        //   - B 的 Q1 和 Q2 也可能在 A 的 Insert 之前 → B 也找不到 A
        // 这导致相似记录对被双向丢失！
        // 
        // IQ 策略：先 Insert，再 Query
        // - 当记录 A 被 Query 时，A 已经被 Insert 到窗口
        // - 如果 B 的 Insert 先于 A 的 Query，A 会找到 B
        // - 如果 B 的 Insert 晚于 A 的 Query，B 的 Query 一定能找到 A
        //   （因为 A 的 Insert 在 A 的 Query 之前，也就是在 B 的 Query 之前）
        // - 保证每对相似记录至少被发现一次
        
        // 阶段1：Insert（写锁，独占）
        {
            uint64_t insert_start = metrics_timestamp();
            uint64_t before_lock = metrics_timestamp();
            std::unique_lock<std::shared_mutex> write_lock(join_rw_mutex_);
            metrics_record_lock_wait(before_lock);
            updateSideWithState(
                current_state, index_id, std::move(data_ptr), now_time_stamp, slot, subtask_index);
            // 统计 Insert 总耗时（含锁等待）
            metrics_record_elapsed(JoinMetrics::instance().qiq_insert_ns, insert_start);
            JoinMetrics::instance().qiq_insert_count.fetch_add(1, std::memory_order_relaxed);
        }
        
        // 阶段2：Query（读锁，可并行）
        size_t query_window_size = 0;
        {
            uint64_t q1_start = metrics_timestamp();
            uint64_t before_lock = metrics_timestamp();
            std::shared_lock<std::shared_mutex> read_lock(join_rw_mutex_);
            metrics_record_lock_wait(before_lock);
            query_window_size = opposite_state->size(subtask_index);
            executeJoinWithState(data_for_join.get(), opposite_state, slot, 
                                subtask_index, local_return_pool);
            // 统计 Query 总耗时（含锁等待）
            metrics_record_elapsed(JoinMetrics::instance().qiq_q1_ns, q1_start);
            JoinMetrics::instance().qiq_q1_count.fetch_add(1, std::memory_order_relaxed);
        }
    } else {
        // ====== 共享策略 + 多线程：全局读写锁 + QIQ（Query-Insert-Query）======
        // 仅用于实验复现（必须显式开启 SAGEFLOW_ALLOW_UNSAFE_QIQ=1）。
        //
        // 注意：QIQ 在 shared + 多线程下可能系统性丢召回（并发可见性问题），
        // 该分支不作为默认策略。
        //
        // 阶段1：Query1（读锁）
        {
            uint64_t q1_start = metrics_timestamp();
            uint64_t before_lock = metrics_timestamp();
            std::shared_lock<std::shared_mutex> read_lock(join_rw_mutex_);
            metrics_record_lock_wait(before_lock);
            executeJoinWithState(data_for_join.get(), opposite_state, slot,
                                 subtask_index, local_return_pool);
            metrics_record_elapsed(JoinMetrics::instance().qiq_q1_ns, q1_start);
            JoinMetrics::instance().qiq_q1_count.fetch_add(1, std::memory_order_relaxed);
        }

        // 阶段2：Insert（写锁）
        {
            uint64_t insert_start = metrics_timestamp();
            uint64_t before_lock = metrics_timestamp();
            std::unique_lock<std::shared_mutex> write_lock(join_rw_mutex_);
            metrics_record_lock_wait(before_lock);
            updateSideWithState(
                current_state, index_id, std::move(data_ptr), now_time_stamp, slot, subtask_index);
            metrics_record_elapsed(JoinMetrics::instance().qiq_insert_ns, insert_start);
            JoinMetrics::instance().qiq_insert_count.fetch_add(1, std::memory_order_relaxed);
        }

        // 阶段3：Query2（读锁）
        {
            uint64_t q2_start = metrics_timestamp();
            uint64_t before_lock = metrics_timestamp();
            std::shared_lock<std::shared_mutex> read_lock(join_rw_mutex_);
            metrics_record_lock_wait(before_lock);
            executeJoinWithState(data_for_join.get(), opposite_state, slot,
                                 subtask_index, local_return_pool);
            metrics_record_elapsed(JoinMetrics::instance().qiq_q2_ns, q2_start);
            JoinMetrics::instance().qiq_q2_count.fetch_add(1, std::memory_order_relaxed);
        }
    }
    
    // 发送 Join 结果
    {
        MetricsTimer t_emit(JoinMetrics::instance().emit_ns);
        for (auto& p : local_return_pool) {
            Response out{ResponseType::Record, std::move(p.second)};
            collector.collect(std::make_unique<Response>(std::move(out)), p.first);
            metrics_increment(JoinMetrics::instance().total_emits);
            metrics_increment(JoinMetrics::instance().emit_count);
            metrics_record_e2e_latency(apply_enter_ns);
        }
    }
    
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
    use_index_ = (left_index_id_ != -1 && right_index_id_ != -1);
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

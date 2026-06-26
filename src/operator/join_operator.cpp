/*
 * JoinOperator public lifecycle and per-record orchestration.
 *
 * Strategy wiring, WindowState IQ execution, VSJoin routing/rebuild, partitioner
 * construction, and result emission live in join_operator_components/.
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
#include "operator/join_operator_components/join_operator_initializer.h"
#include "operator/join_operator_components/join_partitioner_factory.h"
#include "operator/join_operator_components/join_result_emitter.h"
#include "operator/join_operator_components/join_window_state_executor.h"
#include "operator/join_operator_components/vsjoin_router.h"
#include "operator/join_operator_methods/vsjoin_method.h"
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
    if (!global_index_rebuilder_) {
        global_index_rebuilder_ = std::make_unique<VSJoinGlobalIndexRebuilder>(
            strategy_config_,
            concurrency_manager_,
            join_func_.get(),
            left_state_.get(),
            right_state_.get(),
            vsjoin_global_left_id_,
            vsjoin_global_right_id_,
            parallelism_);
    }
    global_index_rebuilder_->start();
}

void JoinOperator::stopGlobalIndexRebuilder() {
    if (global_index_rebuilder_) {
        global_index_rebuilder_->stop();
    }
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

    // Profiler lifetime follows operator open/destruction.
    if (enable_profiling_) {
        std::string profile_path = profile_output_path.empty() 
            ? "profiles/join_operator_profile.prof" 
            : profile_output_path;
        profiler_ = std::make_unique<PerformanceMonitor>(profile_path);
        SAGEFLOW_LOG_INFO("JOIN", "GPERFTOOLS profiling enabled (string method), output={}", profile_path);
    }

    // Convert the legacy method name into the same config-driven path used by
    // the explicit JoinStrategyConfig constructor.
    strategy_config_ = createJoinStrategyConfigFromMethodName(
        join_method_name_raw,
        join_similarity_threshold,
        join_func_->getDim(),
        join_func_->getWindowSize(),
        join_func_->getStepSize());
    
    use_strategy_config_ = true;
    
    // The old lazy path is no longer active; this flag is kept for compatibility.
    is_eager_ = true;
    index_kind_ = InternalIndexKind::NONE;
    use_index_ = false;
    
    SAGEFLOW_LOG_INFO("JOIN", "JoinOperator created with string method '{}' -> config: algorithm={} partition={} window_state={}",
                     join_method_name_raw,
                     toString(strategy_config_.algorithm),
                     toString(strategy_config_.partition_strategy),
                     toString(strategy_config_.window_state_type));
}

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

    // Keep JoinFunction's window settings aligned with the explicit config.
    if (config.window_size_ms > 0) {
        join_func_->setWindow(config.window_size_ms, config.step_size_ms);
    }

    // Profiler lifetime follows operator open/destruction.
    if (enable_profiling_) {
        std::string profile_path = profile_output_path.empty()
            ? "profiles/join_operator_profile.prof"
            : profile_output_path;
        profiler_ = std::make_unique<PerformanceMonitor>(profile_path);
        SAGEFLOW_LOG_INFO("JOIN", "GPERFTOOLS profiling enabled (strategy config), output={}", profile_path);
    }

    // Component initialization is deferred until RuntimeContext parallelism is known.
    is_eager_ = true;
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
        // QIQ counters may be zero in the default IQ path; keep the summary for
        // experiments that explicitly enable QIQ metrics.
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
    
    // Stop profiling after all background work has been stopped.
    if (profiler_) {
        profiler_->StopProfiling();
        SAGEFLOW_LOG_INFO("JOIN", "GPERFTOOLS profiling stopped");
    }
}

void JoinOperator::open() {
  // Compatibility entry point used by older unit tests and stream wrappers.
  RuntimeContext default_context(0, parallelism_);
  open(default_context);
}

void JoinOperator::open(const RuntimeContext& context) {
  validateRuntimeContext(context);
  // Component initialization is shared across subtasks for the operator instance.
  std::call_once(init_flag_, [this, &context]() {
    is_open_ = true;
    parallelism_ = context.getParallelism();
    
    if (profiler_) {
        profiler_->StartProfiling();
        SAGEFLOW_LOG_INFO("JOIN", "GPERFTOOLS profiling started");
    }
    
    initializeWithStrategyConfig(context);

    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        startGlobalIndexRebuilder();
    }
    
    SAGEFLOW_LOG_INFO("JOIN", "JoinOperator opened: subtask={}/{}, shared_state={}", 
                    context.getSubtaskIndex(), context.getParallelism(), 
                    use_shared_state_);
  });
}

auto JoinOperator::process(Response& input_data, int slot) -> std::optional<Response> {
    (void)input_data;
    (void)slot;
    // Join output can contain multiple records, so Collector-based apply is required.
    return std::nullopt;
}

auto JoinOperator::apply(Response&& record, int slot, Collector& collector) -> void {
    RuntimeContext default_context(0, parallelism_);
    apply(std::move(record), slot, collector, default_context);
}

auto JoinOperator::updateSideWithState(
    WindowState* state,
    WindowState* opposite_state,
    int index_id_for_cc,
    RecordView data_ptr,
    int64_t now_time_stamp,
    int slot,
    size_t subtask_index) -> bool {
    JoinWindowStateExecutor::Config config{
        strategy_config_.algorithm,
        use_index_,
        batch_delete_threshold_,
        left_slot_id_,
        right_slot_id_};
    JoinWindowStateExecutor executor(
        config,
        join_func_.get(),
        join_method_.get(),
        left_state_.get(),
        right_state_.get(),
        concurrency_manager_,
        left_index_id_,
        right_index_id_,
        vsjoin_local_left_ids_,
        vsjoin_local_right_ids_);
    return executor.updateSide(
        state, opposite_state, index_id_for_cc, data_ptr, now_time_stamp, slot, subtask_index);
}

void JoinOperator::executeJoinWithState(
    const RecordView& data_view,
    WindowState* opposite_state,
    int slot,
    size_t subtask_index,
    std::vector<JoinOutputItem>& local_return_pool) {
    JoinWindowStateExecutor::Config config{
        strategy_config_.algorithm,
        use_index_,
        batch_delete_threshold_,
        left_slot_id_,
        right_slot_id_,
        strategy_config_.materialization_mode,
        strategy_config_.similarity_mode,
        strategy_config_.similarity_alpha};
    JoinWindowStateExecutor executor(
        config,
        join_func_.get(),
        join_method_.get(),
        left_state_.get(),
        right_state_.get(),
        concurrency_manager_,
        left_index_id_,
        right_index_id_,
        vsjoin_local_left_ids_,
        vsjoin_local_right_ids_);
    executor.executeJoin(data_view, opposite_state, slot, subtask_index, local_return_pool);
}

std::vector<size_t> JoinOperator::computeVSJoinTargetSubtasks(
    const Response& record,
    const RuntimeContext& context,
    size_t subtask_index) const {
    return VSJoinRouter::computeTargetSubtasks(
        record,
        context,
        strategy_config_,
        use_strategy_config_,
        join_func_ ? join_func_->getDim() : strategy_config_.dimension,
        subtask_index);
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
    VSJoinRouter::recordSubtaskDebugStats(
        slot, left_slot_id_, subtask_index, context, strategy_config_.algorithm);
}

int JoinOperator::indexIdForSlot(int slot) const {
    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        return (slot == left_slot_id_) ? vsjoin_global_left_id_ : vsjoin_global_right_id_;
    }
    return (slot == left_slot_id_) ? left_index_id_ : right_index_id_;
}

void JoinOperator::emitJoinResults(
    std::vector<JoinOutputItem>& local_return_pool,
    Collector& collector,
    uint64_t apply_enter_ns) {
    JoinResultEmitter emitter(join_func_.get(), left_slot_id_, strategy_config_.materialization_mode);
    emitter.emit(local_return_pool, collector, apply_enter_ns);
}

auto JoinOperator::apply(Response&& record, int slot, Collector& collector,
                        const RuntimeContext& context) -> void {
    validateRuntimeContext(context);

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

    // Some compatibility callers invoke apply before open; initialize lazily.
    if (!left_state_ || !right_state_) {
        SAGEFLOW_LOG_WARN("JOIN", "WindowState not initialized; opening operator before apply");
        open(context);
        if (!left_state_ || !right_state_) {
            throw std::runtime_error("JoinOperator: WindowState initialization failed before apply");
        }
    }

    std::vector<size_t> target_subtasks;
    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        // Routing reads the transport-owned record before WindowState takes shared ownership.
        target_subtasks = computeVSJoinTargetSubtasks(record, context, subtask_index);
    }

    RecordView data_view = std::move(record.record_);
    
    WindowState* current_state = (slot == left_slot_id_) 
        ? left_state_.get() : right_state_.get();
    WindowState* opposite_state = (slot == left_slot_id_) 
        ? right_state_.get() : left_state_.get();
    
    const int index_id = indexIdForSlot(slot);
    
    // Insert-then-Query is the only active trigger model. Component-level locks
    // in WindowState and ConcurrencyManager provide visibility and safety; the
    // operator does not add a coarse global join lock.
    std::vector<JoinOutputItem> local_return_pool;

    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        // Multicast records are written and queried once per routed target subtask.
        for (size_t target_subtask : target_subtasks) {
            updateSideWithState(current_state, opposite_state, index_id, data_view,
                                now_time_stamp, slot, target_subtask);

            executeJoinWithState(data_view, opposite_state, slot, target_subtask, local_return_pool);
        }
    } else {
        updateSideWithState(
            current_state, opposite_state, index_id, data_view, now_time_stamp, slot, subtask_index);

        executeJoinWithState(data_view, opposite_state, slot,
                            subtask_index, local_return_pool);
    }
    
    emitJoinResults(local_return_pool, collector, apply_enter_ns);
    
    SAGEFLOW_LOG_DEBUG("JOIN_APPLY", "Apply (with context) completed: slot={} results={} subtask={}/{}", 
                      slot, local_return_pool.size(), subtask_index, context.getParallelism());
}

void JoinOperator::initializeWithStrategyConfig(const RuntimeContext& context) {
    auto initialized = JoinOperatorInitializer::initialize(
        strategy_config_,
        concurrency_manager_,
        join_func_.get(),
        context,
        virtual_nodes_per_partition_,
        kMinBatchDeleteThreshold,
        kBatchDeleteDivisor);

    strategy_config_ = initialized.strategy_config;
    join_method_ = std::move(initialized.join_method);
    left_state_ = std::move(initialized.left_state);
    right_state_ = std::move(initialized.right_state);
    left_index_id_ = initialized.left_index_id;
    right_index_id_ = initialized.right_index_id;
    vsjoin_global_left_id_ = initialized.vsjoin_global_left_id;
    vsjoin_global_right_id_ = initialized.vsjoin_global_right_id;
    vsjoin_local_left_ids_ = std::move(initialized.vsjoin_local_left_ids);
    vsjoin_local_right_ids_ = std::move(initialized.vsjoin_local_right_ids);
    use_index_ = initialized.use_index;
    use_shared_state_ = initialized.use_shared_state;
    batch_delete_threshold_ = initialized.batch_delete_threshold;

    // Retain the old index-kind flag for callers that still inspect it internally.
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
}

std::unique_ptr<IPartitioner> JoinOperator::getPreferredPartitioner(
    int dimension, int num_partitions) const {
    return JoinPartitionerFactory::createPreferred(
        strategy_config_, use_strategy_config_, dimension, num_partitions);
}

} // namespace sageFlow

#pragma once

#include <memory>
#include <mutex>
#include <atomic>
#include <string>
#include <algorithm>
#include <vector>

#include "common/data_types.h"
#include "operator/operator.h"
#include "operator/join_operator_methods/base_method.h"
#include "operator/utils/join_strategy_config.h"
#include "operator/join_operator_components/join_result_emitter.h"
#include "concurrency/concurrency_manager.h"
#include "state/window_state.h"
#include "state/partitioned_window_state.h"
#include "state/shared_window_state.h"
#include "operator/join_operator_components/vsjoin_global_index_rebuilder.h"

namespace sageFlow {
  // Forward declaration keeps the header independent of the optional profiler implementation.
  class PerformanceMonitor;

  /**
   * @brief Binary vector-stream join operator.
   *
   * JoinOperator owns the public Operator lifecycle and delegates implementation
   * details to focused components: strategy initialization, WindowState IQ
   * execution, VSJoin routing/rebuild, partitioner creation, and result emission.
   *
   * Runtime data transport remains `Response` with `std::unique_ptr<VectorRecord>`.
   * Records stored in windows or indexes are promoted internally to immutable
   * `RecordView` snapshots.
   */
  class JoinOperator final : public Operator {
   public:
    /**
     * @brief Construct from a legacy method name.
     *
     * The method name is converted to a `JoinStrategyConfig` and initialized in
     * `open(const RuntimeContext&)`, where runtime parallelism is known.
     *
     * @param join_func Join function. Ownership is transferred from the input unique_ptr.
     * @param concurrency_manager Index and storage concurrency manager.
     * @param join_method_name Legacy method name such as `bruteforce`, `ivf`, or `hnsw`.
     * @param join_similarity_threshold Similarity threshold for generated strategy config.
     * @param enable_profiling Whether to enable optional gperftools profiling.
     * @param profile_output_path Optional profiler output path.
     * @param use_shared_state Retained for source compatibility; effective state
     *                         type is derived from the strategy config.
     */
    explicit JoinOperator(std::unique_ptr<Function> &join_func,
                          const std::shared_ptr<ConcurrencyManager> &concurrency_manager,
                          const std::string& join_method_name = "bruteforce",
                          double join_similarity_threshold = 0.8,
                          bool enable_profiling = false,
                          const std::string& profile_output_path = "",
                          bool use_shared_state = false);

    /**
     * @brief Construct from an explicit join strategy config.
     *
     * Component creation is deferred to `open(const RuntimeContext&)` so the
     * initializer can use runtime parallelism for WindowState, partitioning,
     * index ids, eviction settings, and VSJoin components.
     *
     * @param join_func Join function. Ownership is transferred from the input unique_ptr.
     * @param concurrency_manager Index and storage concurrency manager.
     * @param config Strategy config.
     * @param enable_profiling Whether to enable optional gperftools profiling.
     * @param profile_output_path Optional profiler output path.
     */
    explicit JoinOperator(std::unique_ptr<Function> &join_func,
                          const std::shared_ptr<ConcurrencyManager> &concurrency_manager,
                          const JoinStrategyConfig& config,
                          bool enable_profiling = false,
                          const std::string& profile_output_path = "");

    /**
     * @brief Compatibility open using the current `parallelism_` and subtask 0.
     */
    auto open() -> void override;
    
    /**
     * @brief Initialize runtime components for a specific subtask context.
     */
    auto open(const RuntimeContext& context) -> void override;
    
    ~JoinOperator() override;

    /**
     * @brief Compatibility process hook.
     *
     * Join outputs require a `Collector`, so the supported execution path is
     * `apply(...)`; this method returns `std::nullopt`.
     */
    auto process(Response&data, int slot) -> std::optional<Response> override;

    /**
     * @brief Compatibility apply using the current `parallelism_` and subtask 0.
     */
    auto apply(Response&& record, int slot, Collector& collector) -> void override;

    /**
     * @brief Process one input record with explicit subtask semantics.
     *
     * The method preserves per-record Insert-then-Query semantics. For VSJoin,
     * the record can be routed to multiple target subtasks, and the same IQ
     * sequence is executed for each target.
     */
    auto apply(Response&& record, int slot, Collector& collector,
              const RuntimeContext& context) -> void override;

    /**
     * @brief Set planner-assigned left and right input slot ids.
     */
    void setSlots(int left_slot_id, int right_slot_id) {
        left_slot_id_ = left_slot_id;
        right_slot_id_ = right_slot_id;
    }

    /**
     * @brief Compatibility no-op for the removed legacy deque-window path.
     */
    void setRetentionBuffer(int64_t buffer) {
        (void)buffer;
    }

    /**
     * @brief Create the preferred upstream partitioner for this strategy.
     * 
     * Returning `nullptr` means the execution graph should use its default
     * round-robin partitioning. Strategy-specific construction is delegated to
     * `JoinPartitionerFactory`.
     * 
     * @param dimension Vector dimension override. Uses config dimension when 0.
     * @param num_partitions Partition count override. Uses config defaults when 0.
     * @return Preferred partitioner instance, or nullptr for default routing.
     */
    std::unique_ptr<IPartitioner> getPreferredPartitioner(
        int dimension = 0, int num_partitions = 0) const override;

   private:
    enum class InternalIndexKind { NONE, IVF, BRUTEFORCE, VAMANA, HDR_TREE };  // 可扩展

    /**
     * @brief Initialize strategy-owned runtime components.
     *
     * The initializer returns JoinMethod, WindowState instances, index ids,
     * VSJoin local/global ids, and derived settings such as batch delete
     * threshold and shared-state mode.
     */
    void initializeWithStrategyConfig(const RuntimeContext& context);

    /**
     * @brief Delegate current-side insertion, index update, and eviction.
     */
    auto updateSideWithState(
        WindowState* state,
        WindowState* opposite_state,
        int index_id_for_cc,
        RecordView data_ptr,
        int64_t now_time_stamp,
        int slot,
        size_t subtask_index) -> bool;

    /**
     * @brief Delegate opposite-side candidate fetch and join result materialization.
     */
    void executeJoinWithState(
        const RecordView& data_view,
        WindowState* opposite_state,
        int slot,
        size_t subtask_index,
        std::vector<JoinOutputItem>& local_return_pool);

    /**
     * @brief Compute VSJoin target subtasks before taking shared ownership of the record.
     */
    std::vector<size_t> computeVSJoinTargetSubtasks(
        const Response& record,
        const RuntimeContext& context,
        size_t subtask_index) const;

    /**
     * @brief Validate RuntimeContext invariants for this operator instance.
     */
    void validateRuntimeContext(const RuntimeContext& context) const;

    /**
     * @brief Record optional VSJoin subtask distribution diagnostics.
     */
    void recordVSJoinSubtaskDebugStats(
        int slot,
        size_t subtask_index,
        const RuntimeContext& context) const;

    /**
     * @brief Resolve the global/shared index id for an input side.
     */
    int indexIdForSlot(int slot) const;

    /**
     * @brief Emit materialized join results through the collector.
     */
    void emitJoinResults(
        std::vector<JoinOutputItem>& local_return_pool,
        Collector& collector,
        uint64_t apply_enter_ns);

    std::unique_ptr<JoinFunction> join_func_;
    std::shared_ptr<Operator> mother_;
    std::unique_ptr<BaseMethod> join_method_;

    // Runtime state created by JoinOperatorInitializer.
    std::unique_ptr<WindowState> left_state_;
    std::unique_ptr<WindowState> right_state_;
    bool use_shared_state_ = false;

    JoinStrategyConfig strategy_config_;
    bool use_strategy_config_ = false;
    size_t parallelism_ = 1;
    
    /**
     * @brief Return whether the strategy routes records into partition-local state.
     */
    bool isPartitionedStrategy() const {
        if (!use_strategy_config_) return false;
        return strategy_config_.partition_strategy == PartitionStrategy::CENTROID ||
               strategy_config_.partition_strategy == PartitionStrategy::LSH;
    }

    // Protects one-time component initialization across subtasks.
    std::once_flag init_flag_;
    
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;

    // Global/shared index ids for non-VSJoin strategies.
    int left_index_id_  = -1;
    int right_index_id_ = -1;
    InternalIndexKind index_kind_ = InternalIndexKind::NONE;
    bool use_index_ = false;

    // Retained for compatibility with old eager/lazy naming. All methods are eager.
    bool is_eager_ = true;
    double join_similarity_threshold_ = 0.8;

    // Planner-assigned input slots.
    int left_slot_id_ = 0;
    int right_slot_id_ = 1;
    
    // Threshold for flushing WindowState expired UID buffers to indexes/storage.
    static constexpr size_t kMinBatchDeleteThreshold = 50;
    static constexpr size_t kBatchDeleteDivisor = 10;
    size_t batch_delete_threshold_ = kMinBatchDeleteThreshold;

    // VSJoin local indexes are partition-scoped; global indexes are shared read paths.
    std::vector<int> vsjoin_local_left_ids_;
    std::vector<int> vsjoin_local_right_ids_;
    int vsjoin_global_left_id_ = -1;
    int vsjoin_global_right_id_ = -1;

    size_t virtual_nodes_per_partition_ = 8;

    // VSJoin background global-index rebuild lifecycle.
    void startGlobalIndexRebuilder();
    void stopGlobalIndexRebuilder();
    std::unique_ptr<VSJoinGlobalIndexRebuilder> global_index_rebuilder_;
    
    // Optional gperftools profiling support.
    std::unique_ptr<PerformanceMonitor> profiler_;
    bool enable_profiling_ = false;
  };
  }  // namespace sageFlow

#include "operator/join_operator_components/join_operator_initializer.h"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <stdexcept>

#include "operator/join_operator_methods/bruteforce_baseline.h"
#include "operator/join_operator_methods/clustered_join_method.h"
#include "operator/join_operator_methods/hnsw.h"
#include "operator/join_operator_methods/ivf_method.h"
#include "operator/join_operator_methods/lsh_method.h"
#include "operator/join_operator_methods/vsjoin_components/load_monitor.h"
#include "operator/join_operator_methods/vsjoin_components/partition_assignment.h"
#include "operator/join_operator_methods/vsjoin_method.h"
#include "operator/utils/join_config_validator.h"
#include "operator/utils/join_strategy_factory.h"
#include "utils/logger.h"

namespace sageFlow {

namespace {

void applyClusteredRuntimeFixup(JoinStrategyConfig& config, const RuntimeContext& context) {
  if (config.algorithm != JoinAlgorithm::CLUSTERED_JOIN) {
    return;
  }

  const auto runtime_p = static_cast<size_t>(context.getParallelism());
  if (config.num_partitions != static_cast<int>(runtime_p)) {
    SAGEFLOW_LOG_WARN("JOIN",
                      "ClusteredJoin runtime constraint auto-fix: num_partitions={} -> parallelism={}",
                      config.num_partitions,
                      runtime_p);
    config.num_partitions = static_cast<int>(runtime_p);
  }
}

void applyIvfDynamicParams(JoinStrategyConfig& config) {
  if (config.algorithm != JoinAlgorithm::IVF) {
    return;
  }

  const int64_t window_size = config.window_size_ms;
  const int64_t time_interval = config.time_interval_ms;
  const int64_t vector_count =
      (time_interval > 0) ? (window_size / time_interval) : window_size;

  int nlist = std::max(
      32,
      static_cast<int>(4.0 * std::sqrt(static_cast<double>(std::max<int64_t>(1, vector_count)))));
  double nprobe_ratio = 0.30;
  if (const char* v = std::getenv("SAGEFLOW_IVF_NPROBE_RATIO")) {
    try {
      const double parsed = std::stod(v);
      if (parsed > 0.0 && parsed <= 1.0) {
        nprobe_ratio = parsed;
      }
    } catch (...) {
      // ignore invalid override, keep default ratio
    }
  }
  int nprobes = std::max(3, static_cast<int>(nlist * nprobe_ratio));
  nprobes = std::min(nprobes, nlist);

  SAGEFLOW_LOG_INFO("JOIN",
                    "IVF dynamic params (strategy-config): window={}ms time_interval={}ms N≈{} -> nlist={} nprobes={}",
                    window_size,
                    time_interval,
                    vector_count,
                    nlist,
                    nprobes);

  config.ivf_nlist = nlist;
  config.ivf_nprobes = nprobes;
}

double computeEvictionMultiplier(const RuntimeContext& context) {
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
  return eviction_multiplier;
}

void initializeJoinMethod(
    BaseMethod* join_method,
    JoinOperatorInitializer::Result& result,
    const std::shared_ptr<ConcurrencyManager>& concurrency_manager,
    const RuntimeContext& context) {
  if (!join_method) {
    return;
  }

  if (auto* bf = dynamic_cast<BruteForceBaseline*>(join_method)) {
    bf->open(context, result.left_state.get(), result.right_state.get());
    SAGEFLOW_LOG_INFO("JOIN", "BruteForceBaseline method initialized via strategy config");
  } else if (auto* ivf = dynamic_cast<IVFMethod*>(join_method)) {
    ivf->setIndexIds(result.left_index_id, result.right_index_id);
    ivf->open(context, result.left_state.get(), result.right_state.get(), concurrency_manager.get());
    result.use_index = true;
    SAGEFLOW_LOG_INFO("JOIN",
                      "IVFMethod initialized via strategy config, left_idx={} right_idx={}",
                      result.left_index_id,
                      result.right_index_id);
  } else if (dynamic_cast<HNSWJoinMethod*>(join_method)) {
    result.use_index = true;
    SAGEFLOW_LOG_INFO("JOIN", "HNSWJoinMethod initialized via strategy config");
  } else if (auto* lsh = dynamic_cast<LSHMethod*>(join_method)) {
    lsh->open(context, result.left_state.get(), result.right_state.get());
    SAGEFLOW_LOG_INFO("JOIN", "LSHMethod initialized via strategy config");
  } else if (auto* clustered = dynamic_cast<ClusteredJoinMethod*>(join_method)) {
    clustered->initialize(context, concurrency_manager);
    clustered->setWindowStates(result.left_state.get(), result.right_state.get());
    clustered->setIndexIds(result.left_index_id, result.right_index_id);
    clustered->setEffectiveParallelism(1);
    result.use_index = true;

    SAGEFLOW_LOG_INFO("JOIN",
                      "ClusteredJoinMethod initialized via strategy config, "
                      "subtask={}/{} left_idx={} right_idx={} effective_p={} index_type={}",
                      context.getSubtaskIndex(),
                      context.getParallelism(),
                      result.left_index_id,
                      result.right_index_id,
                      clustered->getEffectiveParallelism(),
                      static_cast<int>(result.strategy_config.clustered_index_type));
  }
}

}  // namespace

JoinOperatorInitializer::Result JoinOperatorInitializer::initialize(
    JoinStrategyConfig config,
    const std::shared_ptr<ConcurrencyManager>& concurrency_manager,
    JoinFunction* join_func,
    const RuntimeContext& context,
    size_t virtual_nodes_per_partition,
    size_t min_batch_delete_threshold,
    size_t batch_delete_divisor) {
  JoinConfigValidator::throwIfInvalid(config);

  SAGEFLOW_LOG_INFO("JOIN", "Initializing with strategy config: algorithm={} parallelism={}",
                    toString(config.algorithm), context.getParallelism());

  applyClusteredRuntimeFixup(config, context);
  applyIvfDynamicParams(config);

  auto components = JoinStrategyFactory::create(
      config, concurrency_manager, context.getParallelism());

  Result result;
  result.strategy_config = config;
  result.join_method = std::move(components.join_method);
  result.left_state = std::move(components.left_state);
  result.right_state = std::move(components.right_state);
  result.left_index_id = components.left_index_id;
  result.right_index_id = components.right_index_id;

  if (config.algorithm == JoinAlgorithm::VSJOIN) {
    result.num_logical_partitions =
        static_cast<size_t>(context.getParallelism()) * virtual_nodes_per_partition;
    result.vsjoin_global_left_id = components.global_left_id;
    result.vsjoin_global_right_id = components.global_right_id;
    result.vsjoin_local_left_ids = components.local_left_ids;
    result.vsjoin_local_right_ids = components.local_right_ids;

    if (auto* vsjoin_method = dynamic_cast<VSJoinMethod*>(result.join_method.get())) {
      vsjoin_method->setGlobalIndexIds(result.vsjoin_global_left_id, result.vsjoin_global_right_id);
      vsjoin_method->setLocalIndexIds(result.vsjoin_local_left_ids, result.vsjoin_local_right_ids);
      vsjoin_method->setWindowStates(result.left_state.get(), result.right_state.get());
    }

    SAGEFLOW_LOG_INFO("VSJOIN",
                      "JoinOperator received index ids: global(L={}, R={}) local_sizes(L={}, R={})",
                      result.vsjoin_global_left_id,
                      result.vsjoin_global_right_id,
                      result.vsjoin_local_left_ids.size(),
                      result.vsjoin_local_right_ids.size());
  }

  if (config.algorithm == JoinAlgorithm::VSJOIN) {
    result.use_index =
        (result.vsjoin_global_left_id != -1 && result.vsjoin_global_right_id != -1);
    SAGEFLOW_LOG_INFO("VSJOIN", "use_index_={} (global_left={}, global_right={})",
                      result.use_index,
                      result.vsjoin_global_left_id,
                      result.vsjoin_global_right_id);
  } else {
    result.use_index = (result.left_index_id != -1 && result.right_index_id != -1);
  }

  const int64_t window_size = join_func ? join_func->getWindowSize() : config.window_size_ms;
  const size_t computed_threshold =
      static_cast<size_t>(window_size) * static_cast<size_t>(context.getParallelism()) /
      batch_delete_divisor;
  result.batch_delete_threshold = std::max(min_batch_delete_threshold, computed_threshold);
  SAGEFLOW_LOG_INFO("JOIN",
                    "Batch delete threshold computed (strategy config): {} (window={}, parallelism={})",
                    result.batch_delete_threshold,
                    window_size,
                    context.getParallelism());

  result.use_shared_state = (config.window_state_type == WindowStateType::SHARED);

  const double eviction_multiplier = computeEvictionMultiplier(context);
  if (result.left_state) {
    result.left_state->setEvictionBufferMultiplier(eviction_multiplier);
  }
  if (result.right_state) {
    result.right_state->setEvictionBufferMultiplier(eviction_multiplier);
  }
  SAGEFLOW_LOG_INFO("JOIN", "Eviction buffer multiplier set to {} for parallelism={}",
                    eviction_multiplier, context.getParallelism());

  initializeJoinMethod(result.join_method.get(), result, concurrency_manager, context);

  SAGEFLOW_LOG_INFO("JOIN", "JoinOperator initialized with strategy config: subtask={}/{} shared_state={}",
                    context.getSubtaskIndex(),
                    context.getParallelism(),
                    result.use_shared_state);

  return result;
}

}  // namespace sageFlow

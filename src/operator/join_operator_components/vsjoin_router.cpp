#include "operator/join_operator_components/vsjoin_router.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "operator/join_operator_components/join_partitioner_factory.h"
#include "utils/logger.h"

namespace sageFlow {

std::vector<size_t> VSJoinRouter::computeTargetSubtasks(
    const Response& record,
    const RuntimeContext& context,
    const JoinStrategyConfig& config,
    bool use_strategy_config,
    int dimension,
    size_t subtask_index) {
  auto preferred_partitioner = JoinPartitionerFactory::createPreferred(
      config, use_strategy_config, dimension, static_cast<int>(context.getParallelism()));

  const size_t P = static_cast<size_t>(context.getParallelism());
  std::vector<size_t> target_subtasks;

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
      double avg_targets = (n > 0)
          ? static_cast<double>(total_targets) / static_cast<double>(n)
          : 0.0;
      SAGEFLOW_LOG_INFO(
          "VSJOIN_ROUTING",
          "p={} subtask={}/{} routed_records={} avg_targets={:.3f} multicast_events={} fallback_events={} active_targets={} min_per_target={} max_per_target={}",
          P,
          subtask_index,
          context.getParallelism(),
          n,
          avg_targets,
          mc,
          fb,
          nonzero,
          minc,
          maxc);
    }
  }

  return target_subtasks;
}

void VSJoinRouter::recordSubtaskDebugStats(
    int slot,
    int left_slot_id,
    size_t subtask_index,
    const RuntimeContext& context,
    JoinAlgorithm algorithm) {
  const bool vsjoin_debug_subtask = []() {
    if (const char* v = std::getenv("SAGEFLOW_VSJOIN_DEBUG_SUBTASK")) {
      return std::string(v) == "1";
    }
    return false;
  }();

  if (!vsjoin_debug_subtask || algorithm != JoinAlgorithm::VSJOIN) {
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
    if (slot == left_slot_id) {
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
  SAGEFLOW_LOG_INFO(
      "VSJOIN_SUBTASK",
      "p={} events={} active_subtasks={} total_in(L={},R={}) min_total_per_subtask={} max_total_per_subtask={}",
      p_runtime,
      n,
      active,
      total_left,
      total_right,
      min_total,
      max_total);
}

}  // namespace sageFlow

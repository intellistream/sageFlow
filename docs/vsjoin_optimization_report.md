# VSJoin Three Mechanisms — Optimization Report

**Date**: 2026-03-31  
**Branch**: `feat/vsjoin-exec-mechanisms`  
**Test Run**: `run_20260331_180712`

## 1. Three Mechanisms Implementation Summary

### Mechanism I: Bounded-Staleness Read/Write Decoupling
- **Config knobs**: `vsjoin_rebuild_interval_ms`, `vsjoin_max_staleness_ms`, `vsjoin_snapshot_filter_policy` (WINDOW_ONLY / MAX_STALENESS / AGGRESSIVE)
- **Rebuild loop**: `globalIndexRebuildLoop` — snapshot all partitions → filter by window + staleness → build IVF offline → atomic `replace_index_by_id` swap
- **Foreground isolation**: `ExecuteEager` reads via `concurrency_manager_->query_for_join()` which is lock-free on the index side
- **Metrics added**: `vsjoin_rebuild_count`, `vsjoin_rebuild_duration_ns`, `vsjoin_staleness_sum_ms`, `vsjoin_staleness_max_ms`, `vsjoin_records_filtered_staleness`, probe latency ring buffer

### Mechanism II: Budgeted Boundary Coverage Routing
- **Config knobs**: `vsjoin_route_mode` (UNICAST / BUDGETED / BROADCAST), `vsjoin_fanout_budget`
- **Routing**: `computeVSJoinLogicalPartitions` dispatches by mode; BUDGETED takes top-k from LSH candidates
- **Dedup**: `collectFromIndex` uses `unordered_set<uint64_t>` for UID dedup; `routeToPhysicalSubtasks` deduplicates physical targets
- **Sink dedup**: integration test shows `sink_dedup` column — confirms dedup is active (e.g., 2.3M duplicates at P=4)
- **Metrics added**: `vsjoin_route_total_probes`, `vsjoin_route_total_partitions`, `vsjoin_route_unicast/multicast/broadcast_count`, `vsjoin_dedup_candidates_before/after`

### Mechanism III: Predictable Skew Control Plane
- **RCU AssignmentTable**: Double-buffer with `atomic<vector<int>*>` — lock-free reads, mutex-protected batch writes, atomic pointer swap
- **LoadMonitor**: EWMA smoothing (α=0.2) for latency and backlog, cumulative counters for delta computation
- **Rebalance control**: `vsjoin_rebalance_imbalance_ratio` threshold, `vsjoin_rebalance_max_moves` cap, `vsjoin_rebalance_cooldown_ms` cooldown
- **Metrics added**: `vsjoin_rebalance_rounds`, `vsjoin_rebalance_moves`, `vsjoin_imbalance_ratio_x100`

## 2. Test Results & Baseline Comparison

### Small Dataset (data_size=500, dim=128)

| Method | P=1 Throughput | P=4 | P=8 | P=16 | Recall@P=1 | Recall@P=4 |
|--------|---------------|-----|-----|------|------------|------------|
| BruteForce | 5099 rec/s | 555 | 432 | 307 | 1.000 | 1.000 |
| IVF | 4328 | 619 | 437 | 357 | 1.000 | 1.000 |
| **VSJoin** | **448** | **562** | **319** | **304** | **1.000** | **0.825** |

### Standard Dataset (data_size=2000, dim=128)

| Method | P=1 Throughput | P=4 | P=8 | P=16 | Recall@P=1 |
|--------|---------------|-----|-----|------|------------|
| BruteForce | 722 rec/s | 105 | 246 | 232 | 1.000 |
| IVF | 752 | 108 | 244 | 238 | 0.979 |
| VSJoin | N/A (only 500-size test) | — | — | — | — |

## 3. Performance Bottleneck Analysis

### Key Findings

1. **VSJoin P=1 is 10× slower than BruteForce P=1** (448 vs 5099 rec/s on 500-size)
   - Root cause: two-tier index overhead — local partitioned index + global IVF rebuild adds latency per record
   - The global index rebuild thread adds overhead even at P=1

2. **Throughput degrades with higher parallelism** (all methods show this pattern)
   - P=4: bruteforce drops from 5099→555 (10.9×), ivf from 4328→619 (7×), vsjoin from 448→562 (improves slightly!)
   - P=8/16: further degradation across the board
   - Root cause: shared-nothing partitioned state means each subtask sees a subset of data, reducing per-probe candidate quality
   - Lock contention on shared resources (ConcurrencyManager index operations)

3. **VSJoin recall drops at P>1** (1.0→0.825 at P=4, 0.792 at P=8)
   - Root cause: partitioned state splits data across subtasks; local index only sees its partition; global index is periodically rebuilt and may be stale
   - Multicast/budgeted routing partially compensates but insufficient fanout budget

4. **Massive dedup overhead at high parallelism**
   - P=4: 4.68M emits → 2.34M after dedup (50% duplicates)
   - P=16: 5.15M emits → 2.29M after dedup (56% duplicates)
   - Indicates routing is sending records to too many partitions without proportional recall gain

## 4. Optimization Recommendations (Priority-Ordered)

### P0 — Critical (address recall regression)
1. **Increase global index rebuild frequency for small datasets**: Current 5000ms interval is too long relative to 500-record windows. Use adaptive interval based on record arrival rate.
2. **Warm-start global index**: Pre-populate global index at `open()` time instead of waiting for first rebuild cycle.
3. **Budgeted routing: increase default fanout_budget from 2 to min(3, num_partitions)**: Current budget=2 leaves boundary vectors under-covered at P≥8.

### P1 — High (improve throughput)
4. **Reduce per-record overhead in ExecuteEager**: Profile the hot path — `collectFromIndex` does 2+ index queries per record. Consider batching queries.
5. **Avoid UID-based `unordered_set` dedup on hot path**: Switch to pre-allocated bitset or robin-hood hash for better cache behavior.
6. **Lazy global index query**: Skip global index probe if local index returns sufficient candidates above threshold.

### P2 — Medium (scalability)
7. **Reader-writer lock on global index instead of atomic swap**: Current `replace_index_by_id` might cause brief query failures during swap.
8. **Partition-local IVF instead of BruteForce**: Current local index uses BruteForce which is O(n); switch to IVF when partition size > threshold.
9. **Adaptive rebalance trigger**: Use throughput stall detection instead of fixed imbalance ratio.

### P3 — Low (observability)
10. **Expose probe latency percentiles in test output**: Use the ring buffer to compute p50/p95/p99.
11. **Add per-subtask breakdown in CSV output**: Currently aggregated across subtasks.

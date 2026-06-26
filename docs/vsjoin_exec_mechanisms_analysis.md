# VSJoin Execution Mechanisms: Implementation Analysis

**Date**: 2026-03-31  
**Branch**: `feat/vsjoin-exec-mechanisms`  
**Issues**: #130 (Mechanism I), #131 (Mechanism II), #132 (Mechanism III)

## 1. Implementation Summary

### Mechanism I: Bounded-Staleness Read/Write Decoupling

**Files modified**: `join_strategy_config.h`, `join_operator.h`, `join_operator.cpp`

- Added `VSJoinSnapshotFilterPolicy` enum (`WINDOW_ONLY` / `MAX_STALENESS` / `AGGRESSIVE`)
- Added `vsjoin_max_staleness_ms` config knob (default 0 = disabled)
- `globalIndexRebuildLoop()` now:
  - Tracks rebuild duration via `last_rebuild_duration_ms_` (atomic)
  - Tracks wall-clock timestamp of each rebuild via `last_rebuild_timestamp_ms_`
  - Counts total rebuilds via `rebuild_count_`
  - Applies staleness filter when `vsjoin_snapshot_filter_policy != WINDOW_ONLY`
- Foreground latency already tracked via `reportVSJoinLoadSample()` (EWMA in LoadMonitor)
- **Foreground path remains lock-free**: insert/probe/verify never blocked by rebuild

### Mechanism II: Budgeted Boundary Coverage Routing

**Files modified**: `join_strategy_config.h`, `join_operator_vsjoin_routing.cpp`

- Added `VSJoinRouteMode` enum (`UNICAST` / `BUDGETED` / `BROADCAST`)
- Added `vsjoin_fanout_budget` config knob (default 2)
- `computeVSJoinLogicalPartitions()` now dispatches by route mode:
  - **UNICAST**: Single best partition (lowest routing cost, lowest recall)
  - **BUDGETED** (default): Top-k partitions up to `fanout_budget` (deterministic selection)
  - **BROADCAST**: All partitions (highest recall, highest cost)
- Deduplication at sink already exists via `sink_dedup` counter

### Mechanism III: Predictable Skew Control Plane

**Files modified**: `join_operator.h`, `join_operator.cpp`, `load_monitor.h`, `load_monitor.cpp`

- Added `vsjoin_rebalance_cooldown_ms` config (default 10000ms)
- Added `vsjoin_use_smoothed_load` config (default true)
- `maybeRebalanceVSJoinAssignment()` now:
  - Enforces cooldown period between rebalance rounds
  - Uses EWMA-smoothed latency (`avg_latency_ms`) as primary signal when `use_smoothed_load=true`
  - Falls back to delta-records mode when `use_smoothed_load=false`
- Added `getSmoothedLoad()` to LoadMonitor
- **PartitionAssignment atomic publish verified**: double-buffer with `atomic<vector<int>*>`, lock-free reads via `memory_order_acquire`, batched writes under mutex

## 2. Performance Test Results

All tests: 128-dim vectors, window=10000ms, step=10ms, similarity_threshold=0.8

### Throughput Comparison (records/sec, data_size=2000)

| Method       | p=1    | p=4    | p=8    |
|-------------|--------|--------|--------|
| BruteForce  | 982    | 305    | 253    |
| IVF         | 812    | 288    | 251    |
| VSJoin      | 519    | 501    | 312    |

### Recall Comparison (data_size=2000)

| Method       | p=1    | p=4    | p=8    |
|-------------|--------|--------|--------|
| BruteForce  | 1.000  | 1.000  | 1.000  |
| IVF         | 1.000  | 1.000  | 1.000  |
| VSJoin      | 1.000  | 0.827  | 0.825  |

## 3. Bottleneck Analysis

### Primary: Recall Degradation at p>1

VSJoin recall drops from 100% to ~82% at p≥4. Root cause: **cross-partition match loss**.

With LSH partitioning, vectors are routed to partitions by hash. Two similar vectors from opposite streams may land in different partitions. The local index only covers the current partition's records, and the global index (rebuilt periodically) lags behind.

- **Multicast (fanout_budget=2)** helps boundary vectors but doesn't solve the fundamental problem: similar vectors that hash to completely different partitions are never co-located.
- The ~82% recall ceiling is consistent across p=4,8 suggesting the loss is structural (hash collision misses), not concurrency-related.

### Secondary: No Throughput Scaling

VSJoin at p=1 achieves 519 rps, but p=4 only reaches 501 rps (0.96x). This is because:

1. **Per-target-subtask sequential processing**: Each record iterates over `target_subtasks` sequentially, calling `updateSideWithState()` and `executeJoinWithState()` for each target. With multicast, a single record may touch 2+ partitions.
2. **Global index rebuild contention**: The background rebuild thread takes snapshots from all partitions, creating memory pressure.
3. **IVF query overhead**: Each partition's local brute-force scan is cheap, but the global IVF query adds constant overhead per record.

### Tertiary: BruteForce/IVF Scaling Inversion

BruteForce and IVF show *negative* scaling (p=8 slower than p=1) because they use SharedWindowState with global read-write locks. This is a separate issue from VSJoin.

## 4. Recommended Next Steps

### P0: Fix Recall at High Parallelism
1. **Enhanced Global Index Usage**: Make the global index the primary recall source (it covers all partitions). Currently local+global are queried, but the global index is stale by up to `rebuild_interval_ms`. Reducing `vsjoin_rebuild_interval_ms` would help but at rebuild cost.
2. **Cross-partition probe**: After local query, probe neighboring partitions' local indices (already partially implemented via `local_num_probes_` in VSJoinMethod, but requires proper LSH-based neighbor selection).

### P1: Improve Throughput Scaling
1. **Async multicast dispatch**: Instead of sequentially processing each target subtask, dispatch inserts to a per-partition work queue.
2. **Pipeline rebuild**: Overlap rebuild with foreground processing using double-buffered global index (already partially done via `replace_index_by_id`).

### P2: Coverage-Overhead Metrics
1. Add per-probe counters: `routed_partitions_per_probe`, `duplicate_candidates_before_dedup`
2. Add recall/latency delta tracking across route modes for A/B experiments

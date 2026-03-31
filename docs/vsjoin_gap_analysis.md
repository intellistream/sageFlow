# VSJoin Three Mechanisms — Gap Analysis

**Date**: 2026-03-31
**Branch**: `feat/vsjoin-exec-mechanisms`

## Summary

All three mechanisms have core logic implemented. The primary gaps are in **metrics exposure and observability**.

## Mechanism I: Bounded-Staleness Read/Write Decoupling (Issue #130)

| Feature | Status | Notes |
|---------|--------|-------|
| Config knobs (rebuild_period, max_staleness, filter_policy) | ✅ Done | `vsjoin_rebuild_interval_ms`, `vsjoin_max_staleness_ms`, `vsjoin_snapshot_filter_policy` |
| globalIndexRebuildLoop: snapshot → filter → build IVF → atomic swap | ✅ Done | Properly implemented with validity filtering |
| Foreground path non-blocking during rebuild | ✅ Done | Foreground reads via `concurrency_manager_->query_for_join()` are lock-free |
| Rebuild duration/count metrics | ✅ Done | `last_rebuild_duration_ms_`, `rebuild_count_` atomics |
| **Staleness age distribution metric** | ❌ Missing | Need histogram of `(now - record.timestamp)` at rebuild time |
| **Foreground p50/p95/p99 latency under rebuild** | ❌ Missing | Need latency histogram in ExecuteEager, correlated with rebuild activity |
| Metrics structured output (JSON/CSV) | ❌ Missing | Atomic counters exist but not exposed to metrics collector |

## Mechanism II: Budgeted Boundary Coverage Routing (Issue #131)

| Feature | Status | Notes |
|---------|--------|-------|
| Config knobs (route_mode, fanout_budget) | ✅ Done | `vsjoin_route_mode`, `vsjoin_fanout_budget` |
| UNICAST / BUDGETED / BROADCAST dispatch | ✅ Done | `computeVSJoinLogicalPartitions` fully implements all three modes |
| Deterministic candidate selection (budget cap) | ✅ Done | Takes first `fanout_budget` candidates from LSH |
| Sink-level dedup | ✅ Done | `collectFromIndex` uses `unordered_set<uint64_t>` seen set |
| **Routed partitions per probe metric** | ❌ Missing | Need counter in routing path |
| **Duplicate candidates before dedup metric** | ❌ Missing | Need counter in collectFromIndex |
| **Recall/latency/cost delta metrics** | ❌ Missing | Need per-probe timing + recall estimation |

## Mechanism III: Predictable Skew Control Plane (Issue #132)

| Feature | Status | Notes |
|---------|--------|-------|
| RCU-style AssignmentTable (lock-free read, batched write, atomic publish) | ✅ Done | Double-buffer with `atomic<vector<int>*>` swap |
| LoadMonitor EWMA smoothing | ✅ Done | `K_LATENCY_EWMA_ALPHA=0.2`, `K_BACKLOG_EWMA_ALPHA=0.2` |
| Cumulative signals in LoadStat | ✅ Done | `total_records`, `total_latency_ms`, `total_backlog` |
| Rebalance cooldown | ✅ Done | `vsjoin_rebalance_cooldown_ms` enforced in `maybeRebalanceVSJoinAssignment` |
| Migration cap per round | ✅ Done | `vsjoin_rebalance_max_moves` |
| Trigger threshold (imbalance ratio) | ✅ Done | `vsjoin_rebalance_imbalance_ratio` |
| No synchronous full-state migration on foreground | ✅ Done | Only pointer swap, no data movement |
| **Imbalance ratio metric exposure** | ❌ Missing | Computed but only logged, not in metrics collector |

## Action Plan

1. Add `VSJoinMetrics` struct to centralize all mechanism metrics
2. Instrument `ExecuteEager` with probe-level latency tracking
3. Instrument routing path with partition count + dedup counters
4. Expose metrics through existing `JoinMetricsCollector` framework
5. Add staleness age histogram sampling in rebuild loop

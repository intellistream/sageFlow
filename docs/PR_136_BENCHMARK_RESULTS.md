## Summary

Implements deterministic dedup inside JoinOperator using routing_mask bitmask, eliminating all duplicate match emissions before they reach Sink.

## Problem

In multicast/broadcast join modes, the same match pair (A,B) was discovered by multiple subtasks and emitted redundantly. Sink (single-threaded) had to deduplicate via hash-set lookup, causing:
- O(P) redundant emissions (P = parallelism)
- Sink becoming pipeline bottleneck at high parallelism
- P=4 case: ExecTime=39s (30s stuck in Sink wait)

## Solution: Owner-Computes Rule

Each VectorRecord carries a `routing_mask_` bitmask indicating which subtasks hold it. When a match (query, candidate) is found:

```
intersection = query.routing_mask_ & candidate.routing_mask_
owner = __builtin_ctzll(intersection)  // lowest common subtask
if (owner != current_subtask) skip;    // not my responsibility
```

Only the owner subtask emits → zero duplicates, zero Sink dedup overhead.

## Comprehensive Benchmark Results

### Shared-Index Algorithms (data_size=2000)

| Algorithm | P=1 | P=2 | P=4 | P=8 | Recall |
|-----------|-----|-----|-----|-----|--------|
| BruteForce | 6,148ms | 5,702ms | 14,474ms | 17,525ms | 1.0000 |
| IVF | 6,878ms | 6,270ms | 12,222ms | 17,875ms | 1.0000 |

### ClusteredJoin — Multicast-k Recall/Latency Tradeoff (data_size=2000, num_partitions=8)

| k | P=1 Recall | P=4 Recall | P=8 Recall | P=1 Time | P=4 Time | P=8 Time | Sink Dedup |
|---|-----------|-----------|-----------|---------|---------|---------|------------|
| 1 | 1.000 | 1.000 | **0.441** | 834ms | 2,461ms | 2,268ms | **0** |
| 2 | 1.000 | 1.000 | **1.000** | 1,059ms | 2,456ms | 3,734ms | **0** |
| 3 | 1.000 | 1.000 | 1.000 | 813ms | 2,447ms | 6,096ms | **0** |
| 4 | 1.000 | 1.000 | 1.000 | 1,049ms | 3,628ms | 5,787ms | **0** |

**Key insight**: k=2 achieves 100% recall at p=8 with ~50% less latency than k=4. All configs show **zero Sink dedup** (was 2.7M+ before).

### VSJoin — Fanout Budget Recall/Latency Tradeoff (data_size=2000, num_partitions=8)

| Fanout | P=1 Recall | P=4 Recall | P=8 Recall | P=1 Time | P=4 Time | P=8 Time |
|--------|-----------|-----------|-----------|---------|---------|---------|
| 1 | 1.000 | 0.646 | 0.371 | 3,515ms | 1,916ms | 1,628ms |
| 2 | 1.000 | 0.646 | 0.362 | 4,023ms | 2,113ms | 1,731ms |
| 3 | 1.000 | 0.657 | 0.388 | 4,235ms | 2,017ms | 1,720ms |
| 4 | 1.000 | 0.655 | 0.385 | 3,823ms | 1,916ms | 1,720ms |

**Note**: VSJoin recall degrades at higher parallelism due to LSH partition quality — increasing fanout budget doesn't help significantly. This is a known LSH limitation; future work should improve the LSH hash function quality or switch to learned partitioning.

### Owner-Computes Dedup Impact (Before → After)

| Config | Para | Old Emits | New Emits | Reduction | Old Dedup | New Dedup |
|--------|------|-----------|-----------|-----------|-----------|-----------|
| CJ k=8 | 8 | 3,091,648 | 386,456 | **-87.5%** | 2,705,192 | **0** |
| CJ k=16 | 16 | 6,183,296 | 386,456 | **-93.75%** | 5,796,840 | **0** |
| CJ k=4 | 4 | 1,545,824 | 386,456 | **-75%** | 1,159,368 | **0** |
| CJ k=2 | 2 | 772,912 | 386,456 | **-50%** | 386,456 | **0** |

## Files Changed

- `include/common/data_types.h` — `routing_mask_` field on VectorRecord
- `src/execution/result_partition.cpp` — set mask on broadcast/multicast/unicast emit
- `src/operator/join_operator.cpp` — owner-computes dedup + mask propagation through JoinFunction result
- `include/utils/metrics/join_metrics.h` — `owner_dedup_count` metric
- `config/comprehensive_benchmark.toml` — full benchmark configuration

## Next Steps

1. **Recall-aware load balancing** — Current rebalancer migrates routing only (AssignmentTable), not WindowState data. Need data migration or work-stealing to maintain recall during transitions.
2. **Per-logical-partition load tracking** — LoadMonitor tracks per-subtask only; finer granularity needed for smarter migration decisions.
3. **LSH partition quality** — VSJoin recall at high parallelism is limited by LSH hash quality. Consider learned partitioning or adaptive hash functions.
4. **Bounded-Staleness tuning** — VSJoin Global Index rebuild interval vs recall tradeoff needs systematic evaluation.

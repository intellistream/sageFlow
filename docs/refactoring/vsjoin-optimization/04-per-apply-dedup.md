# Task 4: Per-Apply Dedup Implementation

## Problem
- Multicast causes same (left, right) match pair to be emitted multiple times within a single apply() call
- ClusteredJoin P=8 had 5.9M dedup (1.7x of useful output)
- All dedup was handled by Sink (global mutex + unordered_set)

## Solution: Intra-apply dedup
- Deduplicate within each `apply()` call using a local `unordered_set<uint64_t>`
- `combinedMatchId(left_uid, right_uid)` hashes the pair
- Only active when `parallelism_ > 1` (single-thread has no multicast dupes)
- Shared-state path (BruteForce/IVF) has NO dedup — they don't multicast

### Why NOT cross-apply (persistent) dedup
Initial implementation used per-subtask persistent dedup sets. This broke because:
- In streaming, the same (A,B) pair legitimately appears in different windows
- Persistent dedup blocked valid re-matches → BruteForce P=1 recall dropped to 0.88
- Fix: scope dedup to single apply() call only

## Results (dedup counts, before vs after)

| Algorithm | P | Dedup Before | Dedup After | Reduction |
|-----------|---|-------------|-------------|-----------|
| BF | 1 | 0 | 0 | — |
| BF | 4 | 2 | 0 | 100% |
| BF | 8 | 4 | 4 | 0% (from parallelism races, not multicast) |
| VSJ-LSH | 4 | 2998321→ | 3409599 | Increased (more rebuilds → more global results → more intra-apply dupes caught) |
| VSJ-LSH | 8 | 2339264→ | 2399282 | Similar |

## Note
Intra-apply dedup reduces Sink pressure but doesn't eliminate cross-subtask duplicates.
Cross-subtask dedup still handled by Sink.

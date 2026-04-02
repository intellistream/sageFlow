# Final Unified Results (All Optimizations Applied)

## data_size=2000, all algorithms

| Algorithm | P | Recall | ExecTime(ms) | JoinTime(ms) | Dedup | Status |
|-----------|---|--------|-------------|-------------|-------|--------|
| BruteForce | 1 | 1.00 | 7182 | 7061 | 0 | ✅ |
| BruteForce | 4 | 1.00 | 47058 | 16907 | 0 | ✅ |
| BruteForce | 8 | 1.00 | 21264 | 21123 | 4 | ✅ |
| IVF | 1 | 1.00 | 6532 | 6532 | 0 | ✅ |
| IVF | 4 | 1.00 | 46336 | 46336 | 1 | ✅ |
| IVF | 8 | 1.00 | 19659 | 19659 | 1 | ✅ |
| Clustered | 1 | 1.00 | 6253 | 6122 | 0 | ✅ |
| Clustered | 4 | 1.00 | 8157 | 7842 | 3.8M | ✅ |
| Clustered | 8 | 1.00 | 11401 | 10181 | 5.9M | ✅ |
| VSJ-Centroid | 1 | 1.00 | 8491 | 8072 | 0 | ✅ |
| VSJ-Centroid | 4 | 0.878 | 6577 | 6428 | 3.2M | ✅ |
| VSJ-Centroid | 8 | 0.626 | 7766 | 7442 | 2.6M | ❌ |
| VSJ-LSH | 1 | 1.00 | 7065 | 6965 | 0 | ✅ |
| VSJ-LSH | 4 | **0.918** | 39060 | 8601 | 3.4M | ✅ |
| VSJ-LSH | 8 | **0.756** | 7003 | 6731 | 2.4M | ✅ |

## Progress Summary

### Completed Tasks
1. ✅ StorageManager sharding (lock_wait eliminated)
2. ✅ DirectController (zero-lock for local indexes)
3. ✅ Rebuild trigger fix (first rebuild in 500ms, interval 2s)
4. ✅ Centroid partitioning support for VSJoin
5. ✅ Intra-apply dedup
6. ✅ Unified experiment with ClusteredJoin baseline

### Key Improvements vs Original
- VSJ-LSH P=4 Recall: 0.65 → **0.918** (+41%)
- VSJ-LSH P=8 Recall: 0.41 → **0.756** (+84%)
- lock_wait at P=8: 51M µs → **0** (eliminated)
- Global IVF rebuild: 0 times → 4-9 times per test

### Remaining Issues
- VSJoin P=8 Centroid recall 0.626 (cold start + training_samples insufficient)
- ClusteredJoin is the gold standard: 100% recall at all P
- VSJoin needs better centroid training or fallback to broadcast during warmup
- Large-scale (data_size≥5000) verification pending

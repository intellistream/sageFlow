# Task 2-3: Centroid Partitioning for VSJoin + Rebuild Fix

## Changes Made
1. **`join_operator.cpp` getPreferredPartitioner**: VSJoin now supports CENTROID strategy
   - Creates CentroidPartitioner with multicast enabled when partition_strategy=CENTROID
2. **`join_config_validator.cpp`**: Allow VSJoin + CENTROID combination
3. **`join_strategy_config.cpp`**: Same validation relaxation
4. **`join_operator.cpp` rebuild loop**: First rebuild after 500ms warmup (not full interval)
5. **`join_strategy_config.h`**: Default rebuild interval 5000→2000ms

## Results: VSJoin Centroid vs LSH (data_size=2000)

| Partition Method | P=1 Recall | P=4 Recall | P=8 Recall |
|-----------------|-----------|-----------|-----------|
| LSH (original) | 1.00 | 0.854 | 0.696 |
| **Centroid** | 1.00 | **0.921** | 0.554 |

### Analysis
- P=4: Centroid +7.8% recall over LSH
- P=8: Centroid worse — cold start issue (training_samples=200 too few)
- CentroidPartitioner needs sufficient warmup data to train meaningful centroids
- At P=8 with small data, centroids are poor quality → worse than LSH

### Root cause of P=8 Centroid regression
- Total records ~5200, split across 8 partitions = ~650/partition
- Training samples = 200, barely enough for 8 clusters
- CentroidPartitioner cold_start broadcasts during training → recall drop after training

### Next steps
- Increase training_samples for higher P
- Consider adaptive training_samples = f(P, data_size)
- Global IVF index provides fallback recall regardless of partition method

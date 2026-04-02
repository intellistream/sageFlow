# Task 1: Fix Rebuild Trigger Logic

## Problem
- `globalIndexRebuildLoop` first action was `sleep(interval_ms)` (5000ms default)
- At P≥4, join finished in <5s → rebuild never triggered → Global IVF empty
- Global IVF is queried in `VSJoinMethod::ExecuteEager` step 2 but returns nothing

## Changes
1. **`join_operator.cpp` rebuild loop**: First rebuild waits only 500ms warmup, 
   then uses configured interval. Sleep in 100ms chunks for responsive shutdown.
2. **`join_strategy_config.h`**: Default `vsjoin_rebuild_interval_ms` reduced from 5000→2000ms

## Results (data_size=2000)

| P | Before Fix Recall | After Fix Recall | Rebuild Count Before | Rebuild Count After |
|---|------------------|-----------------|---------------------|---------------------|
| 1 | 1.00 | 1.00 | 1 | 4 |
| 4 | 0.65 | **0.87** | 0 | **9** |
| 8 | 0.41 | **0.76** | 0 | 5+ |

## Verification
- Rebuild logs confirm Global IVF is being built with actual data
- P=4: "2600 unique left (1001 valid)" vs previous empty
- Recall improvement directly attributable to Global IVF contributing candidates

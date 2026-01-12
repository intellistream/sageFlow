# ClusteredJoin 实验测试指南

本文档描述了 ClusteredJoin 算法的系统性实验测试方案，用于分析 `overlap_ratio`、`parallelism` 和 `multicast_k` 参数对召回率和耗时的影响。

---

## 目录

1. [实验概述](#1-实验概述)
2. [实验 A：Overlap Ratio × Parallelism 矩阵实验](#2-实验-a-overlap-ratio--parallelism-矩阵实验)
3. [实验 B：Multicast K 扫描实验](#3-实验-b-multicast-k-扫描实验)
4. [测试配置生成](#4-测试配置生成)
5. [执行测试](#5-执行测试)
6. [结果可视化](#6-结果可视化)
7. [预期结果分析](#7-预期结果分析)

---

## 1. 实验概述

### 1.1 实验目标

1. **实验 A**：分析 `overlap_ratio` 和 `parallelism` 对召回率和耗时的联合影响
   - 固定 `k=0`（使用 overlap_ratio 阈值模式）
   - 变量：`p ∈ {1, 2, 4, 8, 16}`, `overlap_ratio ∈ {0.01, 0.02, 0.05, 0.1, 0.2}`
   - 输出：5×5 矩阵的召回率和耗时数据

2. **实验 B**：分析 `multicast_k` 对召回率和耗时的影响
   - 固定 `p=32`
   - 变量：`k ∈ {1, 2, 4, 8, 12, 24, 32}`
   - 输出：召回率和耗时随 k 变化的曲线

### 1.2 测试参数（通用）

| 参数 | 值 | 说明 |
|------|-----|------|
| `vector_dim` | 128 | 向量维度 |
| `data_sizes` | 2000 | 左右各 2000 条记录 |
| `window_size_ms` | 10000 | 窗口大小 10 秒 |
| `step_size_ms` | 10 | 滑动步长 10ms |
| `similarity_threshold` | 0.8 | 相似度阈值 |
| `clustered_index_type` | bruteforce | 分区内索引类型 |
| `clustered_training_samples` | 500 | 冷启动训练样本数 |
| `clustered_cold_start_enabled` | true | 启用冷启动广播 |
| `clustered_broadcast_dedup` | true | 启用广播去重 |

---

## 2. 实验 A：Overlap Ratio × Parallelism 矩阵实验

### 2.1 实验设计

**固定参数**：
- `clustered_multicast_k = 0`（使用 overlap_ratio 阈值模式）
- `clustered_multicast_enabled = true`

**变量**：
| 维度 | 取值 |
|------|------|
| `parallelism` (p) | 1, 2, 4, 8, 16 |
| `overlap_ratio` (r) | 0.01, 0.02, 0.05, 0.1, 0.2 |

**注意**：由于 ClusteredJoin 要求 `num_partitions == parallelism`，每个 (p, r) 组合需要独立的测试用例。

### 2.2 预期输出矩阵

**召回率矩阵**：
```
          r=0.01  r=0.02  r=0.05  r=0.10  r=0.20
p=1       [?]     [?]     [?]     [?]     [?]
p=2       [?]     [?]     [?]     [?]     [?]
p=4       [?]     [?]     [?]     [?]     [?]
p=8       [?]     [?]     [?]     [?]     [?]
p=16      [?]     [?]     [?]     [?]     [?]
```

**耗时矩阵**：
```
          r=0.01  r=0.02  r=0.05  r=0.10  r=0.20
p=1       [?]     [?]     [?]     [?]     [?]
p=2       [?]     [?]     [?]     [?]     [?]
...
```

### 2.3 可视化方案

推荐使用以下图表类型：

1. **热力图 (Heatmap)**：最直观地展示二维矩阵数据
   - X轴：overlap_ratio
   - Y轴：parallelism
   - 颜色：召回率/耗时
   - 优点：直观展示参数组合的效果

2. **折线图族 (Line Plot Family)**：展示趋势
   - 图1：固定 p，不同 r 的召回率曲线
   - 图2：固定 r，不同 p 的召回率曲线
   - 优点：清晰展示单一变量的影响

3. **3D 曲面图 (Surface Plot)**：展示整体趋势
   - X轴：parallelism
   - Y轴：overlap_ratio
   - Z轴：recall/time
   - 优点：展示参数空间的整体形状

---

## 3. 实验 B：Multicast K 扫描实验

### 3.1 实验设计

**固定参数**：
- `parallelism = 32`
- `num_partitions = 32`
- `overlap_ratio = 0.1`（当 k>0 时不使用）

**变量**：
| 参数 | 取值 |
|------|------|
| `clustered_multicast_k` (k) | 1, 2, 4, 8, 12, 24, 32 |

### 3.2 预期输出

| k | 覆盖率 | 预期召回率 | 预期耗时趋势 |
|---|--------|-----------|-------------|
| 1 | 3.1% | ~10-30% | 最低 |
| 2 | 6.3% | ~20-50% | 低 |
| 4 | 12.5% | ~40-70% | 中低 |
| 8 | 25% | ~60-85% | 中 |
| 12 | 37.5% | ~75-92% | 中高 |
| 24 | 75% | ~90-98% | 高 |
| 32 | 100% | ~95-100% | 最高 |

### 3.3 可视化方案

1. **双轴折线图**：同时展示召回率和耗时
   - 主 Y轴：召回率 (0-100%)
   - 副 Y轴：耗时 (ms)
   - X轴：multicast_k
   - 两条曲线：一条召回率，一条耗时

2. **条形图 + 折线图组合**：
   - 条形图：召回率
   - 折线图：耗时
   - 直观对比权衡

---

## 4. 测试配置生成

### 4.1 实验 A 配置模板

将以下配置添加到 `config/clustered_experiment.toml`：

```toml
# ==================== 实验 A: Overlap Ratio × Parallelism 矩阵 ====================
# 固定 k=0，变化 p 和 overlap_ratio

# --- p=1 系列 ---
[[test_case]]
name = "exp_a_p1_r001"
description = "Experiment A: p=1, overlap_ratio=0.01"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 1
clustered_multicast_k = 0
clustered_overlap_ratio = 0.01
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [1]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p1_r002"
description = "Experiment A: p=1, overlap_ratio=0.02"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 1
clustered_multicast_k = 0
clustered_overlap_ratio = 0.02
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [1]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p1_r005"
description = "Experiment A: p=1, overlap_ratio=0.05"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 1
clustered_multicast_k = 0
clustered_overlap_ratio = 0.05
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [1]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p1_r010"
description = "Experiment A: p=1, overlap_ratio=0.10"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 1
clustered_multicast_k = 0
clustered_overlap_ratio = 0.10
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [1]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p1_r020"
description = "Experiment A: p=1, overlap_ratio=0.20"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 1
clustered_multicast_k = 0
clustered_overlap_ratio = 0.20
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [1]
expected_min_recall = 0.10
enabled = true

# --- p=2 系列 ---
[[test_case]]
name = "exp_a_p2_r001"
description = "Experiment A: p=2, overlap_ratio=0.01"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 2
clustered_multicast_k = 0
clustered_overlap_ratio = 0.01
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [2]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p2_r002"
description = "Experiment A: p=2, overlap_ratio=0.02"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 2
clustered_multicast_k = 0
clustered_overlap_ratio = 0.02
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [2]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p2_r005"
description = "Experiment A: p=2, overlap_ratio=0.05"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 2
clustered_multicast_k = 0
clustered_overlap_ratio = 0.05
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [2]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p2_r010"
description = "Experiment A: p=2, overlap_ratio=0.10"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 2
clustered_multicast_k = 0
clustered_overlap_ratio = 0.10
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [2]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p2_r020"
description = "Experiment A: p=2, overlap_ratio=0.20"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 2
clustered_multicast_k = 0
clustered_overlap_ratio = 0.20
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [2]
expected_min_recall = 0.10
enabled = true

# --- p=4 系列 ---
[[test_case]]
name = "exp_a_p4_r001"
description = "Experiment A: p=4, overlap_ratio=0.01"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 4
clustered_multicast_k = 0
clustered_overlap_ratio = 0.01
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [4]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p4_r002"
description = "Experiment A: p=4, overlap_ratio=0.02"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 4
clustered_multicast_k = 0
clustered_overlap_ratio = 0.02
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [4]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p4_r005"
description = "Experiment A: p=4, overlap_ratio=0.05"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 4
clustered_multicast_k = 0
clustered_overlap_ratio = 0.05
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [4]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p4_r010"
description = "Experiment A: p=4, overlap_ratio=0.10"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 4
clustered_multicast_k = 0
clustered_overlap_ratio = 0.10
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [4]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p4_r020"
description = "Experiment A: p=4, overlap_ratio=0.20"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 4
clustered_multicast_k = 0
clustered_overlap_ratio = 0.20
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [4]
expected_min_recall = 0.10
enabled = true

# --- p=8 系列 ---
[[test_case]]
name = "exp_a_p8_r001"
description = "Experiment A: p=8, overlap_ratio=0.01"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 8
clustered_multicast_k = 0
clustered_overlap_ratio = 0.01
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [8]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p8_r002"
description = "Experiment A: p=8, overlap_ratio=0.02"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 8
clustered_multicast_k = 0
clustered_overlap_ratio = 0.02
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [8]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p8_r005"
description = "Experiment A: p=8, overlap_ratio=0.05"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 8
clustered_multicast_k = 0
clustered_overlap_ratio = 0.05
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [8]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p8_r010"
description = "Experiment A: p=8, overlap_ratio=0.10"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 8
clustered_multicast_k = 0
clustered_overlap_ratio = 0.10
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [8]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p8_r020"
description = "Experiment A: p=8, overlap_ratio=0.20"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 8
clustered_multicast_k = 0
clustered_overlap_ratio = 0.20
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [8]
expected_min_recall = 0.10
enabled = true

# --- p=16 系列 ---
[[test_case]]
name = "exp_a_p16_r001"
description = "Experiment A: p=16, overlap_ratio=0.01"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 16
clustered_multicast_k = 0
clustered_overlap_ratio = 0.01
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [16]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p16_r002"
description = "Experiment A: p=16, overlap_ratio=0.02"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 16
clustered_multicast_k = 0
clustered_overlap_ratio = 0.02
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [16]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p16_r005"
description = "Experiment A: p=16, overlap_ratio=0.05"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 16
clustered_multicast_k = 0
clustered_overlap_ratio = 0.05
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [16]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p16_r010"
description = "Experiment A: p=16, overlap_ratio=0.10"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 16
clustered_multicast_k = 0
clustered_overlap_ratio = 0.10
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [16]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_a_p16_r020"
description = "Experiment A: p=16, overlap_ratio=0.20"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 16
clustered_multicast_k = 0
clustered_overlap_ratio = 0.20
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [16]
expected_min_recall = 0.10
enabled = true
```

### 4.2 实验 B 配置模板

将以下配置添加到 `config/clustered_experiment.toml`（续）：

```toml
# ==================== 实验 B: Multicast K 扫描 (p=32) ====================

[[test_case]]
name = "exp_b_k1"
description = "Experiment B: p=32, k=1 (unicast only)"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 32
clustered_multicast_k = 1
clustered_overlap_ratio = 0.1
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [32]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_b_k2"
description = "Experiment B: p=32, k=2"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 32
clustered_multicast_k = 2
clustered_overlap_ratio = 0.1
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [32]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_b_k4"
description = "Experiment B: p=32, k=4"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 32
clustered_multicast_k = 4
clustered_overlap_ratio = 0.1
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [32]
expected_min_recall = 0.10
enabled = true

[[test_case]]
name = "exp_b_k8"
description = "Experiment B: p=32, k=8"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 32
clustered_multicast_k = 8
clustered_overlap_ratio = 0.1
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [32]
expected_min_recall = 0.30
enabled = true

[[test_case]]
name = "exp_b_k12"
description = "Experiment B: p=32, k=12"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 32
clustered_multicast_k = 12
clustered_overlap_ratio = 0.1
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [32]
expected_min_recall = 0.50
enabled = true

[[test_case]]
name = "exp_b_k24"
description = "Experiment B: p=32, k=24"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 32
clustered_multicast_k = 24
clustered_overlap_ratio = 0.1
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [32]
expected_min_recall = 0.85
enabled = true

[[test_case]]
name = "exp_b_k32"
description = "Experiment B: p=32, k=32 (full coverage)"
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 32
clustered_multicast_k = 32
clustered_overlap_ratio = 0.1
clustered_index_type = "bruteforce"
clustered_multicast_enabled = true
clustered_training_samples = 500
clustered_cold_start_enabled = true
clustered_broadcast_dedup = true
window_size_ms = 10000
step_size_ms = 10
data_sizes = [2000]
parallelism = [32]
expected_min_recall = 0.95
enabled = true
```

---

## 5. 执行测试

### 5.1 准备工作

```bash
# 1. 确保项目已构建
cd /root/sageFlow
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -j $(nproc)

# 2. 创建实验配置文件
# 将上述配置复制到 config/clustered_experiment.toml
```

### 5.2 运行实验 A

```bash
# 运行所有实验 A 测试用例（k=0 矩阵实验）
python scripts/run_integration_test.py \
    --methods clustered_join \
    --config config/clustered_experiment.toml \
    --output-dir test/result/exp_a \
    --verbose

# 或者使用 gtest_filter 运行特定测试
./build/bin/test_join_baseline_integration --gtest_filter="*exp_a*"
```

### 5.3 运行实验 B

```bash
# 运行所有实验 B 测试用例（k 扫描实验）
python scripts/run_integration_test.py \
    --methods clustered_join \
    --config config/clustered_experiment.toml \
    --output-dir test/result/exp_b \
    --verbose

# 或者使用 gtest_filter 运行特定测试
./build/bin/test_join_baseline_integration --gtest_filter="*exp_b*"
```

### 5.4 运行全部实验

```bash
# 一次性运行所有实验
./build/bin/test_join_baseline_integration --gtest_filter="*exp_a*:*exp_b*"

# 带超时控制
timeout 7200 ./build/bin/test_join_baseline_integration --gtest_filter="*exp_a*:*exp_b*"
```

---

## 6. 结果可视化

### 6.1 可视化脚本

创建 `scripts/visualize_clustered_experiment.py`：

```python
#!/usr/bin/env python3
"""
ClusteredJoin 实验结果可视化脚本

生成以下图表：
1. 实验 A：Overlap Ratio × Parallelism 热力图（召回率和耗时）
2. 实验 A：固定 p 的多条折线图
3. 实验 B：Multicast K 双轴折线图
"""

import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import seaborn as sns
from typing import Dict, List, Tuple
import argparse

# 配置
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial']
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['figure.dpi'] = 150

def load_experiment_results(result_dir: str) -> Dict:
    """加载实验结果"""
    results = {}
    result_path = Path(result_dir)
    
    for json_file in result_path.glob('*.json'):
        with open(json_file, 'r') as f:
            data = json.load(f)
            results[json_file.stem] = data
    
    return results

def extract_exp_a_data(results: Dict) -> Tuple[np.ndarray, np.ndarray]:
    """提取实验 A 数据，返回召回率和耗时矩阵"""
    parallelisms = [1, 2, 4, 8, 16]
    ratios = [0.01, 0.02, 0.05, 0.10, 0.20]
    
    recall_matrix = np.zeros((len(parallelisms), len(ratios)))
    time_matrix = np.zeros((len(parallelisms), len(ratios)))
    
    for report_data in results.values():
        if 'detailed_results' not in report_data:
            continue
        for result in report_data['detailed_results']:
            name = result.get('test_case_name', '')
            if not name.startswith('exp_a_'):
                continue
            
            # 解析 p 和 r
            # 格式: exp_a_p{p}_r{r*100}
            parts = name.split('_')
            p = int(parts[2][1:])  # p1 -> 1
            r_str = parts[3][1:]   # r001 -> 001
            r = int(r_str) / 100 if len(r_str) == 3 else int(r_str) / 10
            
            if p in parallelisms and r in ratios:
                i = parallelisms.index(p)
                j = ratios.index(r)
                recall_matrix[i, j] = result.get('recall', 0)
                time_matrix[i, j] = result.get('total_time_ms', 0)
    
    return recall_matrix, time_matrix, parallelisms, ratios

def extract_exp_b_data(results: Dict) -> Tuple[List, List, List]:
    """提取实验 B 数据，返回 k 值、召回率、耗时列表"""
    k_values = []
    recalls = []
    times = []
    
    for report_data in results.values():
        if 'detailed_results' not in report_data:
            continue
        for result in report_data['detailed_results']:
            name = result.get('test_case_name', '')
            if not name.startswith('exp_b_k'):
                continue
            
            # 解析 k
            k = int(name.split('_')[2][1:])  # exp_b_k4 -> 4
            k_values.append(k)
            recalls.append(result.get('recall', 0))
            times.append(result.get('total_time_ms', 0))
    
    # 按 k 排序
    sorted_data = sorted(zip(k_values, recalls, times))
    k_values = [x[0] for x in sorted_data]
    recalls = [x[1] for x in sorted_data]
    times = [x[2] for x in sorted_data]
    
    return k_values, recalls, times

def plot_exp_a_heatmaps(recall_matrix, time_matrix, parallelisms, ratios, output_dir):
    """绘制实验 A 热力图"""
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # 召回率热力图
    ax1 = axes[0]
    im1 = ax1.imshow(recall_matrix, cmap='RdYlGn', aspect='auto', vmin=0, vmax=1)
    ax1.set_xticks(range(len(ratios)))
    ax1.set_xticklabels([f'{r:.2f}' for r in ratios])
    ax1.set_yticks(range(len(parallelisms)))
    ax1.set_yticklabels([f'p={p}' for p in parallelisms])
    ax1.set_xlabel('Overlap Ratio')
    ax1.set_ylabel('Parallelism')
    ax1.set_title('Recall Rate Heatmap')
    plt.colorbar(im1, ax=ax1, label='Recall')
    
    # 添加数值标注
    for i in range(len(parallelisms)):
        for j in range(len(ratios)):
            text = ax1.text(j, i, f'{recall_matrix[i, j]:.2f}',
                          ha='center', va='center', color='black', fontsize=9)
    
    # 耗时热力图
    ax2 = axes[1]
    im2 = ax2.imshow(time_matrix, cmap='YlOrRd', aspect='auto')
    ax2.set_xticks(range(len(ratios)))
    ax2.set_xticklabels([f'{r:.2f}' for r in ratios])
    ax2.set_yticks(range(len(parallelisms)))
    ax2.set_yticklabels([f'p={p}' for p in parallelisms])
    ax2.set_xlabel('Overlap Ratio')
    ax2.set_ylabel('Parallelism')
    ax2.set_title('Execution Time Heatmap')
    plt.colorbar(im2, ax=ax2, label='Time (ms)')
    
    # 添加数值标注
    for i in range(len(parallelisms)):
        for j in range(len(ratios)):
            text = ax2.text(j, i, f'{time_matrix[i, j]:.0f}',
                          ha='center', va='center', color='black', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/exp_a_heatmaps.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_dir}/exp_a_heatmaps.png")

def plot_exp_a_lines(recall_matrix, time_matrix, parallelisms, ratios, output_dir):
    """绘制实验 A 折线图"""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    colors = plt.cm.viridis(np.linspace(0, 1, len(parallelisms)))
    
    # 图1：固定 p，不同 r 的召回率曲线
    ax1 = axes[0, 0]
    for i, p in enumerate(parallelisms):
        ax1.plot(ratios, recall_matrix[i, :], 'o-', color=colors[i], 
                label=f'p={p}', linewidth=2, markersize=8)
    ax1.set_xlabel('Overlap Ratio')
    ax1.set_ylabel('Recall')
    ax1.set_title('Recall vs Overlap Ratio (Fixed Parallelism)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(0, 1.05)
    
    # 图2：固定 r，不同 p 的召回率曲线
    ax2 = axes[0, 1]
    ratio_colors = plt.cm.plasma(np.linspace(0, 1, len(ratios)))
    for j, r in enumerate(ratios):
        ax2.plot(parallelisms, recall_matrix[:, j], 's-', color=ratio_colors[j],
                label=f'r={r:.2f}', linewidth=2, markersize=8)
    ax2.set_xlabel('Parallelism')
    ax2.set_ylabel('Recall')
    ax2.set_title('Recall vs Parallelism (Fixed Overlap Ratio)')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim(0, 1.05)
    
    # 图3：固定 p，不同 r 的耗时曲线
    ax3 = axes[1, 0]
    for i, p in enumerate(parallelisms):
        ax3.plot(ratios, time_matrix[i, :], 'o-', color=colors[i], 
                label=f'p={p}', linewidth=2, markersize=8)
    ax3.set_xlabel('Overlap Ratio')
    ax3.set_ylabel('Time (ms)')
    ax3.set_title('Time vs Overlap Ratio (Fixed Parallelism)')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # 图4：固定 r，不同 p 的耗时曲线
    ax4 = axes[1, 1]
    for j, r in enumerate(ratios):
        ax4.plot(parallelisms, time_matrix[:, j], 's-', color=ratio_colors[j],
                label=f'r={r:.2f}', linewidth=2, markersize=8)
    ax4.set_xlabel('Parallelism')
    ax4.set_ylabel('Time (ms)')
    ax4.set_title('Time vs Parallelism (Fixed Overlap Ratio)')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/exp_a_lines.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_dir}/exp_a_lines.png")

def plot_exp_b(k_values, recalls, times, output_dir):
    """绘制实验 B 双轴折线图"""
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    color1 = '#2196F3'
    color2 = '#F44336'
    
    # 召回率（主 Y 轴）
    ax1.set_xlabel('Multicast K', fontsize=12)
    ax1.set_ylabel('Recall', color=color1, fontsize=12)
    line1 = ax1.plot(k_values, recalls, 'o-', color=color1, linewidth=2.5, 
                     markersize=10, label='Recall')
    ax1.tick_params(axis='y', labelcolor=color1)
    ax1.set_ylim(0, 1.05)
    ax1.grid(True, alpha=0.3)
    
    # 耗时（副 Y 轴）
    ax2 = ax1.twinx()
    ax2.set_ylabel('Time (ms)', color=color2, fontsize=12)
    line2 = ax2.plot(k_values, times, 's--', color=color2, linewidth=2.5, 
                     markersize=10, label='Time')
    ax2.tick_params(axis='y', labelcolor=color2)
    
    # 合并图例
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='center right', fontsize=11)
    
    # 添加覆盖率标注
    ax3 = ax1.twiny()
    ax3.set_xlim(ax1.get_xlim())
    coverage_ticks = [k / 32 * 100 for k in k_values]
    ax3.set_xticks(k_values)
    ax3.set_xticklabels([f'{c:.1f}%' for c in coverage_ticks])
    ax3.set_xlabel('Coverage Rate (k/p)', fontsize=10)
    
    plt.title('Multicast K vs Recall/Time (p=32)', fontsize=14, pad=20)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/exp_b_multicast_k.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_dir}/exp_b_multicast_k.png")
    
    # 额外生成条形图+折线图组合
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    x = np.arange(len(k_values))
    width = 0.6
    
    bars = ax1.bar(x, recalls, width, color=color1, alpha=0.7, label='Recall')
    ax1.set_xlabel('Multicast K', fontsize=12)
    ax1.set_ylabel('Recall', color=color1, fontsize=12)
    ax1.set_xticks(x)
    ax1.set_xticklabels([f'k={k}' for k in k_values])
    ax1.set_ylim(0, 1.1)
    ax1.tick_params(axis='y', labelcolor=color1)
    
    ax2 = ax1.twinx()
    line = ax2.plot(x, times, 's-', color=color2, linewidth=2.5, 
                    markersize=10, label='Time')
    ax2.set_ylabel('Time (ms)', color=color2, fontsize=12)
    ax2.tick_params(axis='y', labelcolor=color2)
    
    # 在条形图上标注数值
    for bar, recall in zip(bars, recalls):
        height = bar.get_height()
        ax1.annotate(f'{recall:.2f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=9)
    
    plt.title('Multicast K: Recall vs Time Trade-off (p=32)', fontsize=14)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/exp_b_bar_line.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_dir}/exp_b_bar_line.png")

def main():
    parser = argparse.ArgumentParser(description='Visualize ClusteredJoin experiment results')
    parser.add_argument('--input-dir', '-i', type=str, required=True,
                       help='Directory containing experiment results')
    parser.add_argument('--output-dir', '-o', type=str, default=None,
                       help='Output directory for charts (default: same as input)')
    args = parser.parse_args()
    
    output_dir = args.output_dir or args.input_dir
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    print(f"Loading results from: {args.input_dir}")
    results = load_experiment_results(args.input_dir)
    
    if not results:
        print("No results found!")
        return 1
    
    print(f"Loaded {len(results)} result files")
    
    # 绘制实验 A 图表
    try:
        recall_matrix, time_matrix, parallelisms, ratios = extract_exp_a_data(results)
        if recall_matrix.sum() > 0:
            plot_exp_a_heatmaps(recall_matrix, time_matrix, parallelisms, ratios, output_dir)
            plot_exp_a_lines(recall_matrix, time_matrix, parallelisms, ratios, output_dir)
        else:
            print("No Experiment A data found")
    except Exception as e:
        print(f"Error generating Experiment A charts: {e}")
    
    # 绘制实验 B 图表
    try:
        k_values, recalls, times = extract_exp_b_data(results)
        if k_values:
            plot_exp_b(k_values, recalls, times, output_dir)
        else:
            print("No Experiment B data found")
    except Exception as e:
        print(f"Error generating Experiment B charts: {e}")
    
    print(f"\nAll charts saved to: {output_dir}")
    return 0

if __name__ == '__main__':
    exit(main())
```

### 6.2 运行可视化

```bash
# 生成所有图表
python scripts/visualize_clustered_experiment.py \
    --input-dir test/result/integration \
    --output-dir test/result/charts

# 或者分别处理实验 A 和实验 B
python scripts/visualize_clustered_experiment.py \
    --input-dir test/result/exp_a \
    --output-dir test/result/charts/exp_a

python scripts/visualize_clustered_experiment.py \
    --input-dir test/result/exp_b \
    --output-dir test/result/charts/exp_b
```

---

## 7. 预期结果分析

### 7.1 实验 A 预期趋势

**召回率趋势**：
- **随 overlap_ratio 增加**：召回率应单调递增
  - r=0.01：召回率最低（~10-30%）
  - r=0.20：召回率最高（~90-100%）
- **随 parallelism 增加**：召回率可能略有下降
  - 原因：更多分区意味着每个分区数据更少，边界效应更明显
  - 但如果冷启动广播有效，召回率应保持稳定

**耗时趋势**：
- **随 overlap_ratio 增加**：耗时应略有增加
  - 原因：更多向量被复制到多个分区
- **随 parallelism 增加**：耗时应显著下降
  - 原因：更多线程并行处理

### 7.2 实验 B 预期趋势

| k | 覆盖率 | 预期召回率 | 预期耗时趋势 |
|---|--------|-----------|-------------|
| 1 | 3.1% | 10-30% | 最低基准 |
| 2 | 6.3% | 20-50% | +5-10% |
| 4 | 12.5% | 40-70% | +15-25% |
| 8 | 25% | 60-85% | +30-50% |
| 12 | 37.5% | 75-92% | +50-70% |
| 24 | 75% | 90-98% | +100-150% |
| 32 | 100% | 95-100% | +200%+ |

**关键洞察**：
- 召回率与 k 呈非线性增长（早期增长快，后期趋于饱和）
- 耗时与 k 呈接近线性增长（每增加一个分区，耗时增加约 3%）
- **最佳 k 值**：取决于召回率和耗时的权衡，通常 k=8-12 是较好的平衡点

### 7.3 结果验证检查清单

- [ ] 实验 A 召回率热力图：右上角（高 r，高 p）应为绿色
- [ ] 实验 A 耗时热力图：左下角（低 r，低 p）应为最深色
- [ ] 实验 B 召回率曲线：应单调递增且趋于饱和
- [ ] 实验 B 耗时曲线：应单调递增
- [ ] k=32 时召回率应接近 100%
- [ ] p=1 时所有 overlap_ratio 的召回率应相同（因为只有 1 个分区）

---

## 附录：快速命令参考

```bash
# === 构建 ===
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON && cmake --build build -j

# === 运行实验 A（全部） ===
./build/bin/test_join_baseline_integration --gtest_filter="*exp_a*" 2>&1 | tee exp_a.log

# === 运行实验 B（全部） ===
./build/bin/test_join_baseline_integration --gtest_filter="*exp_b*" 2>&1 | tee exp_b.log

# === 运行单个测试 ===
./build/bin/test_join_baseline_integration --gtest_filter="*exp_a_p8_r010*"
./build/bin/test_join_baseline_integration --gtest_filter="*exp_b_k8*"

# === 可视化 ===
python scripts/visualize_clustered_experiment.py -i test/result/integration -o test/result/charts

# === 查看结果 ===
cat test/result/integration/*report*.json | jq '.detailed_results[] | {name: .test_case_name, recall: .recall, time: .total_time_ms}'
```

---

## 变更日志

| 日期 | 版本 | 变更内容 |
|------|------|----------|
| 2026-01-05 | v1.0 | 初始版本，包含实验 A 和实验 B 设计 |

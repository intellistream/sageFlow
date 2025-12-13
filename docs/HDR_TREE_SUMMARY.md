# HDR-Tree 索引功能与测试总结

本文档总结了 `HDRTree` (High-Dimensional R-Tree) 索引的现有功能实现、单元测试覆盖范围以及完整的系统测试设置。

## 1. 功能实现 (HDRTree)

`HDRTree` 是一个结合了 PCA 降维和 R-Tree 空间索引的高维向量索引结构。它旨在通过降低维度来加速高维向量的检索，同时利用 R-Tree 的空间划分能力进行高效的范围查询和 k-NN 查询。

### 核心组件

*   **类名**: `sageFlow::HDRTree`
*   **头文件**: `include/index/hdr_tree.h`
*   **源文件**: `src/index/hdr_tree.cpp`

### 主要特性

1.  **PCA 降维 (Dimensionality Reduction)**:
    *   集成 `compute_engine/pca.h`。
    *   支持自动训练 (`tryAutoTrainPCA`)：在插入数据时自动收集样本，达到阈值（默认 500）后触发 PCA 训练。
    *   将高维向量投影到低维空间（默认 64 维）以构建 R-Tree。

2.  **R-Tree 空间索引**:
    *   **节点结构**: `RTreeNode` 包含 MBR (Minimum Bounding Rectangle) 和子节点/数据条目。
    *   **分裂策略**: 实现了 Quadratic Split 算法，优化节点分裂时的 MBR 面积。
    *   **插入策略**: 使用 `ChooseLeaf` 算法选择最佳插入路径，最小化 MBR 扩张。

3.  **查询支持**:
    *   **k-NN 查询 (`query`)**: 查找最近的 k 个邻居。
    *   **范围查询 (`query_for_join`)**: 查找给定阈值内的所有向量，专为 Join 操作优化。
    *   **两阶段验证**: 先在 R-Tree (低维空间) 中进行粗筛，再利用原始向量进行精确距离计算和验证。

4.  **动态更新**:
    *   支持 `insert` (插入) 和 `erase` (删除) 操作。
    *   删除操作会更新 R-Tree 结构（目前实现为标记删除或重构，具体取决于 R-Tree 的实现细节）。

---

## 2. 单元测试 (Unit Tests)

单元测试主要位于 `test/UnitTest/` 目录下。值得注意的是，现有的单元测试主要针对 `HDRForest`（它是 `HDRTree` 的上层封装，管理多个 `LocalHDRTree`），但也间接或直接验证了 `HDRTree` 的核心逻辑。

### 测试文件

*   **`test/UnitTest/test_hdr_tree.cpp`**:
    *   **测试对象**: `HDRForest` (内部使用 `HDRTree`)。
    *   **覆盖场景**:
        *   `InsertionAndExactQuery`: 基础插入和精确匹配查询。
        *   `BatchInsertion`: 批量插入和 Top-K 查询。
        *   `Erase`: 删除记录并验证查询结果不再包含该记录。
        *   `QueryForJoin`: 验证基于阈值的范围查询。
        *   `BuildForestAndRouting`: 测试森林构建和跨分区路由。
        *   `PruningLogic`: 验证基于距离的剪枝逻辑。

*   **`test/UnitTest/test_hdr_tree_minimal.cpp`**:
    *   **测试对象**: `HDRForest`。
    *   **覆盖场景**: 最小化的插入和查询流程，用于快速验证基本可用性。

*   **`test/UnitTest/test_hdr_rknn.cpp`**:
    *   **测试对象**: `HDRForest`。
    *   **覆盖场景**: 验证 RkNN (Reverse k-NN) 在删除节点时的更新逻辑，确保邻居关系的一致性。

---

## 3. 系统测试 (System Tests)

系统测试通过 `test_join_datasource_modes` 二进制文件进行，配置文件为 `config/perf_join_datasource_modes.toml`。该测试直接实例化 `HDRTree` (通过 `ConcurrencyManager` 的 `IndexType::HDRTree`)，验证其在真实 Join 算子中的表现。

### 测试配置

配置文件支持三种模式，通过注释切换：

1.  **Mode 1: Generate -> Save -> Load (Random Data)**
    *   **流程**: 生成随机向量 -> 保存到磁盘 (.fvecs) -> 加载到内存 -> 构建索引 -> 执行 Join。
    *   **目的**: 验证完整的数据持久化、加载和索引构建流程的稳定性。
    *   **验证点**: 确保文件 I/O 无误，索引能正确处理重新加载的数据。

2.  **Mode 2: Direct Load (SIFT Dataset)**
    *   **流程**: 直接加载 SIFT Small 数据集 -> 构建索引 -> 执行 Join。
    *   **目的**: 在真实数据集上评估 `HDRTree` 的准确性 (Recall) 和性能。
    *   **验证点**: 召回率 (Recall) 和 F1 Score。

3.  **Mode 3: Generate -> Direct Use (In-Memory)**
    *   **流程**: 内存中生成随机向量 -> 直接传递给 Join 算子 (无磁盘 I/O)。
    *   **目的**: 排除 I/O 干扰，纯粹测试索引和 Join 算法的内存性能和逻辑正确性。
    *   **验证点**: 内存路径的连通性和基本功能。

### 最近测试结果 (2025-12-09)

在 `parallelism = [1, 2, 4]` 下对 `HDRTree` 进行了全面测试：

| 模式 | 并行度 | 结果 (Recall) | 状态 | 说明 |
| :--- | :--- | :--- | :--- | :--- |
| **Mode 2 (SIFT)** | 1 | 0.979 | ✅ PASS | 高召回率，验证了 PCA+RTree 在真实数据上的有效性 |
| **Mode 2 (SIFT)** | 2 | 0.971 | ✅ PASS | 并行化对召回率影响极小 |
| **Mode 2 (SIFT)** | 4 | 0.957 | ✅ PASS | 保持高召回率 |
| **Mode 1 (Random)** | 1, 2, 4 | 1.000 | ✅ PASS | 随机数据分布均匀，容易达到 100% 召回 |
| **Mode 3 (Memory)** | 1, 2, 4 | ~1.000 | ✅ PASS | 内存路径功能正常 |


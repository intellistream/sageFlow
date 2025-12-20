# FAISS 集成工作总结

**日期:** 2025-12-20
**状态:** 已实现并完成基准测试 (侧重 IVF)

## 1. 功能实现

本项目已集成 **FAISS (Facebook AI Similarity Search)** 作为向量相似度连接操作的后端。此集成旨在利用 FAISS 优化的索引结构（IVF, HNSW）来实现高性能的相似度搜索。

### 1.1 支持的算法
新增了两种 `JoinAlgorithm` 类型：
*   **`FAISS_IVF`**: 使用 FAISS 的倒排文件索引 (Inverted File Index) 实现。
*   **`FAISS_HNSW`**: 使用 FAISS 的层次化导航小世界图 (Hierarchical Navigable Small World) 实现。

### 1.2 混合并行管理 (Hybrid Parallelism)
实现了一个关键特性来智能处理 **混合并行**：
*   **问题**: 当运行多个 SageFlow 算子（算子并行度 > 1）且每个算子内部都使用开启了 OpenMP (OMP) 多线程的 FAISS 时，会导致严重的线程争用和上下文切换开销。
*   **解决方案**: `JoinStrategyFactory` 现在会自动检测这种情况。
    *   如果 `parallelism > 1` 且 `faiss_disable_omp` 为 `false`：
        *   系统强制设置 `faiss_disable_omp = true`。
        *   记录警告日志：`"Detected hybrid parallelism... Forcing FAISS OMP disabled"`。
    *   这确保了在扩展算子数量时，每个算子内部单线程运行，避免 CPU 核心的“超额认购”。

### 1.3 代码结构
*   **`JoinStrategyFactory`**: 创建 FAISS 索引并强制执行并行规则的核心逻辑。
*   **`JoinStrategyConfig`**: 添加了 `faiss_disable_omp` 标志，用于显式控制内部线程。
*   **`scripts/run_faiss_omp_benchmark.py`**: 一个专用的 Python 脚本，用于自动化 OMP 开启与关闭配置的基准测试。

---

## 2. 测试配置设置

集成测试在 `config/integration_test_cases.toml` 中配置。设计了特定的测试用例来验证 OMP 切换逻辑和性能。

### 2.1 关键配置参数
*   `algorithm`: `"faiss_ivf"` 或 `"faiss_hnsw"`
*   `faiss_disable_omp`: 布尔标志。
    *   `true`: 禁用 FAISS 中的 OpenMP（内部单线程）。
    *   `false`: 启用 FAISS 中的 OpenMP（内部多线程）。

### 2.2 测试用例
我们定义了特定的用例来隔离 OMP 的影响：

| 测试用例名称 | 算法 | 并行度 | OMP 设置 | 目的 |
|----------------|-----------|-------------|-------------|---------|
| `faiss_ivf_omp_on` | `faiss_ivf` | 1 | 启用 (`false`) | 内部并行基准 |
| `faiss_ivf_omp_off` | `faiss_ivf` | 1 | 禁用 (`true`) | 单线程执行基准 |
| `faiss_hnsw_omp_on` | `faiss_hnsw` | 1 | 启用 (`false`) | HNSW 内部并行 |
| `faiss_hnsw_omp_off` | `faiss_hnsw` | 1 | 禁用 (`true`) | HNSW 单线程 |

---

## 3. 测试运行结果

### 3.1 IVF 基准测试 (OMP 影响)
**日期:** 2025-12-19
**数据集:** 10,000 条向量记录
**迭代次数:** 2 次

| 配置 | 平均耗时 (ms) | 平均吞吐量 (RPS) | 加速比 |
|---------------|---------------|----------------------|---------|
| **OMP ON (开启)** | **2226.80** | **2785.07** | **1.16x** |
| OMP OFF (关闭) | 2575.71 | 2419.26 | - |

**分析:**
*   对于 **FAISS IVF**，在单算子场景（Parallelism=1）下，开启 OMP (`OMP ON`) 带来了 **约 16% 的性能提升**。
*   这表明即使在这个数据规模下，IVF 的倒排链扫描也能从内部并行化中受益。

### 3.2 HNSW 基准测试 (过往发现)
*   *注：HNSW 的详细日志来自之前的运行。*
*   总体观察：对于 HNSW，`OMP OFF` 在中小批量大小下通常表现优于或通过 `OMP ON`，这可能是因为图遍历中的线程同步开销高于单次查询延迟带来的收益。

### 3.3 结论与建议
*   **IVF**: 受益于 OMP（内部并行）。
*   **HNSW**: 对 OMP 不太敏感，有时关闭 OMP 延迟更低。
*   **生产环境建议**:
    *   对于 **单算子** (`parallelism=1`)：为 IVF 启用 OMP。
    *   对于 **多算子** (`parallelism>1`)：禁用 OMP（由 Factory 自动处理），以最大化系统总吞吐量。

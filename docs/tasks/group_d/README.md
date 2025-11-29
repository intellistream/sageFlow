# Group D: Baseline 方法实现

本目录包含 Group D 所有 Baseline 方法的实现任务文档。

---

## 任务列表

| 任务编号 | 任务名称 | 优先级 | 预估工时 | 论文依据 | 状态 |
|---------|---------|--------|---------|---------|------|
| D-01 | BruteForce Ground Truth | 🔴 高 | 1天 | - | ⬜ 待开始 |
| D-02 | HDR-Tree Baseline | 🟡 中 | 3天 | ADC 2022, WWW 2023 | ⬜ 待开始 |
| D-03 | HNSW Enhanced | 🔴 高 | 2天 | IEEE TPAMI 2018 | ✅ 完成 |
| D-04 | IVF Enhanced | 🔴 高 | 2天 | Faiss | ⬜ 待开始 |
| D-05 | ClusteredJoin | 🟡 中 | 3天 | VectraFlow | ⬜ 待开始 |
| D-06 | S3J/DEBS'23 | 🔴 高 | 3天 | DEBS'23 | ⬜ 待开始 |

---

## 依赖关系

```
D-01 BruteForce ─────────────────────────────────────┐
       │                                             │
       ↓                                             │ (Ground Truth 验证)
D-02 HDR-Tree ────→ 性能对比                         │
       │                                             │
D-03 HNSW Enhanced ─→ 性能对比 ←─────────────────────┤
       │                                             │
D-04 IVF Enhanced ──→ 性能对比 ←─────────────────────┤
       │                                             │
D-05 ClusteredJoin ─→ 性能对比 ←─────────────────────┤
       │                                             │
D-06 S3J ───────────→ 性能对比 ←─────────────────────┘
```

---

## 统一接口

所有 Baseline 必须实现以下统一接口：

```cpp
class BaselineJoinMethod : public BaseMethod {
public:
    /**
     * @brief 获取方法名称
     */
    virtual std::string getName() const = 0;
    
    /**
     * @brief 获取推荐配置
     */
    virtual JoinStrategyConfig getRecommendedConfig() const = 0;
    
    /**
     * @brief Eager 执行模式（单查询）
     */
    virtual std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query, int slot) override = 0;
    
    /**
     * @brief Lazy 执行模式（批量查询）
     */
    virtual std::vector<std::unique_ptr<VectorRecord>> ExecuteLazy(
        const std::deque<std::unique_ptr<VectorRecord>>& queries, 
        int slot) override = 0;
    
    /**
     * @brief 初始化方法
     */
    virtual void open(const RuntimeContext& context, 
                      JoinOperatorState* state) override = 0;
    
    /**
     * @brief 关闭方法
     */
    virtual void close() override = 0;
};
```

---

## 性能评估指标

所有 Baseline 需要报告以下指标：

| 指标 | 描述 | 单位 |
|-----|------|-----|
| Recall@k | 召回率 | % |
| Precision@k | 精确率 | % |
| Latency P50 | 50分位延迟 | ms |
| Latency P99 | 99分位延迟 | ms |
| Throughput | 吞吐量 | records/sec |
| Memory Usage | 内存使用 | MB |

---

## 测试数据集

| 数据集 | 维度 | 规模 | 用途 |
|-------|-----|------|-----|
| SIFT1M | 128 | 1M | 标准评估 |
| GIST1M | 960 | 1M | 高维评估 |
| Random-128 | 128 | 100K | 快速验证 |
| Random-512 | 512 | 100K | 中维评估 |

---

## 快速开始

```bash
# 构建测试
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -j $(nproc)

# 运行 Baseline 测试
ctest --test-dir build -R "Baseline" --output-on-failure

# 运行性能评估
./build/bin/perf_baseline_comparison --dataset sift1m
```

---

## 参考资料

- [VSJOIN_IMPLEMENTATION_ROADMAP.md](../../VSJOIN_IMPLEMENTATION_ROADMAP.md) - 总体路线图
- [TASK_GROUP_C_BASELINES.md](../TASK_GROUP_C_BASELINES.md) - Baseline 主任务文档
- [JOIN_PIPELINE_GUIDE.md](../../JOIN_PIPELINE_GUIDE.md) - Join 流程指南

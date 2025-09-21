# Join 算子测试编写指南

## 1. 目标概述
1. 使用Stream和Execution中的工具构建最小多线程Pipeline（source → join → sink），在不同并行度与多线程环境下正确构建、启动、关闭。
2. 验证 JoinOperator 在多实例(并行度 > 1)下，`BruteForce` 与 `IVF` 方法对滑动窗口内向量的正确 Join 行为：
   - Join 决策＝JoinMethod 内部：相似度判定 + 窗口存在性
   - JoinFunction 仅负责生成下游输出向量（不做“是否可 Join”判定）
3. 构建 baseline（朴素 O(N^2) + 窗口筛选），用于结果集对比。
4. 覆盖不同数据规模与参数配置，验证：
   - 10/100/1000/10000/100000/1000000 级别数据均无崩溃，数据采用可控随机生成（见数据生成策略）
   - 不同并行度（1,2,4,8）下结果一致
   - 正确性（数量、内容、去重、无遗漏）
   - 阈值边界（=threshold、略大、略小）
   - 窗口过期生效
   - 并行安全（无数据竞争/崩溃）
5. 采集总耗时与内部阶段耗时（含加锁等待），支持后续性能回归。

## 2. 目录结构建议
```
test/
  IntegrationTest/
    test_pipeline_basic.cpp
  UnitTest/
    test_join_bruteforce.cpp
    test_join_ivf.cpp
    test_join_baseline.cpp
  Performance/
    test_join_perf_scaling.cpp
config/
  join_pipeline_basic.toml
  join_method_cases.toml
```

## 3. 配置文件（TOML）示例
```toml
[pipeline]
parallelism_source = 2
parallelism_join = 3
parallelism_sink = 1
records_per_source = 10000
vector_dim = 128
similarity_threshold = 0.8
join_method = "ivf_eager"   # 允许: "ivf_eager" "ivf_lazy" "bruteforce_eager" "bruteforce_lazy"

[window]
time_ms = 60000
trigger_interval_ms = 1000

[index.ivf]
nlist = 64
nprobes = 8
rebuild_threshold = 0.05

[data.pattern]
# 控制性数据，用于可预测匹配数量
positive_pairs = 500
near_threshold_pairs = 50
negative_pairs = 500
random_tail = 2000
seed = 42
```

## 4. 数据生成策略
- 随机基础向量：均匀分布或正态分布。
- 构造命中样本：
   - 生成主向量 A，克隆 + 施加微扰(控制余弦/内积变化) 得到 B 以确保相似度 > 阈值。
   - Near-threshold：扰动使得相似度落在 [T\-ε, T\+ε]，验证临界处理。
   - 负样本：随机相互独立（期望低相似度）。
- 记录 ground truth：
   - 仅针对“窗口重叠期间”且“相似度 ≥ 阈值”的 (uid_L, uid_R)。
   - Baseline 直接复算所有候选，产出集合 G。
   - 被测方法产出集合 R。断言：|G △ R| = 0（可打印差异上限 N 条以便诊断）。

## 5. Baseline 设计
- 输入：当前左右窗口向量集合（复制或只读视图）
- 算法：双层循环 + 相似度计算 + 时间/窗口检查
- 输出：匹配对向量经 JoinFunction 处理后的结果 UID 列表或签名哈希集合
- 复杂度：O(N_L * N_R)
- 用途：小/中规模验证（大规模可抽样验证或限制窗口大小）

## 6. Join 正确性断言要点
1. 结果数量匹配
2. 集合相等（可用哈希：`std::hash(uid_L) ^ rotl(uid_R, k)`）
3. 不包含过期窗口成员（其 timestamp < window_limit）
4. 不重复（使用 unordered_set 去重检测）
5. 阈值边界：分别测试
   - exactly = T
   - slightly > T
   - slightly < T （应被过滤）
6. 并行度 > 1：多实例输出合并后仍满足 1–5
7. IVF 与 BruteForce 在相同输入 + 相同阈值下输出一致（理论上 IVF 是加速结构，结果应等价）

## 7. 并行与同步测试
- 使用屏障（`std::barrier` 或自实现 latch）同步所有 source 线程开始发射，避免 warmup 偏差。
- 每条记录附加严格单调 uid（原子递增）。
- Sink 收集所有输出（线程安全队列）。测试结束后聚合校验。
- 压测模式：调高 records_per_source 与 parallelism_join，观察是否死锁/异常。

## 8. 性能与耗时采集
### 8.1 需要埋点的阶段（建议）
1. Window 插入（含复制）
2. Index 插入（IVF / BruteForce 索引管理层）
3. 过期淘汰（含 index erase）
4. 候选检索（ExecuteEager / ExecuteLazy 内部）
5. 相似度过滤（JoinMethod）
6. JoinFunction 执行（生成输出向量）
7. 结果收集 & 发送
8. 锁等待时间（对每个锁累计“等待开始→获取成功”时长）

### 8.2 建议的计时工具
- RAII 计时片段 + 原子累加
- 单元测试中提供全局 `struct JoinMetrics` 存储分类耗时(ns)与计数
- 测试结束打印 JSON 或 TSV 方便外部脚本解析

### 8.3 计时代码示例
```cpp
// metrics.h
#pragma once
#include <atomic>
#include <chrono>
#include <string>
#include <unordered_map>
#include <mutex>

struct JoinMetrics {
  std::atomic<uint64_t> window_insert_ns{0};
  std::atomic<uint64_t> index_insert_ns{0};
  std::atomic<uint64_t> expire_ns{0};
  std::atomic<uint64_t> candidate_fetch_ns{0};
  std::atomic<uint64_t> similarity_ns{0};
  std::atomic<uint64_t> join_function_ns{0};
  std::atomic<uint64_t> emit_ns{0};
  std::atomic<uint64_t> lock_wait_ns{0};

  static JoinMetrics& instance();
};

class ScopedTimer {
 public:
  using Clock = std::chrono::high_resolution_clock;
  ScopedTimer(std::atomic<uint64_t>& slot) : slot_(slot), start_(Clock::now()) {}
  ~ScopedTimer() {
    auto d = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now()-start_).count();
    slot_.fetch_add(static_cast<uint64_t>(d), std::memory_order_relaxed);
  }
 private:
  std::atomic<uint64_t>& slot_;
  Clock::time_point start_;
};

// 锁等待包装
template<typename Mutex>
class TimedLockGuard {
 public:
  TimedLockGuard(Mutex& m) : m_(m) {
    auto t0 = Clock::now();
    m_.lock();
    auto t1 = Clock::now();
    auto d = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    JoinMetrics::instance().lock_wait_ns.fetch_add(d, std::memory_order_relaxed);
  }
  ~TimedLockGuard(){ m_.unlock(); }
 private:
  using Clock = std::chrono::high_resolution_clock;
  Mutex& m_;
};
```

### 8.4 输出与断言
- 打印总耗时与各阶段占比（>50% 则提示关注）
- 将结果写入 `build/metrics/join_<method>_<scale>.json`
- 在性能测试中仅断言“不超过基线 X 倍”（例如 IVF 不应慢于 BruteForce 2 倍（小规模除外））

## 9. 测试分类与示例用例
| 类别 | 用例示例 |
| ---- | -------- |
| 基础功能 | pipeline 构建 + open + close |
| 正确性 | 小窗口(<=20) 可人工穷举 |
| 阈值边界 | T, T±1e-3 |
| 大规模 | 10^5 级别记录（分批），验证无死锁 |
| 并行一致性 | parallelism_join in {1,2,4,8} 结果一致 |
| IVF vs BruteForce | 结果集合相等 |
| Lazy vs Eager | 输出集合一致（触发策略不同） |
| 过期淘汰 | 制造早期记录过期后不再匹配 |
| 去重 | 人工构建多次触发窗口，验证无重复 |
| 性能回归 | 保存上一版本 metrics，比较偏差 < 10%（漂移阈值可配置） |

## 10. GoogleTest 组织建议
- 使用参数化测试：方法 × 模式（`ivf_eager`, `ivf_lazy`, `bruteforce_eager`, `bruteforce_lazy`）
- Fixture 负责：读取 TOML → 构建 pipeline → 注入生成器 → 收集 sink 输出
- TearDown 中校验结果与 metrics 输出

## 11. 必要的可选改动建议
1. 在 `JoinOperator` 提供可选编译开关（宏 `CANDY_ENABLE_METRICS`）以注入 ScopedTimer，避免生产性能影响。
2. 暴露当前窗口快照只读接口（便于测试/基准）：
   - `snapshot_left()` / `snapshot_right()` 返回拷贝或受控视图
3. JoinMethod 内部分阶段拆分函数，便于精准埋点（已部分存在，如 `ExecuteEager`/`ExecuteLazy`）
4. 为相似度函数添加可选统计 hook（调用次数、平均时长）。
5. Sink 增加线程安全批量提取接口减少锁次数。

## 12. 结果校验伪代码示例
```cpp
// 假设 baseline_matches / method_matches 为 unordered_set<uint64_t> 的 pair-hash
ASSERT_EQ(baseline_matches.size(), method_matches.size());
for (auto& h : baseline_matches) {
  ASSERT_TRUE(method_matches.count(h)) << "Missing match hash=" << h;
}
```

## 13. Hash 设计（(uidL, uidR) 无序或有序）
```cpp
struct PairHash {
  size_t operator()(const std::pair<uint64_t,uint64_t>& p) const noexcept {
    uint64_t a = p.first, b = p.second;
    // 若需要无序匹配可排序后再 hash
    uint64_t mix = a * 1315423911u ^ ((b << 13) | (b >> 7));
    return std::hash<uint64_t>{}(mix);
  }
};
```

## 14. 通过标准
- 所有 correctness 测试通过
- IVF/BruteForce 结果集合相等
- 指标文件生成且字段完整
- 大规模压力测试无崩溃；锁等待占比（lock_wait_ns / total_ns）≤ 30%（可调）
- Baseline 与被测方法差异集为空

---

如需我生成具体测试代码骨架，可再按上述分模块输出。
# TODO: QIQ 并发策略优化

## 状态：待实施
## 优先级：高
## 创建日期：2024-12-17

---

## 1. 当前问题分析

### 1.1 现状描述

当前 SageFlow 的 Join 算子在高并行度场景下使用 **QIQ (Query-Insert-Query)** 策略配合全局读写锁 `join_rw_mutex_` 来保证召回率。

```cpp
// 当前实现 (join_operator.cpp)
// 阶段1：Q1 - 读锁
{
    std::shared_lock<std::shared_mutex> read_lock(join_rw_mutex_);
    executeJoinWithState(...);  // 第一次查询
}

// 阶段2：Insert - 写锁 ⚠️ 瓶颈所在
{
    std::unique_lock<std::shared_mutex> write_lock(join_rw_mutex_);
    updateSideWithState(...);   // 插入到窗口和索引
}

// 阶段3：Q2 - 读锁
{
    std::shared_lock<std::shared_mutex> read_lock(join_rw_mutex_);
    executeJoinWithState(...);  // 第二次查询，捕获并发插入的记录
}
```

### 1.2 性能测试数据（2024-12-17）

测试配置：2000 条向量，64 维，窗口 10s

| 算法 | 并行度 | Q1 (µs) | Insert (µs) | Q2 (µs) | Total (µs) | Insert 占比 |
|------|--------|---------|-------------|---------|------------|-------------|
| **BruteForce** | p=2 | 1,564 | 881 | 1,727 | 4,172 | 21% |
| | p=4 | 3,876 | 11,834 | 5,024 | 20,734 | 57% |
| | p=8 | 4,151 | 39,744 | 5,246 | 49,141 | 81% |
| | p=16 | 3,860 | 79,547 | 5,116 | 88,523 | 90% |
| | p=32 | 3,972 | **178,065** | 5,215 | 187,252 | **95%** |
| **IVF** | p=32 | 3,820 | **166,435** | 5,118 | 175,373 | **95%** |
| **HDR-Tree** | p=32 | 4,632 | **215,435** | 5,575 | 225,642 | **95%** |

### 1.3 问题总结

1. **写锁串行化**：全局 `join_rw_mutex_` 的写锁使所有 Insert 操作完全串行，p=32 时 Insert 等待时间高达 178-215ms

2. **并行度越高，锁竞争越严重**：Insert 耗时与并行度呈近似线性关系
   - p=2: ~1ms
   - p=32: ~180ms（增长 180 倍）

3. **Q1/Q2 相对稳定**：读锁竞争较轻，维持在 4-5ms，但 Insert 瓶颈导致整体吞吐受限

4. **实际吞吐量受限**：p=32 时每条记录处理需要 187ms，理论吞吐仅 ~170 records/s（远低于单线程）

---

## 2. 首选方案：PIM-Tree 风格 Delta Buffer（LSM-Tree 思想）

### 2.1 核心思想

参考 PIM-Tree 论文的设计，将写入操作从索引解耦：

- **写入**：Append 到无锁/轻锁的 Delta Buffer（内存）
- **查询**：主索引结果 ∪ Delta Buffer 线性扫描
- **合并**：后台异步将 Delta Buffer 合并到主索引

### 2.2 架构设计

```
┌─────────────────────────────────────────────────────────────────┐
│                     QI + Delta Scan 策略                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Record 到达                                                    │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────────┐                                            │
│  │ Q1: 查询主索引  │◄──── 读锁（shared_lock）                   │
│  │ + 扫描 Delta    │      可并发                                │
│  └────────┬────────┘                                            │
│           │                                                     │
│           ▼                                                     │
│  ┌─────────────────┐                                            │
│  │ I: Append 到    │◄──── 无锁（atomic）或轻锁                  │
│  │    Delta Buffer │      几乎无竞争                            │
│  └────────┬────────┘                                            │
│           │                                                     │
│           ▼                                                     │
│       完成（无需 Q2）                                            │
│                                                                 │
│  ════════════════════════════════════════════════════════════   │
│                                                                 │
│  后台合并线程（异步）：                                          │
│  ┌─────────────────┐      ┌─────────────────┐                   │
│  │  Delta Buffer   │ ───▶ │    主索引       │                   │
│  │  (达到阈值时)   │      │  (写锁，独占)   │                   │
│  └─────────────────┘      └─────────────────┘                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 关键实现细节

```cpp
class DeltaBufferWindowState : public WindowState {
private:
    // 主存储（需要锁保护）
    std::deque<std::unique_ptr<VectorRecord>> main_records_;
    std::shared_mutex main_mutex_;
    
    // Delta Buffer（无锁或轻锁）
    struct alignas(64) DeltaBuffer {  // Cache line 对齐
        std::vector<std::unique_ptr<VectorRecord>> records;
        std::atomic<size_t> size{0};
        std::mutex append_mutex;  // 轻量级互斥（仅保护 vector 扩容）
    };
    DeltaBuffer delta_;
    
    static constexpr size_t kMergeThreshold = 100;  // 合并阈值
    
public:
    // 写入：几乎无锁
    void addRecord(std::unique_ptr<VectorRecord> record, size_t) override {
        {
            std::lock_guard<std::mutex> lock(delta_.append_mutex);
            delta_.records.push_back(std::move(record));
        }
        delta_.size.fetch_add(1, std::memory_order_release);
        
        // 触发异步合并（如果超过阈值）
        if (delta_.size.load() >= kMergeThreshold) {
            triggerAsyncMerge();
        }
    }
    
    // 查询：主存储 + Delta 扫描
    std::vector<VectorRecord*> getAllRecords() {
        std::vector<VectorRecord*> results;
        
        // 1. 读取主存储（读锁）
        {
            std::shared_lock<std::shared_mutex> lock(main_mutex_);
            for (auto& r : main_records_) {
                results.push_back(r.get());
            }
        }
        
        // 2. 扫描 Delta Buffer（轻锁）
        {
            std::lock_guard<std::mutex> lock(delta_.append_mutex);
            for (auto& r : delta_.records) {
                results.push_back(r.get());
            }
        }
        
        return results;
    }
};
```

### 2.4 Join 流程改进

```cpp
// 改进后的 apply 方法
void JoinOperator::apply(...) {
    // 阶段1：查询（主索引 + Delta 扫描）
    auto candidates = queryMainIndex(query);
    auto delta_candidates = scanDeltaBuffer(query);
    merge(candidates, delta_candidates);
    
    // 阶段2：Insert 到 Delta Buffer（无锁 append）
    delta_buffer_.append(std::move(record));
    
    // 无需 Q2！因为：
    // - 同时插入的记录都在 Delta Buffer
    // - 它们会被扫描到
}
```

### 2.5 预期效果

| 并行度 | 当前 Insert | Delta Buffer Insert | 加速比 |
|--------|-------------|---------------------|--------|
| p=2    | 0.8ms       | ~0.01ms             | 80x    |
| p=4    | 12ms        | ~0.02ms             | 600x   |
| p=8    | 40ms        | ~0.03ms             | 1300x  |
| p=16   | 80ms        | ~0.05ms             | 1600x  |
| p=32   | 178ms       | ~0.1ms              | 1780x  |

**总体吞吐量提升**：
- p=32 当前：~170 records/s
- p=32 优化后：~50,000+ records/s（理论值）

### 2.6 优点

1. ✅ **写入几乎无锁**：Delta append 仅需原子操作或轻量互斥
2. ✅ **消除 Q2**：Delta 扫描替代第二次索引查询
3. ✅ **读写分离**：查询和写入互不阻塞
4. ✅ **与流式场景天然匹配**：类似 LSM-Tree 的 write-optimized 设计
5. ✅ **渐进式实现**：可以在现有架构上逐步改造

### 2.7 缺点与挑战

1. ⚠️ **Delta 扫描开销**：Delta Buffer 过大时线性扫描成本增加
   - 缓解：控制合并阈值，保持 Delta 较小（< 100 条）
   
2. ⚠️ **合并时机**：后台合并需要写锁，可能造成短暂延迟
   - 缓解：使用 Copy-on-Write 或在低负载时合并
   
3. ⚠️ **内存占用**：Delta Buffer 额外占用内存
   - 缓解：及时合并，控制 Delta 大小

---

## 3. 备选方案对比

### 3.1 方案 B：分区锁（Partitioned Locking）

**思路**：将窗口/索引按向量空间或 hash 分区，每个分区独立锁

```cpp
class PartitionedIndex {
    struct Partition {
        std::shared_mutex mutex;
        std::unique_ptr<Index> index;
    };
    std::vector<Partition> partitions_;  // N 个分区
    
    void insert(VectorRecord record) {
        size_t pid = hash(record.vector) % partitions_.size();
        std::unique_lock lock(partitions_[pid].mutex);
        partitions_[pid].index->insert(record);
    }
};
```

**优点**：
- ✅ 锁粒度更细，理论上 N 分区可支持 N 并发写入
- ✅ 与现有 `PartitionedWindowState` 架构契合
- ✅ 实现复杂度适中

**缺点**：
- ⚠️ 跨分区查询需要多次加锁（增加延迟）
- ⚠️ 分区不均可能导致热点
- ⚠️ 仍需 QIQ 策略保证召回率

**预期效果**：

| 并行度 | 当前 Insert | 8 分区 Insert | 加速比 |
|--------|-------------|---------------|--------|
| p=8    | 40ms        | ~5ms          | 8x     |
| p=16   | 80ms        | ~10ms         | 8x     |
| p=32   | 178ms       | ~22ms         | 8x     |

**适用场景**：数据分布均匀，分区数可预估

---

### 3.2 方案 C：Copy-on-Write 双缓冲

**思路**：维护两个索引副本，读写分离

```cpp
class COWIndex {
    std::shared_ptr<Index> active_;   // 当前读取的索引
    std::shared_ptr<Index> shadow_;   // 写入的索引
    std::atomic<bool> swapping_{false};
    
    void insert(VectorRecord record) {
        shadow_->insert(record);  // 写入 shadow
    }
    
    void swap() {
        // 定期交换 active 和 shadow
        std::swap(active_, shadow_);
        // shadow 需要从 active 同步数据
    }
};
```

**优点**：
- ✅ 读写完全分离，查询不受写入影响
- ✅ 无锁读取

**缺点**：
- ❌ 内存翻倍（两份索引）
- ❌ 交换时有短暂停顿
- ❌ 实现复杂，需要处理同步问题
- ❌ 不适合流式场景（交换间隔内的新数据查不到）

**预期效果**：Insert 时间降低，但引入周期性延迟

**适用场景**：批处理为主，对实时性要求不高

---

### 3.3 方案 D：无锁并发数据结构

**思路**：使用 Lock-Free 数据结构替代有锁容器

```cpp
// 使用 lock-free queue 或 skip list
#include <folly/ConcurrentSkipList.h>

class LockFreeWindowState {
    folly::ConcurrentSkipList<VectorRecord> records_;
    
    void addRecord(VectorRecord record) {
        records_.insert(std::move(record));  // Lock-free insert
    }
};
```

**优点**：
- ✅ 理论上最佳并发性能
- ✅ 无死锁风险

**缺点**：
- ❌ 实现复杂，调试困难
- ❌ 依赖第三方库（folly, TBB 等）
- ❌ 某些操作（如批量删除）难以 lock-free 实现
- ❌ 内存管理复杂（需要 hazard pointers 或 epoch-based reclamation）

**预期效果**：

| 并行度 | 当前 Insert | Lock-Free Insert | 加速比 |
|--------|-------------|------------------|--------|
| p=32   | 178ms       | ~0.5ms           | 350x   |

**适用场景**：对性能有极致要求，团队有 lock-free 经验

---

### 3.4 方案 E：Batch Insert + 延迟查询

**思路**：收集多条记录后批量插入，减少锁操作次数

```cpp
class BatchingWindowState {
    thread_local std::vector<VectorRecord> batch_buffer_;
    static constexpr size_t kBatchSize = 32;
    
    void addRecord(VectorRecord record) {
        batch_buffer_.push_back(std::move(record));
        if (batch_buffer_.size() >= kBatchSize) {
            flushBatch();
        }
    }
    
    void flushBatch() {
        std::unique_lock lock(mutex_);
        for (auto& r : batch_buffer_) {
            records_.push_back(std::move(r));
        }
        batch_buffer_.clear();
    }
};
```

**优点**：
- ✅ 减少锁操作次数（32 条记录只需 1 次加锁）
- ✅ 实现简单
- ✅ 与现有架构兼容

**缺点**：
- ⚠️ 引入延迟（需要等待批次填满）
- ⚠️ 低流量时可能长时间不触发 flush
- ⚠️ 仍然是串行写入，只是减少了锁的开销

**预期效果**：

| 并行度 | 当前 Insert | Batch Insert (32) | 加速比 |
|--------|-------------|-------------------|--------|
| p=32   | 178ms       | ~6ms              | 30x    |

**适用场景**：流量稳定，可接受微小延迟

---

## 4. 方案对比总结

| 方案 | 复杂度 | Insert 加速 | 内存开销 | 实时性 | 推荐度 |
|------|--------|-------------|----------|--------|--------|
| **A: Delta Buffer (PIM-Tree)** | 中 | 1000x+ | 低 | 高 | ⭐⭐⭐⭐⭐ |
| B: 分区锁 | 中 | 8x | 无 | 高 | ⭐⭐⭐ |
| C: COW 双缓冲 | 高 | 50x | 2x | 中 | ⭐⭐ |
| D: Lock-Free | 高 | 300x | 中 | 高 | ⭐⭐ |
| E: Batch Insert | 低 | 30x | 无 | 中 | ⭐⭐⭐ |

---

## 5. 实施计划

### Phase 1：Delta Buffer 基础实现（1-2 周）

- [ ] 实现 `DeltaBufferWindowState` 类
- [ ] 修改 `JoinOperator::apply()` 使用新的 QI + Delta Scan 策略
- [ ] 实现基础的同步合并逻辑
- [ ] 单元测试验证正确性

### Phase 2：异步合并与优化（1 周）

- [ ] 实现后台合并线程
- [ ] 添加合并阈值配置
- [ ] 性能测试与调优

### Phase 3：分区锁组合（可选，1 周）

- [ ] 将 Delta Buffer 与分区锁结合
- [ ] 支持跨分区查询优化

---

## 6. 参考资料

1. **PIM-Tree**: "PIM-tree: A Skew-resistant Index for Processing-in-Memory" - 提出 Query-Insert-Query 策略
2. **LSM-Tree**: "The Log-Structured Merge-Tree" - Delta Buffer + 合并思想的来源
3. **现有代码**: `src/operator/join_operator.cpp` - 当前 QIQ 实现

---

## 7. 备注

- 当前测试数据来自 `test_join_datasource_modes` 测试
- 召回率在 QIQ 策略下保持 100%（BruteForce/IVF）或 99.8%+（HDR-Tree）
- 优化应在保证召回率的前提下进行

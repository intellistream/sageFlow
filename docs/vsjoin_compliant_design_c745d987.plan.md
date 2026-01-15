---
name: VSJoin Compliant Design
overview: 设计符合 SageFlow 架构约束的 VSJoin 双层索引方案：索引通过 ConcurrencyManager 管理，窗口数据通过 WindowState 管理，JoinMethod 仅负责查询逻辑。
todos:
  - id: p1-vsjoin-method
    content: VSJoinMethodV2 基础实现（ExecuteEager + 双层查询逻辑）
    status: pending
  - id: p2-factory-integration
    content: JoinStrategyFactory 集成（创建 Global + Local 索引对）
    status: pending
    dependencies:
      - p1-vsjoin-method
  - id: p3-operator-path
    content: JoinOperator VSJoin 特殊路径（updateSideWithState 只插 Local）
    status: pending
    dependencies:
      - p2-factory-integration
  - id: p4-rebuild-mechanism
    content: 后台重建机制 GlobalIndexRebuilder
    status: pending
    dependencies:
      - p3-operator-path
  - id: p5-config-validation
    content: 配置验证 + TOML 解析 + 查询参数
    status: pending
    dependencies:
      - p1-vsjoin-method
  - id: p6-integration-test
    content: 集成测试 + 召回率验证
    status: pending
    dependencies:
      - p4-rebuild-mechanism
      - p5-config-validation
---

# VSJoin 双层索引架构设计方案（修订版）

## 架构约束遵循

根据 SageFlow 的核心设计原则：

| 组件 | 职责 | VSJoin 中的角色 |

|-----|------|----------------|

| **ConcurrencyManager** | 索引的创建/插入/查询/删除 | 管理 Global + Local 两组索引 |

| **WindowState** | 窗口内向量数据的存储和访问 | 使用 TwoTierWindowState 管理数据 |

| **JoinMethod** | 实现 ExecuteEager() 查询逻辑 | VSJoinMethodV2 协调双层索引查询 |

| **JoinOperator** | 协调窗口更新、索引插入、Join 执行 | 调用 updateSideWithState + 后台重建触发 |

---

## 1. 整体架构

```mermaid
flowchart TB
    subgraph JoinOperator [JoinOperator 协调层]
        Apply[apply with context]
        Update[updateSideWithState]
        Rebuild[triggerGlobalRebuild]
    end
    
    subgraph WindowState [TwoTierWindowState]
        WS_Add[addRecord]
        WS_Get[getRecordsSnapshot]
        WS_Evict[evictExpired]
        WS_Flush[flushExpiredUids]
    end
    
    subgraph ConcurrencyManager [ConcurrencyManager]
        CM_Global[Global Index Pair - immutable queries]
        CM_Local[Local Index Pair - mutable per partition]
        CM_Insert[insert]
        CM_Query[query_for_join]
        CM_Erase[erase]
    end
    
    subgraph VSJoinMethodV2 [VSJoinMethodV2]
        Exec[ExecuteEager]
        Q1[Query Global - no lock]
        Q2[Query Local - partition lock]
        Merge[Merge and Dedupe]
    end
    
    Apply --> Update
    Update --> WS_Add
    Update --> CM_Insert
    Update --> WS_Evict
    
    Apply --> Exec
    Exec --> Q1
    Exec --> Q2
    Q1 --> CM_Query
    Q2 --> CM_Query
    Q1 --> Merge
    Q2 --> Merge
    
    Rebuild --> WS_Get
    Rebuild --> CM_Global
```

---

## 2. 索引管理策略（通过 ConcurrencyManager）

### 2.1 索引 ID 布局（方案 B：每分区独立 index_id）

```cpp
// 在 StrategyComponents 中扩展
struct StrategyComponents {
    // ... 现有字段 ...
    
    // Global Immutable Index（所有 subtask 共享，只读查询）
    // 共 2 个 index_id
    int global_left_id = -1;
    int global_right_id = -1;
    
    // Local Mutable Index（每分区独立，完全隔离）
    // 共 2 * num_partitions 个 index_id
    // local_left_ids[partition_i] 对应左流第 i 个分区的索引
    std::vector<int> local_left_ids;   // size = num_partitions
    std::vector<int> local_right_ids;  // size = num_partitions
};
```

**索引总数计算**：

- Global Index: 2 个（左右各一个共享索引）
- Local Index: 2 * num_partitions 个（每流每分区一个独立索引）
- **总计**: 2 + 2 * P 个 index_id（P = parallelism = num_partitions）

### 2.2 索引创建流程（在 JoinStrategyFactory 中）

```cpp
// 修改 JoinStrategyFactory::create() 为 VSJOIN 算法
case JoinAlgorithm::VSJOIN: {
    const int P = static_cast<int>(parallelism);  // 分区数 = 并行度
    
    // 1. 创建 Global Immutable Index（IVF/HNSW，用于快速查询）
    IVFParameters global_ivf_params{
        .nlist = config.ivf_nlist,
        .rebuild_threshold = config.ivf_rebuild_threshold,
        .nprobes = config.ivf_nprobes
    };
    
    components.left_index_id = concurrency_manager->create_index(
        "vsjoin_global_left", IndexType::IVF, config.dimension, global_ivf_params);
    components.right_index_id = concurrency_manager->create_index(
        "vsjoin_global_right", IndexType::IVF, config.dimension, global_ivf_params);
    
    // 2. 创建 Local Mutable Index（每分区独立，完全隔离）
    // 每个分区创建独立的 BruteForce 或轻量级 IVF 索引
    // 分区内只有单线程访问，无需复杂索引结构
    components.local_left_ids.resize(P, -1);
    components.local_right_ids.resize(P, -1);
    
    for (int partition = 0; partition < P; ++partition) {
        // 左流分区索引
        std::string left_name = "vsjoin_local_left_p" + std::to_string(partition);
        components.local_left_ids[partition] = concurrency_manager->create_index(
            left_name, IndexType::BruteForce, config.dimension);
        
        // 右流分区索引
        std::string right_name = "vsjoin_local_right_p" + std::to_string(partition);
        components.local_right_ids[partition] = concurrency_manager->create_index(
            right_name, IndexType::BruteForce, config.dimension);
    }
    
    SAGEFLOW_LOG_INFO("VSJOIN_FACTORY", 
        "Created {} Global indexes + {} Local indexes (parallelism={})",
        2, 2 * P, P);
    break;
}
```

### 2.3 索引访问模式

```
subtask_0 → local_left_ids[0], local_right_ids[0]  // 分区 0 独占
subtask_1 → local_left_ids[1], local_right_ids[1]  // 分区 1 独占
...
subtask_i → local_left_ids[i], local_right_ids[i]  // 分区 i 独占

所有 subtask → global_left_id, global_right_id     // 共享只读
```

**优势**：

- 每个分区的 Local Index 由单一 subtask 独占访问，**写入和查询都无需任何锁**
- 完全隔离，每个分区有独立的 ConcurrencyController（虽然由同一个 ConcurrencyManager 管理）
- 语义清晰：subtask_index == partition_index
- **边界向量通过多播复制**：查询时只查本分区，无需跨分区探测，完全无锁

---

## 3. WindowState 设计

### 3.1 复用 TwoTierWindowState

不需要新建 WindowState，复用现有的 [`TwoTierWindowState`](include/state/two_tier_window_state.h)：

```cpp
// TwoTierWindowState 已有的特性完全满足 VSJoin 需求：
// - 分区存储：每个 subtask 独立的窗口
// - Lazy Delete：标记过期而非立即删除
// - 快照支持：getRecordsSnapshot() 线程安全
// - 批量 Flush：flushExpiredUids() 返回待删除 UID

// 在 JoinStrategyFactory::createWindowState() 中
case WindowStateType::TWO_TIER:
    return std::make_unique<TwoTierWindowState>(parallelism, config.two_tier_compact_threshold);
```

### 3.2 配置推荐

```cpp
// VSJoin 推荐配置
strategy_config_.window_state_type = WindowStateType::TWO_TIER;
strategy_config_.partition_strategy = PartitionStrategy::LSH;
strategy_config_.two_tier_compact_threshold = 100;
```

---

## 4. VSJoinMethodV2 实现

### 4.1 类定义

创建 [`include/operator/join_operator_methods/vsjoin_method_v2.h`](include/operator/join_operator_methods/vsjoin_method_v2.h)：

```cpp
class VSJoinMethodV2 : public BaseMethod {
public:
    struct Config {
        double similarity_threshold = 0.8;
        int dimension = 128;
        int num_partitions = 8;
        int multicast_k = 2;  // 边界向量多播到 k 个分区（推荐 2-3）
        
        // 重建策略
        int64_t rebuild_interval_ms = 5000;
        size_t rebuild_threshold = 1000;  // 触发 Global 重建的 Local 索引大小阈值
    };
    
    // ==================== 核心接口 ====================
    
    std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query_record,
        int query_slot,
        size_t subtask_index) override;
    
    // ==================== 初始化（由 JoinOperator 调用） ====================
    
    void initialize(const RuntimeContext& context,
                   std::shared_ptr<ConcurrencyManager> concurrency_manager);
    
    // 设置 Global 索引 ID（共享只读，2 个）
    void setGlobalIndexIds(int left_id, int right_id);
    
    // 设置 Local 索引 ID 数组（每分区独立，2 * num_partitions 个）
    void setLocalIndexIds(const std::vector<int>& left_ids, 
                          const std::vector<int>& right_ids);
    
    // 设置 WindowState（由 JoinOperator 传入）
    void setWindowStates(WindowState* left_state, WindowState* right_state);
    
    // 设置分区器（用于确定查询分区和邻近分区）
    void setPartitioner(std::shared_ptr<VectorSpacePartitioner> partitioner);
    
    // ==================== Local Index 访问辅助 ====================
    
    // 获取当前 subtask 对应的 Local Index ID
    int getLocalLeftIndexId(size_t subtask_index) const {
        return (subtask_index < local_left_ids_.size()) 
            ? local_left_ids_[subtask_index] : -1;
    }
    int getLocalRightIndexId(size_t subtask_index) const {
        return (subtask_index < local_right_ids_.size()) 
            ? local_right_ids_[subtask_index] : -1;
    }
    
    // ==================== 重建支持 ====================
    
    bool needsGlobalRebuild(size_t subtask_index) const;
    std::vector<const VectorRecord*> getRecordsForRebuild(size_t subtask_index) const;

private:
    Config config_;
    
    // ConcurrencyManager（用于索引查询，不持有索引）
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
    
    // Global 索引 ID（共享只读）
    int global_left_id_ = -1;
    int global_right_id_ = -1;
    
    // Local 索引 ID 数组（每分区独立，subtask_i 访问 local_*_ids_[i]）
    std::vector<int> local_left_ids_;   // size = num_partitions
    std::vector<int> local_right_ids_;  // size = num_partitions
    
    // WindowState（由 JoinOperator 传入）
    WindowState* left_state_ = nullptr;
    WindowState* right_state_ = nullptr;
    
    // 分区器（用于确定邻近分区）
    std::shared_ptr<VectorSpacePartitioner> partitioner_;
    
    // ==================== 内部方法 ====================
    
    // 查询 Global Index（无锁，所有 subtask 共享）
    std::vector<uint64_t> queryGlobalIndex(const VectorRecord& query, int target_index_id);
    
    // 查询本分区 Local Index（无锁，独占访问）
    // 注意：不再查询邻近分区，因为边界向量已通过多播复制到本分区
    std::vector<uint64_t> queryLocalIndex(const VectorRecord& query, 
                                          int query_slot,
                                          size_t subtask_index);
    
    // 从 WindowState 解析 UID 到实际记录
    std::vector<std::unique_ptr<VectorRecord>> resolveUidsToRecords(
        const std::vector<uint64_t>& uids, WindowState* state, size_t subtask_index);
};
```

### 4.2 ExecuteEager 实现

```cpp
std::vector<std::unique_ptr<VectorRecord>> VSJoinMethodV2::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot,
    size_t subtask_index) {
    
    // 确定目标 Global 索引和窗口状态
    int global_target = (query_slot == 0) ? global_right_id_ : global_left_id_;
    WindowState* target_state = (query_slot == 0) ? right_state_ : left_state_;
    
    // ====== 第一阶段：查询 Global Index（无锁） ======
    // Global Index 是只读的，所有 subtask 共享，无需锁
    auto global_uids = queryGlobalIndex(query_record, global_target);
    
    // ====== 第二阶段：查询本分区 Local Index（无锁） ======
    // 注意：不再查询邻近分区，因为边界向量已通过多播复制到本分区
    // 本分区独占访问，完全无锁
    auto local_uids = queryLocalIndex(query_record, query_slot, subtask_index);
    
    // ====== 合并去重 ======
    std::unordered_set<uint64_t> uid_set(global_uids.begin(), global_uids.end());
    for (uint64_t uid : local_uids) {
        uid_set.insert(uid);
    }
    
    // ====== 从 WindowState 获取实际记录 ======
    // 过滤掉已过期的 UID
    std::vector<uint64_t> valid_uids;
    for (uint64_t uid : uid_set) {
        if (!target_state->isExpired(uid, subtask_index)) {
            valid_uids.push_back(uid);
        }
    }
    
    return resolveUidsToRecords(valid_uids, target_state, subtask_index);
}

std::vector<uint64_t> VSJoinMethodV2::queryGlobalIndex(
    const VectorRecord& query, int target_index_id) {
    
    if (target_index_id < 0 || !concurrency_manager_) {
        return {};
    }
    
    // 通过 ConcurrencyManager 查询（内部处理并发）
    auto candidates = concurrency_manager_->query_for_join(
        target_index_id, query, config_.similarity_threshold, similarity_alpha_);
    
    std::vector<uint64_t> uids;
    for (const auto& c : candidates) {
        uids.push_back(c->uid_);
    }
    return uids;
}

std::vector<uint64_t> VSJoinMethodV2::queryLocalIndex(
    const VectorRecord& query, int query_slot, size_t subtask_index) {
    
    if (!concurrency_manager_) {
        return {};
    }
    
    // 选择对侧的 Local 索引（只查询本分区）
    // 注意：不再查询邻近分区，因为边界向量已通过多播复制到本分区
    const auto& target_local_ids = (query_slot == 0) 
        ? local_right_ids_ : local_left_ids_;
    
    if (subtask_index >= target_local_ids.size()) {
        return {};
    }
    
    int local_index_id = target_local_ids[subtask_index];
    if (local_index_id < 0) {
        return {};
    }
    
    // 查询本分区的 Local Index（独占访问，无锁）
    auto candidates = concurrency_manager_->query_for_join(
        local_index_id, query, config_.similarity_threshold, similarity_alpha_);
    
    std::vector<uint64_t> uids;
    for (const auto& c : candidates) {
        uids.push_back(c->uid_);
    }
    
    return uids;
}
```

### 4.3 查询流程图（多播方案）

```mermaid
sequenceDiagram
    participant Q as Query Thread (subtask_i)
    participant G as Global Index (shared)
    participant Li as Local Index i (owned)
    participant WS as WindowState
    
    Q->>Q: 1. 确定 query_slot, 选择目标流
    
    rect rgb(200, 230, 200)
        Note over Q,G: Phase 1: Global Query (无锁，只读)
        Q->>G: query_for_join(global_id, query)
        G-->>Q: global_uids
    end
    
    rect rgb(200, 200, 230)
        Note over Q,Li: Phase 2: Local Query - 本分区 (无锁，独占)
        Note over Q,Li: 边界向量已通过多播复制到本分区
        Q->>Li: query_for_join(local_ids[i], query)
        Li-->>Q: local_uids_i
    end
    
    Q->>Q: 3. 合并去重 (unordered_set<uint64_t>)
    Q->>WS: 4. 过滤 expired UIDs
    Q->>WS: 5. resolveUidsToRecords()
    WS-->>Q: final_results
```

**关键设计点**：

- **不再查询邻近分区**：边界向量通过多播已复制到本分区
- **完全无锁查询**：Global 只读无锁，Local 本分区独占无锁
- **去重在查询结果合并时**：使用 `unordered_set<uint64_t>` 高效去重

---

## 5. JoinOperator 集成

### 5.1 新增成员变量

```cpp
// join_operator.h 新增
class JoinOperator : public BinaryOperator {
private:
    // ... 现有成员 ...
    
    // ==================== VSJoin 专用 ====================
    // Local Index ID 数组（每分区独立）
    std::vector<int> vsjoin_local_left_ids_;   // size = parallelism_
    std::vector<int> vsjoin_local_right_ids_;  // size = parallelism_
    
    // Global Index ID（共享只读）
    int vsjoin_global_left_id_ = -1;
    int vsjoin_global_right_id_ = -1;
};
```

### 5.2 修改 initializeWithStrategyConfig()

```cpp
void JoinOperator::initializeWithStrategyConfig(size_t subtask_index) {
    // ... 现有逻辑 ...
    
    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        // 从 StrategyComponents 获取索引 ID
        vsjoin_global_left_id_ = components.left_index_id;
        vsjoin_global_right_id_ = components.right_index_id;
        vsjoin_local_left_ids_ = components.local_left_ids;
        vsjoin_local_right_ids_ = components.local_right_ids;
        
        // 传递给 VSJoinMethodV2
        auto* vsjoin_method = dynamic_cast<VSJoinMethodV2*>(join_method_.get());
        if (vsjoin_method) {
            vsjoin_method->setGlobalIndexIds(vsjoin_global_left_id_, vsjoin_global_right_id_);
            vsjoin_method->setLocalIndexIds(vsjoin_local_left_ids_, vsjoin_local_right_ids_);
            vsjoin_method->setWindowStates(left_state_.get(), right_state_.get());
        }
    }
}
```

### 5.3 修改 updateSideWithState()

```cpp
auto JoinOperator::updateSideWithState(..., const RuntimeContext& context) -> bool {
    // ... 现有逻辑 ...
    
    size_t subtask_index = context.subtask_index;
    
    // VSJoin 特殊处理：只插入到本分区的 Local Index
    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        // 选择本分区对应的 Local Index ID
        const auto& local_ids = (slot == left_slot_id_) 
            ? vsjoin_local_left_ids_ : vsjoin_local_right_ids_;
        
        int local_index_id = (subtask_index < local_ids.size()) 
            ? local_ids[subtask_index] : -1;
        
        if (local_index_id >= 0 && concurrency_manager_) {
            // 本分区独占访问，无锁插入
            concurrency_manager_->insert(local_index_id, std::move(data_for_index_insert));
        }
        
        // Global Index 不在此处插入，由后台重建线程处理
        SAGEFLOW_LOG_DEBUG("VSJOIN", "subtask_{} inserted to local_id={}", 
                          subtask_index, local_index_id);
    } else {
        // 其他算法的正常插入逻辑
        if (use_index_ && concurrency_manager_ && index_id_for_cc != -1) {
            concurrency_manager_->insert(index_id_for_cc, std::move(data_for_index_insert));
        }
    }
    
    // ... 其余逻辑 ...
}
```

### 5.4 分区路由与多播策略

**重要**：VSJoin 使用 LSH 分区器 + 多播策略，确保边界向量不丢失。

```cpp
// 在 JoinOperator::getPreferredPartitioner() 中
if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
    // 创建 LSH 分区器（支持多播）
    auto lsh_partitioner = std::make_unique<LSHPartitionerAdapter>(
        strategy_config_.dimension,
        strategy_config_.vsjoin_num_hash_functions,
        strategy_config_.vsjoin_boundary_threshold);
    
    // 启用多播（边界向量复制到 k 个分区）
    lsh_partitioner->setMulticastEnabled(true);
    lsh_partitioner->setMulticastK(strategy_config_.vsjoin_v2_multicast_k);
    
    return lsh_partitioner;
}
```

**数据流（多播模式）**：

```
Source → LSHPartitioner (multicast_k=2)
         ├─ 主分区 → subtask_i → Local Index i
         └─ 边界分区 → subtask_j → Local Index j (复制)
```

**多播策略**：

- **非边界向量**：路由到主分区（单播）
- **边界向量**：路由到主分区 + k-1 个邻近分区（多播）
- **查询时**：只查本分区，边界向量已通过多播保证存在

**去重处理**：

- **Local Index**：每个分区独立，无需去重
- **Global Index 重建时**：使用 `unordered_set<uint64_t>` 去重
- **查询结果合并时**：使用 `unordered_set<uint64_t>` 去重（O(n) 开销，n 通常 < 1000）

### 5.2 后台重建机制（线程管理设计）

#### 5.2.1 线程模型

**关键设计决策**：

- **保持现有固定线程模型**：不需要改造成线程池
- **线程数量**：并行度 P = 16 → **16个工作线程 + 1个后台线程 = 17个线程**
- **管理方式**：在 `JoinOperator` 内部管理，使用 `std::call_once` 确保只启动一次

**线程分配**：

```
并行度 P = 16
├─ ExecutionVertex[0..15] → 16个工作线程（处理数据流）
└─ GlobalIndexRebuilder → 1个后台线程（周期性重建）
─────────────────────────────────────────────
总计：17个线程
```

#### 5.2.2 实现设计

**在 `join_operator.h` 中添加**：

```cpp
class JoinOperator final : public Operator {
private:
    // ... 现有成员 ...
    
    // ==================== VSJoin 后台重建 ====================
    // 使用 std::call_once 确保只启动一次（所有 subtask 共享同一个 JoinOperator 实例）
    std::once_flag rebuild_thread_started_;
    std::unique_ptr<std::thread> rebuild_thread_;
    std::atomic<bool> rebuild_running_{false};
    std::atomic<int64_t> rebuild_interval_ms_{5000};
    
    // 后台重建循环
    void globalIndexRebuildLoop();
    
    // 启动后台重建线程（由 open() 调用，使用 call_once 保护）
    void startGlobalIndexRebuilder();
    
    // 停止后台重建线程（由析构函数调用）
    void stopGlobalIndexRebuilder();
};
```

**在 `join_operator.cpp` 中实现**：

```cpp
void JoinOperator::open(const RuntimeContext& context) {
    // ... 现有逻辑 ...
    
    // VSJoin 特殊处理：启动后台重建线程
    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        startGlobalIndexRebuilder();
    }
}

void JoinOperator::startGlobalIndexRebuilder() {
    // 使用 std::call_once 确保只启动一次（所有 subtask 共享同一个 JoinOperator）
    std::call_once(rebuild_thread_started_, [this]() {
        rebuild_running_ = true;
        rebuild_interval_ms_ = strategy_config_.vsjoin_v2_rebuild_interval_ms;
        
        rebuild_thread_ = std::make_unique<std::thread>(
            &JoinOperator::globalIndexRebuildLoop, this);
        
        SAGEFLOW_LOG_INFO("VSJOIN_REBUILDER", 
            "Background rebuild thread started (interval={}ms, parallelism={})",
            rebuild_interval_ms_.load(), parallelism_);
    });
}

void JoinOperator::stopGlobalIndexRebuilder() {
    if (rebuild_running_.exchange(false)) {
        if (rebuild_thread_ && rebuild_thread_->joinable()) {
            rebuild_thread_->join();
        }
        SAGEFLOW_LOG_INFO("VSJOIN_REBUILDER", "Background rebuild thread stopped");
    }
}

void JoinOperator::globalIndexRebuildLoop() {
    const int64_t interval_ms = rebuild_interval_ms_.load();
    
    while (rebuild_running_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
        
        if (!rebuild_running_.load()) break;
        
        // ====== 1. 从所有 WindowState 分区收集活跃记录（多播导致重复） ======
        std::unordered_set<uint64_t> seen_left_uids;
        std::unordered_set<uint64_t> seen_right_uids;
        std::vector<const VectorRecord*> unique_left_records;
        std::vector<const VectorRecord*> unique_right_records;
        
        for (size_t p = 0; p < parallelism_; ++p) {
            // 获取分区快照（线程安全）
            auto left_snapshot = left_state_->getRecordsSnapshot(p);
            auto right_snapshot = right_state_->getRecordsSnapshot(p);
            
            for (const auto& r : left_snapshot) {
                if (seen_left_uids.insert(r->uid_).second) {  // 首次出现
                    unique_left_records.push_back(r.get());
                }
            }
            
            for (const auto& r : right_snapshot) {
                if (seen_right_uids.insert(r->uid_).second) {
                    unique_right_records.push_back(r.get());
                }
            }
        }
        
        // ====== 2. 过滤已过期的记录 ======
        int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        int64_t window_lower = logicalWindowLowerBound(now);
        
        std::vector<const VectorRecord*> valid_left_records;
        std::vector<const VectorRecord*> valid_right_records;
        
        for (const auto* r : unique_left_records) {
            if (r->timestamp_ >= window_lower) {
                valid_left_records.push_back(r);
            }
        }
        for (const auto* r : unique_right_records) {
            if (r->timestamp_ >= window_lower) {
                valid_right_records.push_back(r);
            }
        }
        
        // ====== 3. 构建新的 Global Index（离线） ======
        // 注意：需要从 StorageManager 获取实际向量数据
        // 这里简化处理，实际需要：
        // - 创建新的 IVF 索引
        // - 批量插入 valid_*_records
        // - 原子替换旧索引
        
        // ====== 4. 原子替换旧 Index ======
        // TODO: 实现索引原子替换逻辑
        // concurrency_manager_->replaceIndex(vsjoin_global_left_id_, new_left_index);
        // concurrency_manager_->replaceIndex(vsjoin_global_right_id_, new_right_index);
        
        // ====== 5. 清理 Local Index 中已合并的记录 ======
        // 可选：清理 Local Index 中已过期或已合并到 Global 的记录
        
        SAGEFLOW_LOG_INFO("VSJOIN_REBUILD", 
            "Global index rebuilt: {} unique left ({} valid), {} unique right ({} valid)",
            unique_left_records.size(), valid_left_records.size(),
            unique_right_records.size(), valid_right_records.size());
    }
}

JoinOperator::~JoinOperator() {
    // ... 现有逻辑 ...
    
    // 停止后台重建线程
    stopGlobalIndexRebuilder();
}
```

#### 5.2.3 线程生命周期管理

```mermaid
sequenceDiagram
    participant EG as ExecutionGraph
    participant EV as ExecutionVertex[0..15]
    participant JO as JoinOperator (shared)
    participant RB as RebuildThread
    
    Note over EG: buildGraph() 创建 16 个 ExecutionVertex
    
    EG->>EV: start() (16个工作线程)
    EV->>JO: open(context) (16次调用)
    
    rect rgb(200, 230, 200)
        Note over JO: std::call_once 保护
        JO->>JO: startGlobalIndexRebuilder()
        JO->>RB: 创建后台线程 (仅1次)
    end
    
    Note over EV,RB: 运行时：16个工作线程 + 1个后台线程
    
    rect rgb(230, 200, 200)
        Note over RB: 每 5 秒执行一次
        RB->>JO: globalIndexRebuildLoop()
        RB->>JO: 收集所有分区记录
        RB->>JO: 去重 + 过滤过期
        RB->>JO: 重建 Global Index
    end
    
    EG->>EV: stop() (停止工作线程)
    EV->>JO: ~JoinOperator()
    JO->>RB: stopGlobalIndexRebuilder()
    RB-->>JO: join() 等待线程结束
```

#### 5.2.4 关键设计点

| 设计点 | 决策 | 原因 |

|-------|------|------|

| **线程数量** | P + 1（16 + 1 = 17） | 保持固定线程模型，不改造线程池 |

| **启动时机** | `open()` 中使用 `std::call_once` | 所有 subtask 共享同一个 JoinOperator，确保只启动一次 |

| **停止时机** | `~JoinOperator()` 析构时 | 与 JoinOperator 生命周期绑定 |

| **线程安全** | `std::atomic<bool>` 控制停止 | 后台线程安全退出 |

| **数据访问** | 通过 `WindowState::getRecordsSnapshot()` | 线程安全的快照接口 |

#### 5.2.5 与现有框架的集成

**不需要改造线程池**：

- ✅ 保持现有的固定线程模型（每个 ExecutionVertex 一个线程）
- ✅ 后台线程作为 JoinOperator 的附加线程，不影响执行图结构
- ✅ 使用 `std::call_once` 确保线程安全启动
- ✅ 生命周期与 JoinOperator 绑定，无需额外管理

**优势**：

1. **最小侵入**：只在 JoinOperator 内部添加，不影响 ExecutionGraph
2. **线程安全**：使用 `std::call_once` 和 `std::atomic` 保证安全
3. **易于管理**：生命周期清晰，与 JoinOperator 绑定
4. **性能可控**：后台线程独立，不影响工作线程性能

---

## 6. 配置扩展

### 6.1 JoinStrategyConfig 新增字段

```cpp
 // include/operator/utils/join_strategy_config.h
struct JoinStrategyConfig {
    // ... 现有字段 ...
    
    // ==================== VSJoin V2 参数 ====================
    int vsjoin_v2_multicast_k = 2;             // 边界向量多播到 k 个分区（推荐 2-3）
    int64_t vsjoin_v2_rebuild_interval_ms = 5000;  // Global 重建间隔
    size_t vsjoin_v2_rebuild_threshold = 1000;     // 触发重建的阈值
    
    // Local Index 参数（比 Global 更轻量）
    // 注意：Local Index 使用 BruteForce，无需 nlist/nprobes
    IndexType vsjoin_v2_local_index_type = IndexType::BruteForce;
    
    // Global Index 类型（IVF/HNSW）
    IndexType vsjoin_v2_global_index_type = IndexType::IVF;
};
```

---

## 7. 实现路线图

| 阶段 | 任务 | 关键文件 | 预估工时 |

|-----|------|---------|---------|

| **P1** | VSJoinMethodV2 基础实现 | `vsjoin_method_v2.h/cpp` | 2天 |

| **P2** | JoinStrategyFactory 集成 | `join_strategy_factory.cpp` | 1天 |

| **P3** | JoinOperator VSJoin 特殊路径 | `join_operator.cpp` | 1天 |

| **P4** | 后台重建机制 | `global_index_rebuilder.h/cpp` | 1.5天 |

| **P5** | 配置验证 + TOML 解析 | `join_config_validator.cpp`, `join_strategy_config.cpp` | 0.5天 |

| **P6** | 集成测试 | `test_vsjoin_v2.cpp` | 1天 |

**总预估：7 个工作日**

---

## 8. 关键设计决策总结

| 设计点 | 决策 | 原因 |

|-------|------|------|

| 索引管理 | 通过 ConcurrencyManager | 遵循架构约束，索引生命周期统一管理 |

| 窗口数据 | 复用 TwoTierWindowState | 已有分区存储 + Lazy Delete 特性 |

| Global Index 更新 | 后台线程周期性重建 | 避免写锁，保持查询无锁 |

| **Local Index 策略** | **每分区独立 index_id** | **完全隔离，无锁独占，语义清晰** |

| JoinMethod 职责 | 只负责查询协调 | 遵循单一职责，不管理索引生命周期 |

| **边界向量处理** | **多播 + 去重** | **写入时多播到 k 个分区，查询时只查本分区，完全无锁** |

| 分区路由 | LSH Partitioner + Multicast | 相似向量路由到主分区，边界向量多播保证召回率 |

| 去重策略 | 查询结果合并时 UID 去重 | O(n) 开销，n 通常 < 1000，性能影响可忽略 |

---

## 9. 多播 vs 邻近分区探测方案对比

### 9.1 方案选择：多播 + 去重（推荐）

| 维度 | 多播 + 去重 | 邻近分区探测 + 不去重 |

|-----|-----------|-------------------|

| **写入路径** | 边界向量多播到 k 个分区 | 单分区写入 |

| **查询路径** | 只查本分区（无锁） | 本分区 + 邻近分区（需要锁） |

| **存储开销** | k 倍（边界向量，通常 < 20%） | 1 倍 |

| **并发控制** | 本分区无锁写入 | 邻近分区查询需要读锁 |

| **Global Index 去重** | 需要（UID 去重，O(n)） | 不需要 |

| **召回率** | 高（边界向量不丢失） | 依赖探测范围 |

| **查询延迟** | 低（单分区查询） | 较高（多分区查询） |

| **多核扩展性** | 高（完全隔离） | 中（跨分区锁竞争） |

**推荐理由**：

1. ✅ **符合 VSJoin 目标**：无锁/低锁更新、多核扩展性、读写解耦
2. ✅ **查询路径简单**：只查本分区，无跨分区锁竞争
3. ✅ **去重成本低**：查询结果合并时用 `unordered_set<uint64_t>` 去重，O(n) 开销
4. ✅ **已有实现参考**：`CentroidPartitioner` 已支持多播

### 9.2 去重策略设计

**去重时机**：

1. **查询结果合并时**（VSJoinMethodV2::ExecuteEager）
   ```cpp
   std::unordered_set<uint64_t> uid_set;
   for (uint64_t uid : global_uids) uid_set.insert(uid);
   for (uint64_t uid : local_uids) uid_set.insert(uid);
   ```


   - 开销：O(n)，n 通常 < 1000
   - 性能影响：可忽略（< 1ms）

2. **Global Index 重建时**（GlobalIndexRebuilder）
   ```cpp
   std::unordered_set<uint64_t> seen_uids;
   for (auto& record : all_records) {
       if (seen_uids.insert(record->uid_).second) {
           unique_records.push_back(record);
       }
   }
   ```


   - 开销：O(n)，n = 窗口内总记录数
   - 频率：每 5 秒一次（可配置）

**去重性能分析**：

- 典型场景：1000 条记录，10% 边界向量（多播 k=2）
- 重复记录数：~100 条
- 去重开销：`unordered_set` 插入 100 次 ≈ 0.01ms
- **结论**：去重开销可忽略不计

---

## 10. Local Index 方案 B 的优势分析

选择方案 B（每分区独立 index_id）的优势：

| 特性 | 方案 A (PartitionedIndex) | 方案 B (独立 index_id) |

|-----|---------------------------|----------------------|

| 索引隔离 | 分区级锁 | **完全隔离，无锁** |

| ConcurrencyController | 共享一个 | **每分区独立** |

| 语义清晰度 | 隐式分区 | **subtask_i ↔ index_i** |

| 索引数量 | 2 + 2 | 2 + 2*P |

| 管理复杂度 | 低 | 中等 |

| 扩展性 | 受限于内部实现 | **灵活，可独立调整** |

**为何选择方案 B**：

1. **无锁本分区访问**：subtask_i 独占 `local_*_ids[i]`，插入/查询无需任何同步
2. **故障隔离**：一个分区的索引问题不影响其他分区
3. **独立调优**：可以为不同分区配置不同的索引参数
4. **符合 VSJoin Key Idea**：真正实现"分区独立"的设计理念
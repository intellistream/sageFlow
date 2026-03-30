# Task 04: 后台重建机制 GlobalIndexRebuilder

## 任务概述

实现 VSJoin 的 Global Index 后台重建机制，包括：
1. 后台线程管理（启动、停止、生命周期）
2. 周期性重建循环（收集记录、去重、过滤过期、重建索引）
3. 局部 `unordered_set` 去重机制（单线程，无锁）

**预估工时**: 1.5 天  
**依赖**: Task 03 (JoinOperator VSJoin 特殊路径)

## 参考文档

- 主设计文档: `docs/vsjoin_compliant_design_c745d987.plan.md`
  - 第 5.2 节: 后台重建机制（线程管理设计）
  - 第 11 章: 全局重建去重机制设计

## 实现要求

### 1. 修改文件

- **头文件**: `include/operator/join_operator.h`
- **实现文件**: `src/operator/join_operator.cpp`

### 2. 添加成员变量

在 `JoinOperator` 类中添加后台重建相关成员：

```cpp
class JoinOperator : public Operator {
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

### 3. 实现 startGlobalIndexRebuilder()

在 `join_operator.cpp` 中实现：

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
        rebuild_interval_ms_ = strategy_config_.vsjoin_rebuild_interval_ms;
        
        rebuild_thread_ = std::make_unique<std::thread>(
            &JoinOperator::globalIndexRebuildLoop, this);
        
        SAGEFLOW_LOG_INFO("VSJOIN_REBUILDER", 
            "Background rebuild thread started (interval={}ms, parallelism={})",
            rebuild_interval_ms_.load(), parallelism_);
    });
}
```

**关键点**：
- 使用 `std::call_once` 确保只启动一次（所有 subtask 共享同一个 JoinOperator）
- 从配置中读取重建间隔（`vsjoin_rebuild_interval_ms`）

### 4. 实现 stopGlobalIndexRebuilder()

```cpp
void JoinOperator::stopGlobalIndexRebuilder() {
    if (rebuild_running_.exchange(false)) {
        if (rebuild_thread_ && rebuild_thread_->joinable()) {
            rebuild_thread_->join();
        }
        SAGEFLOW_LOG_INFO("VSJOIN_REBUILDER", "Background rebuild thread stopped");
    }
}

JoinOperator::~JoinOperator() {
    // ... 现有逻辑 ...
    
    // 停止后台重建线程
    stopGlobalIndexRebuilder();
}
```

**关键点**：
- 使用 `std::atomic<bool>` 控制停止
- 在析构函数中调用，确保线程安全退出

### 5. 实现 globalIndexRebuildLoop()

**核心实现**（参考设计文档第 5.2.2 节和第 11.2 节）：

```cpp
void JoinOperator::globalIndexRebuildLoop() {
    const int64_t interval_ms = rebuild_interval_ms_.load();
    
    while (rebuild_running_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
        
        if (!rebuild_running_.load()) break;
        
        // ====== 1. 从所有 WindowState 分区收集活跃记录（多播导致重复） ======
        // ⚠️ 关键设计点：去重使用局部 unordered_set，完全局限在重建线程内，无锁无竞争
        std::unordered_set<uint64_t> seen_left_uids;   // 局部容器，不对外共享
        std::unordered_set<uint64_t> seen_right_uids;  // 局部容器，不对外共享
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
        // TODO: 实现索引重建逻辑
        // - 创建新的 IVF 索引
        // - 批量插入 valid_*_records
        // - 原子替换旧索引
        
        // ====== 4. 原子替换旧 Index ======
        // TODO: 实现索引原子替换逻辑
        // concurrency_manager_->replaceIndex(vsjoin_global_left_id_, new_left_index);
        // concurrency_manager_->replaceIndex(vsjoin_global_right_id_, new_right_index);
        
        // ====== 5. 清理 Local Index 中已合并的记录（可选） ======
        // 可选：清理 Local Index 中已过期或已合并到 Global 的记录
        
        SAGEFLOW_LOG_INFO("VSJOIN_REBUILD", 
            "Global index rebuilt: {} unique left ({} valid), {} unique right ({} valid)",
            unique_left_records.size(), valid_left_records.size(),
            unique_right_records.size(), valid_right_records.size());
    }
}
```

## 关键设计点

### 1. 线程模型

- **固定线程模型**：P + 1 个线程（P 个工作线程 + 1 个后台重建线程）
- **启动时机**：`open()` 中使用 `std::call_once` 确保只启动一次
- **停止时机**：`~JoinOperator()` 析构时停止

### 2. 去重机制（重要）

- **局部容器**：`seen_left_uids` 和 `seen_right_uids` 必须是 `globalIndexRebuildLoop()` 的局部变量
- **不对外共享**：确保它们完全局限在重建线程内，无锁无竞争
- **单线程行为**：重建是单线程行为，读取 WindowState 快照本身已通过内部锁/快照机制保证线程安全
- **性能**：O(N) 复杂度，N = 窗口内记录数，重建间隔可配置（默认 5s）

### 3. 索引重建流程

1. **收集记录**：从所有 WindowState 分区收集活跃记录（多播导致重复）
2. **去重**：使用局部 `unordered_set` 去重
3. **过滤过期**：根据窗口下界过滤已过期的记录
4. **重建索引**：创建新的 IVF 索引，批量插入有效记录
5. **原子替换**：原子替换旧索引（需要 ConcurrencyManager 支持）

### 4. 索引原子替换（TODO）

当前设计文档中索引原子替换逻辑标记为 TODO，需要：
- 在 ConcurrencyManager 中实现 `replaceIndex()` 方法
- 或者使用其他方式实现索引的原子替换

**临时方案**：
- 可以先实现记录收集、去重、过滤的逻辑
- 索引重建和替换可以标记为 TODO，后续实现

## 测试要求

### 单元测试

创建 `test/operator/test_vsjoin_rebuild.cpp`：

1. **线程启动测试**
   - 验证后台线程只启动一次（`std::call_once`）
   - 验证线程正确启动和停止

2. **去重测试**
   - 验证多播导致的重复 UID 被正确去重
   - 验证去重后的记录数量正确

3. **过期过滤测试**
   - 验证已过期的记录被正确过滤
   - 验证窗口下界计算正确

4. **周期性重建测试**
   - 验证重建循环按配置的间隔执行
   - 验证重建过程中线程安全退出

### 运行测试

```bash
cd build
ctest -R test_vsjoin_rebuild
```

## 注意事项

1. **线程安全**
   - 使用 `std::call_once` 确保线程只启动一次
   - 使用 `std::atomic<bool>` 控制停止
   - WindowState 快照通过 `getRecordsSnapshot()` 获取，线程安全

2. **去重容器必须是局部变量**
   - ⚠️ **重要**：`seen_left_uids` 和 `seen_right_uids` 必须是 `globalIndexRebuildLoop()` 的局部变量
   - 不能是成员变量，不能对外共享
   - 这保证了去重逻辑完全无锁

3. **内存管理**
   - `unique_left_records` 和 `unique_right_records` 存储的是指针，不拥有所有权
   - WindowState 快照拥有记录的所有权

4. **日志记录**
   - 使用 `SAGEFLOW_LOG_INFO` 记录重建信息
   - 日志标签使用 `"VSJOIN_REBUILDER"` 和 `"VSJOIN_REBUILD"`

5. **错误处理**
   - 检查 `left_state_` 和 `right_state_` 指针有效性
   - 检查 `concurrency_manager_` 指针有效性
   - 处理索引重建失败的情况

6. **性能考虑**
   - 重建间隔可配置（默认 5s）
   - 去重复杂度 O(N)，N = 窗口内记录数
   - 重建过程不应阻塞工作线程

## 验收标准

- [ ] 代码编译通过，无警告
- [ ] 单元测试全部通过
- [ ] 后台线程只启动一次（`std::call_once`）
- [ ] 后台线程正确停止（析构函数中）
- [ ] 去重逻辑正确（局部容器，无锁）
- [ ] 过期记录被正确过滤
- [ ] 重建循环按配置间隔执行
- [ ] 日志记录完整

## 后续任务

完成本任务后，可以继续：
- Task 06: 集成测试 + 召回率验证

## 已知问题

1. **索引原子替换**：当前设计文档中索引原子替换逻辑标记为 TODO，需要后续实现
2. **索引重建**：需要从 StorageManager 获取实际向量数据，当前可能需要简化处理

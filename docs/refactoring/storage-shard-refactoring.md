# StorageManager 分片 + ConcurrencyController 策略模式重构

## 背景

当前 VSJoin 的 Local 索引（`Knn` 类）查询时扫描全局 `StorageManager::records_`（O(N)），
分区并行化没有实际减少计算量。所有索引的写入都竞争 `StorageManager` 的同一把 `unique_lock`。
`BlankController` 对所有索引统一加 `shared_mutex`，单线程独占的 Local 索引也承受锁开销。

## 目标

1. **StorageManager 内置分片**：每个索引可拥有专属 shard，查询只扫自己的数据 O(N/P)
2. **DirectController**：无锁的 ConcurrencyController，用于单线程独占的 Local 索引
3. **Knn 精简**：删除冗余的 `local_records_`，通过 shard_id 路由实现数据隔离
4. **向后兼容**：现有 IVF/HNSW/HDR 等索引不改调用代码，默认走全局 shard

## 改动清单

### 1. StorageManager（storage_manager.h / storage_manager.cpp）

- 新增 `Shard` 内部结构体（records + map + per-shard mutex）
- 将 `records_` / `map_` / `map_mutex_` 重构为 `global_shard_`
- 新增 `shards_` map（int → unique_ptr<Shard>）+ `shards_map_mutex_`
- 所有数据方法增加 `int shard_id = GLOBAL_SHARD` 默认参数
- 新增 `createShard(int shard_id)` / `removeShard(int shard_id)`
- 内部 `resolveShard(shard_id)` 路由方法

### 2. Knn（knn.h / knn.cpp）

- 删除 `local_mutex_` 和 `local_records_`
- `insert()` / `erase()` → 简单返回 true（数据归 StorageManager shard 管理）
- `query()` 传 `index_id_` 给 `storage_manager_->topk()`
- `query_for_join()` 传 `index_id_` 给 `storage_manager_->similarityJoinQuery()`

### 3. ConcurrencyController 策略模式

- 新增 `ControllerPolicy` 枚举：`SHARED_LOCK` / `DIRECT`
- 新增 `DirectController`（direct_controller.h / direct_controller.cpp）
  - 无 `index_mutex_`，零锁开销
  - insert 时传 `index_id_` 路由到专属 shard
- `BlankController` insert/erase 传 `index_id_` 路由（全局 shard 默认值不变）

### 4. ConcurrencyManager

- `create_index()` 重载增加 `ControllerPolicy` 参数
- `DIRECT` 策略时自动调用 `storage_->createShard(index_id)`
- 根据 policy 创建 `DirectController` 或 `BlankController`

### 5. join_strategy_factory

- VSJoin Global 索引：`ControllerPolicy::SHARED_LOCK`（不变）
- VSJoin Local 索引：`ControllerPolicy::DIRECT`（新增）

## 数据流

```
插入 (Local): DirectController → storage->insert(record, index_id_) → shard[index_id_]
查询 (Local): DirectController → knn->query_for_join() → storage->similarityJoinQuery(..., index_id_) → shard[index_id_] O(N/P)
插入 (Global): BlankController → storage->insert(record, GLOBAL_SHARD) → global_shard_
查询 (Global): BlankController → ivf->query_for_join() → storage->getVectorByUid() → global_shard_ (不变)
```

## 并发安全

| 场景 | 锁 | 竞争 |
|------|-----|------|
| Local shard 单线程读写 | per-shard mutex | 零竞争 |
| Global shard 多线程读 | shared_lock | 读并发 OK |
| shards_ map 查找 | shards_map_mutex_ (shared) | 运行期只读 |

## 验证计划

1. 全量 ctest 单元测试通过
2. 集成测试：所有 join 方法（bruteforce, ivf, hnsw, vsjoin, s3j, clustered_join）
3. VSJoin 性能对比：重构前后 throughput / recall / latency

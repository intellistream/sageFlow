## 1. 准备与基线

- [x] 1.1 在 `sageFlow/` 运行 `git status --short` 确认无冲突改动，备份 `window_state.h`、三个 WindowState 实现、`base_method.h`、各 JoinMethod、`join_operator.cpp/.h` 到 `.toolchains/code_backup/`
- [ ] 1.2 用隔离环境跑基线测试并存档：`test_join_operator_state`、`test_join_bruteforce`、`test_join_ivf` 全绿，记录 `test_join_datasource_modes` 小矩阵（bruteforce/ivf，p=1/2/4/8，小窗口）的 recall 与 time_ms
- [ ] 1.3 用 gperftools 采集基线 profile（ivf p=4 小窗口），记录 tcmalloc Populate 与深拷贝相关占比，作为优化前对照

## 2. 类型契约引入

- [x] 2.1 在公共头引入别名 `using RecordView = std::shared_ptr<const VectorRecord>;`（集中管理，便于回退）
- [x] 2.2 所有权边界已定稿（design 决策 4）：传输层 `Response`/队列保持 `unique_ptr`+move 不改；shared_ptr 仅用于算子内部；转换点在 `apply` 入口 move；emit 边界保守保留 `unique_ptr` 构造

## 3. WindowState 接口与实现改造

- [x] 3.1 修改 `include/state/window_state.h`：`getRecordsSnapshot` 返回 `std::vector<RecordView>`；内部记录存储元素类型改为 `RecordView`；明确 addRecord 接收可共享所有权的记录
- [x] 3.2 改造 `SharedWindowState`：`shared_window_` 改为 `std::deque<RecordView>`；`addRecord` 不再深拷贝；`getRecordsSnapshot` 持 `shared_lock` 仅拷指针；保持单一全量结构（不分片）
- [x] 3.3 改造 `PartitionedWindowState`：分区 deque 元素改为 `RecordView`，保持分区级锁与时间戳语义
- [x] 3.4 改造 `TwoTierWindowState`：容器元素改为 `RecordView`，核对与后台 rebuild 线程的 shared_ptr 生命周期同步点
- [x] 3.5 运行 `test_join_operator_state` 验证窗口语义不变

## 4. 候选获取与 JoinMethod 改造

- [x] 4.1 修改 `BaseMethod::ExecuteEager` 返回类型为 `std::vector<RecordView>`
- [x] 4.2 适配所有 JoinMethod 实现（bruteforce/ivf/hnsw/hdr_tree/lsh/clustered_join/vsjoin）的 `ExecuteEager` 返回路径，贯通 `StorageManager`/`ConcurrencyManager::query_for_join` 已有的 `shared_ptr<const VectorRecord>`，不再降级深拷贝
- [x] 4.3 修改 `VSJoinMethod::resolveUidsToRecords` 直接传递 `shared_ptr<const VectorRecord>`
- [x] 4.4 修改 `JoinOperator::getCandidatesFromState` 返回 `std::vector<RecordView>`
- [x] 4.5 运行 `test_join_bruteforce`、`test_join_ivf` 验证候选与召回不变

## 5. apply / 插入 / emit 拷贝点收敛

- [x] 5.1 修改 `apply` 入口：将 [join_operator.cpp:1091](file:///Users/bytedance/icpp-demo/sageFlow/src/operator/join_operator.cpp#L1091) 的 `make_unique<VectorRecord>(*record.record_)` 深拷贝改为 `std::move(record.record_)` 转 `RecordView`（零拷贝接管）；保证 VSJoin 路由 `computeVSJoinTargetSubtasks` 在 move 之前计算或改基于 view
- [x] 5.2 修改 `updateSideWithState`：以 `RecordView` 同时交给窗口与索引/存储（引用计数共享同一实例），消除 `data_for_index_insert` 与 `data_for_join` 的额外深拷贝
- [x] 5.3 修改 `executeJoinWithState`：快照取一次 `vector<RecordView>`；内层时间窗口过滤与相似度比较用裸指针 `const VectorRecord*`，禁止在比较循环内按值拷贝 shared_ptr；跨函数边界用 `const RecordView&` 传引用
- [x] 5.4 emit 边界保守路线：命中对处构造一次结果 `VectorRecord` 放回 `Response` 的 `unique_ptr`（保留），不为未命中项构造，不改 `Response`/队列类型
- [x] 5.5 确认相似度计算与结果构造均在窗口锁释放之后（基于快照视图）执行

## 6. 触发与清理收敛

- [x] 6.1 将算子内 `max_seen_left_ts_/right_ts_` 统一到 `WindowState::getSafeEvictTimestamp`（两侧 watermark 最小值），移除算子级重复时间戳维护
- [x] 6.2 确认过期 `flushExpiredUids` → `ConcurrencyManager` 删除链路使窗口/索引/存储最终一致
- [x] 6.3 移除或隔离未接入的 `isNeedTrigger`/`SlidingWindow` 占位，正式注释“每记录即时 IQ”为唯一触发模型

## 7. 验证与回归

- [x] 7.1 全量 join 单测回归：`test_join_operator_state`、`test_join_bruteforce`、`test_join_ivf`、`test_join_strategy_factory`、`test_vsjoin_*` 全绿
- [x] 7.2 用 debug 日志核对窗口内向量数据流（insert→对侧 query 的 searching N records）与重构前一致
- [x] 7.3 跑 `test_join_datasource_modes` 小矩阵：确认 recall 与改造前一致（不丢召回），且共享索引 p=1/2/4/8 扩展性不退化
- [ ] 7.4 gperftools 对比：tcmalloc Populate / 深拷贝占比相对基线下降；记录前后对照
- [ ] 7.5 还原临时配置，核对 `git status`，整理改动文件清单与生命周期内存管理说明

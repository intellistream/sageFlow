## ADDED Requirements

### Requirement: 不可变共享视图作为全链路传递单位

系统 SHALL 在向量记录进入 `JoinOperator` 之后，以 `std::shared_ptr<const VectorRecord>` 作为窗口状态、候选获取、相似度计算与 emit 之间传递的统一所有权单位。一旦一条 `VectorRecord` 被构造并纳入生命周期管理，其向量数据（`VectorData::data_`）在该记录的存活期内 MUST NOT 被再次深拷贝；跨组件传递 MUST 通过引用计数（拷贝 `shared_ptr`）完成。

#### Scenario: 候选获取不深拷贝向量数据
- **WHEN** `JoinOperator` 通过 `getCandidatesFromState` / `ExecuteEager` 从对侧窗口获取候选
- **THEN** 返回的候选以 `shared_ptr<const VectorRecord>` 形式提供，仅增加引用计数，不复制 `VectorData` 的底层 `char[]`

#### Scenario: 窗口快照不深拷贝向量数据
- **WHEN** 调用窗口状态的快照接口获取当前窗口内记录
- **THEN** 快照中的每个元素是指向既有记录的 `shared_ptr<const VectorRecord>`，不发生逐元素的向量数据深拷贝

### Requirement: 单一权威副本与引用计数转移点

系统 SHALL 为每条进入 Join 的向量维护单一权威 `VectorRecord` 实例，并在窗口状态与 `StorageManager` 之间共享同一实例的所有权（同一 `shared_ptr` 的多个持有者），而非各自持有独立深拷贝。记录的释放 MUST 由最后一个 `shared_ptr` 持有者离开作用域时自动完成。

#### Scenario: 窗口与存储共享同一实例
- **WHEN** 一条记录被插入窗口状态并同时进入 `StorageManager`
- **THEN** 两侧持有指向同一 `VectorRecord` 实例的 `shared_ptr`，记录在两侧任一仍持有时不被释放

#### Scenario: 过期清理后内存安全释放
- **WHEN** 记录从窗口过期被驱逐，且其 UID 从索引/存储批量删除
- **THEN** 当且仅当所有持有者（窗口、存储、在途候选/快照）都释放后，底层 `VectorRecord` 才被销毁，期间不出现悬垂访问

### Requirement: 不可变性保证并发只读安全

系统 SHALL 保证被多个线程并发读取的窗口记录是不可变的（通过 `const VectorRecord`）。任何线程 MUST NOT 在记录已被纳入共享视图后修改其内容；需要修改语义时 MUST 构造新的记录实例。

#### Scenario: 多 subtask 并发查询同一共享窗口
- **WHEN** 多个 subtask 线程并发对同一 `SharedWindowState` 执行查询/快照
- **THEN** 它们读取到的是不可变 `const VectorRecord`，无数据竞争，无需写锁即可安全读取数据内容

### Requirement: 生命周期边界与禁止深拷贝位置的可验证性

系统 SHALL 使全链路的拷贝点可被审计：在 `apply` → `updateSideWithState` → 窗口插入 → 索引/存储插入 → `executeJoinWithState` → emit 路径上，除“进入算子时的一次规范化构造”外，MUST NOT 存在对 `VectorRecord` 的额外深拷贝。

#### Scenario: 审计热路径拷贝次数
- **WHEN** 对单条到达记录走完 insert→query→emit 全流程
- **THEN** 该记录的 `VectorData` 深拷贝次数为常数且与并行度无关，可通过代码审查或计数器确认不随候选数量增长

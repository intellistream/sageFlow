# Log Level Audit (2025-09-11)

本文件汇总当前仍保留为 INFO 级别的日志，分类说明其用途；高频或调试型已在此前降为 DEBUG。若后续需要进一步精简，可按分类调整。

## 1. 框架生命周期 / 控制流 (保留 INFO)
| Phase | 位置 | 说明 |
|-------|------|------|
| ENV | `stream_environment.cpp` start/stop/reset | 环境启动、停止、重置边界事件 |
| GRAPH | `execution_graph.cpp` start/stop | 执行图整体启动/停止（用户可感知的生命周期） |
| VERTEX | `execution_vertex.cpp` (若仍为 INFO) | 单个执行顶点结束标记（可考虑降为 DEBUG，如批量线程多时） |
| LOG | `log_config.cpp` log_level_applied | 记录最终生效日志级别（首次初始化有价值） |

## 2. 数据源 / 存储 (一般低频，可留 INFO)
| Phase | 位置 | 说明 | 建议 |
|-------|------|------|------|
| SOURCE | `simple_stream_source.cpp` 空路径提示 | 配置问题提示 | 保留 |
| SOURCE | `sift_stream_source.cpp` 加载完成 count | 数据集加载完成的规模确认 | 保留 |
| STORAGE | `storage_manager.cpp` 插入记录计数 | 当前存储规模（可能高频：每条插入） | 建议改 DEBUG 或采样输出 |
| SINK | `sink_operator.cpp` 处理记录 | 高频（下游汇聚） | 建议改 DEBUG 或统计聚合 |

## 3. 监控 / Profiling (可留 INFO 或按需 DEBUG)
| Phase | 位置 | 说明 | 建议 |
|-------|------|------|------|
| MONITOR | `monitoring.cpp` profiling_started / stopped | 显式开启/停止事件 | 保留 |
| MONITOR | `monitoring.cpp` timer_started | 轻量边界，多次调用可能冗余 | 可降 DEBUG |
| MONITOR | `monitoring.cpp` task_done | 任务耗时摘要 | 保留（为性能概览） |

## 4. 测试代码 (不影响产线；保持 INFO 便于 CI 输出)
包含 `test/IntegrationTest`, `test/UnitTest`, `test/Performance` 目录下所有 `CANDY_LOG_INFO`：
- 用途：构造成功、数据集规模、匹配结果数、性能结果、配置驱动测试摘要。
- 建议：测试内保持 INFO，便于 CI 日志直接查看；若日志量过大，可筛选高频循环内日志转 DEBUG。

## 5. 建议进一步动作（可选）
1. STORAGE/SINK 高频日志改为 DEBUG，或加节流：例如每 100 或 1000 条输出一次累计计数。
2. MONITOR 中 `timer_started` 改 DEBUG，保持 start/stop 和 task_done 为 INFO。
3. 提供运行参数 `--log-level` 覆盖配置文件/环境变量（当前已支持 config->env；再加命令行解析即可）。
4. 增加一个聚合周期性 INFO（例如每隔 N 秒输出窗口大小/索引大小指标）。

## 6. 现有动态级别机制
初始化顺序：
1. 读取配置 `log.level`（若存在）。
2. 环境变量 `CANDY_LOG_LEVEL` 覆盖配置。
3. 记录最终生效级别：`[LOG] log_level_applied`。

示例配置：
```toml
[log]
level = "debug"  # 支持 trace/debug/info/warn/error/critical/off
```

环境变量覆盖（PowerShell）：
```powershell
$env:CANDY_LOG_LEVEL = "warn"
```

## 7. 后续可扩展点
- 支持日志输出到文件（添加 rotating_file_sink）。
- 支持为特定 phase 设置独立级别：解析形如 "JOIN=debug,ENV=info" 的扩展语法（需要自定义过滤器）。
- 通过 Prometheus 指标/HTTP 端点动态调整级别。

---
本文件生成时间：2025-09-11。
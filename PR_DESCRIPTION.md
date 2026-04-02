# chore/vsjoin-factory-paper-sync-20260303

## 变更概览

本 PR 聚焦三项收敛工作：

1. **工厂化主链路收敛**
   - Join 方法创建已接入主执行链路，策略为 **registry 优先 + switch 兜底**。
   - VSJoin 已完成方法自注册，可通过注册中心直接创建。

2. **配置契约一致性修复（VSJoin）**
   - VSJoin 运行时分区器改为严格按配置契约创建，不再隐式改写为 `Centroid`。
   - `WindowState` 创建不再被 VSJoin 强制覆盖，改为按配置生效。
   - 已同步更新 LSH 对 `WindowState` 的推荐集合。

3. **论文资产并入**
   - 已恢复并提交重命名后的论文目录：
     `docs/research-paper-High_Throughput_Streaming_Vector_Similarity_Joins_on_Multicore_Processors/`
   - 包含 `main.tex`、`References.bib`、`Sections/*` 等核心文件。

> 约束保持：未修改 VSJoin 集成测试占位文件 `test/IntegrationTest/test_vsjoin_integration.cpp`。

---

## 回归与验证（2026-03-03）

### 构建环境

- CMake: `3.27.7`
- 配置与构建：
  - `cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON`
  - `cmake --build build -j $(nproc)`
- 结果：构建成功（仅有既有 warning，无新增编译错误）。

### Join 关键回归

- 执行：`./build/bin/test_join_datasource_modes`
- 结果：`[  PASSED  ] 23 tests.`

### VSJoin 集成核对（配置契约：partition/window_state/index）

- 执行（过滤 VSJoin 相关）：
  - `/usr/bin/python scripts/run_integration_test.py --methods vsjoin --config config/integration_test_cases.toml --gtest-filter='*vsjoin_baseline*:*vsjoin_high_recall*' --output-dir test/result/integration_vsjoin_contract_20260303_063650`
- 结果摘要：
  - 总计 9 组：通过 3，失败 6
  - `p=1` 通过（recall=1.0）
  - `p=2` recall ≈ 0.503，`p=4` recall ≈ 0.253（未达阈值）
  - precision 维持 1.0

### 结论

- **工厂化主链路与契约一致性改动已集成并可构建。**
- **VSJoin 在多并行度场景（p>1）仍存在稳定性/召回退化问题，当前不应标记为“稳定通过”。**

---

## 附：产出路径

- VSJoin 集成回归报告：
  - `test/result/integration_vsjoin_contract_20260303_063650/run_20260303_063650/report.md`
  - `test/result/integration_vsjoin_contract_20260303_063650/run_20260303_063650/report.json`

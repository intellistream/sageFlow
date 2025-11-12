# Join算法研究论文复现项目指南

## 项目概述

本项目旨在复现多篇Join算法相关的研究论文，涵盖以下几个主要研究方向：

1. **流式结构化数据处理**: SplitJoin[ATC'16]
2. **向量批处理相似度连接**: 
   - Implementing Distributed Approximate Similarity Joins using Locality Sensitive Hashing[EDBT'22]
   - Fast Approximate Similarity Join in Vector Databases[SIGMOD'25]
3. **向量流式相似度连接**:
   - Adaptive Distributed Streaming Similarity Joins[DEBS'23]
   - Efficient kNN Join over Dynamic High-Dimensional Data[ADC'22]
4. **静态ANNS索引适配**: 基于Faiss IVF、HNSW和DiskANN Vamana的ε相似度检索topK支持

## 如何使用Issue模板

### 创建主Issue

1. 访问仓库的Issues页面
2. 点击"New Issue"
3. 选择"Join算法研究论文复现"模板
4. 填写项目概述和整体规划
5. 该主Issue将作为整个项目的追踪入口

### 创建子Issue

根据具体的研究方向，创建对应的子Issue：

#### 1. 流式结构化数据Join
- 模板: "流式结构化数据Join"
- 用于: 复现SplitJoin[ATC'16]论文
- 关键技术: 流式分区、动态负载均衡、可调节窗口

#### 2. 向量批处理相似度Join
- 模板: "向量批处理相似度Join"
- 用于: 复现EDBT'22和SIGMOD'25论文
- 关键技术: LSH索引、向量数据库优化、批处理查询

#### 3. 向量流式相似度Join
- 模板: "向量流式相似度Join"
- 用于: 复现DEBS'23和ADC'22论文
- 关键技术: 自适应分区、动态索引、增量kNN计算

#### 4. ANNS索引集成与适配
- 模板: "ANNS索引集成与适配"
- 用于: 集成Faiss IVF、HNSW、DiskANN Vamana
- 关键技术: 索引适配、ε-相似度查询、topK维护

## Issue组织结构

建议按照以下层次组织Issue：

```
主Issue: [Join研究复现] Join算法研究论文复现总项目
├── Sub-Issue 1: [Join-Stream-Structured] 实现SplitJoin算法
├── Sub-Issue 2: [Join-Vector-Batch] 实现LSH相似度连接 (EDBT'22)
├── Sub-Issue 3: [Join-Vector-Batch] 实现向量数据库快速连接 (SIGMOD'25)
├── Sub-Issue 4: [Join-Vector-Stream] 实现自适应流式连接 (DEBS'23)
├── Sub-Issue 5: [Join-Vector-Stream] 实现动态kNN连接 (ADC'22)
├── Sub-Issue 6: [Join-ANNS-Index] 集成Faiss IVF索引
├── Sub-Issue 7: [Join-ANNS-Index] 集成HNSW索引
└── Sub-Issue 8: [Join-ANNS-Index] 集成DiskANN Vamana索引
```

## 工作流程建议

### 1. 初始化阶段
- [ ] 创建主Issue，规划整体项目
- [ ] 确定各子任务的优先级
- [ ] 分配各子任务的负责人
- [ ] 建立项目时间表

### 2. 文献调研阶段
- [ ] 阅读相关论文
- [ ] 查找开源实现
- [ ] 分析技术难点
- [ ] 编写调研报告

### 3. 实现阶段
- [ ] 为每个子任务创建对应的Sub-Issue
- [ ] 按照Issue模板中的实现计划逐步推进
- [ ] 定期更新Issue状态
- [ ] 记录遇到的问题和解决方案

### 4. 测试验证阶段
- [ ] 准备测试数据集
- [ ] 进行功能测试
- [ ] 进行性能测试
- [ ] 与原论文结果对比

### 5. 文档整理阶段
- [ ] 编写代码文档
- [ ] 编写使用说明
- [ ] 整理实验报告
- [ ] 发布复现报告

## 技术栈

### 编程语言
- C++ (核心实现)
- Python (实验脚本、数据分析)

### 主要依赖库
- **流处理**: sageFlow核心框架
- **向量计算**: BLAS, Eigen
- **索引库**: Faiss, hnswlib, DiskANN
- **构建系统**: CMake
- **测试框架**: Google Test

### 数据集
- 结构化数据: TPC-H, NYC Taxi
- 向量数据: SIFT, GIST, Deep1B
- 流式数据: 自定义数据生成器

## 评估标准

### 正确性
- [ ] 功能正确性验证
- [ ] 与论文结果一致性检验
- [ ] 边界情况测试

### 性能指标
- [ ] 延迟 (Latency)
- [ ] 吞吐量 (Throughput)
- [ ] 召回率 (Recall)
- [ ] 准确率 (Precision)
- [ ] 资源利用率 (CPU/Memory/Disk)

### 可扩展性
- [ ] 数据规模扩展性
- [ ] 节点数量扩展性
- [ ] 维度扩展性

## 协作规范

### Issue更新
- 定期更新Issue进度（建议至少每周一次）
- 标记已完成的checklist项
- 及时报告阻塞问题
- 记录重要的技术决策

### 代码规范
- 遵循项目代码风格
- 添加必要的注释
- 编写单元测试
- 进行代码review

### 文档规范
- 使用中文编写文档
- 保持文档与代码同步
- 提供清晰的示例
- 包含必要的图表说明

## 参考资源

### 论文列表
1. SplitJoin: A Scalable, Low-latency Stream Join Architecture with Adjustable Ordering Semantics (ATC'16)
2. Implementing Distributed Approximate Similarity Joins using Locality Sensitive Hashing (EDBT'22)
3. Fast Approximate Similarity Join in Vector Databases (SIGMOD'25)
4. Adaptive Distributed Streaming Similarity Joins (DEBS'23)
5. Efficient kNN Join over Dynamic High-Dimensional Data (ADC'22)

### 相关库
- Faiss: https://github.com/facebookresearch/faiss
- hnswlib: https://github.com/nmslib/hnswlib
- DiskANN: https://github.com/microsoft/DiskANN

### 项目文档
- [设计文档](./Design.md)
- [架构文档](./Structure.md)
- [README](../README.md)

## 常见问题

### Q1: 如何选择实现优先级？
**A**: 建议按照以下顺序：
1. ANNS索引集成（Faiss IVF优先）- 作为基础设施
2. 向量批处理相似度Join - 验证索引功能
3. 向量流式相似度Join - 核心功能
4. 流式结构化数据Join - 扩展功能

### Q2: 如何处理论文中未明确的实现细节？
**A**: 
- 查找论文的补充材料和技术报告
- 参考开源实现
- 在Issue中讨论并记录技术选择
- 必要时联系论文作者

### Q3: 如何验证复现的正确性？
**A**:
- 使用论文中相同或相似的数据集
- 对比关键性能指标
- 复现论文中的实验图表
- 进行消融实验验证各个组件

### Q4: 遇到性能瓶颈怎么办？
**A**:
- 使用性能分析工具（如perf, gprof）
- 检查算法实现是否有优化空间
- 考虑并行化和向量化
- 在Issue中讨论并寻求帮助

## 联系方式

如有问题或建议，请通过以下方式联系：
- 在相关Issue中评论
- 项目讨论区
- 项目维护者邮箱

---

最后更新: 2025-11-12

---
name: "Join算法研究论文复现"
about: "追踪和管理Join算法相关研究论文的复现工作"
title: "[Join研究复现] "
labels: research, join-algorithms
assignees: ''
---

## 项目概述
本项目旨在复现多篇Join算法相关的研究论文，涵盖流式结构化数据处理、向量批处理相似度连接、向量流式相似度连接，以及基于静态ANNS索引的相似度检索优化。

## 研究分类与子任务

### 1. 流式结构化数据处理
- [ ] **SplitJoin [ATC'16]**
  - 论文标题: SplitJoin: A Scalable, Low-latency Stream Join Architecture with Adjustable Ordering Semantics
  - 会议/期刊: USENIX ATC 2016
  - 主要工作: 
    - 实现可扩展、低延迟的流式连接架构
    - 支持可调节的排序语义
    - 处理结构化数据流的实时连接
  - 需要实现的功能:
    - [ ] 流式分区策略
    - [ ] 动态负载均衡
    - [ ] 可调节的窗口机制
    - [ ] 低延迟连接算法
  - 相关Issue: #TODO

### 2. 向量批处理相似度连接

#### 2.1 基于LSH的分布式近似相似度连接
- [ ] **Implementing Distributed Approximate Similarity Joins using Locality Sensitive Hashing [EDBT'22]**
  - 论文标题: Implementing Distributed Approximate Similarity Joins using Locality Sensitive Hashing
  - 会议/期刊: EDBT 2022
  - 主要工作:
    - 使用局部敏感哈希(LSH)实现分布式近似相似度连接
    - 优化大规模向量数据的批处理连接
    - 降低计算复杂度同时保持高召回率
  - 需要实现的功能:
    - [ ] LSH索引构建
    - [ ] 分布式哈希桶管理
    - [ ] 候选对生成与过滤
    - [ ] 相似度阈值调节
  - 相关Issue: #TODO

#### 2.2 向量数据库中的快速近似相似度连接
- [ ] **Fast Approximate Similarity Join in Vector Databases [SIGMOD'25]**
  - 论文标题: Fast Approximate Similarity Join in Vector Databases
  - 会议/期刊: SIGMOD 2025
  - 主要工作:
    - 针对向量数据库优化的快速近似相似度连接算法
    - 利用现代向量索引结构提升性能
    - 支持大规模高维向量批处理连接
  - 需要实现的功能:
    - [ ] 向量索引优化策略
    - [ ] 批处理查询优化
    - [ ] 剪枝与过滤机制
    - [ ] 自适应参数调整
  - 相关Issue: #TODO

### 3. 向量流式相似度连接

#### 3.1 自适应分布式流式相似度连接
- [ ] **Adaptive Distributed Streaming Similarity Joins [DEBS'23]**
  - 论文标题: Adaptive Distributed Streaming Similarity Joins
  - 会议/期刊: DEBS 2023
  - 主要工作:
    - 实现分布式环境下的自适应流式相似度连接
    - 根据数据分布动态调整连接策略
    - 处理高速向量数据流
  - 需要实现的功能:
    - [ ] 自适应分区策略
    - [ ] 动态索引更新
    - [ ] 负载均衡机制
    - [ ] 流式窗口管理
  - 相关Issue: #TODO

#### 3.2 动态高维数据的高效kNN连接
- [ ] **Efficient kNN Join over Dynamic High-Dimensional Data [ADC'22]**
  - 论文标题: Efficient kNN Join over Dynamic High-Dimensional Data
  - 会议/期刊: ADC 2022
  - 主要工作:
    - 针对动态高维数据的高效kNN连接算法
    - 处理数据插入、删除、更新等动态操作
    - 维护连接结果的一致性
  - 需要实现的功能:
    - [ ] 动态索引维护
    - [ ] 增量kNN计算
    - [ ] 结果更新策略
    - [ ] 内存管理优化
  - 相关Issue: #TODO

### 4. 静态ANNS索引适配与集成

#### 4.1 Faiss IVF索引
- [ ] **Faiss IVF (Inverted File Index)**
  - 主要工作:
    - 集成Faiss库中的IVF索引
    - 改造IVF索引以支持ε相似度检索
    - 实现topK查询优化
  - 需要实现的功能:
    - [ ] Faiss库集成
    - [ ] IVF索引构建与管理
    - [ ] ε-相似度查询接口
    - [ ] 流式数据增量更新
    - [ ] topK结果维护
  - 相关Issue: #TODO

#### 4.2 HNSW索引
- [ ] **HNSW (Hierarchical Navigable Small World)**
  - 主要工作:
    - 集成HNSW库
    - 利用层次化导航小世界图结构
    - 支持高效的近似最近邻搜索
  - 需要实现的功能:
    - [ ] HNSW库集成
    - [ ] 分层图构建
    - [ ] ε-相似度搜索适配
    - [ ] 动态节点插入
    - [ ] topK查询优化
  - 相关Issue: #TODO

#### 4.3 DiskANN Vamana索引
- [ ] **DiskANN Vamana**
  - 主要工作:
    - 集成DiskANN的Vamana索引
    - 利用其自带的并行化策略
    - 支持大规模数据的磁盘友好存储
  - 需要实现的功能:
    - [ ] DiskANN库集成
    - [ ] Vamana图构建
    - [ ] 并行查询执行
    - [ ] 内存-磁盘混合管理
    - [ ] 流式连接操作支持
  - 相关Issue: #TODO

## 整体技术路线

### 数据流处理架构
- [ ] 设计统一的流式数据摄入接口
- [ ] 实现通用的窗口管理机制
- [ ] 构建可扩展的状态管理系统

### 索引与查询优化
- [ ] 实现多种索引结构的统一接口
- [ ] 优化索引构建与更新性能
- [ ] 设计自适应查询策略

### 系统集成与测试
- [ ] 集成各个组件
- [ ] 性能基准测试
- [ ] 与原论文结果对比验证

## 实验与评估计划

### 数据集
- [ ] 准备结构化数据流数据集
- [ ] 准备高维向量数据集
- [ ] 准备实时流式数据集

### 评估指标
- [ ] 延迟 (Latency)
- [ ] 吞吐量 (Throughput)
- [ ] 召回率 (Recall)
- [ ] 准确率 (Precision)
- [ ] 资源利用率 (CPU/Memory/Disk)

### 基准对比
- [ ] 与原论文方法对比
- [ ] 与现有系统对比
- [ ] 不同配置下的性能分析

## 文档与可交付成果
- [ ] 代码实现文档
- [ ] API使用说明
- [ ] 实验报告
- [ ] 性能分析报告
- [ ] 论文复现报告

## 相关资源

### 论文链接
- SplitJoin: [论文链接]
- LSH Similarity Join: [论文链接]
- Fast Similarity Join: [论文链接]
- Adaptive Streaming Join: [论文链接]
- kNN Join: [论文链接]

### 代码仓库
- Faiss: https://github.com/facebookresearch/faiss
- HNSW: https://github.com/nmslib/hnswlib
- DiskANN: https://github.com/microsoft/DiskANN

### 相关文档
- [项目设计文档](../../docs/Design.md)
- [项目结构文档](../../docs/Structure.md)

## 备注
请在开始每个子任务之前创建对应的独立Issue，并在上述清单中链接相关Issue编号。每个子Issue应包含详细的实现计划、技术细节和评估标准。

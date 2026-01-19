# SageFlow 接入 SAGE Pipeline 开发指南

## 目录

1. [概述](#1-概述)
2. [架构设计](#2-架构设计)
3. [环境配置](#3-环境配置)
4. [SageFlow Python API 参考](#4-sageflow-python-api-参考)
5. [接入规范](#5-接入规范)
6. [应用场景示例](#6-应用场景示例)
7. [常见问题](#7-常见问题)

---

## 1. 概述

### 1.1 什么是 SageFlow

SageFlow 是一个**向量原生流处理引擎**，使用 C++ 实现核心计算，通过 pybind11 提供 Python 接口。它专为实时 LLM 生成任务设计，提供高性能的向量操作：

- **Join**: 流式向量相似度匹配（支持 BruteForce、IVF、HNSW 等算法）
- **TopK**: 流式 Top-K 向量检索
- **Aggregate**: 窗口内向量聚合（均值、质心等）
- **Filter**: 基于相似度阈值的向量过滤

### 1.2 在 SAGE Pipeline 中的定位

SageFlow 作为 SAGE DataStream Pipeline 的**中间组件**，负责高性能向量计算：

```
┌──────────────────────────────────────────────────────────────────────────┐
│                      SAGE DataStream Pipeline                             │
│                                                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │  Source (from_batch / from_source)                                  │ │
│  │       ↓                                                              │ │
│  │  .map(EmbeddingMapFunction)         # SAGE 上游: 生成 embedding      │ │
│  │       ↓                                                              │ │
│  │  .map(SageFlowOperator)             # SageFlow: C++ 向量处理         │ │
│  │       ↓                              (Join/TopK/Aggregate/Filter)   │ │
│  │  .map(DownstreamProcessor)          # SAGE 下游: 业务逻辑            │ │
│  │       ↓                                                              │ │
│  │  .sink(ResultCollector)             # SAGE Sink: 输出                │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
│  env.submit()  →  SAGE Kernel 统一调度执行                                │
└──────────────────────────────────────────────────────────────────────────┘
```

**职责划分**：
- **SAGE**: 数据源管理、Embedding 生成、下游业务逻辑、Pipeline 调度
- **SageFlow**: 高性能 C++ 向量计算（Join/TopK/Aggregate/Filter）

---

## 2. 架构设计

### 2.1 数据流模型

SageFlow 使用**流式数据模型**，核心数据结构是 `VectorRecord`：

```python
# VectorRecord 逻辑结构
{
    "uid": int,           # 唯一标识符
    "timestamp": int,     # 时间戳 (毫秒)
    "vector": np.ndarray  # 向量数据 (float32)
}
```

### 2.2 核心组件

```
┌─────────────────────────────────────────────────────────────────┐
│                    SageFlow 核心组件                             │
├─────────────────────────────────────────────────────────────────┤
│  StreamEnvironment    # 执行环境，管理所有流                      │
│       │                                                         │
│       ├── SimpleStreamSource  # 数据源（支持动态添加记录）         │
│       │       │                                                 │
│       │       ├── .join()      # 向量 Join 操作                  │
│       │       ├── .topk()      # Top-K 检索                      │
│       │       ├── .aggregate() # 窗口聚合                        │
│       │       ├── .filter()    # 向量过滤                        │
│       │       └── .writeSink() # 输出到 Sink                     │
│       │                                                         │
│       └── Stream              # 中间流（算子链）                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. 环境配置

### 3.1 依赖安装

```bash
# 1. 构建 SageFlow C++ 库
cd sageFlow
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j $(nproc)

# 2. 安装 SAGE 核心包
pip install -e /path/to/SAGE/packages/sage-common
pip install -e /path/to/SAGE/packages/sage-kernel
pip install -e /path/to/SAGE/packages/sage-middleware

# 3. 设置环境变量
export LD_LIBRARY_PATH=/path/to/sageFlow/build/lib:$LD_LIBRARY_PATH
export PYTHONPATH=/path/to/sageFlow/build/lib:$PYTHONPATH
```

### 3.2 验证安装

```python
# 验证 SageFlow
import sys
sys.path.insert(0, "/path/to/sageFlow/build/lib")
import _sage_flow as sf
print("SageFlow version:", sf.__doc__)

# 验证 SAGE Kernel
from sage.kernel.api import LocalEnvironment
from sage.common.core.functions.map_function import MapFunction
print("SAGE Kernel ready")
```

### 3.3 Embedding 服务配置

SageFlow 依赖 Embedding 服务生成向量。推荐配置：

```python
# 环境变量方式
export EMBEDDING_BASE_URL="http://localhost:8090/v1"
export EMBEDDING_MODEL="BAAI/bge-large-en-v1.5"
export EMBEDDING_DIM="1024"
```

```python
# 代码方式
embedder = OpenAICompatibleEmbedding(
    base_url="http://localhost:8090/v1",
    model="BAAI/bge-large-en-v1.5",
    dim=1024,
)
```

---

## 4. SageFlow Python API 参考

### 4.1 StreamEnvironment

执行环境，管理所有数据流的生命周期。

```python
import _sage_flow as sf

# 创建环境
env = sf.StreamEnvironment()

# 添加流
env.addStream(source)

# 执行 Pipeline
env.execute()
```

### 4.2 SimpleStreamSource

数据源，支持动态添加向量记录。

```python
# 创建数据源
source = sf.SimpleStreamSource("my_source")

# 添加记录
source.addRecord(
    uid=1,                    # 唯一标识符
    timestamp=1234567890,     # 时间戳（毫秒）
    vector=np.array([...], dtype=np.float32)  # 向量
)

# 配置 Join 参数
source.setJoinMethod("bruteforce_lazy")  # 算法: bruteforce_lazy, ivf, hnsw
source.setJoinSimilarityThreshold(0.3)   # 相似度阈值
source.setParallelism(4)                 # 并行度
```

### 4.3 流操作算子

#### 4.3.1 Join (向量匹配)

```python
def join_function(
    l_uid: int, l_ts: int, l_vec: np.ndarray,  # 左流记录
    r_uid: int, r_ts: int, r_vec: np.ndarray   # 右流记录
) -> tuple[int, int, np.ndarray] | None:
    """Join 回调函数
    
    Args:
        l_uid, l_ts, l_vec: 左流（查询流）的记录
        r_uid, r_ts, r_vec: 右流（文档流）的记录
    
    Returns:
        (combined_uid, combined_ts, combined_vec) 或 None（过滤）
    """
    # 合并逻辑
    combined_uid = l_uid * 10000 + r_uid
    combined_ts = max(l_ts, r_ts)
    combined_vec = (l_vec + r_vec) / 2
    return (combined_uid, combined_ts, combined_vec.astype(np.float32))

# 使用 Join
result_stream = query_source.join(
    doc_source,        # 右流
    join_function,     # 回调函数
    dim=1024,          # 向量维度
    parallelism=1,     # 并行度
)
```

**支持的 Join 算法**：
| 算法 | 设置方法 | 特点 |
|------|---------|------|
| `bruteforce` | `setJoinMethod("bruteforce")` | 精确匹配 |
| `ivf` | `setJoinMethod("ivf")` | 近似匹配，适合大规模数据 |
| `hnsw` | `setJoinMethod("hnsw")` | 高性能近似匹配 |

#### 4.3.2 TopK (Top-K 检索)

```python
# 基本 TopK
result_stream = source.topk(k=10, dim=1024)

# 增量 TopK (适合流式场景)
result_stream = source.itopk(k=10, dim=1024)
```

#### 4.3.3 Aggregate (窗口聚合)

```python
def aggregate_function(
    records: list[tuple[int, int, np.ndarray]]  # (uid, ts, vec) 列表
) -> tuple[int, int, np.ndarray]:
    """聚合函数
    
    Args:
        records: 窗口内的所有记录
    
    Returns:
        聚合后的单条记录
    """
    if not records:
        return (0, 0, np.zeros(dim, dtype=np.float32))
    
    # 计算质心
    vecs = [r[2] for r in records]
    centroid = np.mean(vecs, axis=0)
    max_ts = max(r[1] for r in records)
    combined_uid = records[0][0]
    
    return (combined_uid, max_ts, centroid.astype(np.float32))

# 使用聚合（滑动窗口）
result_stream = source.aggregate(
    aggregate_function,
    window_size=3000,   # 窗口大小（毫秒）
    slide_size=1000,    # 滑动步长（毫秒）
    dim=1024,
)
```

#### 4.3.4 Filter (过滤)

```python
def filter_function(uid: int, ts: int, vec: np.ndarray) -> bool:
    """过滤函数
    
    Returns:
        True: 保留记录
        False: 丢弃记录
    """
    return np.linalg.norm(vec) > 0.5

result_stream = source.filter(filter_function, dim=1024)
```

#### 4.3.5 Sink (输出)

```python
def sink_function(uid: int, ts: int, vec: np.ndarray) -> None:
    """Sink 回调函数"""
    print(f"Received: uid={uid}, ts={ts}, vec_norm={np.linalg.norm(vec):.4f}")

result_stream.writeSink(sink_function, parallelism=1)
```

---

## 5. 接入规范

### 5.1 SAGE MapFunction 包装规范

将 SageFlow 包装为 SAGE `MapFunction`，需要遵循以下规范：

```python
from sage.common.core.functions.map_function import MapFunction
import _sage_flow as sf
import numpy as np

class SageFlowJoinMapFunction(MapFunction):
    """SageFlow Join 算子 - 包装为 SAGE MapFunction
    
    输入数据格式 (来自上游):
        {
            "id": int,              # 记录 ID
            "text": str,            # 原始文本（可选）
            "embedding": np.ndarray # 向量 (来自 EmbeddingMapFunction)
        }
    
    输出数据格式 (传给下游):
        {
            "id": int,
            "text": str,
            "embedding": np.ndarray,
            "matched_docs": list[int],      # 匹配的文档 ID
            "matched_texts": list[str],     # 匹配的文档文本
            "similarity_scores": list[float] # 相似度分数
        }
    """
    
    def __init__(
        self,
        dim: int,
        doc_vectors: np.ndarray,
        doc_ids: list[int],
        doc_texts: list[str],
        similarity_threshold: float = 0.3,
        join_method: str = "bruteforce_lazy",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.dim = dim
        self.doc_vectors = doc_vectors.astype(np.float32)
        self.doc_ids = doc_ids
        self.doc_texts = doc_texts
        self.similarity_threshold = similarity_threshold
        self.join_method = join_method
        
        # SageFlow 状态 (lazy init)
        self._env = None
        self._initialized = False
        self._results = []
    
    def _init_sageflow(self):
        """懒加载 SageFlow Pipeline"""
        if self._initialized:
            return
            
        self._env = sf.StreamEnvironment()
        self._query_source = sf.SimpleStreamSource("queries")
        self._doc_source = sf.SimpleStreamSource("docs")
        
        # 预加载文档向量到右流
        import time
        base_ts = int(time.time() * 1000)
        for i, (doc_id, vec) in enumerate(zip(self.doc_ids, self.doc_vectors)):
            self._doc_source.addRecord(doc_id, base_ts + i, vec)
        
        # 配置 Join 参数
        self._query_source.setJoinMethod(self.join_method)
        self._query_source.setJoinSimilarityThreshold(self.similarity_threshold)
        
        # 定义 Join 函数
        def join_func(l_uid, l_ts, l_vec, r_uid, r_ts, r_vec):
            combined_uid = l_uid * 10000 + r_uid
            combined_ts = max(l_ts, r_ts)
            combined = (l_vec + r_vec) / 2
            return (combined_uid, combined_ts, combined.astype(np.float32))
        
        # 定义 Sink 函数收集结果
        def sink_func(uid, ts, vec):
            query_id = uid // 10000
            doc_id = uid % 10000
            self._results.append((query_id, doc_id))
        
        # 构建 Pipeline
        _ = (
            self._query_source
            .join(self._doc_source, join_func, dim=self.dim, parallelism=1)
            .writeSink(sink_func, parallelism=1)
        )
        
        self._env.addStream(self._query_source)
        self._env.addStream(self._doc_source)
        self._initialized = True
    
    def execute(self, data: dict) -> dict:
        """执行 SageFlow Join
        
        Args:
            data: 上游传入的数据字典
        
        Returns:
            添加了匹配结果的数据字典
        """
        self._init_sageflow()
        
        embedding = data.get("embedding")
        if embedding is None:
            return {
                **data,
                "matched_docs": [],
                "matched_texts": [],
                "similarity_scores": []
            }
        
        query_id = data.get("id", 0)
        import time
        current_ts = int(time.time() * 1000)
        
        # 清空之前的结果
        self._results = []
        
        # 添加查询向量到左流
        self._query_source.addRecord(query_id, current_ts, embedding)
        
        # 执行 SageFlow
        self._env.execute()
        time.sleep(0.1)  # 等待异步处理
        
        # 收集匹配结果
        matched_docs = []
        matched_texts = []
        for q_id, doc_id in self._results:
            if q_id == query_id and doc_id in self.doc_ids:
                idx = self.doc_ids.index(doc_id)
                matched_docs.append(doc_id)
                matched_texts.append(self.doc_texts[idx])
        
        return {
            **data,
            "matched_docs": matched_docs,
            "matched_texts": matched_texts,
            "similarity_scores": [1.0] * len(matched_docs),
        }
```

### 5.2 上下游数据接口规范

#### 5.2.1 上游输入规范 (Embedding → SageFlow)

上游算子（通常是 `EmbeddingMapFunction`）需要提供：

```python
# 输入数据结构
{
    "id": int,                    # 必需：记录唯一标识
    "text": str,                  # 可选：原始文本
    "embedding": np.ndarray,      # 必需：float32 向量
    "timestamp": int,             # 可选：时间戳（毫秒）
    # ... 其他业务字段透传
}
```

#### 5.2.2 下游输出规范 (SageFlow → 下游)

SageFlow 算子输出需要包含：

```python
# 输出数据结构 (在输入基础上添加)
{
    # 透传的输入字段
    "id": int,
    "text": str,
    "embedding": np.ndarray,
    
    # SageFlow 添加的字段
    "matched_docs": list[int],       # Join: 匹配的文档 ID
    "matched_texts": list[str],      # Join: 匹配的文档文本
    "similarity_scores": list[float],# Join: 相似度分数
    
    # 或者 TopK 结果
    "topk_ids": list[int],           # TopK: Top-K 文档 ID
    "topk_scores": list[float],      # TopK: Top-K 分数
    
    # 或者聚合结果
    "aggregated_vector": np.ndarray, # Aggregate: 聚合后的向量
    "aggregated_count": int,         # Aggregate: 聚合的记录数
}
```

### 5.3 SAGE Pipeline 集成模式

```python
from sage.kernel.api import LocalEnvironment

def build_sage_pipeline():
    """构建 SAGE + SageFlow 集成 Pipeline"""
    
    # 1. 创建 SAGE 组件
    embedder = create_embedder()  # Embedding 服务
    dim = embedder.get_dim()
    
    # 2. 准备文档库
    documents = ["文档1...", "文档2...", "文档3..."]
    doc_vectors = np.array(embedder.embed(documents), dtype=np.float32)
    doc_ids = list(range(len(documents)))
    
    # 3. 创建 SageFlow 算子
    sageflow_join = SageFlowJoinMapFunction(
        dim=dim,
        doc_vectors=doc_vectors,
        doc_ids=doc_ids,
        doc_texts=documents,
        similarity_threshold=0.3,
        join_method="bruteforce_lazy",
    )
    
    # 4. 创建其他 SAGE 算子
    embedding_fn = EmbeddingMapFunction(embedder)
    context_fn = ContextAggregatorMapFunction()
    result_sink = ResultSinkFunction()
    
    # 5. 构建 SAGE Pipeline
    env = LocalEnvironment()
    queries = [{"id": 0, "text": "查询文本"}]
    
    (
        env.from_batch(queries)
        .map(lambda data: embedding_fn.execute(data))      # SAGE: Embedding
        .map(lambda data: sageflow_join.execute(data))     # SageFlow: Join
        .map(lambda data: context_fn.execute(data))        # SAGE: 上下文聚合
        .sink(lambda data: result_sink.execute(data))      # SAGE: 输出
    )
    
    # 6. 执行 Pipeline
    env.submit()
```

---

## 6. 应用场景示例

### 6.1 场景一：流式 RAG

**目标**：实时查询与文档库匹配，为 LLM 提供上下文。

```python
"""
Pipeline: Query → Embedding → SageFlow Join → Context Aggregation → LLM → Response

数据流:
1. 用户查询输入
2. Embedding 生成查询向量
3. SageFlow Join 匹配相关文档
4. 聚合上下文生成 LLM Prompt
5. LLM 生成回答
"""

class StreamingRAGPipeline:
    def __init__(self, documents: list[str], embedder, llm_client):
        self.embedder = embedder
        self.llm = llm_client
        
        # 预处理文档库
        self.doc_vectors = np.array(
            embedder.embed(documents), dtype=np.float32
        )
        self.doc_texts = documents
        self.doc_ids = list(range(len(documents)))
        
        # 创建 SageFlow Join 算子
        self.sageflow_join = SageFlowJoinMapFunction(
            dim=embedder.get_dim(),
            doc_vectors=self.doc_vectors,
            doc_ids=self.doc_ids,
            doc_texts=self.doc_texts,
            similarity_threshold=0.3,
            join_method="bruteforce_lazy",
        )
    
    def query(self, query_text: str) -> str:
        # 1. Embedding
        vec = self.embedder.embed([query_text])[0]
        data = {"id": 0, "text": query_text, "embedding": np.array(vec)}
        
        # 2. SageFlow Join
        result = self.sageflow_join.execute(data)
        
        # 3. 构建 Prompt
        context = "\n".join(result["matched_texts"][:3])
        prompt = f"问题: {query_text}\n上下文:\n{context}\n请回答问题。"
        
        # 4. LLM 生成
        return self.llm.generate(prompt)
```

### 6.2 场景二：相似查询聚合

**目标**：在时间窗口内聚合相似查询，减少 LLM 调用次数。

```python
"""
Pipeline: Queries → Embedding → SageFlow Aggregate → Batch LLM → Broadcast Response

优化效果: 相似查询合并处理，节省 60-80% LLM 调用
"""

class QueryAggregationPipeline:
    def __init__(self, embedder, llm_client, window_size_ms=3000):
        self.embedder = embedder
        self.llm = llm_client
        self.window_size = window_size_ms
        
        # SageFlow 聚合
        self._env = sf.StreamEnvironment()
        self._source = sf.SimpleStreamSource("queries")
        self._groups = []  # 聚合结果
        
        def aggregate_func(records):
            if not records:
                return (0, 0, np.zeros(1024, dtype=np.float32))
            centroid = np.mean([r[2] for r in records], axis=0)
            return (records[0][0], max(r[1] for r in records), centroid)
        
        def sink_func(uid, ts, vec):
            self._groups.append((uid, ts, vec))
        
        _ = (
            self._source
            .aggregate(aggregate_func, window_size=window_size_ms, dim=1024)
            .writeSink(sink_func, parallelism=1)
        )
        
        self._env.addStream(self._source)
    
    def process_batch(self, queries: list[str]) -> list[str]:
        """批量处理查询，相似查询共享响应"""
        # 1. 生成 Embedding
        embeddings = self.embedder.embed(queries)
        
        # 2. 添加到 SageFlow
        base_ts = int(time.time() * 1000)
        for i, (query, vec) in enumerate(zip(queries, embeddings)):
            self._source.addRecord(i, base_ts + i * 100, np.array(vec))
        
        # 3. 执行聚合
        self._groups = []
        self._env.execute()
        time.sleep(0.2)
        
        # 4. 对每个聚合组调用一次 LLM
        group_responses = {}
        for group_id, ts, centroid in self._groups:
            # 找到该组的代表查询
            prompt = f"请回答以下相关问题: {queries[group_id]}"
            group_responses[group_id] = self.llm.generate(prompt)
        
        # 5. 映射回原始查询
        # (简化: 每个查询使用最近组的响应)
        return [group_responses.get(0, "No response")] * len(queries)
```

### 6.3 场景三：会话语义状态管理

**目标**：维护多会话的语义状态，支持快速会话检索。

```python
"""
Pipeline: Messages → Embedding → SageFlow State Update → Session Store

应用: 多轮对话的上下文管理，相似会话检索
"""

class SessionStatePipeline:
    def __init__(self, embedder, dim=1024):
        self.embedder = embedder
        self.dim = dim
        self.session_centroids = {}  # session_id → centroid vector
        self.session_counts = {}     # session_id → message count
        
    def update_session(self, session_id: int, message: str):
        """增量更新会话状态"""
        # 1. 生成消息 Embedding
        vec = np.array(self.embedder.embed([message])[0], dtype=np.float32)
        
        # 2. 增量更新质心
        if session_id not in self.session_centroids:
            self.session_centroids[session_id] = vec
            self.session_counts[session_id] = 1
        else:
            n = self.session_counts[session_id]
            old_centroid = self.session_centroids[session_id]
            # 增量质心公式: new_centroid = old_centroid + (new_vec - old_centroid) / (n + 1)
            self.session_centroids[session_id] = old_centroid + (vec - old_centroid) / (n + 1)
            self.session_counts[session_id] = n + 1
        
        return self.session_centroids[session_id]
    
    def find_similar_sessions(self, query: str, top_k: int = 3) -> list[tuple[int, float]]:
        """检索相似会话"""
        query_vec = np.array(self.embedder.embed([query])[0], dtype=np.float32)
        
        scores = []
        for sid, centroid in self.session_centroids.items():
            sim = np.dot(query_vec, centroid) / (
                np.linalg.norm(query_vec) * np.linalg.norm(centroid) + 1e-8
            )
            scores.append((sid, float(sim)))
        
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:top_k]
```

---

## 7. 常见问题

### 7.1 ImportError: libsageflow.so not found

**原因**：未设置库路径

**解决**：
```bash
export LD_LIBRARY_PATH=/path/to/sageFlow/build/lib:$LD_LIBRARY_PATH
```

### 7.2 SAGE Pipeline 中 .map() 参数问题

**问题**：SAGE `.map()` 期望类或可调用对象，而不是实例

**解决**：使用 lambda 包装实例方法
```python
# 错误
.map(sageflow_operator)

# 正确
.map(lambda data: sageflow_operator.execute(data))
```

### 7.3 SageFlow 异步执行问题

**问题**：`env.execute()` 是异步的，结果可能未就绪

**解决**：添加适当的等待
```python
self._env.execute()
time.sleep(0.1)  # 等待异步处理完成
```

### 7.4 向量维度不匹配

**问题**：Embedding 维度与 SageFlow 配置不一致

**解决**：确保维度一致
```python
# 从 embedder 获取维度
dim = embedder.get_dim()

# 传给 SageFlow
sageflow_join = SageFlowJoinMapFunction(dim=dim, ...)
```

### 7.5 Join 无结果

**可能原因**：
1. 相似度阈值设置过高
2. 文档未正确加载到右流
3. 向量未正确归一化

**调试**：
```python
# 降低阈值
source.setJoinSimilarityThreshold(0.1)

# 检查向量归一化
vec = vec / np.linalg.norm(vec)
```

---

## 附录：完整示例代码

完整的集成示例请参考：
- `sageFlow/examples/python/sage_integrated_pipeline_demo.py`

运行方式：
```bash
cd sageFlow
LD_LIBRARY_PATH=./build/lib:$LD_LIBRARY_PATH python examples/python/sage_integrated_pipeline_demo.py
```


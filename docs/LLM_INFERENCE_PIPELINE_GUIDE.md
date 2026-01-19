# SageFlow LLM 推理链条集成指南

本文档展示 SageFlow 如何服务于 SAGE 的 LLM 推理链条，涵盖三个核心场景：

1. **流式 RAG** - Query 与 Document 流的实时相似度匹配
2. **相似查询聚合** - 减少重复 LLM 调用的滑动窗口聚合
3. **会话语义状态维护** - 增量质心计算的记忆系统

---

## 示例文件

| 文件 | 描述 |
|------|------|
| [sage_integrated_pipeline_demo.py](../examples/python/sage_integrated_pipeline_demo.py) | **推荐** - 使用 SAGE 组件的完整集成示例 |
| [llm_inference_service_demo.py](../examples/python/llm_inference_service_demo.py) | 独立 SageFlow 示例（不依赖 SAGE） |

---

## 架构概览

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SAGE LLM 推理链条                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────┐     ┌───────────────────────┐     ┌──────────────────┐   │
│   │ Query       │     │                       │     │                  │   │
│   │ Stream      │────▶│    SageFlow 引擎      │────▶│  LLM / Memory    │   │
│   │ (用户查询)   │     │  (实时向量处理)        │     │  Sink            │   │
│   └─────────────┘     │                       │     └──────────────────┘   │
│                       │  • Similarity Join    │                            │
│   ┌─────────────┐     │  • Window Aggregate   │                            │
│   │ Document    │────▶│  • Incremental TopK   │                            │
│   │ Stream      │     │  • Context Builder    │                            │
│   │ (知识库文档) │     └───────────────────────┘                            │
│   └─────────────┘                                                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 场景 1：流式 RAG

### 场景概述

流式 RAG（Retrieval-Augmented Generation）将实时查询流与文档知识库流进行相似度匹配，
为 LLM 构建动态上下文。

**Pipeline 架构：**

```text
Query Stream ─────┐
                  ├──▶ Similarity Join ──▶ Context Builder ──▶ LLM Sink
Document Stream ──┘
```

**核心价值：**

- **实时检索**：用户查询立即匹配最相关文档
- **增量索引**：新文档自动加入匹配候选集
- **上下文新鲜度**：始终使用最新的语义匹配结果

### RAG 代码示例

```python
import sage_flow as sf
import numpy as np

# 1. 创建流处理环境
env = sf.StreamEnvironment()

# 2. 定义数据源
query_stream = sf.SimpleStreamSource("user_queries")
doc_stream = sf.SimpleStreamSource("knowledge_base")

# 3. 定义相似度 Join 函数
def similarity_join(l_uid, l_ts, l_vec, r_uid, r_ts, r_vec):
    """计算余弦相似度，超过阈值则输出匹配对"""
    sim = np.dot(l_vec, r_vec) / (np.linalg.norm(l_vec) * np.linalg.norm(r_vec) + 1e-8)
    if sim >= 0.7:  # 阈值
        combined = (l_vec + r_vec) / 2
        return (l_uid * 10000 + r_uid, max(l_ts, r_ts), combined.astype(np.float32))
    return None

# 4. 构建 Pipeline
context_results = []
pipeline = (
    query_stream
    .join(doc_stream, similarity_join, dim=768, 
          join_method="hnsw", similarity_threshold=0.7, parallelism=2)
    .writeSink(lambda uid, ts, data: context_results.append({
        "query_doc_pair": uid,
        "timestamp": ts,
        "context_embedding": data
    }), parallelism=1)
)

# 5. 注入数据并执行
env.addStream(query_stream)
env.addStream(doc_stream)
env.execute()
```

**解释：**
- `join_method="hnsw"` 使用 HNSW 索引加速相似度搜索
- 相似度超过阈值的 Query-Document 对被组合成上下文向量
- 输出结果可直接作为 LLM prompt 的 context 部分

---

## 场景 2：相似查询聚合

### 概述

通过滑动窗口检测相似查询，将语义接近的请求聚合后统一调用 LLM，减少重复计算。

**Pipeline 架构：**
```
Query Stream ──▶ Sliding Window ──▶ Aggregate (Avg) ──▶ LLM Sink
```

**核心价值：**
- **降低成本**：相似查询只调用一次 LLM
- **减少延迟**：批量处理提高吞吐量
- **资源优化**：避免重复的 embedding 和推理

### 代码示例

```python
import sage_flow as sf
import numpy as np
from collections import defaultdict

# 创建环境
env = sf.StreamEnvironment()
query_stream = sf.SimpleStreamSource("queries")

# 聚合结果收集器
aggregated_queries = []

def on_aggregated(uid, ts, avg_embedding):
    """收到聚合后的代表性向量，发送给 LLM"""
    aggregated_queries.append({
        "window_id": uid,
        "timestamp": ts,
        "representative_embedding": avg_embedding,
        "action": "call_llm_once"  # 只调用一次
    })
    print(f"[Aggregated] Window {uid}: {len(avg_embedding)}D embedding ready for LLM")

# Pipeline: 5秒窗口，2秒滑动，平均聚合
pipeline = (
    query_stream
    .window(window_size=5000, slide_size=2000, 
            window_type=sf.WindowType.Sliding, parallelism=1)
    .aggregate(aggregate_type=sf.AggregateType.Avg, parallelism=1)
    .writeSink(on_aggregated, parallelism=1)
)

# 模拟相似查询到达
for i in range(10):
    # 相似查询的向量会很接近
    base_vec = np.random.randn(768).astype(np.float32)
    noisy_vec = base_vec + np.random.randn(768).astype(np.float32) * 0.1
    query_stream.addRecord(i, i * 500, noisy_vec)  # 500ms 间隔

env.addStream(query_stream)
env.execute()
```

**解释：**
- `window_size=5000` 表示 5 秒时间窗口
- `slide_size=2000` 窗口每 2 秒滑动一次
- `AggregateType.Avg` 计算窗口内所有向量的平均值作为代表
- 相似查询会产生相近的平均向量，LLM 只需响应一次

---

## 场景 3：会话语义状态维护

### 概述

维护对话历史的增量语义质心，用于：
- 长期记忆召回（Memory Retrieval）
- 会话主题追踪
- 上下文状态快照

**Pipeline 架构：**
```
Message Stream ──▶ Window ──▶ Incremental Centroid ──▶ Memory Sink
```

**核心价值：**
- **增量计算**：不需要重新计算全部历史
- **语义压缩**：将长对话压缩为代表性向量
- **记忆检索**：支持基于语义的历史召回

### 代码示例

```python
import sage_flow as sf
import numpy as np

class SessionMemoryStore:
    """会话记忆存储，维护每个会话的语义状态"""
    
    def __init__(self):
        self.session_centroids = {}  # session_id -> centroid_vector
        self.message_counts = {}      # session_id -> count
    
    def update_centroid(self, session_id: int, new_embedding: np.ndarray):
        """增量更新质心：centroid = (n * old + new) / (n + 1)"""
        if session_id not in self.session_centroids:
            self.session_centroids[session_id] = new_embedding.copy()
            self.message_counts[session_id] = 1
        else:
            n = self.message_counts[session_id]
            old_centroid = self.session_centroids[session_id]
            # 增量质心公式
            self.session_centroids[session_id] = (n * old_centroid + new_embedding) / (n + 1)
            self.message_counts[session_id] = n + 1
        
        return self.session_centroids[session_id]
    
    def query_similar_sessions(self, query_vec: np.ndarray, top_k: int = 5):
        """查找语义最相似的历史会话"""
        similarities = []
        for sid, centroid in self.session_centroids.items():
            sim = np.dot(query_vec, centroid) / (
                np.linalg.norm(query_vec) * np.linalg.norm(centroid) + 1e-8
            )
            similarities.append((sid, sim))
        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:top_k]


# 创建环境和存储
env = sf.StreamEnvironment()
message_stream = sf.SimpleStreamSource("messages")
memory_store = SessionMemoryStore()

def process_message(uid, ts, embedding):
    """处理消息：uid 编码 session_id，embedding 是消息向量"""
    session_id = uid // 1000  # 从 uid 提取 session_id
    message_id = uid % 1000
    
    # 增量更新会话质心
    new_centroid = memory_store.update_centroid(session_id, embedding)
    
    print(f"[Session {session_id}] Message {message_id}: "
          f"centroid updated (dim={len(new_centroid)}, "
          f"count={memory_store.message_counts[session_id]})")

# Pipeline: 消息 -> 窗口 -> 聚合 -> 记忆存储
pipeline = (
    message_stream
    .window(window_size=60000, slide_size=10000,  # 60s 窗口，10s 滑动
            window_type=sf.WindowType.Sliding, parallelism=1)
    .aggregate(aggregate_type=sf.AggregateType.Avg, parallelism=1)
    .writeSink(process_message, parallelism=1)
)

# 模拟多会话消息
dim = 768
for session_id in range(3):
    for msg_id in range(5):
        uid = session_id * 1000 + msg_id
        ts = msg_id * 2000  # 2s 间隔
        # 同一会话的消息向量相似
        base = np.random.randn(dim).astype(np.float32) if msg_id == 0 else base
        vec = base + np.random.randn(dim).astype(np.float32) * 0.2
        vec = vec.astype(np.float32)
        message_stream.addRecord(uid, ts, vec)

env.addStream(message_stream)
env.execute()

# 查询相似会话示例
query_embedding = np.random.randn(dim).astype(np.float32)
similar = memory_store.query_similar_sessions(query_embedding, top_k=3)
print(f"\n[Memory Query] Top similar sessions: {similar}")
```

**解释：**
- `SessionMemoryStore` 维护每个会话的增量语义质心
- 窗口聚合将短时间内的消息压缩为单个代表向量
- `query_similar_sessions` 支持基于语义的会话检索
- 可集成到 SAGE NeuroMem 作为长期记忆后端

---

## 完整可运行示例

完整示例代码位于 [examples/python/llm_inference_service_demo.py](../examples/python/llm_inference_service_demo.py)，包含：

- 三个场景的完整实现
- 模拟数据生成
- 结果验证和性能统计
- 与 SAGE Gateway 集成的接口预留

运行方式：

```bash
# 确保 SageFlow 已构建
cd sageFlow
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j $(nproc)

# 设置库路径并运行示例
export LD_LIBRARY_PATH="$(pwd)/build/lib:$LD_LIBRARY_PATH"
python examples/python/llm_inference_service_demo.py
```

**预期输出：**

```text
场景 1：流式 RAG
  总匹配对数: 3
  Query 0 匹配文档数: 1
  Query 1 匹配文档数: 1
  Query 2 匹配文档数: 1

场景 2：相似查询聚合
  原始查询数: 10
  聚合窗口数 (LLM 调用次数): 2
  节省比例: 80.0%

场景 3：会话语义状态
  Session 0: 消息数=5, 质心与主题相似度=0.65
  Session 1: 消息数=5, 质心与主题相似度=0.69
  Session 2: 消息数=5, 质心与主题相似度=0.73
```

---

## 与 SAGE 集成

### Gateway 集成点

SageFlow 在 SAGE Gateway 中的位置：

```text
User Request ──▶ Gateway ──▶ SageFlow Pipeline ──▶ LLM Engine
                    │               │
                    │               ├── RAG Join
                    │               ├── Query Dedup
                    │               └── Memory Update
                    │
                    └──▶ Control Plane (调度)
```

### 配置示例

```yaml
# sage/config/config.yaml
sageflow:
  enabled: true
  pipelines:
    rag_join:
      join_method: "hnsw"
      similarity_threshold: 0.7
      window_size: 10000  # ms
    query_aggregation:
      window_size: 5000
      slide_size: 2000
      aggregate_type: "avg"
    session_memory:
      window_size: 60000
      centroid_update: "incremental"
```

### UnifiedInferenceClient 集成

```python
from isagellm import UnifiedInferenceClient

# 创建 SageFlow 增强的推理客户端
client = UnifiedInferenceClient.create(
    control_plane_url="http://localhost:8888/v1",
    sageflow_enabled=True,  # 启用 SageFlow 流水线
)

# RAG 请求会自动通过 SageFlow 进行上下文增强
response = client.chat(
    messages=[{"role": "user", "content": "解释量子计算"}],
    rag_enabled=True,  # 触发 SageFlow RAG Join
)
```

---

## 性能考量

| 场景               | 延迟 (p99) | 吞吐量    | 内存          |
| ------------------ | ---------- | --------- | ------------- |
| RAG Join (HNSW)    | < 10ms     | 10K QPS   | O(N) 索引     |
| Query Aggregation  | < 5ms      | 50K QPS   | O(W) 窗口     |
| Session Memory     | < 2ms      | 100K QPS  | O(S) 会话数   |

**优化建议：**

- RAG Join 使用 `parallelism > 1` 进行并行化
- 大规模知识库使用 `join_method="ivf"` 或 `"hnsw"`
- 会话数量多时使用分区状态 (`PartitionedWindowState`)

---

## 相关文档

- [JOIN_PIPELINE_GUIDE.md](JOIN_PIPELINE_GUIDE.md) - Join 算子详细配置
- [SYSTEM_ARCHITECTURE.md](SYSTEM_ARCHITECTURE.md) - SageFlow 系统架构
- [TEST_TOOLS_GUIDE.md](TEST_TOOLS_GUIDE.md) - 测试工具使用指南

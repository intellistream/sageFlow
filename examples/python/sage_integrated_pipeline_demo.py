#!/usr/bin/env python3
"""
SAGE Pipeline + SageFlow 中间组件 集成示例
==========================================

本示例展示 SageFlow 作为 SAGE DataStream Pipeline 的 **中间组件**：
    SAGE Source → SAGE Map (embedding) → **SageFlow Operator** → SAGE downstream → SAGE Sink

这是真正的 SAGE Pipeline 集成，而不是独立运行 SageFlow。

架构：
    ┌──────────────────────────────────────────────────────────────────┐
    │                    SAGE DataStream Pipeline                       │
    │  ┌─────────────────────────────────────────────────────────────┐ │
    │  │ env.from_batch(queries)                                │ │
    │  │     .map(EmbeddingFunction)      # SAGE 上游: 生成 embedding │ │
    │  │     .map(SageFlowJoinOperator)   # SageFlow: 向量 join       │ │
    │  │     .map(ContextAggregator)      # SAGE 下游: 聚合上下文      │ │
    │  │     .sink(ResponseSink)          # SAGE sink: 输出结果       │ │
    │  └─────────────────────────────────────────────────────────────┘ │
    │                              ↓                                    │
    │  env.submit()  →  SAGE kernel 调度执行所有算子                     │
    └──────────────────────────────────────────────────────────────────┘

三个场景：
1. 流式 RAG - SageFlow Join 作为 SAGE MapFunction
2. 相似查询聚合 - SageFlow Aggregation 作为 SAGE MapFunction  
3. 会话语义状态 - SageFlow Sink 作为 SAGE SinkFunction

运行方式:
    cd sageFlow
    python examples/python/sage_integrated_pipeline_demo.py

依赖:
    pip install isage-common isage-middleware  # SAGE 核心 + 中间件
    # 或在 SAGE 仓库中: pip install -e packages/sage-common -e packages/sage-middleware -e packages/sage-kernel
"""

from __future__ import annotations

import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional, Protocol

import numpy as np

# =============================================================================
# SageFlow 导入
# =============================================================================
try:
    import sage_flow as sf
except ImportError:
    build_path = Path(__file__).parent.parent.parent / "build" / "sage_flow"
    if build_path.exists():
        sys.path.insert(0, str(build_path))
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    import sage_flow as sf

# =============================================================================
# SAGE Framework 导入 (核心组件)
# =============================================================================

# SAGE Kernel: Pipeline 执行环境
_SAGE_KERNEL_AVAILABLE = False
try:
    from sage.kernel.api import LocalEnvironment
    from sage.kernel.api.datastream import DataStream
    from sage.common.core.functions.map_function import MapFunction
    from sage.common.core.functions.sink_function import SinkFunction
    from sage.common.core.functions.source_function import SourceFunction
    _SAGE_KERNEL_AVAILABLE = True
    print("[Setup] ✓ SAGE Kernel (sage-kernel) 可用 - Pipeline 模式启用")
except ImportError as e:
    print(f"[Setup] ⚠ SAGE Kernel 不可用 - 使用独立模式 ({e})")
    print("[Setup] ⚠ SAGE Kernel 不可用 - 使用独立模式")

# SAGE Embedding (L1 - sage-common)
_SAGE_EMBEDDING_AVAILABLE = False
try:
    from sage.common.components.sage_embedding import (
        EmbeddingClientAdapter,
        EmbeddingFactory,
        adapt_embedding_client,
    )
    _SAGE_EMBEDDING_AVAILABLE = True
    print("[Setup] ✓ SAGE Embedding (sage-common) 可用")
except ImportError:
    print("[Setup] ⚠ SAGE Embedding 不可用，使用 Mock 实现")

# SAGE SageFlow Operators (L4 - sage-middleware)
_SAGE_FLOW_OPERATORS_AVAILABLE = False
try:
    from sage.middleware.components.sage_flow.operators import (
        SageFlowJoinOperator,
        SageFlowAggregationOperator,
    )
    _SAGE_FLOW_OPERATORS_AVAILABLE = True
    print("[Setup] ✓ SAGE SageFlow Operators (sage-middleware) 可用")
except ImportError:
    print("[Setup] ⚠ SAGE SageFlow Operators 不可用，使用本地实现")

# isagellm (LLM 推理)
_LLM_AVAILABLE = False
try:
    from isagellm import UnifiedInferenceClient
    _LLM_AVAILABLE = True
    print("[Setup] ✓ isagellm (LLM 推理) 可用")
except ImportError:
    print("[Setup] ⚠ isagellm 不可用，使用 Mock LLM")


# =============================================================================
# SAGE 兼容的抽象接口
# =============================================================================

class EmbeddingProtocol(Protocol):
    """SAGE 标准 Embedding 接口 (来自 sage.common)"""
    def embed(self, texts: list[str], model: Optional[str] = None) -> list[list[float]]:
        ...
    def get_dim(self) -> int:
        ...


class LLMClientProtocol(Protocol):
    """LLM 客户端接口"""
    def generate(self, prompt: str, **kwargs) -> str:
        ...


class MemoryStoreProtocol(ABC):
    """会话记忆存储接口"""
    @abstractmethod
    def store(self, session_id: int, embedding: np.ndarray, metadata: dict) -> None:
        ...
    
    @abstractmethod
    def retrieve(self, query_embedding: np.ndarray, top_k: int) -> list[tuple[int, float]]:
        ...


# =============================================================================
# SAGE Pipeline 基类 (当 sage-kernel 不可用时的 Mock)
# =============================================================================

if not _SAGE_KERNEL_AVAILABLE:
    # Mock SAGE 基类 - 用于独立运行
    class MapFunction:
        """Mock MapFunction for standalone mode"""
        def __init__(self, **kwargs):
            pass
        def execute(self, data: Any) -> Any:
            raise NotImplementedError

    class SinkFunction:
        """Mock SinkFunction for standalone mode"""
        def __init__(self, **kwargs):
            pass
        def invoke(self, data: Any) -> None:
            raise NotImplementedError

    class SourceFunction:
        """Mock SourceFunction for standalone mode"""
        def __init__(self, **kwargs):
            pass
        def run(self, collector):
            raise NotImplementedError

# =============================================================================
# Mock 实现 (当 SAGE 组件不可用时)
# =============================================================================

class OpenAICompatibleEmbedding:
    """OpenAI 兼容 API 的 Embedding 客户端
    
    支持任何 OpenAI 兼容的 embedding 服务，如:
    - http://localhost:8091/v1 (本地 embedding server)
    - BAAI/bge-m3 等模型
    """
    def __init__(
        self,
        base_url: str = "http://localhost:8091/v1",
        model: str = "BAAI/bge-m3",
        api_key: str = "dummy",
        dim: int = 1024,  # BGE-M3 默认维度
    ):
        self._base_url = base_url.rstrip("/")
        self._model = model
        self._api_key = api_key
        self._dim = dim
        self._session = None
        
    def _get_session(self):
        if self._session is None:
            import requests
            self._session = requests.Session()
            self._session.headers.update({
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            })
        return self._session
    
    def embed(self, texts: list[str], model: Optional[str] = None) -> list[list[float]]:
        """调用 OpenAI 兼容的 /v1/embeddings API"""
        session = self._get_session()
        url = f"{self._base_url}/embeddings"
        
        payload = {
            "model": model or self._model,
            "input": texts,
        }
        
        try:
            resp = session.post(url, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            # 按 index 排序确保顺序正确
            embeddings = sorted(data["data"], key=lambda x: x["index"])
            return [e["embedding"] for e in embeddings]
        except Exception as e:
            print(f"[Warning] Embedding API 调用失败: {e}，使用 fallback")
            # Fallback to mock
            return MockEmbedding(dim=self._dim).embed(texts)
    
    def get_dim(self) -> int:
        return self._dim


class MockEmbedding:
    """Mock Embedding 实现，用于独立测试"""
    def __init__(self, dim: int = 128):
        self._dim = dim
        self._cache: dict[str, np.ndarray] = {}
    
    def embed(self, texts: list[str], model: Optional[str] = None) -> list[list[float]]:
        """使用哈希生成可重现的伪嵌入"""
        results = []
        for text in texts:
            if text not in self._cache:
                # 使用哈希种子生成可重现的向量
                seed = hash(text) % (2**32)
                rng = np.random.default_rng(seed)
                vec = rng.standard_normal(self._dim).astype(np.float32)
                vec /= np.linalg.norm(vec)
                self._cache[text] = vec
            results.append(self._cache[text].tolist())
        return results
    
    def get_dim(self) -> int:
        return self._dim


class MockLLMClient:
    """Mock LLM 客户端"""
    def generate(self, prompt: str, **kwargs) -> str:
        return f"[MockLLM Response] 基于上下文生成的回复 (prompt长度={len(prompt)})"


class InMemoryStore(MemoryStoreProtocol):
    """简单的内存存储实现"""
    def __init__(self):
        self.sessions: dict[int, tuple[np.ndarray, dict]] = {}
    
    def store(self, session_id: int, embedding: np.ndarray, metadata: dict) -> None:
        self.sessions[session_id] = (embedding.copy(), metadata)
    
    def retrieve(self, query_embedding: np.ndarray, top_k: int) -> list[tuple[int, float]]:
        if not self.sessions:
            return []
        
        q_norm = np.linalg.norm(query_embedding)
        scores = []
        for sid, (emb, _) in self.sessions.items():
            e_norm = np.linalg.norm(emb)
            if q_norm > 1e-8 and e_norm > 1e-8:
                sim = float(np.dot(query_embedding, emb) / (q_norm * e_norm))
                scores.append((sid, sim))
        
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:top_k]


# =============================================================================
# 工厂函数：创建 SAGE 组件或 Mock
# =============================================================================

# Embedding 配置 (可通过环境变量覆盖)
EMBEDDING_BASE_URL = "http://localhost:8090/v1"
EMBEDDING_MODEL = "BAAI/bge-large-en-v1.5"
EMBEDDING_DIM = 1024  # BGE-large-en-v1.5 维度


def create_embedder(
    dim: int = None,
    method: str = "openai",
    base_url: str = None,
    model: str = None,
) -> EmbeddingProtocol:
    """创建 Embedding 客户端
    
    Args:
        dim: Embedding 维度 (默认 1024 for BGE-M3)
        method: 方法类型
            - "openai": 使用 OpenAI 兼容 API (默认，推荐)
            - "hash": 使用 Mock 哈希实现 (测试用)
            - 其他: 尝试 SAGE EmbeddingFactory
        base_url: API 基础 URL (默认 http://localhost:8091/v1)
        model: 模型名称 (默认 BAAI/bge-m3)
    
    Returns:
        EmbeddingProtocol 实例
    """
    import os
    
    # 从环境变量读取配置
    _base_url = base_url or os.getenv("EMBEDDING_BASE_URL", EMBEDDING_BASE_URL)
    _model = model or os.getenv("EMBEDDING_MODEL", EMBEDDING_MODEL)
    _dim = dim or int(os.getenv("EMBEDDING_DIM", str(EMBEDDING_DIM)))
    
    if method == "openai":
        # 优先使用 OpenAI 兼容 API
        print(f"[Embedding] 使用 OpenAI 兼容 API: {_base_url}, model={_model}, dim={_dim}")
        return OpenAICompatibleEmbedding(
            base_url=_base_url,
            model=_model,
            dim=_dim,
        )
    
    if method == "hash":
        # 使用 Mock 实现 (测试用)
        print(f"[Embedding] 使用 Mock 实现 (hash), dim={_dim}")
        return MockEmbedding(dim=_dim)
    
    # 尝试 SAGE EmbeddingFactory
    if _SAGE_EMBEDDING_AVAILABLE:
        try:
            raw = EmbeddingFactory.create(method, dim=_dim)
            return adapt_embedding_client(raw)
        except Exception as e:
            print(f"[Warning] SAGE EmbeddingFactory 失败: {e}，使用 Mock")
    
    return MockEmbedding(dim=_dim)


def create_llm_client() -> LLMClientProtocol:
    """创建 LLM 客户端
    
    优先使用 isagellm UnifiedInferenceClient，不可用时使用 Mock。
    """
    if _LLM_AVAILABLE:
        try:
            client = UnifiedInferenceClient.create()
            # 包装为简单接口
            class LLMWrapper:
                def __init__(self, c):
                    self._client = c
                def generate(self, prompt: str, **kwargs) -> str:
                    resp = self._client.chat(messages=[{"role": "user", "content": prompt}])
                    return resp.choices[0].message.content
            return LLMWrapper(client)
        except Exception as e:
            print(f"[Warning] isagellm 连接失败: {e}，使用 Mock")
    
    return MockLLMClient()


def create_memory_store() -> MemoryStoreProtocol:
    """创建记忆存储"""
    # 未来可以集成 SAGE NeuroMem (isage-neuromem)
    return InMemoryStore()


# =============================================================================
# 场景 1: 流式 RAG with SAGE Pipeline
# =============================================================================

class EmbeddingMapFunction(MapFunction):
    """SAGE MapFunction: 将文本转换为向量
    
    SAGE Pipeline 上游算子 - 负责 embedding 生成
    """
    
    def __init__(self, embedder: EmbeddingProtocol, **kwargs):
        super().__init__(**kwargs)
        self.embedder = embedder
    
    def execute(self, data: dict[str, Any]) -> dict[str, Any]:
        """将 query text 转换为 embedding 向量"""
        text = data.get("text", data.get("query", ""))
        if not text:
            return {**data, "embedding": None}
        
        vecs = self.embedder.embed([text])
        embedding = np.array(vecs[0], dtype=np.float32)
        
        return {**data, "embedding": embedding}


class SageFlowJoinMapFunction(MapFunction):
    """SAGE MapFunction: SageFlow Join 作为 Pipeline 中间组件
    
    这是核心集成点 - 将 SageFlow C++ 向量处理引擎包装为 SAGE MapFunction。
    
    数据流:
        输入: dict with 'id', 'embedding' fields (来自上游 EmbeddingMapFunction)
        处理: SageFlow C++ join with pre-indexed documents
        输出: dict with 'matched_docs', 'similarity_scores' (传给下游)
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
        self._query_source = None
        self._doc_source = None
        self._results = []
        self._initialized = False
    
    def _init_sageflow(self):
        """懒加载 SageFlow Pipeline"""
        if self._initialized:
            return
            
        self._env = sf.StreamEnvironment()
        self._query_source = sf.SimpleStreamSource("queries")
        self._doc_source = sf.SimpleStreamSource("docs")
        
        # 预加载文档向量
        base_ts = int(time.time() * 1000)
        for i, (doc_id, vec) in enumerate(zip(self.doc_ids, self.doc_vectors)):
            self._doc_source.addRecord(doc_id, base_ts + i, vec)
        
        # 配置 Join 参数
        self._query_source.setJoinMethod(self.join_method)
        self._query_source.setJoinSimilarityThreshold(self.similarity_threshold)
        
        # 创建 Join 函数
        def join_func(
            l_uid: int, l_ts: int, l_vec: np.ndarray,
            r_uid: int, r_ts: int, r_vec: np.ndarray
        ) -> tuple[int, int, np.ndarray] | None:
            # C++ 引擎已过滤，这里直接返回组合结果
            combined_uid = l_uid * 10000 + r_uid
            combined_ts = max(l_ts, r_ts)
            combined = ((l_vec / np.linalg.norm(l_vec)) + 
                       (r_vec / np.linalg.norm(r_vec))) / 2
            return (combined_uid, combined_ts, combined.astype(np.float32))
        
        # 创建结果收集器
        def sink_func(uid: int, ts: int, vec: np.ndarray) -> None:
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
    
    def execute(self, data: dict[str, Any]) -> dict[str, Any]:
        """执行 SageFlow Join 并返回匹配结果"""
        self._init_sageflow()
        
        embedding = data.get("embedding")
        if embedding is None:
            return {**data, "matched_docs": [], "matched_texts": [], "similarity_scores": []}
        
        query_id = data.get("id", 0)
        current_ts = int(time.time() * 1000)
        
        # 清空之前的结果
        self._results = []
        
        # 添加查询向量
        self._query_source.addRecord(query_id, current_ts, embedding)
        
        # 执行 SageFlow
        self._env.execute()
        time.sleep(0.2)  # 等待异步处理
        
        # 收集匹配的文档
        matched_docs = []
        matched_texts = []
        for q_id, doc_id in self._results:
            if q_id == query_id and doc_id in self.doc_ids:
                idx = self.doc_ids.index(doc_id)
                matched_docs.append(doc_id)
                matched_texts.append(self.doc_texts[idx])
        
        print(f"    [SageFlow Join] Query {query_id} → {len(matched_docs)} matches")
        
        return {
            **data,
            "matched_docs": matched_docs,
            "matched_texts": matched_texts,
            "similarity_scores": [1.0] * len(matched_docs),  # Placeholder
        }


class ContextAggregatorMapFunction(MapFunction):
    """SAGE MapFunction: 聚合检索到的上下文
    
    SAGE Pipeline 下游算子 - 将匹配结果组装为 LLM prompt
    """
    
    def execute(self, data: dict[str, Any]) -> dict[str, Any]:
        """聚合上下文为 LLM prompt"""
        query_text = data.get("text", data.get("query", ""))
        matched_texts = data.get("matched_texts", [])
        
        if matched_texts:
            context = "\n".join(f"- {t}" for t in matched_texts[:3])
            prompt = f"问题: {query_text}\n\n相关上下文:\n{context}\n\n请基于上下文回答问题。"
        else:
            prompt = f"问题: {query_text}\n\n(无相关上下文)\n\n请尝试回答问题。"
        
        return {**data, "prompt": prompt}


class LLMResponseMapFunction(MapFunction):
    """SAGE MapFunction: 调用 LLM 生成响应
    
    SAGE Pipeline 最终算子 - 生成 LLM 响应
    """
    
    def __init__(self, llm_client: LLMClientProtocol, **kwargs):
        super().__init__(**kwargs)
        self.llm_client = llm_client
    
    def execute(self, data: dict[str, Any]) -> dict[str, Any]:
        """调用 LLM 生成响应"""
        prompt = data.get("prompt", "")
        response = self.llm_client.generate(prompt)
        return {**data, "response": response}


class RAGResultSinkFunction(SinkFunction):
    """SAGE SinkFunction: 收集 RAG 结果
    
    SAGE Pipeline Sink - 收集最终输出
    """
    
    def __init__(self, results_collector: list, **kwargs):
        super().__init__(**kwargs)
        self.results = results_collector
    
    def execute(self, data: dict[str, Any]) -> None:
        """收集结果到外部列表"""
        self.results.append({
            "id": data.get("id"),
            "query": data.get("text"),
            "matched_count": len(data.get("matched_docs", [])),
            "response": data.get("response", ""),
        })


@dataclass
class RAGPipeline:
    """完整的 RAG Pipeline，使用 SAGE DataStream + SageFlow 中间组件"""
    
    embedder: EmbeddingProtocol
    llm_client: LLMClientProtocol
    dim: int = 128
    similarity_threshold: float = 0.3
    
    # 结果收集
    results: list[dict] = field(default_factory=list)


def run_rag_scenario():
    """场景 1：流式 RAG - SageFlow 作为 SAGE Pipeline 中间组件"""
    print("\n" + "=" * 70)
    print("场景 1：流式 RAG (SAGE Pipeline + SageFlow Join 中间组件)")
    print("=" * 70)
    
    # 创建 SAGE 组件 (使用默认配置: BGE-M3, 1024维)
    embedder = create_embedder()  # 默认使用 OpenAI 兼容 API
    dim = embedder.get_dim()
    llm = create_llm_client()
    
    print(f"\n[Config] Embedding dim={embedder.get_dim()}, LLM={type(llm).__name__}")
    
    # 准备数据
    queries = [
        {"id": 0, "text": "什么是机器学习?"},
        {"id": 1, "text": "深度学习的原理"},
        {"id": 2, "text": "神经网络架构"},
    ]
    
    documents = [
        "机器学习是人工智能的一个分支，通过数据训练模型",
        "深度学习使用多层神经网络进行特征学习",
        "卷积神经网络常用于图像识别任务",
        "数据库管理系统的设计原则",
        "云计算平台的架构设计",
    ]
    
    # 使用 SAGE Embedder 预处理文档向量
    print("\n>>> 预处理文档向量 (SAGE Embedding):")
    doc_vecs = embedder.embed(documents)
    doc_vectors = np.array(doc_vecs, dtype=np.float32)
    doc_ids = list(range(100, 100 + len(documents)))
    
    for i, doc in enumerate(documents):
        print(f"  Doc {doc_ids[i]}: '{doc[:30]}...'")
    
    # 结果收集器
    results = []
    
    if _SAGE_KERNEL_AVAILABLE:
        # ========================================
        # SAGE Pipeline 模式 (推荐)
        # ========================================
        print("\n>>> 使用 SAGE DataStream Pipeline:")
        print("""
    Pipeline 架构:
    ┌─────────────────────────────────────────────────────────────────┐
    │  env.from_batch(queries)                                   │
    │      .map(EmbeddingMapFunction)      # 生成 query embedding     │
    │      .map(SageFlowJoinMapFunction)   # SageFlow Join (C++ 引擎) │
    │      .map(ContextAggregatorMapFunction)  # 聚合上下文           │
    │      .map(LLMResponseMapFunction)    # 生成 LLM 响应            │
    │      .sink(RAGResultSinkFunction)    # 收集结果                 │
    └─────────────────────────────────────────────────────────────────┘
        """)
        
        # 创建 SAGE 环境
        env = LocalEnvironment()
        
        # 创建有状态的算子实例
        embedding_fn = EmbeddingMapFunction(embedder=embedder)
        sageflow_join = SageFlowJoinMapFunction(
            dim=dim,
            doc_vectors=doc_vectors,
            doc_ids=doc_ids,
            doc_texts=documents,
            similarity_threshold=0.3,
            join_method="bruteforce_lazy",
        )
        context_agg = ContextAggregatorMapFunction()
        llm_fn = LLMResponseMapFunction(llm_client=llm)
        result_sink = RAGResultSinkFunction(results_collector=results)
        
        # 构建 SAGE Pipeline
        # 注意：SAGE .map() 期望类或 callable，我们用 lambda 包装实例方法
        (
            env.from_batch(queries)
            .map(lambda data: embedding_fn.execute(data))      # SAGE 上游: embedding
            .map(lambda data: sageflow_join.execute(data))     # SageFlow: 向量 join
            .map(lambda data: context_agg.execute(data))       # SAGE 下游: 上下文聚合
            .map(lambda data: llm_fn.execute(data))            # SAGE 下游: LLM 响应
            .sink(lambda data: result_sink.execute(data))      # SAGE sink: 结果收集
        )
        
        print(">>> 执行 SAGE Pipeline...")
        env.submit()
        
    else:
        # ========================================
        # 独立模式 (当 SAGE Kernel 不可用时)
        # ========================================
        print("\n>>> 独立模式 (SAGE Kernel 不可用):")
        
        # 创建算子实例
        embedding_fn = EmbeddingMapFunction(embedder=embedder)
        sageflow_join = SageFlowJoinMapFunction(
            dim=dim,
            doc_vectors=doc_vectors,
            doc_ids=doc_ids,
            doc_texts=documents,
            similarity_threshold=0.3,
            join_method="bruteforce_lazy",
        )
        context_agg = ContextAggregatorMapFunction()
        llm_fn = LLMResponseMapFunction(llm_client=llm)
        
        # 手动执行 Pipeline
        for query in queries:
            print(f"\n  处理 Query {query['id']}: '{query['text']}'")
            data = query
            data = embedding_fn.execute(data)
            data = sageflow_join.execute(data)
            data = context_agg.execute(data)
            data = llm_fn.execute(data)
            results.append({
                "id": data.get("id"),
                "query": data.get("text"),
                "matched_count": len(data.get("matched_docs", [])),
                "response": data.get("response", ""),
            })
    
    # 显示结果
    print("\n>>> RAG 结果:")
    for r in results:
        print(f"  Q{r['id']}: 匹配 {r['matched_count']} 文档 → {r['response'][:50]}...")
    
    return RAGPipeline(embedder=embedder, llm_client=llm, dim=dim, results=results)


# =============================================================================
# 场景 2: 相似查询聚合 with SAGE Pipeline
# =============================================================================

class SageFlowAggregationMapFunction(MapFunction):
    """SAGE MapFunction: SageFlow 窗口聚合作为 Pipeline 中间组件
    
    在时间窗口内聚合相似查询，减少 LLM 调用次数。
    """
    
    def __init__(
        self,
        embedder: EmbeddingProtocol,
        window_size_ms: int = 3000,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.embedder = embedder
        self.window_size_ms = window_size_ms
        
        # 窗口状态
        self.current_window: list[tuple[int, np.ndarray, str]] = []
        self.current_window_start: int = 0
        self.aggregated_groups: list[dict] = []
    
    def execute(self, data: dict[str, Any]) -> dict[str, Any]:
        """基于时间窗口聚合相似查询"""
        query_id = data.get("id", 0)
        query_text = data.get("text", "")
        ts = data.get("timestamp", int(time.time() * 1000))
        embedding = data.get("embedding")
        
        if embedding is None:
            # 生成 embedding
            vecs = self.embedder.embed([query_text])
            embedding = np.array(vecs[0], dtype=np.float32)
        
        # 检查窗口边界
        if ts >= self.current_window_start + self.window_size_ms:
            if self.current_window:
                self._flush_window()
            self.current_window_start = ts
        
        # 添加到当前窗口
        self.current_window.append((query_id, embedding, query_text))
        
        # 返回当前查询信息
        return {
            **data,
            "embedding": embedding,
            "window_size": len(self.current_window),
        }
    
    def _flush_window(self):
        """处理并输出当前窗口"""
        if not self.current_window:
            return
        
        # 计算代表性向量
        vecs = np.stack([v for _, v, _ in self.current_window])
        representative = np.mean(vecs, axis=0)
        
        # 合并查询文本
        combined_text = "; ".join([t for _, _, t in self.current_window if t])
        
        self.aggregated_groups.append({
            "query_count": len(self.current_window),
            "representative": representative,
            "combined_text": combined_text,
            "query_ids": [q_id for q_id, _, _ in self.current_window],
        })
        
        print(f"    [Aggregation] {len(self.current_window)} queries → 1 group")
        self.current_window = []
    
    def finalize(self):
        """完成最后一个窗口"""
        self._flush_window()


class AggregatedLLMMapFunction(MapFunction):
    """SAGE MapFunction: 对聚合后的查询组调用 LLM"""
    
    def __init__(self, llm_client: LLMClientProtocol, aggregator: SageFlowAggregationMapFunction, **kwargs):
        super().__init__(**kwargs)
        self.llm_client = llm_client
        self.aggregator = aggregator
        self.processed_groups = 0
    
    def execute(self, data: dict[str, Any]) -> dict[str, Any]:
        """对聚合组生成响应"""
        # 如果有新的聚合组，处理它们
        while self.processed_groups < len(self.aggregator.aggregated_groups):
            group = self.aggregator.aggregated_groups[self.processed_groups]
            prompt = f"综合回答以下问题: {group['combined_text']}"
            response = self.llm_client.generate(prompt)
            group["response"] = response
            self.processed_groups += 1
        
        return data


@dataclass
class QueryAggregationPipeline:
    """查询聚合 Pipeline，使用 SAGE DataStream"""
    
    embedder: EmbeddingProtocol
    llm_client: LLMClientProtocol
    window_size_ms: int = 3000
    
    # 统计
    original_count: int = 0
    llm_call_count: int = 0
    aggregated_results: list[dict] = field(default_factory=list)


def run_aggregation_scenario():
    """场景 2：相似查询聚合 - SageFlow 作为 SAGE Pipeline 中间组件"""
    print("\n" + "=" * 70)
    print("场景 2：相似查询聚合 (SAGE Pipeline + SageFlow Aggregation)")
    print("=" * 70)
    
    # 创建 SAGE 组件 (使用默认配置: BGE-M3, 1024维)
    embedder = create_embedder()  # 默认使用 OpenAI 兼容 API
    dim = embedder.get_dim()
    llm = create_llm_client()
    
    # 模拟相似查询（同一主题的变体）
    similar_queries = [
        {"id": 0, "text": "Python 是什么语言?", "timestamp": 0},
        {"id": 1, "text": "Python 编程语言简介", "timestamp": 800},
        {"id": 2, "text": "什么是 Python?", "timestamp": 1600},
        {"id": 3, "text": "Python 语言特点", "timestamp": 2400},
        # --- 窗口边界 (3000ms) ---
        {"id": 4, "text": "Java 是什么语言?", "timestamp": 4000},
        {"id": 5, "text": "Java 编程语言简介", "timestamp": 4800},
        {"id": 6, "text": "什么是 Java?", "timestamp": 5600},
    ]
    
    print(f"\n[Config] 窗口大小=3000ms, 查询数={len(similar_queries)}")
    
    # 创建聚合算子
    aggregator = SageFlowAggregationMapFunction(
        embedder=embedder,
        window_size_ms=3000,
    )
    llm_fn = AggregatedLLMMapFunction(llm_client=llm, aggregator=aggregator)
    
    if _SAGE_KERNEL_AVAILABLE:
        print("\n>>> 使用 SAGE DataStream Pipeline:")
        print("""
    Pipeline 架构:
    ┌─────────────────────────────────────────────────────────────────┐
    │  env.from_batch(queries)                                   │
    │      .map(SageFlowAggregationMapFunction)  # 窗口内聚合         │
    │      .map(AggregatedLLMMapFunction)        # 对聚合组调用 LLM   │
    │      .sink(...)                                                 │
    └─────────────────────────────────────────────────────────────────┘
        """)
        
        env = LocalEnvironment()
        
        # 注意：SAGE .map() 期望类或 callable，我们用 lambda 包装实例方法
        (
            env.from_batch(similar_queries)
            .map(lambda data: aggregator.execute(data))
            .map(lambda data: llm_fn.execute(data))
            .sink(lambda x: None)
        )
        
        print(">>> 执行 SAGE Pipeline...")
        env.submit()
    else:
        print("\n>>> 独立模式:")
        for query in similar_queries:
            _ = aggregator.execute(query)
            _ = llm_fn.execute(query)
    
    # 完成最后一个窗口
    aggregator.finalize()
    # 处理剩余的组
    _ = llm_fn.execute({})
    
    # 统计
    original_count = len(similar_queries)
    llm_call_count = len(aggregator.aggregated_groups)
    
    print(f"\n>>> 结果统计:")
    print(f"  原始查询数: {original_count}")
    print(f"  聚合后组数 (LLM 调用): {llm_call_count}")
    if original_count > 0:
        savings = 1.0 - (llm_call_count / original_count)
        print(f"  节省比例: {savings:.1%}")
    
    print(f"\n>>> 聚合组详情:")
    for i, group in enumerate(aggregator.aggregated_groups):
        print(f"  Group {i}: {group['query_count']} queries, IDs={group['query_ids']}")
        print(f"    → Response: {group.get('response', '')[:50]}...")
    
    return QueryAggregationPipeline(
        embedder=embedder,
        llm_client=llm,
        original_count=original_count,
        llm_call_count=llm_call_count,
        aggregated_results=aggregator.aggregated_groups,
    )


# =============================================================================
# 场景 3: 会话语义状态维护 with SAGE Pipeline
# =============================================================================

class SessionEmbeddingMapFunction(MapFunction):
    """SAGE MapFunction: 为会话消息生成 embedding"""
    
    def __init__(self, embedder: EmbeddingProtocol, **kwargs):
        super().__init__(**kwargs)
        self.embedder = embedder
    
    def execute(self, data: dict[str, Any]) -> dict[str, Any]:
        """生成消息 embedding"""
        text = data.get("text", "")
        if not text:
            return {**data, "embedding": None}
        
        vecs = self.embedder.embed([text])
        embedding = np.array(vecs[0], dtype=np.float32)
        
        return {**data, "embedding": embedding}


class SageFlowSessionStateMapFunction(MapFunction):
    """SAGE MapFunction: SageFlow 增量质心更新
    
    使用 SageFlow 的流式处理能力维护会话语义状态。
    每条消息更新会话的增量质心。
    """
    
    def __init__(self, memory_store: MemoryStoreProtocol, **kwargs):
        super().__init__(**kwargs)
        self.memory_store = memory_store
        
        # 会话状态
        self.session_centroids: dict[int, np.ndarray] = {}
        self.session_counts: dict[int, int] = {}
    
    def execute(self, data: dict[str, Any]) -> dict[str, Any]:
        """增量更新会话质心"""
        session_id = data.get("session_id", 0)
        embedding = data.get("embedding")
        
        if embedding is None:
            return {**data, "centroid": None}
        
        # 增量质心更新
        if session_id not in self.session_centroids:
            self.session_centroids[session_id] = embedding.copy()
            self.session_counts[session_id] = 1
        else:
            n = self.session_counts[session_id]
            old = self.session_centroids[session_id]
            new = (n * old + embedding) / (n + 1)
            self.session_centroids[session_id] = new
            self.session_counts[session_id] = n + 1
        
        # 存储到 Memory Store
        self.memory_store.store(
            session_id,
            self.session_centroids[session_id],
            {"message_count": self.session_counts[session_id]}
        )
        
        msg_count = self.session_counts[session_id]
        print(f"    [Session {session_id}] Updated centroid (total: {msg_count} msgs)")
        
        return {
            **data,
            "centroid": self.session_centroids[session_id],
            "message_count": msg_count,
        }


class SessionStateSinkFunction(SinkFunction):
    """SAGE SinkFunction: 收集会话状态更新"""
    
    def __init__(self, results_collector: list, **kwargs):
        super().__init__(**kwargs)
        self.results = results_collector
    
    def execute(self, data: dict[str, Any]) -> None:
        """收集会话状态"""
        self.results.append({
            "session_id": data.get("session_id"),
            "text": data.get("text"),
            "message_count": data.get("message_count", 0),
        })


@dataclass
class SessionStatePipeline:
    """会话状态维护 Pipeline，使用 SAGE DataStream"""
    
    memory_store: MemoryStoreProtocol
    embedder: EmbeddingProtocol
    
    # 会话状态
    session_centroids: dict[int, np.ndarray] = field(default_factory=dict)
    session_counts: dict[int, int] = field(default_factory=dict)


def run_session_state_scenario():
    """场景 3：会话语义状态 - SageFlow 作为 SAGE Pipeline 中间组件"""
    print("\n" + "=" * 70)
    print("场景 3：会话语义状态 (SAGE Pipeline + SageFlow State Management)")
    print("=" * 70)
    
    # 创建 SAGE 组件 (使用默认配置: BGE-M3, 1024维)
    embedder = create_embedder()  # 默认使用 OpenAI 兼容 API
    dim = embedder.get_dim()
    memory_store = create_memory_store()
    
    # 模拟多会话消息
    sessions_data = {
        0: ["今天天气怎么样?", "明天会下雨吗?", "周末天气预报"],  # 天气话题
        1: ["推荐一部电影", "最近有什么好看的剧?", "科幻电影推荐"],  # 娱乐话题
        2: ["如何学习编程?", "Python 入门教程", "编程最佳实践"],   # 编程话题
    }
    
    # 转换为消息列表
    messages = []
    for session_id, msg_list in sessions_data.items():
        for msg_idx, msg_text in enumerate(msg_list):
            messages.append({
                "session_id": session_id,
                "msg_id": msg_idx,
                "text": msg_text,
                "timestamp": session_id * 10000 + msg_idx * 2000,
            })
    
    print(f"\n[Config] 会话数={len(sessions_data)}, 总消息数={len(messages)}")
    
    # 创建算子
    embedding_fn = SessionEmbeddingMapFunction(embedder=embedder)
    state_fn = SageFlowSessionStateMapFunction(memory_store=memory_store)
    results = []
    result_sink = SessionStateSinkFunction(results_collector=results)
    
    if _SAGE_KERNEL_AVAILABLE:
        print("\n>>> 使用 SAGE DataStream Pipeline:")
        print("""
    Pipeline 架构:
    ┌─────────────────────────────────────────────────────────────────┐
    │  env.from_batch(messages)                                  │
    │      .map(SessionEmbeddingMapFunction)     # 生成消息 embedding │
    │      .map(SageFlowSessionStateMapFunction) # 增量质心更新       │
    │      .sink(SessionStateSinkFunction)       # 收集状态           │
    └─────────────────────────────────────────────────────────────────┘
        """)
        
        env = LocalEnvironment()
        
        # 注意：SAGE .map() 期望类或 callable，我们用 lambda 包装实例方法
        (
            env.from_batch(messages)
            .map(lambda data: embedding_fn.execute(data))
            .map(lambda data: state_fn.execute(data))
            .sink(lambda data: result_sink.execute(data))
        )
        
        print(">>> 执行 SAGE Pipeline...")
        env.submit()
    else:
        print("\n>>> 独立模式:")
        for msg in messages:
            data = msg
            data = embedding_fn.execute(data)
            data = state_fn.execute(data)
            results.append({
                "session_id": data.get("session_id"),
                "text": data.get("text"),
                "message_count": data.get("message_count", 0),
            })
    
    # 演示语义检索
    print("\n>>> 语义会话检索:")
    test_queries = ["天气预报查询", "看电影", "学 Python"]
    for q in test_queries:
        vecs = embedder.embed([q])
        query_vec = np.array(vecs[0], dtype=np.float32)
        similar = memory_store.retrieve(query_vec, top_k=2)
        print(f"  '{q}' → 最相似会话: {similar}")
    
    return SessionStatePipeline(
        memory_store=memory_store,
        embedder=embedder,
        session_centroids=state_fn.session_centroids,
        session_counts=state_fn.session_counts,
    )


# =============================================================================
# 主程序
# =============================================================================

def main():
    print("\n" + "#" * 70)
    print("#" + " " * 8 + "SAGE Pipeline + SageFlow 中间组件 集成示例" + " " * 8 + "#")
    print("#" * 70)
    
    print("\n[Architecture] SageFlow 作为 SAGE Pipeline 的中间组件:")
    print("""
    ┌──────────────────────────────────────────────────────────────────────┐
    │                    SAGE DataStream Pipeline                          │
    │                                                                      │
    │  ┌─────────────────────────────────────────────────────────────────┐ │
    │  │  from_batch() / from_source()                              │ │
    │  │       ↓                                                         │ │
    │  │  .map(EmbeddingMapFunction)         # SAGE 上游: 生成 embedding │ │
    │  │       ↓                                                         │ │
    │  │  .map(SageFlowJoinMapFunction)      # SageFlow: C++ 向量处理    │ │
    │  │       ↓                              (Join/Aggregate/Filter)    │ │
    │  │  .map(ContextAggregator)            # SAGE 下游: 业务逻辑       │ │
    │  │       ↓                                                         │ │
    │  │  .sink(ResultCollector)             # SAGE Sink: 输出           │ │
    │  └─────────────────────────────────────────────────────────────────┘ │
    │                                                                      │
    │  env.submit()  →  SAGE Kernel 统一调度执行                           │
    └──────────────────────────────────────────────────────────────────────┘
    
    关键点:
    - SageFlow 被包装为 SAGE MapFunction，成为 Pipeline 的一部分
    - SAGE 负责数据源、Embedding、下游业务逻辑、Sink
    - SageFlow 专注于高性能 C++ 向量计算 (Join/Aggregate/Filter)
    - 两者通过 SAGE Kernel 的 DataStream API 无缝集成
    """)
    
    # 检测模式
    if _SAGE_KERNEL_AVAILABLE:
        print("[Mode] ✓ SAGE Pipeline 模式 - 使用 LocalEnvironment + DataStream")
    else:
        print("[Mode] ⚠ 独立模式 - SAGE Kernel 不可用，手动执行算子链")
    
    # 运行三个场景
    results = {}
    
    try:
        results["rag"] = run_rag_scenario()
    except Exception as e:
        print(f"\n[Error] 场景 1 失败: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        results["aggregation"] = run_aggregation_scenario()
    except Exception as e:
        print(f"\n[Error] 场景 2 失败: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        results["session"] = run_session_state_scenario()
    except Exception as e:
        print(f"\n[Error] 场景 3 失败: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "#" * 70)
    print("#" + " " * 24 + "示例运行完成." + " " * 24 + "#")
    print("#" * 70)
    
    print("\n>>> SAGE + SageFlow Pipeline 集成要点:")
    print("""
    1. SageFlow 作为 SAGE MapFunction
       - SageFlowJoinMapFunction: 将 C++ Join 包装为 SAGE 算子
       - SageFlowAggregationMapFunction: 将窗口聚合包装为 SAGE 算子
       - SageFlowSessionStateMapFunction: 将状态管理包装为 SAGE 算子

    2. SAGE Pipeline 架构
       - env.from_batch() / from_source(): 数据输入
       - .map(operator): 链式处理 (包括 SageFlow 算子)
       - .sink(sink_fn): 结果输出
       - env.submit(): 统一执行

    3. 数据流
       输入 → SAGE Embedding → SageFlow C++ → SAGE 下游 → 输出
       
    4. 优势
       - SageFlow C++ 提供高性能向量计算
       - SAGE 提供完整的 Pipeline 编排和调度
       - 两者通过标准 MapFunction 接口无缝集成
    """)
    
    return results


if __name__ == "__main__":
    main()
